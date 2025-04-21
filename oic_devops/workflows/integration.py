"""
Integration workflows module for the OIC DevOps package.

This module provides workflow operations for managing integrations.
"""

import os
import logging
import time
import datetime
from typing import Dict, Any, List, Optional, Union, Tuple

from oic_devops.client import OICClient
from oic_devops.exceptions import OICError
from oic_devops.workflows.base import BaseWorkflow, WorkflowResult


class IntegrationWorkflows(BaseWorkflow):
    """
    Workflow operations for managing integrations.
    
    This class provides higher-level operations for working with integrations,
    such as bulk activation/deactivation, dependency management, and
    schedule management.
    """
    
    def execute(self, *args, **kwargs) -> WorkflowResult:
        """
        Execute the specified integration workflow.
        
        This is a dispatcher method that calls the appropriate workflow
        based on the operation argument.
        
        Args:
            operation: The workflow operation to execute.
            **kwargs: Additional arguments specific to the workflow.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        operation = kwargs.pop("operation", None)
        
        if operation == "bulk_activate":
            return self.bulk_activate_integrations(**kwargs)
        elif operation == "bulk_deactivate":
            return self.bulk_deactivate_integrations(**kwargs)
        elif operation == "manage_schedules":
            return self.manage_integration_schedules(**kwargs)
        elif operation == "find_dependencies":
            return self.find_integration_dependencies(**kwargs)
        elif operation == "restart":
            return self.restart_integration(**kwargs)
        elif operation == "trace_instances":
            return self.trace_integration_instances(**kwargs)
        else:
            result = WorkflowResult(success=False, message=f"Unknown integration workflow operation: {operation}")
            result.add_error(f"Unknown operation: {operation}")
            return result
    
    def bulk_activate_integrations(
        self,
        integration_ids: Optional[List[str]] = None,
        filter_query: Optional[str] = None,
        continue_on_error: bool = True,
        verify_activation: bool = True,
        sequential: bool = True,
        wait_time: int = 10
    ) -> WorkflowResult:
        """
        Activate multiple integrations.
        
        This workflow:
        1. Gets a list of integrations to activate
        2. Activates each integration
        3. Optionally verifies activation status
        
        Args:
            integration_ids: List of integration IDs to activate. If provided, filter_query is ignored.
            filter_query: Query to filter integrations to activate.
            continue_on_error: Whether to continue if some activations fail.
            verify_activation: Whether to verify integrations are activated.
            sequential: Whether to activate integrations one at a time.
            wait_time: Time to wait between operations in seconds.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = "Activating integrations"
        
        # Get list of integrations to activate
        integrations_to_activate = []
        
        if integration_ids:
            # Use provided integration IDs
            self.logger.info(f"Using provided list of {len(integration_ids)} integration IDs")
            
            for integration_id in integration_ids:
                try:
                    integration = self.client.integrations.get(integration_id)
                    integrations_to_activate.append({
                        "id": integration_id,
                        "name": integration.get("name", "Unknown"),
                        "status": integration.get("status", "UNKNOWN")
                    })
                except OICError as e:
                    self.logger.error(f"Failed to get integration {integration_id}: {str(e)}")
                    if not continue_on_error:
                        result.add_error(f"Failed to get integration {integration_id}", e, integration_id)
                        result.success = False
                        return result
                    # Otherwise continue with the next integration
            
        else:
            # Get integrations based on filter
            try:
                params = {}
                if filter_query:
                    params["q"] = filter_query
                
                # Add status filter to only get configurable integrations
                params["status"] = "CONFIGURED"
                    
                self.logger.info("Getting list of integrations")
                integrations = self.client.integrations.list(params=params)
                
                for integration in integrations:
                    integration_id = integration.get("id")
                    if integration_id:
                        integrations_to_activate.append({
                            "id": integration_id,
                            "name": integration.get("name", "Unknown"),
                            "status": integration.get("status", "UNKNOWN")
                        })
                
            except OICError as e:
                self.logger.error(f"Failed to get integrations list: {str(e)}")
                result.add_error("Failed to get integrations list", e)
                result.success = False
                return result
        
        # If no integrations to activate, we're done
        if not integrations_to_activate:
            result.message = "No integrations found to activate"
            return result
        
        # Activate each integration
        successful_activations = []
        failed_activations = []
        
        self.logger.info(f"Activating {len(integrations_to_activate)} integrations")
        result.details["total_count"] = len(integrations_to_activate)
        
        for integration in integrations_to_activate:
            integration_id = integration["id"]
            integration_name = integration["name"]
            current_status = integration["status"]
            
            # Skip already activated integrations
            if current_status == "ACTIVATED":
                self.logger.info(f"Integration {integration_name} is already activated, skipping")
                successful_activations.append({
                    "id": integration_id,
                    "name": integration_name,
                    "status": current_status
                })
                result.add_resource("integration", integration_id, {
                    "name": integration_name,
                    "status": current_status,
                    "activation_result": "already_activated"
                })
                continue
            
            # Activate the integration
            try:
                self.logger.info(f"Activating integration: {integration_name}")
                self.client.integrations.activate(integration_id)
                
                # Verify activation if requested
                activation_verified = True
                if verify_activation:
                    # Wait a moment before checking
                    if sequential:
                        self.logger.info(f"Waiting {wait_time}s for activation to complete")
                        time.sleep(wait_time)
                    
                    # Check activation status
                    try:
                        integration_status = self.client.integrations.get(integration_id)
                        new_status = integration_status.get("status")
                        
                        if new_status != "ACTIVATED":
                            self.logger.warning(f"Integration {integration_name} not fully activated, status: {new_status}")
                            activation_verified = False
                    except OICError as e:
                        self.logger.error(f"Failed to verify activation of integration {integration_name}: {str(e)}")
                        activation_verified = False
                
                # Record successful activation
                if activation_verified:
                    successful_activations.append({
                        "id": integration_id,
                        "name": integration_name,
                        "status": "ACTIVATED"
                    })
                    result.add_resource("integration", integration_id, {
                        "name": integration_name,
                        "status": "ACTIVATED",
                        "activation_result": "success"
                    })
                else:
                    failed_activations.append({
                        "id": integration_id,
                        "name": integration_name,
                        "error": "Activation verification failed"
                    })
                    result.add_error(f"Activation verification failed for {integration_name}", resource_id=integration_id)
                    result.add_resource("integration", integration_id, {
                        "name": integration_name,
                        "activation_result": "verification_failed"
                    })
                    
                    # Stop processing if we shouldn't continue on error
                    if not continue_on_error:
                        result.success = False
                        result.message = f"Activation verification failed for {integration_name}"
                        break
                
                # Wait between activations if sequential
                if sequential and integration != integrations_to_activate[-1]:  # Not the last one
                    self.logger.info(f"Waiting {wait_time}s before next activation")
                    time.sleep(wait_time)
                    
            except OICError as e:
                self.logger.error(f"Failed to activate integration {integration_name}: {str(e)}")
                failed_activations.append({
                    "id": integration_id,
                    "name": integration_name,
                    "error": str(e)
                })
                result.add_error(f"Failed to activate integration {integration_name}", e, integration_id)
                result.add_resource("integration", integration_id, {
                    "name": integration_name,
                    "activation_result": "error",
                    "error": str(e)
                })
                
                # Stop processing if we shouldn't continue on error
                if not continue_on_error:
                    result.success = False
                    result.message = f"Failed to activate integration {integration_name}"
                    break
        
        # Update overall workflow status
        if failed_activations:
            result.success = False
            result.message = f"Activated {len(successful_activations)} integrations, {len(failed_activations)} failed"
        else:
            result.message = f"Successfully activated {len(successful_activations)} integrations"
        
        # Add details to the result
        result.details["successful_count"] = len(successful_activations)
        result.details["failed_count"] = len(failed_activations)
        result.details["successful_activations"] = successful_activations
        result.details["failed_activations"] = failed_activations
        
        return result
    
    def bulk_deactivate_integrations(
        self,
        integration_ids: Optional[List[str]] = None,
        filter_query: Optional[str] = None,
        continue_on_error: bool = True,
        verify_deactivation: bool = True,
        sequential: bool = True,
        wait_time: int = 10
    ) -> WorkflowResult:
        """
        Deactivate multiple integrations.
        
        This workflow:
        1. Gets a list of integrations to deactivate
        2. Deactivates each integration
        3. Optionally verifies deactivation status
        
        Args:
            integration_ids: List of integration IDs to deactivate. If provided, filter_query is ignored.
            filter_query: Query to filter integrations to deactivate.
            continue_on_error: Whether to continue if some deactivations fail.
            verify_deactivation: Whether to verify integrations are deactivated.
            sequential: Whether to deactivate integrations one at a time.
            wait_time: Time to wait between operations in seconds.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = "Deactivating integrations"
        
        # Get list of integrations to deactivate
        integrations_to_deactivate = []
        
        if integration_ids:
            # Use provided integration IDs
            self.logger.info(f"Using provided list of {len(integration_ids)} integration IDs")
            
            for integration_id in integration_ids:
                try:
                    integration = self.client.integrations.get(integration_id)
                    integrations_to_deactivate.append({
                        "id": integration_id,
                        "name": integration.get("name", "Unknown"),
                        "status": integration.get("status", "UNKNOWN")
                    })
                except OICError as e:
                    self.logger.error(f"Failed to get integration {integration_id}: {str(e)}")
                    if not continue_on_error:
                        result.add_error(f"Failed to get integration {integration_id}", e, integration_id)
                        result.success = False
                        return result
                    # Otherwise continue with the next integration
            
        else:
            # Get integrations based on filter
            try:
                params = {}
                if filter_query:
                    params["q"] = filter_query
                
                # Add status filter to only get activated integrations
                params["status"] = "ACTIVATED"
                    
                self.logger.info("Getting list of integrations")
                integrations = self.client.integrations.list(params=params)
                
                for integration in integrations:
                    integration_id = integration.get("id")
                    if integration_id:
                        integrations_to_deactivate.append({
                            "id": integration_id,
                            "name": integration.get("name", "Unknown"),
                            "status": integration.get("status", "UNKNOWN")
                        })
                
            except OICError as e:
                self.logger.error(f"Failed to get integrations list: {str(e)}")
                result.add_error("Failed to get integrations list", e)
                result.success = False
                return result
        
        # If no integrations to deactivate, we're done
        if not integrations_to_deactivate:
            result.message = "No integrations found to deactivate"
            return result
        
        # Deactivate each integration
        successful_deactivations = []
        failed_deactivations = []
        
        self.logger.info(f"Deactivating {len(integrations_to_deactivate)} integrations")
        result.details["total_count"] = len(integrations_to_deactivate)
        
        for integration in integrations_to_deactivate:
            integration_id = integration["id"]
            integration_name = integration["name"]
            current_status = integration["status"]
            
            # Skip already deactivated integrations
            if current_status != "ACTIVATED":
                self.logger.info(f"Integration {integration_name} is not activated (status: {current_status}), skipping")
                successful_deactivations.append({
                    "id": integration_id,
                    "name": integration_name,
                    "status": current_status
                })
                result.add_resource("integration", integration_id, {
                    "name": integration_name,
                    "status": current_status,
                    "deactivation_result": "already_deactivated"
                })
                continue
            
            # Deactivate the integration
            try:
                self.logger.info(f"Deactivating integration: {integration_name}")
                self.client.integrations.deactivate(integration_id)
                
                # Verify deactivation if requested
                deactivation_verified = True
                if verify_deactivation:
                    # Wait a moment before checking
                    if sequential:
                        self.logger.info(f"Waiting {wait_time}s for deactivation to complete")
                        time.sleep(wait_time)
                    
                    # Check deactivation status
                    try:
                        integration_status = self.client.integrations.get(integration_id)
                        new_status = integration_status.get("status")
                        
                        if new_status == "ACTIVATED":
                            self.logger.warning(f"Integration {integration_name} still activated, deactivation failed")
                            deactivation_verified = False
                    except OICError as e:
                        self.logger.error(f"Failed to verify deactivation of integration {integration_name}: {str(e)}")
                        deactivation_verified = False
                
                # Record successful deactivation
                if deactivation_verified:
                    successful_deactivations.append({
                        "id": integration_id,
                        "name": integration_name,
                        "status": "CONFIGURED"  # Assume configured status after deactivation
                    })
                    result.add_resource("integration", integration_id, {
                        "name": integration_name,
                        "status": "CONFIGURED",
                        "deactivation_result": "success"
                    })
                else:
                    failed_deactivations.append({
                        "id": integration_id,
                        "name": integration_name,
                        "error": "Deactivation verification failed"
                    })
                    result.add_error(f"Deactivation verification failed for {integration_name}", resource_id=integration_id)
                    result.add_resource("integration", integration_id, {
                        "name": integration_name,
                        "deactivation_result": "verification_failed"
                    })
                    
                    # Stop processing if we shouldn't continue on error
                    if not continue_on_error:
                        result.success = False
                        result.message = f"Deactivation verification failed for {integration_name}"
                        break
                
                # Wait between deactivations if sequential
                if sequential and integration != integrations_to_deactivate[-1]:  # Not the last one
                    self.logger.info(f"Waiting {wait_time}s before next deactivation")
                    time.sleep(wait_time)
                    
            except OICError as e:
                self.logger.error(f"Failed to deactivate integration {integration_name}: {str(e)}")
                failed_deactivations.append({
                    "id": integration_id,
                    "name": integration_name,
                    "error": str(e)
                })
                result.add_error(f"Failed to deactivate integration {integration_name}", e, integration_id)
                result.add_resource("integration", integration_id, {
                    "name": integration_name,
                    "deactivation_result": "error",
                    "error": str(e)
                })
                
                # Stop processing if we shouldn't continue on error
                if not continue_on_error:
                    result.success = False
                    result.message = f"Failed to deactivate integration {integration_name}"
                    break
        
        # Update overall workflow status
        if failed_deactivations:
            result.success = False
            result.message = f"Deactivated {len(successful_deactivations)} integrations, {len(failed_deactivations)} failed"
        else:
            result.message = f"Successfully deactivated {len(successful_deactivations)} integrations"
        
        # Add details to the result
        result.details["successful_count"] = len(successful_deactivations)
        result.details["failed_count"] = len(failed_deactivations)
        result.details["successful_deactivations"] = successful_deactivations
        result.details["failed_deactivations"] = failed_deactivations
        
        return result
    
    def manage_integration_schedules(
        self,
        action: str,  # "enable", "disable", "update"
        integration_ids: Optional[List[str]] = None,
        filter_query: Optional[str] = None,
        schedule_data: Optional[Dict[str, Any]] = None,
        continue_on_error: bool = True
    ) -> WorkflowResult:
        """
        Manage schedules for integrations.
        
        This workflow:
        1. Gets a list of scheduled integrations
        2. Performs the requested action on each schedule
        
        Args:
            action: The action to perform: "enable", "disable", or "update".
            integration_ids: List of integration IDs to manage. If provided, filter_query is ignored.
            filter_query: Query to filter integrations to manage.
            schedule_data: Data for schedule updates. Required for "update" action.
            continue_on_error: Whether to continue if some operations fail.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Managing integration schedules: {action}"
        
        # Validate action
        valid_actions = ["enable", "disable", "update"]
        if action not in valid_actions:
            result.success = False
            result.message = f"Invalid action: {action}. Must be one of: {', '.join(valid_actions)}"
            result.add_error(f"Invalid action: {action}")
            return result
        
        # For update action, schedule_data is required
        if action == "update" and not schedule_data:
            result.success = False
            result.message = "Schedule data is required for update action"
            result.add_error("Missing schedule_data for update action")
            return result
        
        # Get list of integrations to manage
        integrations_to_manage = []
        
        if integration_ids:
            # Use provided integration IDs
            self.logger.info(f"Using provided list of {len(integration_ids)} integration IDs")
            
            for integration_id in integration_ids:
                try:
                    integration = self.client.integrations.get(integration_id)
                    
                    # Check if this is a scheduled integration
                    integration_type = integration.get("integrationType")
                    if integration_type != "SCHEDULED":
                        self.logger.warning(f"Integration {integration.get('name', integration_id)} is not scheduled (type: {integration_type}), skipping")
                        continue
                    
                    integrations_to_manage.append({
                        "id": integration_id,
                        "name": integration.get("name", "Unknown"),
                        "status": integration.get("status", "UNKNOWN"),
                        "schedule": integration.get("schedule", {})
                    })
                except OICError as e:
                    self.logger.error(f"Failed to get integration {integration_id}: {str(e)}")
                    if not continue_on_error:
                        result.add_error(f"Failed to get integration {integration_id}", e, integration_id)
                        result.success = False
                        return result
                    # Otherwise continue with the next integration
            
        else:
            # Get integrations based on filter
            try:
                params = {
                    "integrationType": "SCHEDULED"  # Only get scheduled integrations
                }
                if filter_query:
                    params["q"] = filter_query
                    
                self.logger.info("Getting list of scheduled integrations")
                integrations = self.client.integrations.list(params=params)
                
                for integration in integrations:
                    integration_id = integration.get("id")
                    if integration_id:
                        # Get full details to access schedule information
                        try:
                            integration_detail = self.client.integrations.get(integration_id)
                            integrations_to_manage.append({
                                "id": integration_id,
                                "name": integration_detail.get("name", "Unknown"),
                                "status": integration_detail.get("status", "UNKNOWN"),
                                "schedule": integration_detail.get("schedule", {})
                            })
                        except OICError as e:
                            self.logger.error(f"Failed to get details for integration {integration_id}: {str(e)}")
                            if not continue_on_error:
                                result.add_error(f"Failed to get integration details", e, integration_id)
                                result.success = False
                                return result
                
            except OICError as e:
                self.logger.error(f"Failed to get integrations list: {str(e)}")
                result.add_error("Failed to get integrations list", e)
                result.success = False
                return result
        
        # If no integrations to manage, we're done
        if not integrations_to_manage:
            result.message = "No scheduled integrations found to manage"
            return result
        
        # Perform the requested action on each integration
        successful_operations = []
        failed_operations = []
        
        self.logger.info(f"Managing schedules for {len(integrations_to_manage)} integrations")
        result.details["total_count"] = len(integrations_to_manage)
        
        for integration in integrations_to_manage:
            integration_id = integration["id"]
            integration_name = integration["name"]
            current_status = integration["status"]
            current_schedule = integration["schedule"]
            
            self.logger.info(f"Managing schedule for integration: {integration_name}")
            
            try:
                # Get full integration details for update
                integration_detail = self.client.integrations.get(integration_id)
                
                # Find the schedule in the integration detail
                if "schedule" not in integration_detail:
                    self.logger.warning(f"Integration {integration_name} does not have a schedule field")
                    failed_operations.append({
                        "id": integration_id,
                        "name": integration_name,
                        "error": "No schedule field in integration"
                    })
                    result.add_error(f"No schedule field in integration {integration_name}", resource_id=integration_id)
                    result.add_resource("integration", integration_id, {
                        "name": integration_name,
                        "schedule_action": action,
                        "result": "error",
                        "error": "No schedule field in integration"
                    })
                    continue
                
                # Perform the requested action
                if action == "enable":
                    # Enable the schedule
                    integration_detail["schedule"]["enabled"] = True
                    action_description = "enabled"
                    
                elif action == "disable":
                    # Disable the schedule
                    integration_detail["schedule"]["enabled"] = False
                    action_description = "disabled"
                    
                elif action == "update":
                    # Update the schedule with provided data
                    integration_detail["schedule"].update(schedule_data)
                    action_description = "updated"
                
                # Update the integration
                self.logger.info(f"Updating integration {integration_name} to {action_description} schedule")
                update_result = self.client.integrations.update(integration_id, integration_detail)
                
                # Record successful operation
                successful_operations.append({
                    "id": integration_id,
                    "name": integration_name,
                    "schedule": update_result.get("schedule", {})
                })
                result.add_resource("integration", integration_id, {
                    "name": integration_name,
                    "schedule_action": action,
                    "result": "success",
                    "schedule": update_result.get("schedule", {})
                })
                
            except OICError as e:
                self.logger.error(f"Failed to {action} schedule for integration {integration_name}: {str(e)}")
                failed_operations.append({
                    "id": integration_id,
                    "name": integration_name,
                    "error": str(e)
                })
                result.add_error(f"Failed to {action} schedule", e, integration_id)
                result.add_resource("integration", integration_id, {
                    "name": integration_name,
                    "schedule_action": action,
                    "result": "error",
                    "error": str(e)
                })
                
                # Stop processing if we shouldn't continue on error
                if not continue_on_error:
                    result.success = False
                    result.message = f"Failed to {action} schedule for integration {integration_name}"
                    break
        
        # Update overall workflow status
        if failed_operations:
            result.success = False
            result.message = f"{action.capitalize()}d schedules for {len(successful_operations)} integrations, {len(failed_operations)} failed"
        else:
            result.message = f"Successfully {action}d schedules for {len(successful_operations)} integrations"
        
        # Add details to the result
        result.details["successful_count"] = len(successful_operations)
        result.details["failed_count"] = len(failed_operations)
        result.details["successful_operations"] = successful_operations
        result.details["failed_operations"] = failed_operations
        
        return result
        
    def find_integration_dependencies(
        self,
        integration_id: str,
        include_connections: bool = True,
        include_lookups: bool = True,
        include_libraries: bool = True
    ) -> WorkflowResult:
        """
        Find dependencies for an integration.
        
        This workflow:
        1. Gets the integration details
        2. Analyzes the integration to find all dependencies
        3. Returns a list of dependencies by type
        
        Args:
            integration_id: ID of the integration to analyze.
            include_connections: Whether to include connection dependencies.
            include_lookups: Whether to include lookup dependencies.
            include_libraries: Whether to include library dependencies.
            
        Returns:
            WorkflowResult: The workflow execution result with dependencies.
        """
        result = WorkflowResult()
        result.message = f"Finding dependencies for integration {integration_id}"
        
        # Get the integration details
        try:
            self.logger.info(f"Getting details for integration {integration_id}")
            integration = self.client.integrations.get(integration_id)
            integration_name = integration.get("name", "Unknown")
            
            result.add_resource("integration", integration_id, {"name": integration_name})
            result.details["integration_name"] = integration_name
            
        except OICError as e:
            self.logger.error(f"Failed to get integration {integration_id}: {str(e)}")
            result.add_error(f"Failed to get integration {integration_id}", e, integration_id)
            result.success = False
            return result
        
        # Initialize dependency lists
        connections = []
        lookups = []
        libraries = []
        
        # Check references section which contains dependencies
        if "references" in integration and isinstance(integration["references"], list):
            for ref in integration["references"]:
                if not isinstance(ref, dict):
                    continue
                    
                ref_type = ref.get("type")
                ref_id = ref.get("id")
                ref_name = ref.get("name", "Unknown")
                
                if not ref_id:
                    continue
                
                # Add to appropriate list based on type
                if include_connections and ref_type == "CONNECTION":
                    connections.append({
                        "id": ref_id,
                        "name": ref_name
                    })
                    result.add_resource("connection", ref_id, {
                        "name": ref_name
                    })
                    
                elif include_lookups and ref_type == "LOOKUP":
                    lookups.append({
                        "id": ref_id,
                        "name": ref_name
                    })
                    result.add_resource("lookup", ref_id, {
                        "name": ref_name
                    })
                    
                elif include_libraries and ref_type == "LIBRARY":
                    libraries.append({
                        "id": ref_id,
                        "name": ref_name
                    })
                    result.add_resource("library", ref_id, {
                        "name": ref_name
                    })
        
        # Some OIC versions store connection information differently
        # Check invoke/trigger sections if they exist
        if include_connections:
            for section in ["triggers", "invokes"]:
                if section in integration and isinstance(integration[section], list):
                    for item in integration[section]:
                        if not isinstance(item, dict):
                            continue
                            
                        conn_id = item.get("connectionId")
                        conn_name = item.get("connectionName", "Unknown")
                        
                        if conn_id:
                            # Check if we already have this connection
                            if not any(conn["id"] == conn_id for conn in connections):
                                connections.append({
                                    "id": conn_id,
                                    "name": conn_name
                                })
                                result.add_resource("connection", conn_id, {
                                    "name": conn_name
                                })
        
        # Update result with dependency counts
        result.details["dependencies"] = {
            "connections": connections,
            "lookups": lookups,
            "libraries": libraries
        }
        
        result.details["dependency_counts"] = {
            "connections": len(connections),
            "lookups": len(lookups),
            "libraries": len(libraries),
            "total": len(connections) + len(lookups) + len(libraries)
        }
        
        # Update result message
        total_deps = result.details["dependency_counts"]["total"]
        if total_deps == 0:
            result.message = f"No dependencies found for integration {integration_name}"
        else:
            dep_parts = []
            if connections:
                dep_parts.append(f"{len(connections)} connections")
            if lookups:
                dep_parts.append(f"{len(lookups)} lookups")
            if libraries:
                dep_parts.append(f"{len(libraries)} libraries")
                
            deps_str = ", ".join(dep_parts)
            result.message = f"Found {total_deps} dependencies for integration {integration_name}: {deps_str}"
        
        return result
    
    def restart_integration(
        self,
        integration_id: str,
        verify_restart: bool = True,
        wait_time: int = 10
    ) -> WorkflowResult:
        """
        Restart an integration by deactivating and then activating it.
        
        This workflow:
        1. Gets the integration details
        2. Deactivates the integration if it's active
        3. Activates the integration
        4. Verifies the integration is active if requested
        
        Args:
            integration_id: ID of the integration to restart.
            verify_restart: Whether to verify the integration is active after restart.
            wait_time: Time to wait between operations in seconds.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Restarting integration {integration_id}"
        
        # Get the integration details
        try:
            self.logger.info(f"Getting details for integration {integration_id}")
            integration = self.client.integrations.get(integration_id)
            integration_name = integration.get("name", "Unknown")
            current_status = integration.get("status", "UNKNOWN")
            
            result.add_resource("integration", integration_id, {
                "name": integration_name,
                "initial_status": current_status
            })
            result.details["integration_name"] = integration_name
            result.details["initial_status"] = current_status
            
        except OICError as e:
            self.logger.error(f"Failed to get integration {integration_id}: {str(e)}")
            result.add_error(f"Failed to get integration {integration_id}", e, integration_id)
            result.success = False
            return result
        
        # Deactivate the integration if it's active
        if current_status == "ACTIVATED":
            try:
                self.logger.info(f"Deactivating integration: {integration_name}")
                self.client.integrations.deactivate(integration_id)
                
                # Wait for deactivation to complete
                self.logger.info(f"Waiting {wait_time}s for deactivation to complete")
                time.sleep(wait_time)
                
                # Verify deactivation if requested
                if verify_restart:
                    try:
                        integration_status = self.client.integrations.get(integration_id)
                        if integration_status.get("status") == "ACTIVATED":
                            self.logger.warning(f"Integration {integration_name} still activated after deactivation attempt")
                            result.add_error(f"Deactivation verification failed", resource_id=integration_id)
                            result.success = False
                            result.message = f"Failed to deactivate integration {integration_name}"
                            return result
                    except OICError as e:
                        self.logger.error(f"Failed to verify deactivation: {str(e)}")
                        result.add_error(f"Failed to verify deactivation", e, integration_id)
                        result.success = False
                        result.message = f"Failed to verify deactivation of integration {integration_name}"
                        return result
                    
            except OICError as e:
                self.logger.error(f"Failed to deactivate integration {integration_name}: {str(e)}")
                result.add_error(f"Failed to deactivate integration", e, integration_id)
                result.success = False
                result.message = f"Failed to deactivate integration {integration_name}"
                return result
        
        # Activate the integration
        try:
            self.logger.info(f"Activating integration: {integration_name}")
            self.client.integrations.activate(integration_id)
            
            # Wait for activation to complete
            self.logger.info(f"Waiting {wait_time}s for activation to complete")
            time.sleep(wait_time)
            
            # Verify activation if requested
            if verify_restart:
                try:
                    integration_status = self.client.integrations.get(integration_id)
                    final_status = integration_status.get("status")
                    
                    result.details["final_status"] = final_status
                    result.add_resource("integration", integration_id, {
                        "final_status": final_status
                    })
                    
                    if final_status != "ACTIVATED":
                        self.logger.warning(f"Integration {integration_name} not fully activated, status: {final_status}")
                        result.add_error(f"Activation verification failed", resource_id=integration_id)
                        result.success = False
                        result.message = f"Failed to activate integration {integration_name}, final status: {final_status}"
                        return result
                        
                except OICError as e:
                    self.logger.error(f"Failed to verify activation: {str(e)}")
                    result.add_error(f"Failed to verify activation", e, integration_id)
                    result.success = False
                    result.message = f"Failed to verify activation of integration {integration_name}"
                    return result
                
        except OICError as e:
            self.logger.error(f"Failed to activate integration {integration_name}: {str(e)}")
            result.add_error(f"Failed to activate integration", e, integration_id)
            result.success = False
            result.message = f"Failed to activate integration {integration_name}"
            return result
        
        # Successfully restarted
        result.message = f"Successfully restarted integration {integration_name}"
        return result
    
    def trace_integration_instances(
        self,
        integration_id: Optional[str] = None,
        instance_id: Optional[str] = None,
        start_time: Optional[Union[str, datetime.datetime]] = None,
        end_time: Optional[Union[str, datetime.datetime]] = None,
        status: Optional[str] = None,
        include_activities: bool = True,
        include_payloads: bool = False,
        max_instances: int = 10
    ) -> WorkflowResult:
        """
        Trace integration instances.
        
        This workflow:
        1. Gets integration instances based on filters
        2. Optionally gets activities for each instance
        3. Optionally gets payloads for activities
        
        Args:
            integration_id: Optional ID of the integration to trace.
            instance_id: Optional ID of a specific instance to trace.
            start_time: Optional start time for the trace window.
            end_time: Optional end time for the trace window.
            status: Optional status filter (e.g., "COMPLETED", "FAILED").
            include_activities: Whether to include instance activities.
            include_payloads: Whether to include activity payloads.
            max_instances: Maximum number of instances to trace.
            
        Returns:
            WorkflowResult: The workflow execution result with trace information.
        """
        result = WorkflowResult()
        
        # Set up monitoring params
        params = {}
        trace_description = []
        
        if integration_id:
            params["integrationId"] = integration_id
            trace_description.append(f"integration {integration_id}")
            
            # Get integration name if possible
            try:
                integration = self.client.integrations.get(integration_id)
                integration_name = integration.get("name", "Unknown")
                result.add_resource("integration", integration_id, {"name": integration_name})
            except OICError:
                pass  # Continue even if we can't get the name
        
        if instance_id:
            # If specific instance ID is provided, other params are ignored
            trace_description = [f"instance {instance_id}"]
        
        if status:
            params["status"] = status
            trace_description.append(f"status {status}")
        
        if start_time:
            if isinstance(start_time, datetime.datetime):
                params["startTime"] = start_time.isoformat()
            else:
                params["startTime"] = start_time
            trace_description.append(f"from {params['startTime']}")
        
        if end_time:
            if isinstance(end_time, datetime.datetime):
                params["endTime"] = end_time.isoformat()
            else:
                params["endTime"] = end_time
            trace_description.append(f"to {params['endTime']}")
        
        result.message = f"Tracing instances for {', '.join(trace_description)}"
        
        # Get instances
        try:
            if instance_id:
                # Get a specific instance
                self.logger.info(f"Getting specific instance {instance_id}")
                instance = self.client.monitoring.get_instance(instance_id)
                instances = [instance]
            else:
                # Get instances based on filters
                self.logger.info(f"Getting integration instances with filters: {params}")
                instances = self.client.monitoring.get_instances(**params)
                
                # Limit the number of instances
                if len(instances) > max_instances:
                    self.logger.info(f"Limiting to {max_instances} of {len(instances)} instances")
                    instances = instances[:max_instances]
            
            if not instances:
                result.message = "No instances found matching the criteria"
                return result
                
            result.details["instance_count"] = len(instances)
            
        except OICError as e:
            self.logger.error(f"Failed to get instances: {str(e)}")
            result.add_error("Failed to get instances", e)
            result.success = False
            return result
        
        # Process each instance
        for instance in instances:
            instance_id = instance.get("id")
            if not instance_id:
                continue
                
            # Add instance to results
            result.add_resource("instance", instance_id, {
                "integrationId": instance.get("integrationId"),
                "status": instance.get("status"),
                "startTime": instance.get("startTime"),
                "endTime": instance.get("endTime"),
                "message": instance.get("message")
            })
            
            # Get activities if requested
            if include_activities and instance_id:
                try:
                    self.logger.info(f"Getting activities for instance {instance_id}")
                    activities = self.client.monitoring.get_instance_activities(instance_id)
                    
                    # Add activities to the instance resource
                    instance_resource = result.resources.get("instance", {}).get(instance_id, {})
                    instance_resource["activities"] = []
                    
                    for activity in activities:
                        activity_id = activity.get("id")
                        if not activity_id:
                            continue
                            
                        activity_info = {
                            "id": activity_id,
                            "activityName": activity.get("activityName"),
                            "status": activity.get("status"),
                            "startTime": activity.get("startTime"),
                            "endTime": activity.get("endTime"),
                            "message": activity.get("message")
                        }
                        
                        # Get payloads if requested
                        if include_payloads and activity_id:
                            activity_info["payloads"] = {}
                            
                            # Get request payload
                            try:
                                request_payload = self.client.monitoring.get_instance_payload(
                                    instance_id, activity_id, "request"
                                )
                                activity_info["payloads"]["request"] = request_payload
                            except OICError as e:
                                self.logger.warning(f"Failed to get request payload for activity {activity_id}: {str(e)}")
                            
                            # Get response payload
                            try:
                                response_payload = self.client.monitoring.get_instance_payload(
                                    instance_id, activity_id, "response"
                                )
                                activity_info["payloads"]["response"] = response_payload
                            except OICError as e:
                                self.logger.warning(f"Failed to get response payload for activity {activity_id}: {str(e)}")
                        
                        # Add activity to the instance
                        instance_resource["activities"].append(activity_info)
                    
                except OICError as e:
                    self.logger.warning(f"Failed to get activities for instance {instance_id}: {str(e)}")
                    # Continue with other instances rather than failing
        
        # Update result message
        instance_count = len(instances)
        if instance_count == 1:
            instance = instances[0]
            integration_id = instance.get("integrationId", "Unknown")
            instance_id = instance.get("id", "Unknown")
            status = instance.get("status", "Unknown")
            
            result.message = f"Successfully traced instance {instance_id} for integration {integration_id} (status: {status})"
        else:
            result.message = f"Successfully traced {instance_count} instances for {', '.join(trace_description)}"
        
        return result
