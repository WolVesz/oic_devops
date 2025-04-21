"""
Connection workflows module for the OIC DevOps package.

This module provides workflow operations for managing connections.
"""

import logging
import time
from typing import Dict, Any, List, Optional, Union

from oic_devops.client import OICClient
from oic_devops.exceptions import OICError
from oic_devops.workflows.base import BaseWorkflow, WorkflowResult


class ConnectionWorkflows(BaseWorkflow):
    """
    Workflow operations for managing connections.
    
    This class provides higher-level operations for working with connections,
    such as updating credentials, testing all connections, and finding dependent
    integrations.
    """
    
    def execute(self, *args, **kwargs) -> WorkflowResult:
        """
        Execute the specified connection workflow.
        
        This is a dispatcher method that calls the appropriate workflow
        based on the operation argument.
        
        Args:
            operation: The workflow operation to execute.
            **kwargs: Additional arguments specific to the workflow.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        operation = kwargs.pop("operation", None)
        
        if operation == "update_credentials":
            return self.update_credentials(**kwargs)
        elif operation == "test_all":
            return self.test_all_connections(**kwargs)
        elif operation == "find_dependents":
            return self.find_dependent_integrations(**kwargs)
        elif operation == "update_and_restart":
            return self.update_credentials_and_restart_integrations(**kwargs)
        else:
            result = WorkflowResult(success=False, message=f"Unknown connection workflow operation: {operation}")
            result.add_error(f"Unknown operation: {operation}")
            return result
    
    def update_credentials(
        self,
        connection_id: str,
        credentials: Dict[str, Any],
        test_connection: bool = True
    ) -> WorkflowResult:
        """
        Update credentials for a connection.
        
        This workflow:
        1. Gets the current connection configuration
        2. Updates the credentials
        3. Updates the connection
        4. Optionally tests the connection
        
        Args:
            connection_id: ID of the connection to update.
            credentials: Dict containing credential fields to update.
                Keys depend on the connection type but typically include
                'password', 'securityToken', etc.
            test_connection: Whether to test the connection after updating.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Updating credentials for connection {connection_id}"
        
        # Get current connection details
        try:
            self.logger.info(f"Getting current configuration for connection {connection_id}")
            connection = self.client.connections.get(connection_id)
            result.add_resource("connection", connection_id, {"name": connection.get("name", "Unknown")})
            
        except OICError as e:
            self.logger.error(f"Failed to get connection {connection_id}: {str(e)}")
            result.add_error(f"Failed to get connection {connection_id}", e, connection_id)
            return result
        
        # Update credentials in connection properties
        # Handle different connection types with different credential structures
        try:
            # Update credentials based on connection type
            connection_type = connection.get("connectionType", "")
            
            # Check for different property locations based on connection type
            credential_updated = False
            
            # Check in securityProperties
            if "securityProperties" in connection:
                for key, value in credentials.items():
                    if key in connection["securityProperties"]:
                        self.logger.debug(f"Updating credential '{key}' in securityProperties")
                        connection["securityProperties"][key] = value
                        credential_updated = True
            
            # Check in connectionProperties
            if "connectionProperties" in connection:
                for key, value in credentials.items():
                    if key in connection["connectionProperties"]:
                        self.logger.debug(f"Updating credential '{key}' in connectionProperties")
                        connection["connectionProperties"][key] = value
                        credential_updated = True
            
            # Check in top-level properties for some connection types
            if "properties" in connection:
                for key, value in credentials.items():
                    if key in connection["properties"]:
                        self.logger.debug(f"Updating credential '{key}' in properties")
                        connection["properties"][key] = value
                        credential_updated = True
            
            # Generic approach for other property locations
            for key, value in credentials.items():
                if key in connection:
                    self.logger.debug(f"Updating credential '{key}' at top level")
                    connection[key] = value
                    credential_updated = True
            
            if not credential_updated:
                error_msg = f"No credential fields were updated. Available fields didn't match provided credentials"
                self.logger.error(error_msg)
                result.add_error(error_msg, resource_id=connection_id)
                return result
                
        except Exception as e:
            self.logger.error(f"Failed to update credential values: {str(e)}")
            result.add_error("Failed to update credential values", e, connection_id)
            return result
        
        # Update the connection
        try:
            self.logger.info(f"Updating connection {connection_id}")
            update_result = self.client.connections.update(connection_id, connection)
            result.details["update_result"] = {"status": "success"}
            
        except OICError as e:
            self.logger.error(f"Failed to update connection {connection_id}: {str(e)}")
            result.add_error(f"Failed to update connection {connection_id}", e, connection_id)
            return result
        
        # Test the connection if requested
        if test_connection:
            try:
                self.logger.info(f"Testing connection {connection_id}")
                test_result = self.client.connections.test(connection_id)
                
                # Check test result
                if test_result.get("status") == "SUCCESS" or test_result.get("state") == "SUCCESS":
                    result.details["test_result"] = {"status": "success"}
                    self.logger.info(f"Connection test successful for {connection_id}")
                else:
                    error_msg = test_result.get("message", "Unknown test failure")
                    self.logger.error(f"Connection test failed: {error_msg}")
                    result.success = False
                    result.message = f"Credentials updated but test failed: {error_msg}"
                    result.add_error("Connection test failed", resource_id=connection_id)
                    result.details["test_result"] = {"status": "failure", "message": error_msg}
                    
            except OICError as e:
                self.logger.error(f"Failed to test connection {connection_id}: {str(e)}")
                result.success = False
                result.message = "Credentials updated but test failed"
                result.add_error("Failed to test connection", e, connection_id)
                result.details["test_result"] = {"status": "error", "message": str(e)}
        
        if result.success:
            result.message = f"Successfully updated credentials for connection {connection_id}"
            if test_connection:
                result.message += " and verified connection is working"
                
        return result
    
    def test_all_connections(
        self,
        filter_query: Optional[str] = None,
        continue_on_error: bool = True
    ) -> WorkflowResult:
        """
        Test all connections or a filtered subset of connections.
        
        Args:
            filter_query: Optional query to filter connections.
            continue_on_error: Whether to continue testing if some tests fail.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = "Testing connections"
        
        # Get list of connections
        try:
            params = {}
            if filter_query:
                params["q"] = filter_query
                
            self.logger.info("Getting list of connections")
            connections = self.client.connections.list(params=params)
            
            if not connections:
                result.message = "No connections found to test"
                return result
                
            result.details["connection_count"] = len(connections)
            self.logger.info(f"Found {len(connections)} connections to test")
            
        except OICError as e:
            self.logger.error(f"Failed to get connections list: {str(e)}")
            result.add_error("Failed to get connections list", e)
            return result
        
        # Test each connection
        success_count = 0
        failed_connections = []
        
        for connection in connections:
            connection_id = connection.get("id")
            connection_name = connection.get("name", "Unknown")
            
            if not connection_id:
                self.logger.warning(f"Skipping connection with no ID: {connection_name}")
                continue
                
            self.logger.info(f"Testing connection {connection_name} ({connection_id})")
            
            try:
                test_result = self.client.connections.test(connection_id)
                
                # Check if test was successful
                if test_result.get("status") == "SUCCESS" or test_result.get("state") == "SUCCESS":
                    self.logger.info(f"Connection test successful for {connection_name}")
                    success_count += 1
                    result.add_resource("connection", connection_id, {
                        "name": connection_name,
                        "test_result": "success"
                    })
                else:
                    error_msg = test_result.get("message", "Unknown test failure")
                    self.logger.error(f"Connection test failed for {connection_name}: {error_msg}")
                    failed_connections.append({
                        "id": connection_id,
                        "name": connection_name,
                        "error": error_msg
                    })
                    result.add_resource("connection", connection_id, {
                        "name": connection_name,
                        "test_result": "failure",
                        "error": error_msg
                    })
                    
                    if not continue_on_error:
                        result.success = False
                        result.message = f"Connection test failed for {connection_name}"
                        result.add_error(f"Connection test failed: {error_msg}", resource_id=connection_id)
                        break
                        
            except OICError as e:
                self.logger.error(f"Error testing connection {connection_name}: {str(e)}")
                failed_connections.append({
                    "id": connection_id,
                    "name": connection_name,
                    "error": str(e)
                })
                result.add_resource("connection", connection_id, {
                    "name": connection_name,
                    "test_result": "error",
                    "error": str(e)
                })
                
                if not continue_on_error:
                    result.success = False
                    result.message = f"Error testing connection {connection_name}"
                    result.add_error(f"Error testing connection", e, connection_id)
                    break
        
        # Update result message and details
        if result.success:
            if not failed_connections:
                result.message = f"All {success_count} connections tested successfully"
            else:
                result.success = False
                result.message = f"{success_count} connections tested successfully, {len(failed_connections)} failed"
                
        result.details["success_count"] = success_count
        result.details["failed_count"] = len(failed_connections)
        result.details["failed_connections"] = failed_connections
        
        return result
    
    def find_dependent_integrations(
        self,
        connection_id: str,
        check_active_only: bool = False
    ) -> WorkflowResult:
        """
        Find all integrations that depend on a specific connection.
        
        This workflow:
        1. Gets all integrations
        2. Checks each integration for references to the connection
        3. Returns a list of dependent integrations
        
        Args:
            connection_id: ID of the connection to check for dependencies.
            check_active_only: Whether to check only active integrations.
            
        Returns:
            WorkflowResult: The workflow execution result with dependent integrations.
        """
        result = WorkflowResult()
        result.message = f"Finding integrations dependent on connection {connection_id}"
        
        # Get the connection details first to verify it exists
        try:
            self.logger.info(f"Getting details for connection {connection_id}")
            connection = self.client.connections.get(connection_id)
            connection_name = connection.get("name", "Unknown")
            
            result.add_resource("connection", connection_id, {"name": connection_name})
            result.details["connection_name"] = connection_name
            
        except OICError as e:
            self.logger.error(f"Failed to get connection {connection_id}: {str(e)}")
            result.add_error(f"Failed to get connection {connection_id}", e, connection_id)
            return result
        
        # Get list of integrations
        try:
            params = {}
            if check_active_only:
                params["status"] = "ACTIVATED"
                
            self.logger.info("Getting list of integrations")
            integrations = self.client.integrations.list(params=params)
            
            if not integrations:
                result.message = f"No integrations found that could depend on connection {connection_name}"
                return result
                
            self.logger.info(f"Found {len(integrations)} integrations to check")
            
        except OICError as e:
            self.logger.error(f"Failed to get integrations list: {str(e)}")
            result.add_error("Failed to get integrations list", e)
            return result
        
        # Check each integration for dependencies
        dependent_integrations = []
        
        for integration in integrations:
            integration_id = integration.get("id")
            integration_name = integration.get("name", "Unknown")
            
            if not integration_id:
                self.logger.warning(f"Skipping integration with no ID: {integration_name}")
                continue
                
            self.logger.debug(f"Checking integration {integration_name} ({integration_id})")
            
            try:
                # Get detailed integration information to check for connections
                integration_detail = self.client.integrations.get(integration_id)
                
                # Check various places where connection references might be stored
                # This depends on the structure of the integration JSON from the OIC API
                is_dependent = False
                
                # Check direct connections list if it exists
                if "connections" in integration_detail and isinstance(integration_detail["connections"], list):
                    if connection_id in integration_detail["connections"]:
                        is_dependent = True
                
                # Check references section which often contains connection references
                if "references" in integration_detail and isinstance(integration_detail["references"], list):
                    for ref in integration_detail["references"]:
                        if isinstance(ref, dict) and ref.get("type") == "CONNECTION" and ref.get("id") == connection_id:
                            is_dependent = True
                            break
                
                # Some OIC versions store trigger/invoke connection information differently
                # Check invoke/trigger sections if they exist
                for section in ["triggers", "invokes"]:
                    if section in integration_detail and isinstance(integration_detail[section], list):
                        for item in integration_detail[section]:
                            if isinstance(item, dict) and item.get("connectionId") == connection_id:
                                is_dependent = True
                                break
                
                # If dependent, add to list
                if is_dependent:
                    self.logger.info(f"Integration {integration_name} depends on connection {connection_name}")
                    
                    # Gather additional useful information about the integration
                    status = integration_detail.get("status", "UNKNOWN")
                    pattern = integration_detail.get("pattern", "UNKNOWN")
                    integration_type = integration_detail.get("integrationType", "UNKNOWN")
                    
                    dependent_integration = {
                        "id": integration_id,
                        "name": integration_name,
                        "status": status,
                        "pattern": pattern,
                        "type": integration_type
                    }
                    
                    dependent_integrations.append(dependent_integration)
                    result.add_resource("integration", integration_id, dependent_integration)
                
            except OICError as e:
                self.logger.warning(f"Error checking integration {integration_name}: {str(e)}")
                # Continue checking other integrations rather than failing
        
        # Update result message and details
        count = len(dependent_integrations)
        if count == 0:
            result.message = f"No integrations found that depend on connection {connection_name}"
        else:
            result.message = f"Found {count} integrations that depend on connection {connection_name}"
            
        result.details["dependent_count"] = count
        result.details["dependent_integrations"] = dependent_integrations
        
        return result

    def update_credentials_and_restart_integrations(
        self, 
        connection_id: str,
        credentials: Dict[str, Any],
        restart_scope: str = "all",  # "all", "active", "none"
        sequential_restart: bool = True,
        verify_restart: bool = True,
        wait_time: int = 10
    ) -> WorkflowResult:
        """
        Update connection credentials and restart dependent integrations.
        
        This workflow:
        1. Updates credentials for the connection
        2. Finds all dependent integrations
        3. Deactivates and reactivates each integration to restart them with new credentials
        
        Args:
            connection_id: ID of the connection to update.
            credentials: Dict containing credential fields to update.
            restart_scope: Which integrations to restart: "all", "active", or "none".
            sequential_restart: Whether to restart integrations one at a time.
            verify_restart: Whether to verify integrations are active after restart.
            wait_time: Time to wait between operations in seconds.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Updating credentials and restarting integrations for connection {connection_id}"
        
        # Step 1: Update credentials
        self.logger.info(f"Updating credentials for connection {connection_id}")
        update_result = self.update_credentials(
            connection_id=connection_id,
            credentials=credentials,
            test_connection=True
        )
        
        # Merge results
        result.merge(update_result)
        
        # If credential update failed, stop
        if not update_result.success:
            self.logger.error("Credential update failed, stopping workflow")
            result.message = "Failed to update credentials, integrations not restarted"
            return result
        
        # Get connection name for better logging
        connection_name = "Unknown"
        if "connection" in update_result.resources and connection_id in update_result.resources["connection"]:
            connection_name = update_result.resources["connection"][connection_id].get("name", "Unknown")
        
        # Step 2: If no restart needed, we're done
        if restart_scope == "none":
            result.message = f"Successfully updated credentials for connection {connection_name}, no integrations restarted"
            return result
        
        # Step 3: Find dependent integrations
        self.logger.info(f"Finding integrations dependent on connection {connection_id}")
        dependent_result = self.find_dependent_integrations(
            connection_id=connection_id,
            check_active_only=(restart_scope == "active")
        )
        
        # Merge results
        result.merge(dependent_result)
        
        # If finding dependents failed, report but continue
        if not dependent_result.success:
            self.logger.warning("Error finding dependent integrations, but continuing")
        
        # Get list of integrations to restart
        integrations_to_restart = []
        
        if "integration" in dependent_result.resources:
            for integration_id, integration_data in dependent_result.resources["integration"].items():
                # For "active" scope, only restart already-active integrations
                if restart_scope == "active" and integration_data.get("status") != "ACTIVATED":
                    self.logger.info(f"Skipping inactive integration: {integration_data.get('name', integration_id)}")
                    continue
                
                integrations_to_restart.append({
                    "id": integration_id,
                    "name": integration_data.get("name", "Unknown"),
                    "status": integration_data.get("status", "UNKNOWN")
                })
        
        # If no integrations to restart, we're done
        if not integrations_to_restart:
            self.logger.info("No integrations to restart")
            result.message = f"Successfully updated credentials for connection {connection_name}, no integrations to restart"
            return result
        
        # Step 4: Restart each integration
        successful_restarts = []
        failed_restarts = []
        
        self.logger.info(f"Restarting {len(integrations_to_restart)} integrations")
        
        for integration in integrations_to_restart:
            integration_id = integration["id"]
            integration_name = integration["name"]
            current_status = integration["status"]
            
            self.logger.info(f"Processing integration: {integration_name} (current status: {current_status})")
            
            # Only deactivate if already activated
            deactivate_needed = (current_status == "ACTIVATED")
            restart_success = True
            restart_error = None
            
            # Step 4a: Deactivate if needed
            if deactivate_needed:
                try:
                    self.logger.info(f"Deactivating integration: {integration_name}")
                    self.client.integrations.deactivate(integration_id)
                    
                    # Wait for deactivation to complete if sequential restart
                    if sequential_restart:
                        self.logger.info(f"Waiting {wait_time}s for deactivation to complete")
                        time.sleep(wait_time)
                        
                        # Verify deactivation if requested
                        if verify_restart:
                            integration_status = self.client.integrations.get(integration_id)
                            if integration_status.get("status") != "CONFIGURED":
                                self.logger.warning(f"Integration {integration_name} not fully deactivated, status: {integration_status.get('status')}")
                    
                except OICError as e:
                    self.logger.error(f"Failed to deactivate integration {integration_name}: {str(e)}")
                    restart_success = False
                    restart_error = f"Deactivation failed: {str(e)}"
            
            # Step 4b: Activate the integration
            if restart_success:  # Only if deactivation succeeded or wasn't needed
                try:
                    self.logger.info(f"Activating integration: {integration_name}")
                    self.client.integrations.activate(integration_id)
                    
                    # Wait for activation to complete if sequential restart
                    if sequential_restart:
                        self.logger.info(f"Waiting {wait_time}s for activation to complete")
                        time.sleep(wait_time)
                        
                        # Verify activation if requested
                        if verify_restart:
                            integration_status = self.client.integrations.get(integration_id)
                            if integration_status.get("status") != "ACTIVATED":
                                self.logger.warning(f"Integration {integration_name} not fully activated, status: {integration_status.get('status')}")
                                restart_success = False
                                restart_error = f"Activation verification failed, status: {integration_status.get('status')}"
                    
                except OICError as e:
                    self.logger.error(f"Failed to activate integration {integration_name}: {str(e)}")
                    restart_success = False
                    restart_error = f"Activation failed: {str(e)}"
            
            # Record the result
            if restart_success:
                successful_restarts.append({
                    "id": integration_id,
                    "name": integration_name
                })
                result.add_resource("restarted_integration", integration_id, {
                    "name": integration_name,
                    "result": "success"
                })
            else:
                failed_restarts.append({
                    "id": integration_id,
                    "name": integration_name,
                    "error": restart_error
                })
                result.add_error(
                    f"Failed to restart integration {integration_name}", 
                    resource_id=integration_id
                )
                result.add_resource("restarted_integration", integration_id, {
                    "name": integration_name,
                    "result": "failure",
                    "error": restart_error
                })
        
        # Update overall workflow status
        if failed_restarts:
            result.success = False
            result.message = (
                f"Updated credentials for connection {connection_name}, "
                f"but {len(failed_restarts)} of {len(integrations_to_restart)} integration restarts failed"
            )
        else:
            result.message = (
                f"Successfully updated credentials for connection {connection_name} "
                f"and restarted {len(successful_restarts)} integrations"
            )
        
        # Add details to the result
        result.details["restart_results"] = {
            "successful_count": len(successful_restarts),
            "failed_count": len(failed_restarts),
            "successful_restarts": successful_restarts,
            "failed_restarts": failed_restarts
        }
        
        return result
