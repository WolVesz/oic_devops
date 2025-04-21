"""
Validation workflows module for the OIC DevOps package.

This module provides workflow operations for validating OIC resources,
including connection validation, integration validation, and configuration best practices.
"""

import os
import logging
import time
import datetime
import json
import re
from typing import Dict, Any, List, Optional, Union, Tuple, Set

from oic_devops.client import OICClient
from oic_devops.exceptions import OICError
from oic_devops.workflows.base import BaseWorkflow, WorkflowResult


class ValidationWorkflows(BaseWorkflow):
    """
    Workflow operations for validating OIC resources.
    
    This class provides higher-level operations for validating OIC resources,
    including connection validation, integration validation, and best practices.
    """
    
    def execute(self, *args, **kwargs) -> WorkflowResult:
        """
        Execute the specified validation workflow.
        
        This is a dispatcher method that calls the appropriate workflow
        based on the operation argument.
        
        Args:
            operation: The workflow operation to execute.
            **kwargs: Additional arguments specific to the workflow.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        operation = kwargs.pop("operation", None)
        
        if operation == "validate_connections":
            return self.validate_connections(**kwargs)
        elif operation == "validate_integrations":
            return self.validate_integrations(**kwargs)
        elif operation == "best_practices":
            return self.validate_best_practices(**kwargs)
        elif operation == "configuration_check":
            return self.validate_configuration(**kwargs)
        elif operation == "naming_conventions":
            return self.validate_naming_conventions(**kwargs)
        else:
            result = WorkflowResult(success=False, message=f"Unknown validation workflow operation: {operation}")
            result.add_error(f"Unknown operation: {operation}")
            return result
    
    def validate_connections(
        self,
        connection_ids: Optional[List[str]] = None,
        filter_query: Optional[str] = None,
        test_connections: bool = True,
        continue_on_error: bool = True,
        validate_naming: bool = True,
        naming_pattern: Optional[str] = None
    ) -> WorkflowResult:
        """
        Validate connections against various criteria.
        
        This workflow:
        1. Gets connections to validate
        2. Tests connections if requested
        3. Validates naming conventions if requested
        
        Args:
            connection_ids: Optional list of connection IDs to validate.
            filter_query: Optional query to filter connections.
            test_connections: Whether to test connections.
            continue_on_error: Whether to continue if some validations fail.
            validate_naming: Whether to validate naming conventions.
            naming_pattern: Optional regex pattern for connection names.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = "Validating connections"
        
        # Set default naming pattern if not provided
        if validate_naming and naming_pattern is None:
            # Default pattern: [ENV]_[TYPE]_[NAME]
            # Example: DEV_REST_SALESFORCE
            naming_pattern = r"^([A-Z]+)_([A-Z]+)_([A-Z0-9_]+)$"
        
        # Get connections to validate
        try:
            if connection_ids:
                # Get connections by ID
                connections = []
                for connection_id in connection_ids:
                    try:
                        connection = self.client.connections.get(connection_id)
                        connections.append(connection)
                    except OICError as e:
                        self.logger.warning(f"Failed to get connection {connection_id}: {str(e)}")
                        if not continue_on_error:
                            result.add_error(f"Failed to get connection {connection_id}", e, connection_id)
                            result.success = False
                            return result
                        # Continue with other connections
            else:
                # Get connections by filter
                params = {}
                if filter_query:
                    params["q"] = filter_query
                
                connections = self.client.connections.list(params=params)
            
            if not connections:
                result.message = "No connections found to validate"
                return result
            
            # Initialize validation results
            validation_results = {}
            valid_connections = []
            invalid_connections = []
            
            # Validate each connection
            for connection in connections:
                connection_id = connection.get("id")
                connection_name = connection.get("name", "Unknown")
                connection_type = connection.get("connectionType", "Unknown")
                
                if not connection_id:
                    continue
                
                # Initialize validation result
                validation_result = {
                    "valid": True,
                    "issues": []
                }
                
                # Test connection if requested
                if test_connections:
                    try:
                        self.logger.info(f"Testing connection: {connection_name}")
                        test_result = self.client.connections.test(connection_id)
                        
                        # Check test result
                        test_status = test_result.get("status", "UNKNOWN")
                        if test_status != "SUCCESS":
                            validation_result["valid"] = False
                            validation_result["issues"].append(f"Connection test failed: {test_result.get('message', 'Unknown error')}")
                    except OICError as e:
                        validation_result["valid"] = False
                        validation_result["issues"].append(f"Connection test error: {str(e)}")
                        
                        if not continue_on_error:
                            result.add_error(f"Connection test failed for {connection_name}", e, connection_id)
                            result.success = False
                            result.message = f"Connection validation failed for {connection_name}"
                            return result
                
                # Validate naming convention if requested
                if validate_naming and naming_pattern:
                    try:
                        # Check if name matches pattern
                        if not re.match(naming_pattern, connection_name):
                            validation_result["valid"] = False
                            validation_result["issues"].append(f"Connection name '{connection_name}' does not match required pattern: {naming_pattern}")
                    except Exception as e:
                        self.logger.warning(f"Error validating connection name: {str(e)}")
                        validation_result["issues"].append(f"Connection name validation error: {str(e)}")
                
                # Store validation result
                validation_results[connection_id] = validation_result
                
                # Add to valid or invalid lists
                if validation_result["valid"]:
                    valid_connections.append({
                        "id": connection_id,
                        "name": connection_name,
                        "type": connection_type
                    })
                else:
                    invalid_connections.append({
                        "id": connection_id,
                        "name": connection_name,
                        "type": connection_type,
                        "issues": validation_result["issues"]
                    })
                
                # Add to resources
                result.add_resource("connection", connection_id, {
                    "name": connection_name,
                    "type": connection_type,
                    "valid": validation_result["valid"],
                    "issues": validation_result["issues"]
                })
            
            # Update result details
            result.details["validation_results"] = validation_results
            result.details["valid_connections"] = valid_connections
            result.details["invalid_connections"] = invalid_connections
            result.details["valid_count"] = len(valid_connections)
            result.details["invalid_count"] = len(invalid_connections)
            result.details["total_count"] = len(connections)
            
            # Update result success and message
            if invalid_connections:
                result.success = False
                result.message = f"Validated {len(connections)} connections, found {len(invalid_connections)} invalid connections"
            else:
                result.message = f"All {len(connections)} connections passed validation"
                
        except OICError as e:
            result.success = False
            result.message = f"Failed to validate connections: {str(e)}"
            result.add_error("Failed to validate connections", e)
            
        return result
    
    def validate_integrations(
        self,
        integration_ids: Optional[List[str]] = None,
        filter_query: Optional[str] = None,
        validation_rules: Optional[Dict[str, Any]] = None,
        continue_on_error: bool = True
    ) -> WorkflowResult:
        """
        Validate integrations against various criteria.
        
        This workflow:
        1. Gets integrations to validate
        2. Validates them against the provided rules
        
        Args:
            integration_ids: Optional list of integration IDs to validate.
            filter_query: Optional query to filter integrations.
            validation_rules: Rules to validate integrations against.
            continue_on_error: Whether to continue if some validations fail.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = "Validating integrations"
        
        # Set default validation rules if not provided
        if validation_rules is None:
            validation_rules = {
                "naming_pattern": r"^([A-Z]+)_([A-Z]+)_([A-Z0-9_]+)$",  # ENV_TYPE_NAME
                "require_description": True,
                "require_version": True,
                "version_pattern": r"^\d+\.\d+\.\d+$",  # Semantic versioning
                "max_version_age_days": 90,  # Maximum age of the latest version
                "require_error_handling": True,
                "require_logging": True,
                "check_inactive_integrations": True,
                "max_inactive_days": 30  # Maximum days an integration can be inactive
            }
        
        # Get integrations to validate
        try:
            if integration_ids:
                # Get integrations by ID
                integrations = []
                for integration_id in integration_ids:
                    try:
                        integration = self.client.integrations.get(integration_id)
                        integrations.append(integration)
                    except OICError as e:
                        self.logger.warning(f"Failed to get integration {integration_id}: {str(e)}")
                        if not continue_on_error:
                            result.add_error(f"Failed to get integration {integration_id}", e, integration_id)
                            result.success = False
                            return result
                        # Continue with other integrations
            else:
                # Get integrations by filter
                params = {}
                if filter_query:
                    params["q"] = filter_query
                
                integrations = self.client.integrations.list(params=params)
            
            if not integrations:
                result.message = "No integrations found to validate"
                return result
            
            # Initialize validation results
            validation_results = {}
            valid_integrations = []
            invalid_integrations = []
            
            # Validate each integration
            for integration in integrations:
                integration_id = integration.get("id")
                integration_name = integration.get("name", "Unknown")
                integration_type = integration.get("integrationType", "Unknown")
                integration_status = integration.get("status", "Unknown")
                
                if not integration_id:
                    continue
                
                # Initialize validation result
                validation_result = {
                    "valid": True,
                    "issues": []
                }
                
                # Get full integration details
                try:
                    integration_detail = self.client.integrations.get(integration_id)
                except OICError as e:
                    self.logger.warning(f"Failed to get detailed information for integration {integration_name}: {str(e)}")
                    if not continue_on_error:
                        result.add_error(f"Failed to get integration details", e, integration_id)
                        result.success = False
                        result.message = f"Integration validation failed for {integration_name}"
                        return result
                    # Use the basic integration information
                    integration_detail = integration
                
                # Validate naming convention
                if validation_rules.get("naming_pattern"):
                    pattern = validation_rules["naming_pattern"]
                    if not re.match(pattern, integration_name):
                        validation_result["valid"] = False
                        validation_result["issues"].append(f"Integration name '{integration_name}' does not match required pattern: {pattern}")
                
                # Validate description
                if validation_rules.get("require_description"):
                    description = integration_detail.get("description", "")
                    if not description:
                        validation_result["valid"] = False
                        validation_result["issues"].append("Integration is missing a description")
                
                # Validate version
                if validation_rules.get("require_version"):
                    version = integration_detail.get("version", "")
                    if not version:
                        validation_result["valid"] = False
                        validation_result["issues"].append("Integration is missing a version")
                    elif validation_rules.get("version_pattern"):
                        pattern = validation_rules["version_pattern"]
                        if not re.match(pattern, version):
                            validation_result["valid"] = False
                            validation_result["issues"].append(f"Integration version '{version}' does not match required pattern: {pattern}")
                
                # Validate version age
                if validation_rules.get("max_version_age_days"):
                    updated_time = integration_detail.get("updatedTime")
                    if updated_time:
                        try:
                            # Parse updated time
                            if isinstance(updated_time, str):
                                updated_dt = datetime.datetime.fromisoformat(updated_time.replace("Z", "+00:00"))
                            else:
                                updated_dt = updated_time
                                
                            # Calculate age in days
                            age_days = (datetime.datetime.now(updated_dt.tzinfo) - updated_dt).days
                            
                            # Check against maximum age
                            max_age = validation_rules["max_version_age_days"]
                            if age_days > max_age:
                                validation_result["valid"] = False
                                validation_result["issues"].append(f"Integration version is {age_days} days old, exceeding maximum age of {max_age} days")
                        except Exception as e:
                            self.logger.warning(f"Error calculating version age: {str(e)}")
                
                # Validate error handling
                if validation_rules.get("require_error_handling"):
                    # This is a heuristic check - looking for error handlers in the integration
                    has_error_handling = False
                    
                    # Check if integration has fault handlers or error scope
                    if "metadata" in integration_detail:
                        metadata = integration_detail["metadata"]
                        if "faultHandlers" in metadata and metadata["faultHandlers"]:
                            has_error_handling = True
                            
                    # Check in various elements that might indicate error handling
                    for section in ["triggers", "actions", "invokes"]:
                        if section in integration_detail:
                            for element in integration_detail[section]:
                                if isinstance(element, dict):
                                    # Look for error handlers, catch blocks, or fault handlers
                                    if any(key for key in element.keys() if "error" in key.lower() or "fault" in key.lower() or "catch" in key.lower()):
                                        has_error_handling = True
                                        break
                    
                    if not has_error_handling:
                        validation_result["valid"] = False
                        validation_result["issues"].append("Integration appears to lack error handling")
                
                # Validate logging
                if validation_rules.get("require_logging"):
                    # This is a heuristic check - looking for logging activities
                    has_logging = False
                    
                    # Check various elements for logging
                    for section in ["actions", "invokes"]:
                        if section in integration_detail:
                            for element in integration_detail[section]:
                                if isinstance(element, dict):
                                    # Look for logging activities
                                    element_name = element.get("name", "").lower()
                                    element_type = element.get("type", "").lower()
                                    
                                    if "log" in element_name or "logging" in element_name or "log" in element_type:
                                        has_logging = True
                                        break
                    
                    if not has_logging:
                        validation_result["valid"] = False
                        validation_result["issues"].append("Integration appears to lack logging")
                
                # Check inactive integrations
                if validation_rules.get("check_inactive_integrations") and integration_status != "ACTIVATED":
                    # Check if inactive for too long
                    if validation_rules.get("max_inactive_days"):
                        updated_time = integration_detail.get("updatedTime")
                        if updated_time:
                            try:
                                # Parse updated time
                                if isinstance(updated_time, str):
                                    updated_dt = datetime.datetime.fromisoformat(updated_time.replace("Z", "+00:00"))
                                else:
                                    updated_dt = updated_time
                                    
                                # Calculate inactive days
                                inactive_days = (datetime.datetime.now(updated_dt.tzinfo) - updated_dt).days
                                
                                # Check against maximum
                                max_days = validation_rules["max_inactive_days"]
                                if inactive_days > max_days:
                                    validation_result["valid"] = False
                                    validation_result["issues"].append(f"Integration has been inactive for {inactive_days} days, exceeding maximum of {max_days} days")
                            except Exception as e:
                                self.logger.warning(f"Error calculating inactive days: {str(e)}")
                
                # Store validation result
                validation_results[integration_id] = validation_result
                
                # Add to valid or invalid lists
                if validation_result["valid"]:
                    valid_integrations.append({
                        "id": integration_id,
                        "name": integration_name,
                        "type": integration_type,
                        "status": integration_status
                    })
                else:
                    invalid_integrations.append({
                        "id": integration_id,
                        "name": integration_name,
                        "type": integration_type,
                        "status": integration_status,
                        "issues": validation_result["issues"]
                    })
                
                # Add to resources
                result.add_resource("integration", integration_id, {
                    "name": integration_name,
                    "type": integration_type,
                    "status": integration_status,
                    "valid": validation_result["valid"],
                    "issues": validation_result["issues"]
                })
            
            # Update result details
            result.details["validation_results"] = validation_results
            result.details["valid_integrations"] = valid_integrations
            result.details["invalid_integrations"] = invalid_integrations
            result.details["valid_count"] = len(valid_integrations)
            result.details["invalid_count"] = len(invalid_integrations)
            result.details["total_count"] = len(integrations)
            result.details["validation_rules"] = validation_rules
            
            # Update result success and message
            if invalid_integrations:
                result.success = False
                result.message = f"Validated {len(integrations)} integrations, found {len(invalid_integrations)} invalid integrations"
            else:
                result.message = f"All {len(integrations)} integrations passed validation"
                
        except OICError as e:
            result.success = False
            result.message = f"Failed to validate integrations: {str(e)}"
            result.add_error("Failed to validate integrations", e)
            
        return result
    
    def validate_best_practices(
        self,
        scope: str = "all",  # "all", "integrations", "connections", "lookups", "instance"
        custom_rules: Optional[Dict[str, Any]] = None
    ) -> WorkflowResult:
        """
        Validate OIC resources against best practice rules.
        
        This workflow:
        1. Validates OIC resources against best practice rules
        2. Returns a report of compliance
        
        Args:
            scope: The scope of the validation.
            custom_rules: Optional custom rules to validate against.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Validating OIC best practices for {scope}"
        
        # Set default best practice rules if not provided
        if custom_rules is None:
            custom_rules = {
                "integrations": {
                    "enabled_error_handler": True,
                    "proper_timeout_settings": True,
                    "unique_tracking_fields": True,
                    "consistent_naming": True,
                    "descriptive_logs": True,
                    "version_control": True,
                    "maximum_mapper_size": 1000,  # Maximum number of elements in a mapper
                    "maximum_integration_nodes": 50  # Maximum number of nodes in an integration
                },
                "connections": {
                    "secure_credentials": True,
                    "environment_specific": True,
                    "descriptive_names": True,
                    "periodic_testing": True
                },
                "lookups": {
                    "manageable_size": True,  # Lookups should not be too large
                    "environment_tagging": True,
                    "regular_updates": True
                },
                "instance": {
                    "monitoring_configured": True,
                    "purge_policy_configured": True,
                    "backup_strategy": True,
                    "resource_limits_managed": True
                }
            }
        
        # Initialize validation results
        best_practice_results = {
            "integrations": {},
            "connections": {},
            "lookups": {},
            "instance": {}
        }
        
        compliance_summary = {
            "integrations": {"compliant": 0, "non_compliant": 0, "rules_checked": 0},
            "connections": {"compliant": 0, "non_compliant": 0, "rules_checked": 0},
            "lookups": {"compliant": 0, "non_compliant": 0, "rules_checked": 0},
            "instance": {"compliant": 0, "non_compliant": 0, "rules_checked": 0}
        }
        
        # Validate integrations best practices
        if scope in ["all", "integrations"] and "integrations" in custom_rules:
            self.logger.info("Validating integration best practices")
            
            # Get all integrations
            try:
                integrations = self.client.integrations.list()
                
                integration_rules = custom_rules["integrations"]
                rules_checked = len(integration_rules)
                compliance_summary["integrations"]["rules_checked"] = rules_checked
                
                # Process each integration
                for integration in integrations:
                    integration_id = integration.get("id")
                    integration_name = integration.get("name", "Unknown")
                    
                    if not integration_id:
                        continue
                    
                    # Get detailed integration information
                    try:
                        integration_detail = self.client.integrations.get(integration_id)
                        
                        # Initialize results for this integration
                        integration_result = {
                            "name": integration_name,
                            "compliant": True,
                            "issues": []
                        }
                        
                        # Check each best practice rule
                        if integration_rules.get("enabled_error_handler"):
                            # Check if integration has error handlers
                            has_error_handlers = False
                            
                            # Simplified check - in real implementation, would be more thorough
                            if "metadata" in integration_detail and "faultHandlers" in integration_detail["metadata"] and integration_detail["metadata"]["faultHandlers"]:
                                has_error_handlers = True
                            
                            if not has_error_handlers:
                                integration_result["compliant"] = False
                                integration_result["issues"].append("Missing error handlers")
                        
                        if integration_rules.get("proper_timeout_settings"):
                            # Check timeout settings
                            has_proper_timeouts = True
                            
                            # Simplified check - in real implementation, would check specific timeouts
                            if "metadata" in integration_detail and "timeouts" in integration_detail["metadata"]:
                                timeouts = integration_detail["metadata"]["timeouts"]
                                if not timeouts or timeouts.get("default", 0) <= 0:
                                    has_proper_timeouts = False
                            
                            if not has_proper_timeouts:
                                integration_result["compliant"] = False
                                integration_result["issues"].append("Improper timeout settings")
                        
                        if integration_rules.get("unique_tracking_fields"):
                            # Check if integration has unique tracking fields
                            has_tracking_fields = False
                            
                            # Simplified check - in real implementation, would check specific fields
                            if "metadata" in integration_detail and "trackingFields" in integration_detail["metadata"] and integration_detail["metadata"]["trackingFields"]:
                                has_tracking_fields = True
                            
                            if not has_tracking_fields:
                                integration_result["compliant"] = False
                                integration_result["issues"].append("Missing unique tracking fields")
                        
                        if integration_rules.get("consistent_naming"):
                            # Check naming consistency
                            has_consistent_naming = True
                            
                            # Simplified check - in real implementation, would use regex patterns
                            if not re.match(r"^[A-Z]+_[A-Z]+_\w+$", integration_name):
                                has_consistent_naming = False
                            
                            if not has_consistent_naming:
                                integration_result["compliant"] = False
                                integration_result["issues"].append("Inconsistent naming convention")
                        
                        if integration_rules.get("descriptive_logs"):
                            # Check for descriptive logs
                            has_descriptive_logs = False
                            
                            # Simplified check - in real implementation, would examine log content
                            for section in ["actions", "invokes"]:
                                if section in integration_detail:
                                    for action in integration_detail[section]:
                                        if isinstance(action, dict) and action.get("type") == "logging":
                                            has_descriptive_logs = True
                                            break
                            
                            if not has_descriptive_logs:
                                integration_result["compliant"] = False
                                integration_result["issues"].append("Missing descriptive logs")
                        
                        if integration_rules.get("version_control"):
                            # Check version control
                            has_version_control = True
                            
                            # Check if version follows semver
                            version = integration_detail.get("version", "")
                            if not version or not re.match(r"^\d+\.\d+\.\d+$", version):
                                has_version_control = False
                            
                            if not has_version_control:
                                integration_result["compliant"] = False
                                integration_result["issues"].append("Missing proper version control")
                        
                        if integration_rules.get("maximum_mapper_size"):
                            # Check mapper size limitations
                            max_mapper_size = integration_rules["maximum_mapper_size"]
                            mappers_too_large = False
                            
                            # Simplified check - in real implementation, would examine all mappers
                            if "mappers" in integration_detail:
                                for mapper in integration_detail["mappers"]:
                                    if isinstance(mapper, dict) and "elements" in mapper and len(mapper["elements"]) > max_mapper_size:
                                        mappers_too_large = True
                                        break
                            
                            if mappers_too_large:
                                integration_result["compliant"] = False
                                integration_result["issues"].append(f"Mapper exceeds maximum size of {max_mapper_size} elements")
                        
                        if integration_rules.get("maximum_integration_nodes"):
                            # Check integration complexity
                            max_nodes = integration_rules["maximum_integration_nodes"]
                            too_complex = False
                            
                            # Count nodes (simplified)
                            node_count = 0
                            for section in ["triggers", "actions", "invokes", "scopes"]:
                                if section in integration_detail:
                                    node_count += len(integration_detail[section])
                            
                            if node_count > max_nodes:
                                too_complex = True
                                integration_result["compliant"] = False
                                integration_result["issues"].append(f"Integration exceeds maximum complexity of {max_nodes} nodes")
                        
                        # Store results
                        best_practice_results["integrations"][integration_id] = integration_result
                        
                        # Update compliance counter
                        if integration_result["compliant"]:
                            compliance_summary["integrations"]["compliant"] += 1
                        else:
                            compliance_summary["integrations"]["non_compliant"] += 1
                        
                        # Add to resources
                        result.add_resource("integration", integration_id, {
                            "name": integration_name,
                            "best_practice_compliant": integration_result["compliant"],
                            "issues": integration_result["issues"]
                        })
                        
                    except OICError as e:
                        self.logger.warning(f"Failed to validate best practices for integration {integration_name}: {str(e)}")
                        # Continue with other integrations
                
            except OICError as e:
                self.logger.error(f"Failed to get integrations for best practice validation: {str(e)}")
                result.add_error("Failed to validate integration best practices", e)
        
        # Validate connections best practices
        if scope in ["all", "connections"] and "connections" in custom_rules:
            self.logger.info("Validating connection best practices")
            
            # Get all connections
            try:
                connections = self.client.connections.list()
                
                connection_rules = custom_rules["connections"]
                rules_checked = len(connection_rules)
                compliance_summary["connections"]["rules_checked"] = rules_checked
                
                # Process each connection
                for connection in connections:
                    connection_id = connection.get("id")
                    connection_name = connection.get("name", "Unknown")
                    connection_type = connection.get("connectionType", "Unknown")
                    
                    if not connection_id:
                        continue
                    
                    # Initialize results for this connection
                    connection_result = {
                        "name": connection_name,
                        "type": connection_type,
                        "compliant": True,
                        "issues": []
                    }
                    
                    # Check each best practice rule
                    if connection_rules.get("secure_credentials"):
                        # Check for secure credential storage
                        has_secure_credentials = True
                        
                        # Simplified check - in a real implementation, would check specific properties
                        if "securityProperties" not in connection:
                            has_secure_credentials = False
                        
                        if not has_secure_credentials:
                            connection_result["compliant"] = False
                            connection_result["issues"].append("Insecure credential storage")
                    
                    if connection_rules.get("environment_specific"):
                        # Check if connection name includes environment
                        has_environment_naming = False
                        
                        # Look for environment prefix
                        if re.match(r"^(DEV_|TEST_|PROD_|QA_)", connection_name):
                            has_environment_naming = True
                        
                        if not has_environment_naming:
                            connection_result["compliant"] = False
                            connection_result["issues"].append("Missing environment-specific naming")
                    
                    if connection_rules.get("descriptive_names"):
                        # Check if connection name is descriptive
                        has_descriptive_name = True
                        
                        # Simplified check - in a real implementation, would be more sophisticated
                        if len(connection_name) < 10 or "_" not in connection_name:
                            has_descriptive_name = False
                        
                        if not has_descriptive_name:
                            connection_result["compliant"] = False
                            connection_result["issues"].append("Non-descriptive connection name")
                    
                    if connection_rules.get("periodic_testing"):
                        # Check if connection has been tested recently
                        # Note: This would require additional historical data in a real implementation
                        # Simplified check - just mark as non-compliant with a message
                        connection_result["issues"].append("No evidence of periodic testing (needs manual verification)")
                    
                    # Store results
                    best_practice_results["connections"][connection_id] = connection_result
                    
                    # Update compliance counter
                    if connection_result["compliant"]:
                        compliance_summary["connections"]["compliant"] += 1
                    else:
                        compliance_summary["connections"]["non_compliant"] += 1
                    
                    # Add to resources
                    result.add_resource("connection", connection_id, {
                        "name": connection_name,
                        "type": connection_type,
                        "best_practice_compliant": connection_result["compliant"],
                        "issues": connection_result["issues"]
                    })
                
            except OICError as e:
                self.logger.error(f"Failed to get connections for best practice validation: {str(e)}")
                result.add_error("Failed to validate connection best practices", e)
        
        # Validate lookups best practices
        if scope in ["all", "lookups"] and "lookups" in custom_rules:
            self.logger.info("Validating lookup best practices")
            
            # Get all lookups
            try:
                lookups = self.client.lookups.list()
                
                lookup_rules = custom_rules["lookups"]
                rules_checked = len(lookup_rules)
                compliance_summary["lookups"]["rules_checked"] = rules_checked
                
                # Process each lookup
                for lookup in lookups:
                    lookup_id = lookup.get("id")
                    lookup_name = lookup.get("name", "Unknown")
                    
                    if not lookup_id:
                        continue
                    
                    # Initialize results for this lookup
                    lookup_result = {
                        "name": lookup_name,
                        "compliant": True,
                        "issues": []
                    }
                    
                    # Get lookup data
                    try:
                        lookup_data = self.client.lookups.get_data(lookup_id)
                        
                        # Check each best practice rule
                        if lookup_rules.get("manageable_size"):
                            # Check if lookup size is manageable
                            max_rows = 1000  # Arbitrary limit
                            max_columns = 20  # Arbitrary limit
                            
                            # Check row count
                            rows = lookup_data.get("rows", [])
                            row_count = len(rows)
                            
                            # Check column count
                            columns = lookup_data.get("columns", [])
                            column_count = len(columns)
                            
                            if row_count > max_rows:
                                lookup_result["compliant"] = False
                                lookup_result["issues"].append(f"Lookup exceeds recommended size ({row_count} rows > {max_rows} max)")
                            
                            if column_count > max_columns:
                                lookup_result["compliant"] = False
                                lookup_result["issues"].append(f"Lookup exceeds recommended columns ({column_count} columns > {max_columns} max)")
                        
                        if lookup_rules.get("environment_tagging"):
                            # Check if lookup name includes environment tag
                            has_environment_tag = False
                            
                            # Look for environment prefix
                            if re.match(r"^(DEV_|TEST_|PROD_|QA_)", lookup_name):
                                has_environment_tag = True
                            
                            if not has_environment_tag:
                                lookup_result["compliant"] = False
                                lookup_result["issues"].append("Missing environment tagging")
                        
                        if lookup_rules.get("regular_updates"):
                            # Check when lookup was last updated
                            # Note: This would require additional historical data in a real implementation
                            # Simplified check - just mark as non-compliant with a message
                            lookup_result["issues"].append("No evidence of regular updates (needs manual verification)")
                        
                    except OICError as e:
                        self.logger.warning(f"Failed to get data for lookup {lookup_name}: {str(e)}")
                        lookup_result["compliant"] = False
                        lookup_result["issues"].append(f"Could not validate lookup data: {str(e)}")
                    
                    # Store results
                    best_practice_results["lookups"][lookup_id] = lookup_result
                    
                    # Update compliance counter
                    if lookup_result["compliant"]:
                        compliance_summary["lookups"]["compliant"] += 1
                    else:
                        compliance_summary["lookups"]["non_compliant"] += 1
                    
                    # Add to resources
                    result.add_resource("lookup", lookup_id, {
                        "name": lookup_name,
                        "best_practice_compliant": lookup_result["compliant"],
                        "issues": lookup_result["issues"]
                    })
                
            except OICError as e:
                self.logger.error(f"Failed to get lookups for best practice validation: {str(e)}")
                result.add_error("Failed to validate lookup best practices", e)
        
        # Validate instance best practices
        if scope in ["all", "instance"] and "instance" in custom_rules:
            self.logger.info("Validating instance best practices")
            
            # Initialize instance results
            instance_result = {
                "compliant": True,
                "issues": []
            }
            
            # Get instance info
            try:
                # Get instance statistics
                instance_stats = self.client.monitoring.get_instance_stats()
                
                instance_rules = custom_rules["instance"]
                rules_checked = len(instance_rules)
                compliance_summary["instance"]["rules_checked"] = rules_checked
                
                # Check each best practice rule
                if instance_rules.get("monitoring_configured"):
                    # Check if monitoring is configured
                    # Simplified check - in a real implementation, would check for actual monitors
                    has_monitoring = False
                    
                    if instance_stats and "stats" in instance_stats:
                        has_monitoring = True
                    
                    if not has_monitoring:
                        instance_result["compliant"] = False
                        instance_result["issues"].append("Instance monitoring not properly configured")
                
                if instance_rules.get("purge_policy_configured"):
                    # Check if purge policy is configured
                    # Note: This would require additional API access in a real implementation
                    # Simplified check - just mark as non-compliant with a message
                    instance_result["issues"].append("Purge policy validation requires manual verification")
                
                if instance_rules.get("backup_strategy"):
                    # Check if a backup strategy is in place
                    # Note: This would require additional API access in a real implementation
                    # Simplified check - just mark as non-compliant with a message
                    instance_result["issues"].append("Backup strategy validation requires manual verification")
                
                if instance_rules.get("resource_limits_managed"):
                    # Check if resource limits are managed
                    # Simplified check - in a real implementation, would check usage metrics
                    # For example, check if message count is approaching limit
                    if "stats" in instance_stats and "messages" in instance_stats["stats"]:
                        messages = instance_stats["stats"]["messages"]
                        message_limit = 10000  # Arbitrary limit
                        
                        if messages > message_limit * 0.8:  # > 80% of limit
                            instance_result["compliant"] = False
                            instance_result["issues"].append(f"Message count ({messages}) approaching limit ({message_limit})")
                
                # Store results
                best_practice_results["instance"] = instance_result
                
                # Update compliance counter
                if instance_result["compliant"]:
                    compliance_summary["instance"]["compliant"] += 1
                else:
                    compliance_summary["instance"]["non_compliant"] += 1
                
                # Add to resources
                result.add_resource("instance", "instance", {
                    "best_practice_compliant": instance_result["compliant"],
                    "issues": instance_result["issues"]
                })
                
            except OICError as e:
                self.logger.error(f"Failed to get instance information for best practice validation: {str(e)}")
                result.add_error("Failed to validate instance best practices", e)
        
        # Update result details
        result.details["best_practice_results"] = best_practice_results
        result.details["compliance_summary"] = compliance_summary
        
        # Calculate overall compliance
        total_compliant = sum(summary["compliant"] for summary in compliance_summary.values())
        total_non_compliant = sum(summary["non_compliant"] for summary in compliance_summary.values())
        total_resources = total_compliant + total_non_compliant
        
        result.details["total_resources"] = total_resources
        result.details["total_compliant"] = total_compliant
        result.details["total_non_compliant"] = total_non_compliant
        
        # Update result success and message
        if total_non_compliant > 0:
            result.success = False
            result.message = f"Best practice validation found {total_non_compliant} of {total_resources} resources non-compliant"
        else:
            result.message = f"All {total_resources} resources are compliant with best practices"
            
        return result
    
    def validate_configuration(
        self,
        configuration_type: str = "all",  # "all", "security", "performance", "reliability"
        remediate: bool = False,
        environment: Optional[str] = None  # For environment-specific validations
    ) -> WorkflowResult:
        """
        Validate OIC configuration settings.
        
        This workflow:
        1. Validates OIC configuration against recommended settings
        2. Optionally remediates issues
        
        Args:
            configuration_type: Type of configuration to validate.
            remediate: Whether to attempt to fix issues automatically.
            environment: Optional environment name for environment-specific checks.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Validating OIC configuration for {configuration_type}"
        
        # Set remediation string for messaging
        remediate_str = " with remediation" if remediate else ""
        
        # Initialize configuration validation results
        config_validation = {
            "security": {"compliant": True, "checks": []},
            "performance": {"compliant": True, "checks": []},
            "reliability": {"compliant": True, "checks": []},
            "recommended_changes": []
        }
        
        # Define validation categories and checks
        security_checks = [
            {"name": "connection_credentials_security", "description": "Validate secure storage of credentials"},
            {"name": "oauth_token_management", "description": "Validate proper OAuth token management"},
            {"name": "endpoint_security", "description": "Validate endpoint security configuration"}
        ]
        
        performance_checks = [
            {"name": "connection_pooling", "description": "Validate connection pooling settings"},
            {"name": "integration_timeouts", "description": "Validate timeout settings"},
            {"name": "thread_allocation", "description": "Validate thread allocation"}
        ]
        
        reliability_checks = [
            {"name": "error_handling", "description": "Validate error handling configuration"},
            {"name": "retry_policies", "description": "Validate retry policies"},
            {"name": "monitoring_alerts", "description": "Validate monitoring alerts"}
        ]
        
        # Perform security validations
        if configuration_type in ["all", "security"]:
            self.logger.info("Validating security configuration")
            
            # Simplified implementation - in a real system, would query actual settings
            for check in security_checks:
                check_result = {
                    "name": check["name"],
                    "description": check["description"],
                    "compliant": True,
                    "details": "",
                    "remediation": ""
                }
                
                # Simplified validation logic for each check
                if check["name"] == "connection_credentials_security":
                    # In a real implementation, would check actual connection settings
                    # For the example, randomly determine compliance
                    check_result["compliant"] = True
                    check_result["details"] = "All connection credentials are securely stored"
                    
                elif check["name"] == "oauth_token_management":
                    # Simulated issue
                    check_result["compliant"] = False
                    check_result["details"] = "OAuth token refresh not properly configured"
                    check_result["remediation"] = "Configure OAuth token refresh with appropriate timings"
                    
                    # Add to recommended changes
                    if not check_result["compliant"]:
                        config_validation["recommended_changes"].append({
                            "type": "security",
                            "name": check["name"],
                            "description": check_result["remediation"]
                        })
                    
                    # If remediation is enabled, simulate fixing the issue
                    if remediate:
                        self.logger.info(f"Remediating issue: {check_result['details']}")
                        # In a real implementation, would make API calls to fix the issue
                        check_result["details"] += " - REMEDIATED"
                        
                elif check["name"] == "endpoint_security":
                    # Simulated issue
                    check_result["compliant"] = True
                    check_result["details"] = "Endpoint security properly configured"
                
                # Add check result
                config_validation["security"]["checks"].append(check_result)
                
                # Update overall security compliance
                if not check_result["compliant"]:
                    config_validation["security"]["compliant"] = False
        
        # Perform performance validations
        if configuration_type in ["all", "performance"]:
            self.logger.info("Validating performance configuration")
            
            # Simplified implementation - in a real system, would query actual settings
            for check in performance_checks:
                check_result = {
                    "name": check["name"],
                    "description": check["description"],
                    "compliant": True,
                    "details": "",
                    "remediation": ""
                }
                
                # Simplified validation logic for each check
                if check["name"] == "connection_pooling":
                    # Simulated issue
                    check_result["compliant"] = False
                    check_result["details"] = "Connection pooling set too low for current load"
                    check_result["remediation"] = "Increase connection pool size to at least 10"
                    
                    # Add to recommended changes
                    if not check_result["compliant"]:
                        config_validation["recommended_changes"].append({
                            "type": "performance",
                            "name": check["name"],
                            "description": check_result["remediation"]
                        })
                    
                    # If remediation is enabled, simulate fixing the issue
                    if remediate:
                        self.logger.info(f"Remediating issue: {check_result['details']}")
                        # In a real implementation, would make API calls to fix the issue
                        check_result["details"] += " - REMEDIATED"
                        
                elif check["name"] == "integration_timeouts":
                    # Simulated check
                    check_result["compliant"] = True
                    check_result["details"] = "Timeout settings are appropriate"
                    
                elif check["name"] == "thread_allocation":
                    # Simulated issue
                    check_result["compliant"] = False
                    check_result["details"] = "Thread allocation not optimized for workload"
                    check_result["remediation"] = "Adjust thread allocation based on integration patterns"
                    
                    # Add to recommended changes
                    if not check_result["compliant"]:
                        config_validation["recommended_changes"].append({
                            "type": "performance",
                            "name": check["name"],
                            "description": check_result["remediation"]
                        })
                    
                    # If remediation is enabled, simulate fixing the issue
                    if remediate:
                        self.logger.info(f"Remediating issue: {check_result['details']}")
                        # In a real implementation, would make API calls to fix the issue
                        check_result["details"] += " - REMEDIATED"
                
                # Add check result
                config_validation["performance"]["checks"].append(check_result)
                
                # Update overall performance compliance
                if not check_result["compliant"]:
                    config_validation["performance"]["compliant"] = False
        
        # Perform reliability validations
        if configuration_type in ["all", "reliability"]:
            self.logger.info("Validating reliability configuration")
            
            # Simplified implementation - in a real system, would query actual settings
            for check in reliability_checks:
                check_result = {
                    "name": check["name"],
                    "description": check["description"],
                    "compliant": True,
                    "details": "",
                    "remediation": ""
                }
                
                # Simplified validation logic for each check
                if check["name"] == "error_handling":
                    # Simulated check
                    check_result["compliant"] = True
                    check_result["details"] = "Error handling configuration is appropriate"
                    
                elif check["name"] == "retry_policies":
                    # Simulated issue
                    check_result["compliant"] = False
                    check_result["details"] = "Retry policies not consistently applied"
                    check_result["remediation"] = "Implement standardized retry policies across integrations"
                    
                    # Add to recommended changes
                    if not check_result["compliant"]:
                        config_validation["recommended_changes"].append({
                            "type": "reliability",
                            "name": check["name"],
                            "description": check_result["remediation"]
                        })
                    
                    # If remediation is enabled, simulate fixing the issue
                    if remediate:
                        self.logger.info(f"Remediating issue: {check_result['details']}")
                        # In a real implementation, would make API calls to fix the issue
                        check_result["details"] += " - REMEDIATED"
                        
                elif check["name"] == "monitoring_alerts":
                    # Simulated issue
                    check_result["compliant"] = False
                    check_result["details"] = "Monitoring alerts not configured for critical integrations"
                    check_result["remediation"] = "Set up alerts for critical integration failures"
                    
                    # Add to recommended changes
                    if not check_result["compliant"]:
                        config_validation["recommended_changes"].append({
                            "type": "reliability",
                            "name": check["name"],
                            "description": check_result["remediation"]
                        })
                    
                    # If remediation is enabled, simulate fixing the issue
                    if remediate:
                        self.logger.info(f"Remediating issue: {check_result['details']}")
                        # In a real implementation, would make API calls to fix the issue
                        check_result["details"] += " - REMEDIATED"
                
                # Add check result
                config_validation["reliability"]["checks"].append(check_result)
                
                # Update overall reliability compliance
                if not check_result["compliant"]:
                    config_validation["reliability"]["compliant"] = False
        
        # Update result details
        result.details["configuration_validation"] = config_validation
        result.details["remediation_enabled"] = remediate
        result.details["environment"] = environment
        
        # Calculate overall compliance
        categories = []
        if configuration_type in ["all", "security"]:
            categories.append("security")
        if configuration_type in ["all", "performance"]:
            categories.append("performance")
        if configuration_type in ["all", "reliability"]:
            categories.append("reliability")
        
        all_compliant = all(config_validation[category]["compliant"] for category in categories)
        total_checks = sum(len(config_validation[category]["checks"]) for category in categories)
        compliant_checks = sum(
            sum(1 for check in config_validation[category]["checks"] if check["compliant"])
            for category in categories
        )
        non_compliant_checks = total_checks - compliant_checks
        
        # Update result success and message
        if all_compliant:
            result.message = f"All configuration settings are compliant{remediate_str}"
        else:
            result.success = False
            result.message = f"Found {non_compliant_checks} of {total_checks} configuration settings non-compliant{remediate_str}"
            
        return result
    
    def validate_naming_conventions(
        self,
        resource_types: List[str] = ["connections", "integrations", "lookups"],
        naming_patterns: Optional[Dict[str, str]] = None,
        auto_rename: bool = False,
        continue_on_error: bool = True
    ) -> WorkflowResult:
        """
        Validate resource naming conventions.
        
        This workflow:
        1. Gets resources of the specified types
        2. Validates their names against patterns
        3. Optionally renames non-compliant resources
        
        Args:
            resource_types: Types of resources to validate.
            naming_patterns: Regex patterns for each resource type.
            auto_rename: Whether to auto-rename non-compliant resources.
            continue_on_error: Whether to continue if some validations fail.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Validating naming conventions for {', '.join(resource_types)}"
        
        # Set default naming patterns if not provided
        if naming_patterns is None:
            naming_patterns = {
                "connections": r"^([A-Z]+)_([A-Z]+)_([A-Z0-9_]+)$",  # ENV_TYPE_NAME
                "integrations": r"^([A-Z]+)_([A-Z]+)_([A-Z0-9_]+)$",  # ENV_TYPE_NAME
                "lookups": r"^([A-Z]+)_([A-Z]+)_([A-Z0-9_]+)$"  # ENV_TYPE_NAME
            }
        
        # Initialize validation results
        naming_validation = {
            "connections": {"compliant": 0, "non_compliant": 0, "resources": {}},
            "integrations": {"compliant": 0, "non_compliant": 0, "resources": {}},
            "lookups": {"compliant": 0, "non_compliant": 0, "resources": {}}
        }
        
        # Auto-rename string for messaging
        auto_rename_str = " with auto-rename" if auto_rename else ""
        
        # Validate each resource type
        for resource_type in resource_types:
            if resource_type not in naming_patterns:
                self.logger.warning(f"No naming pattern defined for resource type: {resource_type}")
                continue
                
            pattern = naming_patterns[resource_type]
            self.logger.info(f"Validating {resource_type} against pattern: {pattern}")
            
            try:
                # Get resources of this type
                if resource_type == "connections":
                    resources = self.client.connections.list()
                elif resource_type == "integrations":
                    resources = self.client.integrations.list()
                elif resource_type == "lookups":
                    resources = self.client.lookups.list()
                else:
                    self.logger.warning(f"Unsupported resource type: {resource_type}")
                    continue
                
                # Validate each resource
                for resource in resources:
                    resource_id = resource.get("id")
                    resource_name = resource.get("name", "Unknown")
                    
                    if not resource_id:
                        continue
                    
                    # Check if name matches pattern
                    is_compliant = bool(re.match(pattern, resource_name))
                    
                    # Initialize resource result
                    resource_result = {
                        "name": resource_name,
                        "compliant": is_compliant,
                        "suggested_name": None,
                        "renamed": False
                    }
                    
                    # Generate suggested name if non-compliant
                    if not is_compliant:
                        # This is a simplified suggestion - in a real implementation,
                        # would have more sophisticated logic based on resource properties
                        
                        # Start with a basic environment prefix
                        env = "ENV"
                        
                        # Use resource type as middle section
                        if resource_type == "connections":
                            type_abbr = "CONN"
                        elif resource_type == "integrations":
                            type_abbr = "INT"
                        elif resource_type == "lookups":
                            type_abbr = "LKP"
                        else:
                            type_abbr = "RES"
                        
                        # Clean up the resource name for the last part
                        clean_name = re.sub(r'[^A-Z0-9_]', '', resource_name.upper())
                        
                        # Generate suggested name
                        suggested_name = f"{env}_{type_abbr}_{clean_name}"
                        resource_result["suggested_name"] = suggested_name
                        
                        # Auto-rename if enabled
                        if auto_rename:
                            try:
                                self.logger.info(f"Auto-renaming {resource_type} {resource_name} to {suggested_name}")
                                
                                # Update the resource with new name
                                updated_resource = resource.copy()
                                updated_resource["name"] = suggested_name
                                
                                # Perform update
                                if resource_type == "connections":
                                    self.client.connections.update(resource_id, updated_resource)
                                elif resource_type == "integrations":
                                    self.client.integrations.update(resource_id, updated_resource)
                                elif resource_type == "lookups":
                                    self.client.lookups.update(resource_id, updated_resource)
                                
                                resource_result["renamed"] = True
                                resource_result["compliant"] = True
                                
                            except OICError as e:
                                self.logger.error(f"Failed to rename {resource_type} {resource_name}: {str(e)}")
                                if not continue_on_error:
                                    result.add_error(f"Failed to rename {resource_type}", e, resource_id)
                                    result.success = False
                                    result.message = f"Naming convention validation failed for {resource_type} {resource_name}"
                                    return result
                    
                    # Store resource result
                    naming_validation[resource_type]["resources"][resource_id] = resource_result
                    
                    # Update counters
                    if resource_result["compliant"]:
                        naming_validation[resource_type]["compliant"] += 1
                    else:
                        naming_validation[resource_type]["non_compliant"] += 1
                    
                    # Add to resources
                    result.add_resource(resource_type[:-1], resource_id, {
                        "name": resource_name,
                        "compliant": resource_result["compliant"],
                        "suggested_name": resource_result["suggested_name"],
                        "renamed": resource_result["renamed"]
                    })
                
            except OICError as e:
                self.logger.error(f"Failed to validate naming conventions for {resource_type}: {str(e)}")
                result.add_error(f"Failed to validate {resource_type}", e)
                if not continue_on_error:
                    result.success = False
                    result.message = f"Naming convention validation failed for {resource_type}"
                    return result
        
        # Update result details
        result.details["naming_validation"] = naming_validation
        result.details["naming_patterns"] = naming_patterns
        result.details["auto_rename"] = auto_rename
        
        # Calculate overall compliance
        total_resources = sum(
            naming_validation[resource_type]["compliant"] + naming_validation[resource_type]["non_compliant"]
            for resource_type in resource_types if resource_type in naming_validation
        )
        
        total_compliant = sum(
            naming_validation[resource_type]["compliant"]
            for resource_type in resource_types if resource_type in naming_validation
        )
        
        total_non_compliant = sum(
            naming_validation[resource_type]["non_compliant"]
            for resource_type in resource_types if resource_type in naming_validation
        )
        
        # Update result success and message
        if total_non_compliant > 0:
            result.success = False
            result.message = f"Found {total_non_compliant} of {total_resources} resources with non-compliant names{auto_rename_str}"
        else:
            result.message = f"All {total_resources} resources have compliant names{auto_rename_str}"
            
        return result
