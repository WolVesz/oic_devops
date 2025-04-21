"""
Schedule workflows module for the OIC DevOps package.

This module provides workflow operations for managing scheduled integrations,
including batch schedule updates, schedule imports/exports, and schedule validations.
"""

import os
import logging
import time
import datetime
import json
import csv
from typing import Dict, Any, List, Optional, Union, Tuple

from oic_devops.client import OICClient
from oic_devops.exceptions import OICError
from oic_devops.workflows.base import BaseWorkflow, WorkflowResult


class ScheduleWorkflows(BaseWorkflow):
    """
    Workflow operations for managing integration schedules.
    
    This class provides higher-level operations for working with integration schedules,
    such as batch updates, schedule tracking, and schedule imports/exports.
    """
    
    def execute(self, *args, **kwargs) -> WorkflowResult:
        """
        Execute the specified schedule workflow.
        
        This is a dispatcher method that calls the appropriate workflow
        based on the operation argument.
        
        Args:
            operation: The workflow operation to execute.
            **kwargs: Additional arguments specific to the workflow.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        operation = kwargs.pop("operation", None)
        
        if operation == "update_schedules":
            return self.update_integration_schedules(**kwargs)
        elif operation == "export_schedules":
            return self.export_integration_schedules(**kwargs)
        elif operation == "import_schedules":
            return self.import_integration_schedules(**kwargs)
        elif operation == "validate_schedules":
            return self.validate_integration_schedules(**kwargs)
        elif operation == "list_schedules":
            return self.list_integration_schedules(**kwargs)
        else:
            result = WorkflowResult(success=False, message=f"Unknown schedule workflow operation: {operation}")
            result.add_error(f"Unknown operation: {operation}")
            return result
    
    def update_integration_schedules(
        self,
        schedule_updates: Dict[str, Dict[str, Any]],
        by_identifier: bool = False,
        continue_on_error: bool = True
    ) -> WorkflowResult:
        """
        Update schedules for multiple integrations.
        
        This workflow:
        1. Gets the integrations to update
        2. Updates the schedule for each integration
        
        Args:
            schedule_updates: Dict mapping integration IDs/identifiers to schedule updates.
            by_identifier: Whether to use integration identifiers instead of IDs.
            continue_on_error: Whether to continue if some updates fail.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Updating schedules for {len(schedule_updates)} integrations"
        
        # Validate schedule_updates
        if not schedule_updates:
            result.message = "No schedule updates provided"
            return result
        
        # Initialize counters
        successful_updates = []
        failed_updates = []
        
        # Process each integration
        for int_key, schedule_data in schedule_updates.items():
            # Get the integration ID if using identifiers
            if by_identifier:
                try:
                    # Find integration by identifier
                    params = {
                        "q": f"identifier:{int_key}",
                        "integrationType": "SCHEDULED"  # Only interested in scheduled integrations
                    }
                    matching_integrations = self.client.integrations.list(params=params)
                    
                    if not matching_integrations:
                        self.logger.error(f"No integration found with identifier {int_key}")
                        error_msg = f"Integration not found with identifier {int_key}"
                        failed_updates.append({
                            "identifier": int_key,
                            "error": error_msg
                        })
                        result.add_error(error_msg)
                        
                        if not continue_on_error:
                            result.success = False
                            result.message = error_msg
                            break
                        else:
                            continue
                    
                    # Use the first matching integration
                    integration_id = matching_integrations[0]["id"]
                    integration_name = matching_integrations[0]["name"]
                    
                except OICError as e:
                    self.logger.error(f"Failed to find integration with identifier {int_key}: {str(e)}")
                    error_msg = f"Failed to find integration: {str(e)}"
                    failed_updates.append({
                        "identifier": int_key,
                        "error": error_msg
                    })
                    result.add_error(error_msg, e)
                    
                    if not continue_on_error:
                        result.success = False
                        result.message = error_msg
                        break
                    else:
                        continue
            else:
                # Using integration ID directly
                integration_id = int_key
                integration_name = None  # Will get from the integration details
            
            try:
                # Get integration details
                integration = self.client.integrations.get(integration_id)
                
                # Get integration name if not already known
                if not integration_name:
                    integration_name = integration.get("name", "Unknown")
                
                # Verify this is a scheduled integration
                if integration.get("integrationType") != "SCHEDULED":
                    self.logger.error(f"Integration {integration_name} is not a scheduled integration")
                    error_msg = f"Integration {integration_name} is not a scheduled integration"
                    failed_updates.append({
                        "id": integration_id,
                        "name": integration_name,
                        "error": error_msg
                    })
                    result.add_error(error_msg, resource_id=integration_id)
                    
                    if not continue_on_error:
                        result.success = False
                        result.message = error_msg
                        break
                    else:
                        continue
                
                # Ensure schedule exists
                if "schedule" not in integration:
                    self.logger.error(f"Integration {integration_name} has no schedule field")
                    error_msg = f"Integration {integration_name} has no schedule field"
                    failed_updates.append({
                        "id": integration_id,
                        "name": integration_name,
                        "error": error_msg
                    })
                    result.add_error(error_msg, resource_id=integration_id)
                    
                    if not continue_on_error:
                        result.success = False
                        result.message = error_msg
                        break
                    else:
                        continue
                
                # Get current schedule
                current_schedule = integration["schedule"]
                
                # Create a copy for update
                updated_schedule = current_schedule.copy()
                
                # Update schedule with provided data
                updated_schedule.update(schedule_data)
                
                # Update the integration's schedule
                integration["schedule"] = updated_schedule
                
                # Update the integration
                self.logger.info(f"Updating schedule for integration {integration_name}")
                self.client.integrations.update(integration_id, integration)
                
                # Get the updated integration to verify changes
                updated_integration = self.client.integrations.get(integration_id)
                updated_schedule_verification = updated_integration.get("schedule", {})
                
                # Record successful update
                successful_updates.append({
                    "id": integration_id,
                    "name": integration_name,
                    "previous_schedule": current_schedule,
                    "updated_schedule": updated_schedule_verification
                })
                
                # Add to resources
                result.add_resource("integration", integration_id, {
                    "name": integration_name,
                    "schedule_updated": True,
                    "schedule": updated_schedule_verification
                })
                
            except OICError as e:
                self.logger.error(f"Failed to update schedule for integration {integration_id}: {str(e)}")
                error_msg = f"Failed to update schedule: {str(e)}"
                failed_updates.append({
                    "id": integration_id,
                    "name": integration_name if integration_name else "Unknown",
                    "error": error_msg
                })
                result.add_error(error_msg, e, integration_id)
                
                if not continue_on_error:
                    result.success = False
                    result.message = error_msg
                    break
        
        # Update result details
        result.details["successful_updates"] = successful_updates
        result.details["failed_updates"] = failed_updates
        result.details["successful_count"] = len(successful_updates)
        result.details["failed_count"] = len(failed_updates)
        
        # Update result message
        if failed_updates:
            result.success = False
            result.message = f"Updated schedules for {len(successful_updates)} integrations, {len(failed_updates)} failed"
        else:
            result.message = f"Successfully updated schedules for {len(successful_updates)} integrations"
        
        return result
    
    def export_integration_schedules(
        self,
        integration_ids: Optional[List[str]] = None,
        filter_query: Optional[str] = None,
        export_file_path: str = "integration_schedules.json",
        format: str = "json"  # "json", "csv", "yaml"
    ) -> WorkflowResult:
        """
        Export schedules for integrations.
        
        This workflow:
        1. Gets the schedules for specified integrations
        2. Exports them to a file in the specified format
        
        Args:
            integration_ids: Optional list of integration IDs to export schedules for.
            filter_query: Optional query to filter integrations.
            export_file_path: Path to save the exported schedules.
            format: Format for the export file.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = "Exporting integration schedules"
        
        # Ensure directory exists
        export_dir = os.path.dirname(export_file_path)
        if export_dir and not os.path.exists(export_dir):
            try:
                os.makedirs(export_dir)
            except Exception as e:
                result.success = False
                result.message = f"Failed to create export directory {export_dir}"
                result.add_error("Failed to create export directory", e)
                return result
        
        # Get integrations by ID or filter
        try:
            if integration_ids:
                # Get integrations by ID
                integrations = []
                for integration_id in integration_ids:
                    try:
                        integration = self.client.integrations.get(integration_id)
                        
                        # Only include scheduled integrations
                        if integration.get("integrationType") == "SCHEDULED":
                            integrations.append(integration)
                        else:
                            self.logger.info(f"Integration {integration.get('name', integration_id)} is not a scheduled integration, skipping")
                    except OICError as e:
                        self.logger.warning(f"Failed to get integration {integration_id}: {str(e)}")
                        # Continue with other integrations
            else:
                # Get integrations by filter
                params = {
                    "integrationType": "SCHEDULED"  # Only get scheduled integrations
                }
                if filter_query:
                    params["q"] = filter_query
                
                integrations = self.client.integrations.list(params=params)
            
            if not integrations:
                result.message = "No scheduled integrations found to export"
                return result
                
            # Extract schedule data
            schedules_data = {}
            
            for integration in integrations:
                integration_id = integration.get("id")
                integration_name = integration.get("name", "Unknown")
                integration_identifier = integration.get("identifier", "Unknown")
                
                if not integration_id:
                    continue
                
                # Check if integration has a schedule
                if "schedule" not in integration:
                    self.logger.warning(f"Integration {integration_name} has no schedule field, skipping")
                    continue
                
                schedule = integration["schedule"]
                
                # Store schedule data with metadata
                schedules_data[integration_id] = {
                    "name": integration_name,
                    "identifier": integration_identifier,
                    "schedule": schedule
                }
                
                # Add to resources
                result.add_resource("integration", integration_id, {
                    "name": integration_name,
                    "identifier": integration_identifier,
                    "has_schedule": True
                })
            
            # Export schedules in the specified format
            if format == "json":
                try:
                    with open(export_file_path, 'w') as f:
                        json.dump(schedules_data, f, indent=2)
                        
                    self.logger.info(f"Exported {len(schedules_data)} schedules to {export_file_path}")
                    result.details["export_file"] = export_file_path
                    result.details["exported_count"] = len(schedules_data)
                    
                except Exception as e:
                    result.success = False
                    result.message = f"Failed to write schedules to JSON file: {str(e)}"
                    result.add_error("Failed to write JSON file", e)
                    return result
                    
            elif format == "csv":
                try:
                    # Create CSV with essential schedule fields
                    with open(export_file_path, 'w', newline='') as f:
                        writer = csv.writer(f)
                        
                        # Write header with common schedule fields
                        writer.writerow([
                            "Integration ID", "Name", "Identifier", "Enabled", 
                            "Frequency", "Start Date", "Time", "End Date", 
                            "Days", "Hours", "Minutes"
                        ])
                        
                        # Write data for each integration
                        for integration_id, data in schedules_data.items():
                            schedule = data["schedule"]
                            
                            # Extract common schedule fields with defaults
                            enabled = schedule.get("enabled", False)
                            frequency = schedule.get("frequency", "")
                            start_date = schedule.get("startDate", "")
                            time = schedule.get("time", "")
                            end_date = schedule.get("endDate", "")
                            
                            # Handle recurring schedule parameters
                            days = ""
                            hours = ""
                            minutes = ""
                            
                            if "recurringSchedule" in schedule:
                                recurring = schedule["recurringSchedule"]
                                if "dayOfWeek" in recurring:
                                    days = ",".join(recurring["dayOfWeek"])
                                if "hour" in recurring:
                                    hours = recurring["hour"]
                                if "minute" in recurring:
                                    minutes = recurring["minute"]
                            
                            # Write row
                            writer.writerow([
                                integration_id,
                                data["name"],
                                data["identifier"],
                                enabled,
                                frequency,
                                start_date,
                                time,
                                end_date,
                                days,
                                hours,
                                minutes
                            ])
                    
                    self.logger.info(f"Exported {len(schedules_data)} schedules to {export_file_path}")
                    result.details["export_file"] = export_file_path
                    result.details["exported_count"] = len(schedules_data)
                    
                except Exception as e:
                    result.success = False
                    result.message = f"Failed to write schedules to CSV file: {str(e)}"
                    result.add_error("Failed to write CSV file", e)
                    return result
                    
            elif format == "yaml":
                try:
                    import yaml
                    with open(export_file_path, 'w') as f:
                        yaml.dump(schedules_data, f, default_flow_style=False)
                        
                    self.logger.info(f"Exported {len(schedules_data)} schedules to {export_file_path}")
                    result.details["export_file"] = export_file_path
                    result.details["exported_count"] = len(schedules_data)
                    
                except Exception as e:
                    result.success = False
                    result.message = f"Failed to write schedules to YAML file: {str(e)}"
                    result.add_error("Failed to write YAML file", e)
                    return result
            else:
                result.success = False
                result.message = f"Unsupported export format: {format}"
                result.add_error(f"Unsupported export format: {format}")
                return result
            
            # Update result message
            result.message = f"Successfully exported schedules for {len(schedules_data)} integrations to {export_file_path}"
                
        except OICError as e:
            result.success = False
            result.message = f"Failed to export schedules: {str(e)}"
            result.add_error("Failed to export schedules", e)
            
        return result
    
    def import_integration_schedules(
        self,
        import_file_path: str,
        match_by: str = "id",  # "id", "identifier", "name"
        continue_on_error: bool = True,
        dry_run: bool = False
    ) -> WorkflowResult:
        """
        Import schedules for integrations.
        
        This workflow:
        1. Loads schedules from a file
        2. Updates integration schedules in OIC
        
        Args:
            import_file_path: Path to the file with schedule data.
            match_by: How to match integrations in the import file.
            continue_on_error: Whether to continue if some imports fail.
            dry_run: Whether to perform a dry run (no actual updates).
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        
        dry_run_str = " (DRY RUN)" if dry_run else ""
        result.message = f"Importing integration schedules{dry_run_str}"
        
        # Check if import file exists
        if not os.path.exists(import_file_path):
            result.success = False
            result.message = f"Import file {import_file_path} does not exist"
            result.add_error("Import file does not exist")
            return result
        
        # Determine file format and load data
        try:
            # Import file format based on extension
            file_ext = os.path.splitext(import_file_path)[1].lower()
            
            if file_ext == ".json":
                with open(import_file_path, 'r') as f:
                    schedules_data = json.load(f)
                    
            elif file_ext == ".csv":
                # Parse CSV file into dictionary
                schedules_data = {}
                with open(import_file_path, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Get integration identifier
                        integration_id = row.get("Integration ID", "")
                        
                        if not integration_id:
                            continue
                        
                        # Convert CSV fields to schedule object
                        schedule = {
                            "enabled": row.get("Enabled", "").lower() == "true",
                            "frequency": row.get("Frequency", ""),
                            "startDate": row.get("Start Date", ""),
                            "time": row.get("Time", ""),
                        }
                        
                        # Add end date if provided
                        if row.get("End Date"):
                            schedule["endDate"] = row["End Date"]
                        
                        # Add recurring schedule if applicable
                        days = row.get("Days", "")
                        hours = row.get("Hours", "")
                        minutes = row.get("Minutes", "")
                        
                        if days or hours or minutes:
                            schedule["recurringSchedule"] = {}
                            if days:
                                schedule["recurringSchedule"]["dayOfWeek"] = days.split(",")
                            if hours:
                                schedule["recurringSchedule"]["hour"] = hours
                            if minutes:
                                schedule["recurringSchedule"]["minute"] = minutes
                        
                        # Add to schedules data
                        schedules_data[integration_id] = {
                            "name": row.get("Name", ""),
                            "identifier": row.get("Identifier", ""),
                            "schedule": schedule
                        }
                        
            elif file_ext == ".yaml" or file_ext == ".yml":
                import yaml
                with open(import_file_path, 'r') as f:
                    schedules_data = yaml.safe_load(f)
            else:
                result.success = False
                result.message = f"Unsupported import file format: {file_ext}"
                result.add_error(f"Unsupported file format: {file_ext}")
                return result
            
            # Validate schedules data
            if not isinstance(schedules_data, dict):
                result.success = False
                result.message = "Invalid import file format: root must be a dictionary"
                result.add_error("Invalid import file format")
                return result
                
            if not schedules_data:
                result.message = "No schedule data found in import file"
                return result
                
            # Process each schedule
            successful_imports = []
            failed_imports = []
            
            for src_key, data in schedules_data.items():
                # Get schedule data
                if not isinstance(data, dict) or "schedule" not in data:
                    self.logger.warning(f"Invalid schedule data for key {src_key}, skipping")
                    failed_imports.append({
                        "key": src_key,
                        "error": "Invalid schedule data format"
                    })
                    continue
                
                schedule = data["schedule"]
                integration_name = data.get("name", "Unknown")
                integration_identifier = data.get("identifier", "Unknown")
                
                # Find the target integration based on match_by
                try:
                    if match_by == "id":
                        # Use the key from import file as integration ID
                        integration_id = src_key
                        target_integration = None
                        
                        try:
                            target_integration = self.client.integrations.get(integration_id)
                        except OICError:
                            target_integration = None
                            
                    elif match_by == "identifier":
                        # Find integration by identifier
                        params = {
                            "q": f"identifier:{integration_identifier}",
                            "integrationType": "SCHEDULED"
                        }
                        matching_integrations = self.client.integrations.list(params=params)
                        target_integration = matching_integrations[0] if matching_integrations else None
                        integration_id = target_integration["id"] if target_integration else None
                        
                    elif match_by == "name":
                        # Find integration by name
                        params = {
                            "q": f"name:{integration_name}",
                            "integrationType": "SCHEDULED"
                        }
                        matching_integrations = self.client.integrations.list(params=params)
                        target_integration = matching_integrations[0] if matching_integrations else None
                        integration_id = target_integration["id"] if target_integration else None
                        
                    else:
                        self.logger.error(f"Invalid match_by value: {match_by}")
                        failed_imports.append({
                            "key": src_key,
                            "error": f"Invalid match_by value: {match_by}"
                        })
                        
                        if not continue_on_error:
                            result.success = False
                            result.message = f"Invalid match_by value: {match_by}"
                            break
                        else:
                            continue
                    
                    # Check if integration was found
                    if not target_integration:
                        error_msg = f"Integration not found using {match_by}: {src_key}"
                        self.logger.error(error_msg)
                        failed_imports.append({
                            "key": src_key,
                            "error": error_msg
                        })
                        
                        if not continue_on_error:
                            result.success = False
                            result.message = error_msg
                            break
                        else:
                            continue
                    
                    # Verify this is a scheduled integration
                    if target_integration.get("integrationType") != "SCHEDULED":
                        error_msg = f"Integration {integration_name} is not a scheduled integration"
                        self.logger.error(error_msg)
                        failed_imports.append({
                            "key": src_key,
                            "error": error_msg
                        })
                        
                        if not continue_on_error:
                            result.success = False
                            result.message = error_msg
                            break
                        else:
                            continue
                    
                    # Skip update if dry run
                    if dry_run:
                        self.logger.info(f"DRY RUN: Would update schedule for integration {integration_name}")
                        successful_imports.append({
                            "id": integration_id,
                            "name": integration_name,
                            "schedule": schedule
                        })
                        
                        # Add to resources
                        result.add_resource("integration", integration_id, {
                            "name": integration_name,
                            "dry_run": True,
                            "schedule": schedule
                        })
                        
                        continue
                    
                    # Update the integration schedule
                    integration_data = target_integration.copy()
                    integration_data["schedule"] = schedule
                    
                    self.logger.info(f"Updating schedule for integration {integration_name}")
                    self.client.integrations.update(integration_id, integration_data)
                    
                    # Record successful import
                    successful_imports.append({
                        "id": integration_id,
                        "name": integration_name,
                        "schedule": schedule
                    })
                    
                    # Add to resources
                    result.add_resource("integration", integration_id, {
                        "name": integration_name,
                        "schedule_updated": True,
                        "schedule": schedule
                    })
                    
                except OICError as e:
                    error_msg = f"Failed to update schedule: {str(e)}"
                    self.logger.error(f"Error updating schedule for {src_key}: {str(e)}")
                    failed_imports.append({
                        "key": src_key,
                        "error": error_msg
                    })
                    result.add_error(error_msg, e, integration_id if 'integration_id' in locals() else None)
                    
                    if not continue_on_error:
                        result.success = False
                        result.message = error_msg
                        break
            
            # Update result details
            result.details["successful_imports"] = successful_imports
            result.details["failed_imports"] = failed_imports
            result.details["successful_count"] = len(successful_imports)
            result.details["failed_count"] = len(failed_imports)
            result.details["dry_run"] = dry_run
            
            # Update result message
            if failed_imports:
                result.success = False
                result.message = f"Imported schedules for {len(successful_imports)} integrations, {len(failed_imports)} failed{dry_run_str}"
            else:
                result.message = f"Successfully imported schedules for {len(successful_imports)} integrations{dry_run_str}"
                
        except Exception as e:
            result.success = False
            result.message = f"Failed to import schedules: {str(e)}"
            result.add_error("Failed to import schedules", e)
            
        return result
    
    def validate_integration_schedules(
        self,
        integration_ids: Optional[List[str]] = None,
        filter_query: Optional[str] = None,
        validation_rules: Optional[Dict[str, Any]] = None
    ) -> WorkflowResult:
        """
        Validate integration schedules against rules.
        
        This workflow:
        1. Gets the schedules for specified integrations
        2. Validates them against provided rules
        
        Args:
            integration_ids: Optional list of integration IDs to validate.
            filter_query: Optional query to filter integrations.
            validation_rules: Rules to validate schedules against.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = "Validating integration schedules"
        
        # Set default validation rules if not provided
        if validation_rules is None:
            validation_rules = {
                "enabled": None,  # None means don't check
                "frequency": None,
                "time_range": {
                    "earliest": "00:00:00",
                    "latest": "23:59:59"
                },
                "allowed_days": None,  # None means all days allowed
                "forbidden_days": None,  # None means no forbidden days
                "min_interval_minutes": 5,  # Minimum interval in minutes
                "max_concurrent": None,  # Maximum concurrent schedules in same timeframe
                "require_end_date": False  # Whether schedules must have an end date
            }
        
        # Get integrations by ID or filter
        try:
            if integration_ids:
                # Get integrations by ID
                integrations = []
                for integration_id in integration_ids:
                    try:
                        integration = self.client.integrations.get(integration_id)
                        
                        # Only include scheduled integrations
                        if integration.get("integrationType") == "SCHEDULED":
                            integrations.append(integration)
                        else:
                            self.logger.info(f"Integration {integration.get('name', integration_id)} is not a scheduled integration, skipping")
                    except OICError as e:
                        self.logger.warning(f"Failed to get integration {integration_id}: {str(e)}")
                        # Continue with other integrations
            else:
                # Get integrations by filter
                params = {
                    "integrationType": "SCHEDULED"  # Only get scheduled integrations
                }
                if filter_query:
                    params["q"] = filter_query
                
                integrations = self.client.integrations.list(params=params)
            
            if not integrations:
                result.message = "No scheduled integrations found to validate"
                return result
            
            # Initialize validation results
            all_valid = True
            validation_results = {}
            
            # Calculate concurrent schedules if needed
            concurrent_schedules = {}
            if validation_rules.get("max_concurrent") is not None:
                # Group schedules by time to check concurrency
                for integration in integrations:
                    integration_id = integration.get("id")
                    if not integration_id or "schedule" not in integration:
                        continue
                        
                    schedule = integration["schedule"]
                    if not schedule.get("enabled", False):
                        continue
                        
                    # Get schedule time
                    schedule_time = schedule.get("time", "00:00:00")
                    
                    # Add to concurrent schedules
                    if schedule_time not in concurrent_schedules:
                        concurrent_schedules[schedule_time] = []
                    concurrent_schedules[schedule_time].append(integration_id)
            
            # Validate each integration schedule
            for integration in integrations:
                integration_id = integration.get("id")
                integration_name = integration.get("name", "Unknown")
                
                if not integration_id:
                    continue
                
                # Check if integration has a schedule
                if "schedule" not in integration:
                    self.logger.warning(f"Integration {integration_name} has no schedule field, skipping")
                    continue
                
                schedule = integration["schedule"]
                
                # Initialize validation result for this integration
                validation_result = {
                    "valid": True,
                    "issues": []
                }
                
                # Validate enabled status
                if validation_rules.get("enabled") is not None:
                    enabled = schedule.get("enabled", False)
                    if enabled != validation_rules["enabled"]:
                        validation_result["valid"] = False
                        validation_result["issues"].append(
                            f"Schedule enabled status {enabled} does not match required status {validation_rules['enabled']}"
                        )
                
                # Validate frequency
                if validation_rules.get("frequency") is not None:
                    frequency = schedule.get("frequency", "")
                    if frequency != validation_rules["frequency"]:
                        validation_result["valid"] = False
                        validation_result["issues"].append(
                            f"Schedule frequency {frequency} does not match required frequency {validation_rules['frequency']}"
                        )
                
                # Validate time range
                if validation_rules.get("time_range") is not None:
                    time_range = validation_rules["time_range"]
                    schedule_time = schedule.get("time", "00:00:00")
                    
                    if time_range.get("earliest") and schedule_time < time_range["earliest"]:
                        validation_result["valid"] = False
                        validation_result["issues"].append(
                            f"Schedule time {schedule_time} is earlier than allowed time {time_range['earliest']}"
                        )
                        
                    if time_range.get("latest") and schedule_time > time_range["latest"]:
                        validation_result["valid"] = False
                        validation_result["issues"].append(
                            f"Schedule time {schedule_time} is later than allowed time {time_range['latest']}"
                        )
                
                # Validate allowed days
                if validation_rules.get("allowed_days") is not None:
                    if "recurringSchedule" in schedule and "dayOfWeek" in schedule["recurringSchedule"]:
                        days = schedule["recurringSchedule"]["dayOfWeek"]
                        allowed_days = validation_rules["allowed_days"]
                        
                        for day in days:
                            if day not in allowed_days:
                                validation_result["valid"] = False
                                validation_result["issues"].append(
                                    f"Schedule includes day {day} which is not in allowed days {allowed_days}"
                                )
                
                # Validate forbidden days
                if validation_rules.get("forbidden_days") is not None:
                    if "recurringSchedule" in schedule and "dayOfWeek" in schedule["recurringSchedule"]:
                        days = schedule["recurringSchedule"]["dayOfWeek"]
                        forbidden_days = validation_rules["forbidden_days"]
                        
                        for day in days:
                            if day in forbidden_days:
                                validation_result["valid"] = False
                                validation_result["issues"].append(
                                    f"Schedule includes forbidden day {day}"
                                )
                
                # Validate minimum interval
                if validation_rules.get("min_interval_minutes") is not None:
                    if "recurringSchedule" in schedule:
                        recurring = schedule["recurringSchedule"]
                        
                        if "minute" in recurring:
                            minute = recurring["minute"]
                            min_interval = validation_rules["min_interval_minutes"]
                            
                            # Check if minute is divisible by min_interval
                            if int(minute) % min_interval != 0:
                                validation_result["valid"] = False
                                validation_result["issues"].append(
                                    f"Schedule minute {minute} does not meet minimum interval {min_interval} minutes"
                                )
                
                # Validate max concurrent schedules
                if validation_rules.get("max_concurrent") is not None:
                    schedule_time = schedule.get("time", "00:00:00")
                    if schedule_time in concurrent_schedules:
                        concurrent_count = len(concurrent_schedules[schedule_time])
                        max_concurrent = validation_rules["max_concurrent"]
                        
                        if concurrent_count > max_concurrent:
                            validation_result["valid"] = False
                            validation_result["issues"].append(
                                f"Schedule time {schedule_time} has {concurrent_count} concurrent schedules, exceeding maximum of {max_concurrent}"
                            )
                
                # Validate end date requirement
                if validation_rules.get("require_end_date") is True:
                    if "endDate" not in schedule or not schedule["endDate"]:
                        validation_result["valid"] = False
                        validation_result["issues"].append(
                            "Schedule is missing required end date"
                        )
                
                # Store validation result
                validation_results[integration_id] = validation_result
                
                # Update overall validity
                if not validation_result["valid"]:
                    all_valid = False
                
                # Add to resources
                result.add_resource("integration", integration_id, {
                    "name": integration_name,
                    "schedule_valid": validation_result["valid"],
                    "schedule_issues": validation_result["issues"],
                    "schedule": schedule
                })
            
            # Update result details
            result.details["validation_results"] = validation_results
            result.details["all_valid"] = all_valid
            result.details["validated_count"] = len(validation_results)
            result.details["validation_rules"] = validation_rules
            
            # Count valid and invalid schedules
            valid_count = 0
            invalid_count = 0
            
            for _, val_result in validation_results.items():
                if val_result["valid"]:
                    valid_count += 1
                else:
                    invalid_count += 1
            
            result.details["valid_count"] = valid_count
            result.details["invalid_count"] = invalid_count
            
            # Update result message
            if all_valid:
                result.message = f"All {valid_count} integration schedules are valid"
            else:
                result.success = False
                result.message = f"Found {invalid_count} invalid integration schedules out of {len(validation_results)} total"
                
        except OICError as e:
            result.success = False
            result.message = f"Failed to validate schedules: {str(e)}"
            result.add_error("Failed to validate schedules", e)
            
        return result
    
    def list_integration_schedules(
        self,
        integration_ids: Optional[List[str]] = None,
        filter_query: Optional[str] = None,
        include_disabled: bool = True,
        group_by: str = "none"  # "none", "time", "day", "frequency"
    ) -> WorkflowResult:
        """
        List integration schedules.
        
        This workflow:
        1. Gets the schedules for specified integrations
        2. Formats and returns them, optionally grouped
        
        Args:
            integration_ids: Optional list of integration IDs to list schedules for.
            filter_query: Optional query to filter integrations.
            include_disabled: Whether to include disabled schedules.
            group_by: How to group the schedules in the result.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = "Listing integration schedules"
        
        # Get integrations by ID or filter
        try:
            if integration_ids:
                # Get integrations by ID
                integrations = []
                for integration_id in integration_ids:
                    try:
                        integration = self.client.integrations.get(integration_id)
                        
                        # Only include scheduled integrations
                        if integration.get("integrationType") == "SCHEDULED":
                            integrations.append(integration)
                        else:
                            self.logger.info(f"Integration {integration.get('name', integration_id)} is not a scheduled integration, skipping")
                    except OICError as e:
                        self.logger.warning(f"Failed to get integration {integration_id}: {str(e)}")
                        # Continue with other integrations
            else:
                # Get integrations by filter
                params = {
                    "integrationType": "SCHEDULED"  # Only get scheduled integrations
                }
                if filter_query:
                    params["q"] = filter_query
                
                integrations = self.client.integrations.list(params=params)
            
            if not integrations:
                result.message = "No scheduled integrations found"
                return result
            
            # Process each integration schedule
            schedules = {}
            active_schedules = []
            inactive_schedules = []
            
            for integration in integrations:
                integration_id = integration.get("id")
                integration_name = integration.get("name", "Unknown")
                
                if not integration_id:
                    continue
                
                # Check if integration has a schedule
                if "schedule" not in integration:
                    self.logger.debug(f"Integration {integration_name} has no schedule field, skipping")
                    continue
                
                schedule = integration["schedule"]
                enabled = schedule.get("enabled", False)
                
                # Skip disabled schedules if requested
                if not include_disabled and not enabled:
                    continue
                
                # Extract schedule details
                schedule_details = {
                    "id": integration_id,
                    "name": integration_name,
                    "enabled": enabled,
                    "frequency": schedule.get("frequency", "UNKNOWN"),
                    "startDate": schedule.get("startDate", ""),
                    "time": schedule.get("time", ""),
                    "endDate": schedule.get("endDate", ""),
                    "recurringSchedule": schedule.get("recurringSchedule", {})
                }
                
                # Add days of week for easier reference
                if "recurringSchedule" in schedule and "dayOfWeek" in schedule["recurringSchedule"]:
                    schedule_details["days"] = schedule["recurringSchedule"]["dayOfWeek"]
                else:
                    schedule_details["days"] = []
                
                # Add to schedules
                schedules[integration_id] = schedule_details
                
                # Add to active or inactive lists
                if enabled:
                    active_schedules.append(schedule_details)
                else:
                    inactive_schedules.append(schedule_details)
                
                # Add to resources
                result.add_resource("integration", integration_id, {
                    "name": integration_name,
                    "schedule": schedule
                })
            
            # Group schedules if requested
            grouped_schedules = {}
            
            if group_by == "time":
                # Group by schedule time
                for int_id, schedule in schedules.items():
                    time = schedule.get("time", "UNKNOWN")
                    
                    if time not in grouped_schedules:
                        grouped_schedules[time] = []
                        
                    grouped_schedules[time].append(schedule)
                    
            elif group_by == "day":
                # Group by days of week
                for int_id, schedule in schedules.items():
                    days = schedule.get("days", [])
                    
                    if not days:
                        # Handle schedules with no days specified
                        if "NONE" not in grouped_schedules:
                            grouped_schedules["NONE"] = []
                            
                        grouped_schedules["NONE"].append(schedule)
                    else:
                        for day in days:
                            if day not in grouped_schedules:
                                grouped_schedules[day] = []
                                
                            grouped_schedules[day].append(schedule)
                            
            elif group_by == "frequency":
                # Group by frequency
                for int_id, schedule in schedules.items():
                    frequency = schedule.get("frequency", "UNKNOWN")
                    
                    if frequency not in grouped_schedules:
                        grouped_schedules[frequency] = []
                        
                    grouped_schedules[frequency].append(schedule)
            
            # Update result details
            result.details["schedules"] = schedules
            result.details["active_schedules"] = active_schedules
            result.details["inactive_schedules"] = inactive_schedules
            result.details["total_count"] = len(schedules)
            result.details["active_count"] = len(active_schedules)
            result.details["inactive_count"] = len(inactive_schedules)
            
            if group_by != "none":
                result.details["grouped_schedules"] = grouped_schedules
                result.details["group_by"] = group_by
            
            # Update result message
            result.message = f"Found {len(active_schedules)} active and {len(inactive_schedules)} inactive integration schedules"
                
        except OICError as e:
            result.success = False
            result.message = f"Failed to list schedules: {str(e)}"
            result.add_error("Failed to list schedules", e)
            
        return result
