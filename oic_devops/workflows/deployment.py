"""
Deployment workflows module for the OIC DevOps package.

This module provides workflow operations for deploying OIC resources
across environments.
"""

import os
import logging
import time
import json
from typing import Dict, Any, List, Optional, Union, Tuple

from oic_devops.client import OICClient
from oic_devops.exceptions import OICError
from oic_devops.workflows.base import BaseWorkflow, WorkflowResult


class DeploymentWorkflows(BaseWorkflow):
    """
    Workflow operations for deploying OIC resources.
    
    This class provides higher-level operations for deploying resources
    across environments, including export, import, and promotion operations.
    """
    
    def execute(self, *args, **kwargs) -> WorkflowResult:
        """
        Execute the specified deployment workflow.
        
        This is a dispatcher method that calls the appropriate workflow
        based on the operation argument.
        
        Args:
            operation: The workflow operation to execute.
            **kwargs: Additional arguments specific to the workflow.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        operation = kwargs.pop("operation", None)
        
        if operation == "export_integration":
            return self.export_integration(**kwargs)
        elif operation == "import_integration":
            return self.import_integration(**kwargs)
        elif operation == "promote_integration":
            return self.promote_integration(**kwargs)
        elif operation == "export_package":
            return self.export_package(**kwargs)
        elif operation == "import_package":
            return self.import_package(**kwargs)
        elif operation == "clone_environment":
            return self.clone_environment(**kwargs)
        else:
            result = WorkflowResult(success=False, message=f"Unknown deployment workflow operation: {operation}")
            result.add_error(f"Unknown operation: {operation}")
            return result
    
    def export_integration(
        self,
        integration_id: str,
        export_file_path: str,
        include_dependencies: bool = False,
        overwrite: bool = False
    ) -> WorkflowResult:
        """
        Export an integration to a file.
        
        This workflow:
        1. Gets the integration details
        2. Exports the integration to a file
        3. Optionally exports dependencies
        
        Args:
            integration_id: ID of the integration to export.
            export_file_path: Path to save the exported integration file.
            include_dependencies: Whether to export dependencies.
            overwrite: Whether to overwrite existing files.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Exporting integration {integration_id}"
        
        # Check if export file already exists
        if os.path.exists(export_file_path) and not overwrite:
            result.success = False
            result.message = f"Export file {export_file_path} already exists and overwrite is False"
            result.add_error("Export file already exists")
            return result
        
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
        
        # Export the integration
        try:
            self.logger.info(f"Exporting integration {integration_name} to {export_file_path}")
            export_result = self.client.integrations.export(integration_id, export_file_path)
            
            result.details["export_file_path"] = export_file_path
            
        except OICError as e:
            self.logger.error(f"Failed to export integration {integration_name}: {str(e)}")
            result.add_error(f"Failed to export integration {integration_name}", e, integration_id)
            result.success = False
            return result
        
        # Export dependencies if requested
        if include_dependencies:
            self.logger.info(f"Exporting dependencies for integration {integration_name}")
            
            # Find dependencies
            try:
                from oic_devops.workflows.integration import IntegrationWorkflows
                integration_workflows = IntegrationWorkflows(self.client)
                
                dependency_result = integration_workflows.find_integration_dependencies(
                    integration_id=integration_id,
                    include_connections=True,
                    include_lookups=True,
                    include_libraries=True
                )
                
                if not dependency_result.success:
                    self.logger.warning("Failed to find dependencies, continuing with export")
                    result.details["dependency_export"] = {
                        "status": "failed",
                        "message": "Failed to find dependencies"
                    }
                else:
                    # Export each dependency
                    dep_counts = {
                        "connections": 0,
                        "lookups": 0,
                        "libraries": 0
                    }
                    
                    # Build the dependency export directory
                    base_name = os.path.splitext(export_file_path)[0]
                    dep_dir = f"{base_name}_dependencies"
                    os.makedirs(dep_dir, exist_ok=True)
                    
                    # Export connections
                    connections = dependency_result.details.get("dependencies", {}).get("connections", [])
                    for connection in connections:
                        connection_id = connection.get("id")
                        connection_name = connection.get("name", "Unknown")
                        
                        if connection_id:
                            # For connections, we can only export configuration as JSON
                            try:
                                connection_details = self.client.connections.get(connection_id)
                                conn_file_path = os.path.join(dep_dir, f"connection_{connection_id}.json")
                                
                                with open(conn_file_path, "w") as f:
                                    json.dump(connection_details, f, indent=2)
                                    
                                dep_counts["connections"] += 1
                                self.logger.info(f"Exported connection {connection_name} to {conn_file_path}")
                            except Exception as e:
                                self.logger.warning(f"Failed to export connection {connection_name}: {str(e)}")
                    
                    # Export lookups
                    lookups = dependency_result.details.get("dependencies", {}).get("lookups", [])
                    for lookup in lookups:
                        lookup_id = lookup.get("id")
                        lookup_name = lookup.get("name", "Unknown")
                        
                        if lookup_id:
                            try:
                                lookup_file_path = os.path.join(dep_dir, f"lookup_{lookup_id}.csv")
                                self.client.lookups.export(lookup_id, lookup_file_path)
                                dep_counts["lookups"] += 1
                                self.logger.info(f"Exported lookup {lookup_name} to {lookup_file_path}")
                            except Exception as e:
                                self.logger.warning(f"Failed to export lookup {lookup_name}: {str(e)}")
                    
                    # Export libraries
                    libraries = dependency_result.details.get("dependencies", {}).get("libraries", [])
                    for library in libraries:
                        library_id = library.get("id")
                        library_name = library.get("name", "Unknown")
                        
                        if library_id:
                            try:
                                library_file_path = os.path.join(dep_dir, f"library_{library_id}.jar")
                                self.client.libraries.export(library_id, library_file_path)
                                dep_counts["libraries"] += 1
                                self.logger.info(f"Exported library {library_name} to {library_file_path}")
                            except Exception as e:
                                self.logger.warning(f"Failed to export library {library_name}: {str(e)}")
                    
                    # Update result with dependency export information
                    result.details["dependency_export"] = {
                        "status": "success",
                        "directory": dep_dir,
                        "counts": dep_counts
                    }
                    
                    # Update message
                    dep_total = sum(dep_counts.values())
                    if dep_total > 0:
                        result.message = f"Exported integration {integration_name} and {dep_total} dependencies"
                    
            except Exception as e:
                self.logger.error(f"Failed to export dependencies: {str(e)}")
                result.details["dependency_export"] = {
                    "status": "failed",
                    "message": str(e)
                }
                # Continue with the main export result
        
        # Successfully exported
        if result.message == f"Exporting integration {integration_id}":
            result.message = f"Successfully exported integration {integration_name} to {export_file_path}"
            
        return result
    
    def import_integration(
        self,
        import_file_path: str,
        import_plan: Optional[Dict[str, Any]] = None,
        overwrite_existing: bool = True
    ) -> WorkflowResult:
        """
        Import an integration from a file.
        
        This workflow:
        1. Validates the import file
        2. Imports the integration
        
        Args:
            import_file_path: Path to the integration file to import.
            import_plan: Optional import plan with configuration options.
            overwrite_existing: Whether to overwrite existing integrations.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Importing integration from {import_file_path}"
        
        # Check if import file exists
        if not os.path.exists(import_file_path):
            result.success = False
            result.message = f"Import file {import_file_path} does not exist"
            result.add_error("Import file does not exist")
            return result
        
        # Prepare import data
        import_data = {}
        
        if import_plan:
            import_data.update(import_plan)
        
        # Set overwrite flag
        import_data["overwrite"] = overwrite_existing
        
        # Import the integration
        try:
            self.logger.info(f"Importing integration from {import_file_path}")
            import_result = self.client.integrations.import_integration(import_file_path, import_data)
            
            # Check import result
            if not import_result:
                result.success = False
                result.message = "Import returned empty result"
                result.add_error("Empty import result")
                return result
            
            # Extract integration information from result
            integration_id = import_result.get("id")
            integration_name = import_result.get("name", "Unknown")
            
            if not integration_id:
                result.success = False
                result.message = "Import result missing integration ID"
                result.add_error("Missing integration ID in import result")
                return result
            
            result.add_resource("integration", integration_id, {
                "name": integration_name,
                "status": import_result.get("status", "UNKNOWN")
            })
            
            result.details["import_result"] = {
                "id": integration_id,
                "name": integration_name,
                "status": import_result.get("status")
            }
            
            result.message = f"Successfully imported integration {integration_name}"
            
        except OICError as e:
            self.logger.error(f"Failed to import integration: {str(e)}")
            result.add_error("Failed to import integration", e)
            result.success = False
            result.message = f"Failed to import integration from {import_file_path}"
            return result
        
        return result
    
    def promote_integration(
        self,
        integration_id: str,
        target_client: OICClient,
        activate_after_import: bool = False,
        include_dependencies: bool = False,
        connection_map: Optional[Dict[str, str]] = None
    ) -> WorkflowResult:
        """
        Promote an integration from one environment to another.
        
        This workflow:
        1. Exports the integration from the source environment
        2. Imports the integration to the target environment
        3. Optionally activates the integration in the target environment
        
        Args:
            integration_id: ID of the integration in the source environment.
            target_client: OICClient for the target environment.
            activate_after_import: Whether to activate the integration after import.
            include_dependencies: Whether to include dependencies.
            connection_map: Optional mapping of source connection IDs to target connection IDs.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Promoting integration {integration_id} to target environment"
        
        # Create a temporary directory for export files
        import os
        import tempfile
        from datetime import datetime
        
        temp_dir = tempfile.mkdtemp(prefix="oic_promotion_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_file_path = os.path.join(temp_dir, f"integration_{integration_id}_{timestamp}.iar")
        
        self.logger.info(f"Using temporary directory: {temp_dir}")
        result.details["temp_directory"] = temp_dir
        
        # Step 1: Export the integration from source environment
        self.logger.info(f"Exporting integration {integration_id} from source environment")
        export_result = self.export_integration(
            integration_id=integration_id,
            export_file_path=export_file_path,
            include_dependencies=include_dependencies
        )
        
        # Check export result
        if not export_result.success:
            result.success = False
            result.message = "Failed to export integration from source environment"
            result.merge(export_result)
            return result
        
        # Get integration name for better logging
        integration_name = export_result.details.get("integration_name", "Unknown")
        result.details["integration_name"] = integration_name
        
        # Step 2: Prepare import plan if connection mapping is provided
        import_plan = {}
        
        if connection_map:
            import_plan["connectionMap"] = connection_map
            self.logger.info(f"Using connection mapping: {connection_map}")
        
        # Step 3: Import the integration to target environment
        self.logger.info(f"Importing integration {integration_name} to target environment")
        
        try:
            # Create a new workflow for the target environment
            target_workflow = DeploymentWorkflows(target_client)
            
            import_result = target_workflow.import_integration(
                import_file_path=export_file_path,
                import_plan=import_plan,
                overwrite_existing=True
            )
            
            # Check import result
            if not import_result.success:
                result.success = False
                result.message = "Failed to import integration to target environment"
                result.merge(import_result)
                return result
            
            # Get the imported integration ID
            target_integration_id = None
            if "integration" in import_result.resources:
                target_integration_ids = list(import_result.resources["integration"].keys())
                if target_integration_ids:
                    target_integration_id = target_integration_ids[0]
            
            if not target_integration_id:
                self.logger.warning("Could not determine target integration ID from import result")
                result.details["target_integration_id"] = "Unknown"
            else:
                result.details["target_integration_id"] = target_integration_id
            
            # Step 4: Activate the integration if requested
            if activate_after_import and target_integration_id:
                self.logger.info(f"Activating integration {integration_name} in target environment")
                
                try:
                    from oic_devops.workflows.integration import IntegrationWorkflows
                    target_integration_workflows = IntegrationWorkflows(target_client)
                    
                    activate_result = target_integration_workflows.bulk_activate_integrations(
                        integration_ids=[target_integration_id],
                        verify_activation=True
                    )
                    
                    if not activate_result.success:
                        result.success = False
                        result.message = f"Integration promoted but activation failed in target environment"
                        result.details["activation_status"] = "failed"
                        result.merge(activate_result)
                    else:
                        result.details["activation_status"] = "success"
                        
                except Exception as e:
                    self.logger.error(f"Failed to activate integration in target environment: {str(e)}")
                    result.add_error("Failed to activate integration", e, target_integration_id)
                    result.success = False
                    result.message = f"Integration promoted but activation failed in target environment"
                    result.details["activation_status"] = "error"
            
            # Update result message
            if result.success:
                if activate_after_import:
                    result.message = f"Successfully promoted and activated integration {integration_name}"
                else:
                    result.message = f"Successfully promoted integration {integration_name}"
            
        except Exception as e:
            self.logger.error(f"Failed to promote integration: {str(e)}")
            result.add_error("Failed to promote integration", e)
            result.success = False
            result.message = f"Failed to promote integration {integration_name}"
            return result
        
        # Clean up temporary files
        try:
            import shutil
            shutil.rmtree(temp_dir)
            self.logger.info(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            self.logger.warning(f"Failed to clean up temporary directory: {str(e)}")
            
        return result
    
    def export_package(
        self,
        package_id: str,
        export_file_path: str,
        overwrite: bool = False
    ) -> WorkflowResult:
        """
        Export a package to a file.
        
        This workflow:
        1. Gets the package details
        2. Exports the package to a file
        
        Args:
            package_id: ID of the package to export.
            export_file_path: Path to save the exported package file.
            overwrite: Whether to overwrite existing files.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Exporting package {package_id}"
        
        # Check if export file already exists
        if os.path.exists(export_file_path) and not overwrite:
            result.success = False
            result.message = f"Export file {export_file_path} already exists and overwrite is False"
            result.add_error("Export file already exists")
            return result
        
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
        
        # Get the package details
        try:
            self.logger.info(f"Getting details for package {package_id}")
            package = self.client.packages.get(package_id)
            package_name = package.get("name", "Unknown")
            
            result.add_resource("package", package_id, {"name": package_name})
            result.details["package_name"] = package_name
            
        except OICError as e:
            self.logger.error(f"Failed to get package {package_id}: {str(e)}")
            result.add_error(f"Failed to get package {package_id}", e, package_id)
            result.success = False
            return result
        
        # Export the package
        try:
            self.logger.info(f"Exporting package {package_name} to {export_file_path}")
            export_result = self.client.packages.export(package_id, export_file_path)
            
            result.details["export_file_path"] = export_file_path
            result.message = f"Successfully exported package {package_name} to {export_file_path}"
            
        except OICError as e:
            self.logger.error(f"Failed to export package {package_name}: {str(e)}")
            result.add_error(f"Failed to export package {package_name}", e, package_id)
            result.success = False
            result.message = f"Failed to export package {package_name}"
            return result
        
        return result
    
    def import_package(
        self,
        import_file_path: str,
        import_plan: Optional[Dict[str, Any]] = None,
        overwrite_existing: bool = True
    ) -> WorkflowResult:
        """
        Import a package from a file.
        
        This workflow:
        1. Validates the import file
        2. Imports the package
        
        Args:
            import_file_path: Path to the package file to import.
            import_plan: Optional import plan with configuration options.
            overwrite_existing: Whether to overwrite existing packages.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = f"Importing package from {import_file_path}"
        
        # Check if import file exists
        if not os.path.exists(import_file_path):
            result.success = False
            result.message = f"Import file {import_file_path} does not exist"
            result.add_error("Import file does not exist")
            return result
        
        # Prepare import data
        import_data = {}
        
        if import_plan:
            import_data.update(import_plan)
        
        # Set overwrite flag
        import_data["overwrite"] = overwrite_existing
        
        # Import the package
        try:
            self.logger.info(f"Importing package from {import_file_path}")
            import_result = self.client.packages.import_package(import_file_path, import_data)
            
            # Check import result
            if not import_result:
                result.success = False
                result.message = "Import returned empty result"
                result.add_error("Empty import result")
                return result
            
            # Extract package information from result
            package_id = import_result.get("id")
            package_name = import_result.get("name", "Unknown")
            
            if not package_id:
                result.success = False
                result.message = "Import result missing package ID"
                result.add_error("Missing package ID in import result")
                return result
            
            result.add_resource("package", package_id, {
                "name": package_name
            })
            
            result.details["import_result"] = {
                "id": package_id,
                "name": package_name
            }
            
            result.message = f"Successfully imported package {package_name}"
            
        except OICError as e:
            self.logger.error(f"Failed to import package: {str(e)}")
            result.add_error("Failed to import package", e)
            result.success = False
            result.message = f"Failed to import package from {import_file_path}"
            return result
        
        return result
    
    def clone_environment(
        self,
        target_client: OICClient,
        resource_types: List[str] = ["connections", "lookups", "libraries", "integrations"],
        exclude_filters: Optional[Dict[str, str]] = None,
        include_filters: Optional[Dict[str, str]] = None,
        activate_integrations: bool = False
    ) -> WorkflowResult:
        """
        Clone resources from one environment to another.
        
        This workflow:
        1. Exports resources from the source environment
        2. Imports resources to the target environment
        
        Args:
            target_client: OICClient for the target environment.
            resource_types: Types of resources to clone.
            exclude_filters: Optional filters to exclude resources.
            include_filters: Optional filters to include resources.
            activate_integrations: Whether to activate integrations after import.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = "Cloning environment resources"
        
        # Create a temporary directory for export files
        import os
        import tempfile
        from datetime import datetime
        
        temp_dir = tempfile.mkdtemp(prefix="oic_cloning_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        self.logger.info(f"Using temporary directory: {temp_dir}")
        result.details["temp_directory"] = temp_dir
        
        # Initialize resource counters
        resource_counters = {
            "connections": {"exported": 0, "imported": 0, "failed": 0},
            "lookups": {"exported": 0, "imported": 0, "failed": 0},
            "libraries": {"exported": 0, "imported": 0, "failed": 0},
            "integrations": {"exported": 0, "imported": 0, "failed": 0},
            "activated": 0
        }
        
        # Helper function to apply filters
        def should_process_resource(resource, resource_type):
            # Check exclude filters
            if exclude_filters and resource_type in exclude_filters:
                exclude_query = exclude_filters[resource_type]
                # Simple string match for now - could be enhanced
                if exclude_query in resource.get("name", "") or exclude_query in resource.get("id", ""):
                    return False
            
            # Check include filters
            if include_filters and resource_type in include_filters:
                include_query = include_filters[resource_type]
                # Simple string match for now - could be enhanced
                if include_query not in resource.get("name", "") and include_query not in resource.get("id", ""):
                    return False
            
            return True
        
        # Process each resource type
        for resource_type in resource_types:
            resource_dir = os.path.join(temp_dir, resource_type)
            os.makedirs(resource_dir, exist_ok=True)
            
            self.logger.info(f"Processing {resource_type}")
            
            try:
                # Get list of resources
                if resource_type == "connections":
                    resources = self.client.connections.list()
                    export_func = lambda r_id, path: self._export_connection_as_json(r_id, path)
                    import_func = lambda path: self._import_connection_from_json(path, target_client)
                    
                elif resource_type == "lookups":
                    resources = self.client.lookups.list()
                    export_func = lambda r_id, path: self.client.lookups.export(r_id, path)
                    import_func = lambda path: target_client.lookups.import_lookup(path)
                    
                elif resource_type == "libraries":
                    resources = self.client.libraries.list()
                    export_func = lambda r_id, path: self.client.libraries.export(r_id, path)
                    import_func = lambda path: target_client.libraries.import_library(path)
                    
                elif resource_type == "integrations":
                    resources = self.client.integrations.list()
                    export_func = lambda r_id, path: self.client.integrations.export(r_id, path)
                    import_func = lambda path: target_client.integrations.import_integration(path, {"overwrite": True})
                    
                else:
                    self.logger.warning(f"Unsupported resource type: {resource_type}")
                    continue
                
                # Process each resource
                for resource in resources:
                    resource_id = resource.get("id")
                    resource_name = resource.get("name", "Unknown")
                    
                    if not resource_id or not should_process_resource(resource, resource_type):
                        continue
                    
                    # Export the resource
                    try:
                        export_path = os.path.join(resource_dir, f"{resource_type}_{resource_id}.{self._get_extension(resource_type)}")
                        self.logger.info(f"Exporting {resource_type} {resource_name} to {export_path}")
                        
                        export_func(resource_id, export_path)
                        resource_counters[resource_type]["exported"] += 1
                        
                        # Import the resource to target environment
                        try:
                            self.logger.info(f"Importing {resource_type} {resource_name} to target environment")
                            import_result = import_func(export_path)
                            
                            resource_counters[resource_type]["imported"] += 1
                            
                            # Track imported integration IDs for activation if needed
                            if resource_type == "integrations" and activate_integrations:
                                if import_result and "id" in import_result:
                                    # Keep track of imported integration ID
                                    integration_id = import_result["id"]
                                    
                                    # Activate the integration
                                    try:
                                        target_client.integrations.activate(integration_id)
                                        resource_counters["activated"] += 1
                                        self.logger.info(f"Activated integration {resource_name} in target environment")
                                    except OICError as e:
                                        self.logger.warning(f"Failed to activate integration {resource_name}: {str(e)}")
                            
                        except Exception as e:
                            self.logger.error(f"Failed to import {resource_type} {resource_name}: {str(e)}")
                            resource_counters[resource_type]["failed"] += 1
                        
                    except Exception as e:
                        self.logger.error(f"Failed to export {resource_type} {resource_name}: {str(e)}")
                        resource_counters[resource_type]["failed"] += 1
                
            except OICError as e:
                self.logger.error(f"Failed to get {resource_type} list: {str(e)}")
                result.add_error(f"Failed to get {resource_type} list", e)
                continue
        
        # Update result with counters
        result.details["resource_counters"] = resource_counters
        
        # Build result message
        message_parts = []
        total_exported = 0
        total_imported = 0
        total_failed = 0
        
        for resource_type in resource_types:
            counters = resource_counters[resource_type]
            exported = counters["exported"]
            imported = counters["imported"]
            failed = counters["failed"]
            
            total_exported += exported
            total_imported += imported
            total_failed += failed
            
            if exported > 0:
                message_parts.append(f"{imported}/{exported} {resource_type}")
        
        if message_parts:
            if total_failed == 0:
                result.message = f"Successfully cloned {', '.join(message_parts)}"
            else:
                result.success = False
                result.message = f"Cloned {', '.join(message_parts)}, but {total_failed} resources failed"
                
            if activate_integrations and resource_counters["activated"] > 0:
                result.message += f" and activated {resource_counters['activated']} integrations"
        else:
            result.message = "No resources were cloned"
        
        # Clean up temporary files
        try:
            import shutil
            shutil.rmtree(temp_dir)
            self.logger.info(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            self.logger.warning(f"Failed to clean up temporary directory: {str(e)}")
            
        return result
    
    def _export_connection_as_json(self, connection_id: str, file_path: str) -> str:
        """
        Export a connection as JSON since direct export is not supported.
        
        Args:
            connection_id: ID of the connection to export.
            file_path: Path to save the exported connection file.
            
        Returns:
            str: Path to the exported connection file.
        """
        connection = self.client.connections.get(connection_id)
        
        with open(file_path, "w") as f:
            json.dump(connection, f, indent=2)
            
        return file_path
    
    def _import_connection_from_json(self, file_path: str, client: OICClient) -> Dict[str, Any]:
        """
        Import a connection from a JSON file.
        
        Args:
            file_path: Path to the connection JSON file.
            client: OICClient for the target environment.
            
        Returns:
            Dict: The import result.
        """
        with open(file_path, "r") as f:
            connection = json.load(f)
        
        # Check if connection already exists
        try:
            existing_connections = client.connections.list()
            for existing in existing_connections:
                if existing.get("identifier") == connection.get("identifier"):
                    # Update existing connection
                    return client.connections.update(existing["id"], connection)
        except:
            pass
        
        # Create new connection
        return client.connections.create(connection)
    
    def _get_extension(self, resource_type: str) -> str:
        """
        Get the file extension for a resource type.
        
        Args:
            resource_type: Type of resource.
            
        Returns:
            str: File extension for the resource type.
        """
        if resource_type == "connections":
            return "json"
        elif resource_type == "lookups":
            return "csv"
        elif resource_type == "libraries":
            return "jar"
        elif resource_type == "integrations":
            return "iar"
        else:
            return "bin"
