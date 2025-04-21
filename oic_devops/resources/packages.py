"""
Packages resource module for the OIC DevOps package.

This module provides functionality for managing OIC packages.
"""

import os
import logging
from typing import Dict, Any, Optional, List, Union, BinaryIO

from oic_devops.resources.base import BaseResource
from oic_devops.exceptions import OICValidationError, OICAPIError


class PackagesResource(BaseResource):
    """
    Class for managing OIC packages.
    
    Provides methods for listing, retrieving, creating, importing, and
    exporting packages.
    """
    
    def __init__(self, client):
        """
        Initialize the packages resource client.
        
        Args:
            client: The parent OICClient instance.
        """
        super().__init__(client)
        self.base_path = "/ic/api/integration/v1/packages"
    
    def list(self, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List all packages.
        
        Args:
            params: Optional query parameters such as:
                - limit: Maximum number of items to return.
                - offset: Number of items to skip.
                - fields: Comma-separated list of fields to include.
                - q: Search query.
                - orderBy: Field to order by.
                
        Returns:
            List[Dict]: List of packages.
        """
        return super().list(params)
    
    def get(self, package_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a specific package by ID.
        
        Args:
            package_id: ID of the package to retrieve.
            params: Optional query parameters.
                
        Returns:
            Dict: The package data.
        """
        return super().get(package_id, params)
    
    def create(self, data: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create a new package.
        
        Args:
            data: Package data, including:
                - name: Name of the package.
                - identifier: Unique identifier for the package.
                - resources: List of resources to include in the package.
                - ... and other package-specific properties.
            params: Optional query parameters.
                
        Returns:
            Dict: The created package data.
            
        Raises:
            OICValidationError: If required fields are missing.
        """
        # Validate required fields
        required_fields = ["name", "identifier", "resources"]
        for field in required_fields:
            if field not in data:
                raise OICValidationError(f"Missing required field for package creation: {field}")
        
        return super().create(data, params)
    
    def export(
        self,
        package_id: str,
        file_path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Export a specific package to a file.
        
        Args:
            package_id: ID of the package to export.
            file_path: Path to save the exported package file.
            params: Optional query parameters.
                
        Returns:
            str: Path to the exported package file.
            
        Raises:
            OICAPIError: If the export fails.
        """
        # Set custom headers for binary content
        headers = {
            "Accept": "application/octet-stream",
        }
        
        # Make the export request
        response = self.client.request(
            "GET",
            self._get_endpoint(package_id, "export"),
            params=params,
            headers=headers,
        )
        
        # Check if the response contains binary content
        if "content" in response and isinstance(response["content"], bytes):
            # Write the content to the file
            try:
                with open(file_path, "wb") as f:
                    f.write(response["content"])
                self.logger.info(f"Package exported to {file_path}")
                return file_path
            except Exception as e:
                raise OICAPIError(f"Failed to write export file: {str(e)}")
        else:
            raise OICAPIError("Export response did not contain binary content")
    
    def import_package(
        self,
        file_path: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Import a package from a file.
        
        Args:
            file_path: Path to the package file to import.
            data: Optional import data.
            params: Optional query parameters.
                
        Returns:
            Dict: The import result data.
            
        Raises:
            OICValidationError: If the file does not exist.
            OICAPIError: If the import fails.
        """
        # Validate the file exists
        if not os.path.exists(file_path):
            raise OICValidationError(f"Package file not found: {file_path}")
        
        # Set up the file for upload
        try:
            with open(file_path, "rb") as f:
                files = {"file": (os.path.basename(file_path), f, "application/octet-stream")}
                
                # Make the import request with data as form fields
                headers = {"Accept": "application/json"}
                
                # Make a custom request that includes both files and form data
                return self.client.request(
                    "POST",
                    self._get_endpoint(action="import"),
                    data=data,
                    params=params,
                    files=files,
                    headers=headers,
                )
        except Exception as e:
            raise OICAPIError(f"Failed to import package: {str(e)}")
    
    def get_resources(
        self,
        package_id: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get the resources for a specific package.
        
        Args:
            package_id: ID of the package to get resources for.
            params: Optional query parameters.
                
        Returns:
            List[Dict]: List of package resources.
        """
        response = self.client.get(
            self._get_endpoint(package_id, "resources"),
            params=params,
        )
        
        # Extract resources from the response
        if "items" in response:
            return response["items"]
        elif "elements" in response:
            return response["elements"]
        elif isinstance(response, list):
            return response
        else:
            self.logger.warning(f"Unexpected response format from get_resources endpoint: {response.keys() if isinstance(response, dict) else type(response)}")
            return []
    
    def add_resource(
        self,
        package_id: str,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Add a resource to a specific package.
        
        Args:
            package_id: ID of the package to add a resource to.
            data: Resource data, including:
                - resourceType: Type of the resource (e.g., "INTEGRATION", "CONNECTION").
                - resourceId: ID of the resource to add.
            params: Optional query parameters.
                
        Returns:
            Dict: The response data.
            
        Raises:
            OICValidationError: If required fields are missing.
        """
        # Validate required fields
        required_fields = ["resourceType", "resourceId"]
        for field in required_fields:
            if field not in data:
                raise OICValidationError(f"Missing required field for adding resource: {field}")
        
        return self.client.post(
            self._get_endpoint(package_id, "resources"),
            data=data,
            params=params,
        )
    
    def remove_resource(
        self,
        package_id: str,
        resource_id: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Remove a resource from a specific package.
        
        Args:
            package_id: ID of the package to remove a resource from.
            resource_id: ID of the resource to remove.
            params: Optional query parameters.
                
        Returns:
            Dict: The response data.
        """
        return self.client.delete(
            f"{self._get_endpoint(package_id, 'resources')}/{resource_id}",
            params=params,
        )
