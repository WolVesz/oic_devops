"""
Base resource module for the OIC DevOps package.

This module provides the base class for all resource-specific clients.
"""

import json
import logging
from typing import Dict, Any, Optional, List, Union


class BaseResource:
    """
    Base class for all resource-specific clients.
    
    This class provides common functionality for interacting with
    specific types of OIC resources.
    """
    
    def __init__(self, client):
        """
        Initialize the resource client.
        
        Args:
            client: The parent OICClient instance.
        """
        self.client = client
        self.logger = logging.getLogger(f"oic_devops.{self.__class__.__name__}")
    
    def _get_endpoint(self, resource_id: Optional[str] = None, action: Optional[str] = None) -> str:
        """
        Get the API endpoint for the resource.
        
        Args:
            resource_id: Optional ID of a specific resource.
            action: Optional action to perform on the resource.
            
        Returns:
            str: The API endpoint.
        """
        endpoint = f"{self.base_path}"
        
        if resource_id:
            endpoint = f"{endpoint}/{resource_id}"
            
        if action:
            endpoint = f"{endpoint}/{action}"
            
        return endpoint
    
    def list(self, params: Optional[Dict[str, Any]] = None, raw: Optional[Dict[str, Any]] = False) -> List[Dict[str, Any]]:
        """
        List all resources of this type.
        
        Args:
            params: Optional query parameters.
            json: Option whether to return primary items or full output json
            
        Returns:
            List[Dict]: List of resources.
            Json: raw API response of multiple integrations
        """
        response = self.client.get(self._get_endpoint(), params=params)

        if raw:
            return response
        
        # Different API endpoints might return the items in different ways
        # Check for common patterns and extract the items
        if "items" in response:
            return response["items"]
        elif "elements" in response:
            return response["elements"]
        elif isinstance(response, list):
            return response
        else:
            self.logger.warning(f"Unexpected response format from list endpoint: {response.keys() if isinstance(response, dict) else type(response)}")
            return []
    
    def get(self, resource_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a specific resource by ID.
        
        Args:
            resource_id: ID of the resource to retrieve.
            params: Optional query parameters.
            
        Returns:
            Dict: The resource data.
        """
        return self.client.get(self._get_endpoint(resource_id), params=params)
    
    def create(self, data: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create a new resource.
        
        Args:
            data: Resource data.
            params: Optional query parameters.
            
        Returns:
            Dict: The created resource data.
        """
        return self.client.post(self._get_endpoint(), data=data, params=params)
    
    def update(
        self,
        resource_id: str,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Update a specific resource.
        
        Args:
            resource_id: ID of the resource to update.
            data: Updated resource data.
            params: Optional query parameters.
            
        Returns:
            Dict: The updated resource data.
        """
        return self.client.put(self._get_endpoint(resource_id), data=data, params=params)
    
    def delete(
        self,
        resource_id: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Delete a specific resource.
        
        Args:
            resource_id: ID of the resource to delete.
            params: Optional query parameters.
            
        Returns:
            Dict: The response data.
        """
        return self.client.delete(self._get_endpoint(resource_id), params=params)
    
    def execute_action(
        self,
        action: str,
        resource_id: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        method: str = "POST",
    ) -> Dict[str, Any]:
        """
        Execute a custom action on a resource.
        
        Args:
            action: The action to execute.
            resource_id: Optional ID of the resource to execute the action on.
            data: Optional data to send with the request.
            params: Optional query parameters.
            method: HTTP method to use (GET, POST, PUT, etc). Default is POST.
            
        Returns:
            Dict: The response data.
        """
        endpoint = self._get_endpoint(resource_id, action)
        
        if method.upper() == "GET":
            return self.client.get(endpoint, params=params)
        elif method.upper() == "POST":
            return self.client.post(endpoint, data=data, params=params)
        elif method.upper() == "PUT":
            return self.client.put(endpoint, data=data, params=params)
        elif method.upper() == "DELETE":
            return self.client.delete(endpoint, params=params)
        elif method.upper() == "PATCH":
            return self.client.patch(endpoint, data=data, params=params)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
