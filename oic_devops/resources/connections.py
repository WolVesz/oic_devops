"""
Connections resource module for the OIC DevOps package.

This module provides functionality for managing OIC connections.
"""

import logging
from typing import Dict, Any, Optional, List, Union

from oic_devops.resources.base import BaseResource
from oic_devops.exceptions import OICValidationError


class ConnectionsResource(BaseResource):
    """
    Class for managing OIC connections.
    
    Provides methods for listing, retrieving, creating, updating,
    and deleting connections, as well as testing connections.
    """
    
    def __init__(self, client):
        """
        Initialize the connections resource client.
        
        Args:
            client: The parent OICClient instance.
        """
        super().__init__(client)
        self.base_path = "/ic/api/integration/v1/connections"
    
    def list(self, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List all connections.
        
        Args:
            params: Optional query parameters such as:
                - limit: Maximum number of items to return.
                - offset: Number of items to skip.
                - fields: Comma-separated list of fields to include.
                - q: Search query.
                - orderBy: Field to order by.
                
        Returns:
            List[Dict]: List of connections.
        """
        return super().list(params)
    
    def get(self, connection_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a specific connection by ID.
        
        Args:
            connection_id: ID of the connection to retrieve.
            params: Optional query parameters.
                
        Returns:
            Dict: The connection data.
        """
        return super().get(connection_id, params)
    
    def create(self, data: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create a new connection.
        
        Args:
            data: Connection data, including:
                - name: Name of the connection.
                - identifier: Unique identifier for the connection.
                - connectionType: Type of the connection (e.g., "REST", "SOAP").
                - ... and other connection-specific properties.
            params: Optional query parameters.
                
        Returns:
            Dict: The created connection data.
            
        Raises:
            OICValidationError: If required fields are missing.
        """
        # Validate required fields
        required_fields = ["name", "identifier", "connectionType"]
        for field in required_fields:
            if field not in data:
                raise OICValidationError(f"Missing required field for connection creation: {field}")
        
        return super().create(data, params)
    
    def update(
        self,
        connection_id: str,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Update a specific connection.
        
        Args:
            connection_id: ID of the connection to update.
            data: Updated connection data.
            params: Optional query parameters.
                
        Returns:
            Dict: The updated connection data.
        """
        return super().update(connection_id, data, params)
    
    def delete(
        self,
        connection_id: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Delete a specific connection.
        
        Args:
            connection_id: ID of the connection to delete.
            params: Optional query parameters.
                
        Returns:
            Dict: The response data.
        """
        return super().delete(connection_id, params)
    
    def test(
        self,
        connection_id: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Test a specific connection.
        
        Args:
            connection_id: ID of the connection to test.
            params: Optional query parameters.
                
        Returns:
            Dict: The test result data.
        """
        return self.execute_action("test", connection_id, params=params, method="POST")
    
    def clone(
        self,
        connection_id: str,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Clone a specific connection.
        
        Args:
            connection_id: ID of the connection to clone.
            data: Data for the cloned connection, including:
                - name: Name of the cloned connection.
                - identifier: Unique identifier for the cloned connection.
            params: Optional query parameters.
                
        Returns:
            Dict: The cloned connection data.
            
        Raises:
            OICValidationError: If required fields are missing.
        """
        # Validate required fields
        required_fields = ["name", "identifier"]
        for field in required_fields:
            if field not in data:
                raise OICValidationError(f"Missing required field for connection cloning: {field}")
        
        return self.execute_action("clone", connection_id, data=data, params=params, method="POST")
    
    def get_types(self, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Get all available connection types.
        
        Args:
            params: Optional query parameters.
                
        Returns:
            List[Dict]: List of connection types.
        """
        response = self.client.get(f"{self.base_path}/types", params=params)
        
        if "items" in response:
            return response["items"]
        elif "elements" in response:
            return response["elements"]
        elif isinstance(response, list):
            return response
        else:
            self.logger.warning(f"Unexpected response format from get_types endpoint: {response.keys() if isinstance(response, dict) else type(response)}")
            return []
    
    def get_type(self, type_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a specific connection type by ID.
        
        Args:
            type_id: ID of the connection type to retrieve.
            params: Optional query parameters.
                
        Returns:
            Dict: The connection type data.
        """
        return self.client.get(f"{self.base_path}/types/{type_id}", params=params)
