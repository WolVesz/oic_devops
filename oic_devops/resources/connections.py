"""
Connections resource module for the OIC DevOps package.

This module provides functionality for managing OIC connections.
"""

import logging
import pandas as pd
from datetime import datetime
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
        return super().list(params, raw=True)

    def list_all(self, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Automatically paginates through the API to provide the complete list of connections.

        Args:
            params: Optional query parameters such as:
                - limit: Maximum number of items to return.
                - offset: Number of items to skip.
                - fields: Comma-separated list of fields to include.
                - q: Search query.
                - orderBy: Field to order by.
                - status: Filter by status (e.g., "ACTIVATED", "CONFIGURED").

        Returns:
            List[Dict]: List of integrations.
        """

        has_more = True
        output = []
        pages = 0

        if not params:
            params = dict()

        while has_more is True:
            params['offset'] = pages
            content = self.list(params=params)
            output.extend(content['items'])
            has_more = content['hasMore']
            pages += content['totalResults']
            self.logger.info(f'Number of Connections Acquired in List: {pages}')

        return output

    def df(self, **kwargs):
        """
        Creates a pandas Dataframe with the full contents of list_all.

        Args:
            params: Optional query parameters such as:
                - limit: Maximum number of items to return.
                - offset: Number of items to skip.
                - fields: Comma-separated list of fields to include.
                - q: Search query.
                - orderBy: Field to order by.
                - status: Filter by status (e.g., "ACTIVATED", "CONFIGURED").
            update:

        Returns:
            List[Dict]: List of integrations.
        """

        output = self.list_all(**kwargs)

        values = []
        for item in output:
            values.append(
                {
                    "connection_id": item['id'],
                    "connection_name": item['name'],
                    'connection_status': item['status'],
                    'connection_is_locked': item['lockedFlag'],
                    'connection_adapter': item['adapterType']['displayName'],
                    'connection_adapter_type': item['adapterType']['type'],
                    'connection_role': item['role'],
                    'connection_security_policy': item['securityPolicy'],
                    'connection_created_date': item['created'],
                    'connection_updated_date': item['lastUpdated'],
                    'connection_updated_by': item['lastUpdatedBy'],
                }
            )

        df = pd.DataFrame(values)
        df['connection_acquired_at'] = datetime.now()
        df['connection_acquired_at'] = pd.to_datetime(df['connection_acquired_at'])
        return df

    def get(self, connection_id: str, params: Optional[Dict[str, Any]] = None, raw = False) -> Dict[str, Any]:
        """
        Get a specific connection by ID.
        
        Args:
            connection_id: ID of the connection to retrieve.
            params: Optional query parameters.
            raw: to return the raw json or provide as a pd.Series
                
        Returns:
            Dict or pd.Series: The connection data
        """
        data = super().get(connection_id, params)

        if raw:
            return data

        # Builds structured output
        struct_output = {
            'connection_id': connection_id,
            'is_locked': data['lockedFlag'],
            'lock_date': data['lockedDate'] if 'LockedData' in data.keys() else None,
            'locked_by': data['lockedBy'] if 'LockedData' in data.keys() else None,
            'last_update_user': data['lastUpdatedBy'],
            'created_user': data['createdBy']
        }

        # optional extended fields
        struct_output.update({
            'adapter_type': None,
            'user_property_value': None,
            'user_property_name': None,
            'created_user': None,
            'last_update_user': None
        })

        if 'adapterType' in data.keys():
            struct_output['adapter_name'] = data['adapterType']['displayName']
            struct_output['adapter_type'] = data['adapterType']['type']

        if 'securityProperties' in data.keys():
            for value in data['securityProperties']:
                if value['displayName'].upper().strip() == 'USERNAME' or value['displayName'].upper().strip() == 'USER NAME':
                    if 'propertyValue' in value.keys():
                        struct_output['user_property_value'] = value['propertyValue']
                    if 'propertyName' in value.keys():
                        struct_output['user_property_name'] = value['propertyName']
                    else:
                        raise Exception("new way to get a username:")

        return pd.Series(struct_output)

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
