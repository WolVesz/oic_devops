"""
Monitoring resource module for the OIC DevOps package.

This module provides functionality for monitoring OIC resources.
"""

import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta

from oic_devops.resources.base import BaseResource
from oic_devops.exceptions import OICValidationError


class MonitoringResource(BaseResource):
    """
    Class for monitoring OIC resources.
    
    Provides methods for retrieving instance statistics, integration instance
    details, and integration execution statistics.
    """
    
    def __init__(self, client):
        """
        Initialize the monitoring resource client.
        
        Args:
            client: The parent OICClient instance.
        """
        super().__init__(client)
        self.base_path = "/ic/api/integration/v1/monitoring"
    
    def get_instance_stats(
        self,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Get instance statistics.
        
        Args:
            params: Optional query parameters.
                
        Returns:
            Dict: Instance statistics.
        """
        return self.client.get(f"{self.base_path}/instanceStats", params=params)
    
    def get_instances(
        self,
        integration_id: Optional[str] = None,
        status: Optional[str] = None,
        start_time: Optional[Union[datetime, str]] = None,
        end_time: Optional[Union[datetime, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get integration instances.
        
        Args:
            integration_id: Optional integration ID to filter by.
            status: Optional status to filter by (e.g., "COMPLETED", "FAILED").
            start_time: Optional start time to filter by.
            end_time: Optional end time to filter by.
            params: Optional additional query parameters.
                
        Returns:
            List[Dict]: List of integration instances.
        """
        # Initialize parameters if None
        if params is None:
            params = {}
        
        # Add filters to parameters
        if integration_id:
            params["integrationId"] = integration_id
        
        if status:
            params["status"] = status
        
        # Format datetime objects to strings if provided
        if start_time:
            if isinstance(start_time, datetime):
                params["startTime"] = start_time.isoformat()
            else:
                params["startTime"] = start_time
        
        if end_time:
            if isinstance(end_time, datetime):
                params["endTime"] = end_time.isoformat()
            else:
                params["endTime"] = end_time
        
        # Make the request
        response = self.client.get(f"{self.base_path}/instances", params=params)
        
        # Extract instances from the response
        if "items" in response:
            return response["items"]
        elif "elements" in response:
            return response["elements"]
        elif isinstance(response, list):
            return response
        else:
            self.logger.warning(f"Unexpected response format from get_instances endpoint: {response.keys() if isinstance(response, dict) else type(response)}")
            return []
    
    def get_instance(
        self,
        instance_id: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Get a specific integration instance by ID.
        
        Args:
            instance_id: ID of the instance to retrieve.
            params: Optional query parameters.
                
        Returns:
            Dict: The instance data.
        """
        return self.client.get(f"{self.base_path}/instances/{instance_id}", params=params)
    
    def get_instance_activities(
        self,
        instance_id: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get activities for a specific integration instance.
        
        Args:
            instance_id: ID of the instance to retrieve activities for.
            params: Optional query parameters.
                
        Returns:
            List[Dict]: List of instance activities.
        """
        response = self.client.get(
            f"{self.base_path}/instances/{instance_id}/activities",
            params=params,
        )
        
        # Extract activities from the response
        if "items" in response:
            return response["items"]
        elif "elements" in response:
            return response["elements"]
        elif isinstance(response, list):
            return response
        else:
            self.logger.warning(f"Unexpected response format from get_instance_activities endpoint: {response.keys() if isinstance(response, dict) else type(response)}")
            return []
    
    def get_instance_payload(
        self,
        instance_id: str,
        activity_id: str,
        direction: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Get the payload for a specific integration instance activity.
        
        Args:
            instance_id: ID of the instance.
            activity_id: ID of the activity.
            direction: Direction of the payload ("request" or "response").
            params: Optional query parameters.
                
        Returns:
            Dict: The payload data.
            
        Raises:
            OICValidationError: If the direction is invalid.
        """
        # Validate direction
        if direction not in ["request", "response"]:
            raise OICValidationError(f"Invalid payload direction: {direction}. Must be 'request' or 'response'.")
        
        return self.client.get(
            f"{self.base_path}/instances/{instance_id}/activities/{activity_id}/payload/{direction}",
            params=params,
        )
    
    def purge_instances(
        self,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Purge integration instances.
        
        Args:
            data: Purge criteria, including:
                - integrationId: Optional ID of the integration to purge instances for.
                - status: Optional status to filter by (e.g., "COMPLETED", "FAILED").
                - startTime: Optional start time to filter by.
                - endTime: Optional end time to filter by.
            params: Optional query parameters.
                
        Returns:
            Dict: The purge result data.
        """
        return self.client.post(
            f"{self.base_path}/instances/purge",
            data=data,
            params=params,
        )
    
    def resubmit_instance(
        self,
        instance_id: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Resubmit a specific integration instance.
        
        Args:
            instance_id: ID of the instance to resubmit.
            params: Optional query parameters.
                
        Returns:
            Dict: The resubmit result data.
        """
        return self.client.post(
            f"{self.base_path}/instances/{instance_id}/resubmit",
            params=params,
        )
    
    def get_integration_stats(
        self,
        integration_id: Optional[str] = None,
        start_time: Optional[Union[datetime, str]] = None,
        end_time: Optional[Union[datetime, str]] = None,
        interval: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Get statistics for integrations.
        
        Args:
            integration_id: Optional integration ID to filter by.
            start_time: Optional start time to filter by.
            end_time: Optional end time to filter by.
            interval: Optional interval for grouping statistics (e.g., "hour", "day", "week").
            params: Optional additional query parameters.
                
        Returns:
            Dict: Integration statistics.
        """
        # Initialize parameters if None
        if params is None:
            params = {}
        
        # Add filters to parameters
        if integration_id:
            params["integrationId"] = integration_id
        
        if interval:
            params["interval"] = interval
        
        # Format datetime objects to strings if provided
        if start_time:
            if isinstance(start_time, datetime):
                params["startTime"] = start_time.isoformat()
            else:
                params["startTime"] = start_time
        
        if end_time:
            if isinstance(end_time, datetime):
                params["endTime"] = end_time.isoformat()
            else:
                params["endTime"] = end_time
        
        # Make the request
        return self.client.get(f"{self.base_path}/integrationStats", params=params)
    
    def get_errors(
        self,
        start_time: Optional[Union[datetime, str]] = None,
        end_time: Optional[Union[datetime, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get error details for integrations.
        
        Args:
            start_time: Optional start time to filter by.
            end_time: Optional end time to filter by.
            params: Optional additional query parameters.
                
        Returns:
            List[Dict]: List of error details.
        """
        # Initialize parameters if None
        if params is None:
            params = {}
        
        # Format datetime objects to strings if provided
        if start_time:
            if isinstance(start_time, datetime):
                params["startTime"] = start_time.isoformat()
            else:
                params["startTime"] = start_time
        
        if end_time:
            if isinstance(end_time, datetime):
                params["endTime"] = end_time.isoformat()
            else:
                params["endTime"] = end_time
        
        # Make the request
        response = self.client.get(f"{self.base_path}/errors", params=params)
        
        # Extract errors from the response
        if "items" in response:
            return response["items"]
        elif "elements" in response:
            return response["elements"]
        elif isinstance(response, list):
            return response
        else:
            self.logger.warning(f"Unexpected response format from get_errors endpoint: {response.keys() if isinstance(response, dict) else type(response)}")
            return []
