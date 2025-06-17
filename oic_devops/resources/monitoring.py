"""
Monitoring resource module for the OIC DevOps package.

This module provides functionality for monitoring OIC resources.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from oic_devops.exceptions import OICAPIError, OICValidationError
from oic_devops.resources.base import BaseResource
from oic_devops.utils.str import camel_to_snake


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
		self.base_path = '/ic/api/integration/v1/monitoring'

	def df(self, **kwargs) -> pd.DataFrame:
		output = self.list_all(**kwargs)

		df = pd.DataFrame(output)
		df.columns = [camel_to_snake(col) for col in df.columns]

		df['instance_acquired_at'] = datetime.now()

		for col in [
			'instance_creation_date',
			'instance_date',
			'instance_processing_end_date',
			'instance_recieved_date',
		]:
			if col in df.columns:
				df[col] = pd.to_datetime(df[col])

		return df

	def list(self, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
		"""
		List all instances.

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
		return super().list(params, raw=True, resource_id='instances')

	def list_all(
		self,
		integration_id: Optional[str] = None,
		status: Optional[str] = None,
		timewindow: Optional[str] = 'RETENTIONPERIOD',
		start_time: Optional[Union[datetime, str]] = None,
		end_time: Optional[Union[datetime, str]] = None,
		limit: Optional[int] = 50,
		params: Optional[Dict[str, Any]] = None,
	) -> List[Dict[str, Any]]:
		"""
		Get integration instances.

		Args:
		    integration_id: Optional integration ID to filter by.
		    status: Optional status to filter by (e.g., "COMPLETED", "FAILED").
		    timewindow: 1h, 6h, 1d, 2d, 3d, RETENTIONPERIOD
		    start_time: Optional start time to filter by.
		    end_time: Optional end time to filter by.
		    limit: number of rows returned in a call.
		    params: Optional additional query parameters.

		Returns:
		    List[Dict]: List of integration instances.

		"""
		has_more = True
		output = []
		pages = 0
		expected_records = 9999999

		# Initialize parameters if None
		if params is None:
			params = dict()

		# Add filters to parameters
		if integration_id:
			params['integrationId'] = integration_id

		if status:
			params['status'] = status

		# Format datetime objects to strings if provided
		if start_time:
			if isinstance(start_time, datetime):
				params['startTime'] = start_time.isoformat()

		if end_time:
			if isinstance(end_time, datetime):
				params['endTime'] = end_time.isoformat()

		while has_more is True:
			try:
				params['offset'] = pages
				print(params)
				content = self.list(params=params)
				output.extend(content['items'])
				pages += content['totalResults']
				expected_records = content['totalRecordsCount']
				self.logger.info(f'Number of Instances Acquired in List: {pages}')

			except OICAPIError as excp:
				# has_more = content['hasMore'] #Rest API is borked here.
				if len(output) == expected_records:
					has_more = True
				elif len(output) > expected_records:
					has_more = True
					output = list(set(output))
				elif excp.status_code == 400:
					self.logger.info(f"""
                            
                            Monitoring.List_All has failed due to a 400 error code. 
                            
                            This is expected due to non-disclosed limitations of the monitor/instances api. You can 
                            only have a max of a 500 offset, 50 limit, and has_more is always false. 
                            
                            You recieved {len(output)} out of an expected: {expected_records}""")
					return output
				else:
					raise excp

		return output

	def get_instance(
		self, instance_id: str, params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Get a specific integration instance by ID.

		Args:
		    instance_id: ID of the instance to retrieve.
		    params: Optional query parameters.

		Returns:
		    Dict: The instance data.

		"""
		return self.client.get(
			f'{self.base_path}/instances/{instance_id}', params=params
		)

	def get_instance_activities(
		self, instance_id: str, params: Optional[Dict[str, Any]] = None
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
			f'{self.base_path}/instances/{instance_id}/activities', params=params
		)

		# Extract activities from the response
		if 'items' in response:
			return response['items']
		if 'elements' in response:
			return response['elements']
		if isinstance(response, list):
			return response
		self.logger.warning(
			f'Unexpected response format from get_instance_activities endpoint: {response.keys() if isinstance(response, dict) else type(response)}'
		)
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
		if direction not in ['request', 'response']:
			raise OICValidationError(
				f"Invalid payload direction: {direction}. Must be 'request' or 'response'."
			)

		return self.client.get(
			f'{self.base_path}/instances/{instance_id}/activities/{activity_id}/payload/{direction}',
			params=params,
		)

	def purge_instances(
		self, data: Dict[str, Any], params: Optional[Dict[str, Any]] = None
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
			f'{self.base_path}/instances/purge', data=data, params=params
		)

	def resubmit_instance(
		self, instance_id: str, params: Optional[Dict[str, Any]] = None
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
			f'{self.base_path}/instances/{instance_id}/resubmit', params=params
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
			params['integrationId'] = integration_id

		if interval:
			params['interval'] = interval

		# Format datetime objects to strings if provided
		if start_time:
			if isinstance(start_time, datetime):
				params['startTime'] = start_time.isoformat()
			else:
				params['startTime'] = start_time

		if end_time:
			if isinstance(end_time, datetime):
				params['endTime'] = end_time.isoformat()
			else:
				params['endTime'] = end_time

		# Make the request
		return self.client.get(f'{self.base_path}/integrationStats', params=params)

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
				params['startTime'] = start_time.isoformat()
			else:
				params['startTime'] = start_time

		if end_time:
			if isinstance(end_time, datetime):
				params['endTime'] = end_time.isoformat()
			else:
				params['endTime'] = end_time

		# Make the request
		response = self.client.get(f'{self.base_path}/errors', params=params)

		# Extract errors from the response
		if 'items' in response:
			return response['items']
		if 'elements' in response:
			return response['elements']
		if isinstance(response, list):
			return response
		self.logger.warning(
			f'Unexpected response format from get_errors endpoint: {response.keys() if isinstance(response, dict) else type(response)}'
		)
		return []
