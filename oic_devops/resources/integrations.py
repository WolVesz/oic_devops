"""
Integrations resource module for the OIC DevOps package.

This module provides functionality for managing OIC integrations.
"""

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from oic_devops.exceptions import OICAPIError, OICValidationError
from oic_devops.resources.base import BaseResource
from oic_devops.utils.str import camel_to_snake


class IntegrationsResource(BaseResource):
	"""
	Class for managing OIC integrations.

	Provides methods for listing, retrieving, creating, updating, and
	deleting integrations, as well as activating, deactivating, and
	importing/exporting integrations.
	"""

	def __init__(self, client):
		"""
		Initialize the integrations resource client.

		Args:
		    client: The parent OICClient instance.

		"""
		super().__init__(client)
		self.base_path = '/ic/api/integration/v1/integrations'

	def list(self, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
		"""
		List all integrations.

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
		return super().list(params, raw=True)

	def list_all(self, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
		"""
		Automatically paginates through the API to provide the complete list of integrations.

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
			if not content.get('limit'):
				continue
			pages += content['limit']
			self.logger.info(f'Number of Integrations Acquired in List: {pages}')

		return output

	def df(self, explode = False, **kwargs):
		"""
		Creates a pandas Dataframe with the full contents of list_all.

		Args:
			explode: if you wish to break out integration by all used connections.
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

		df = pd.DataFrame(output)
		df.columns = [camel_to_snake(col) for col in df.columns]

		df['integrations_acquired_at'] = datetime.now()
		df['integrations_acquired_at'] = pd.to_datetime(df['integrations_acquired_at'])
		if explode:
			df = df.explode('end_points')
			df['end_points'] = df['end_points'].fillna({})
			df['connection_id'] = df['end_points'].apply(
				lambda x: x.get('connection', {}).get('id', None)
				if isinstance(x, dict)
				else None
			)
		return df

	# TODO: setup async for workflow speed ups
	def get(
		self, integration_id: str, params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Get a specific integration by ID.

		Args:
		    integration_id: ID of the integration to retrieve.
		    params: Optional query parameters.

		Returns:
		    Dict: The integration data.

		"""
		return super().get(integration_id, params)

	def create(
		self, data: Dict[str, Any], params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Create a new integration.

		Args:
		    data: Integration data, including:
		        - name: Name of the integration.
		        - identifier: Unique identifier for the integration.
		        - integrationType: Type of the integration (e.g., "SCHEDULED").
		        - ... and other integration-specific properties.
		    params: Optional query parameters.

		Returns:
		    Dict: The created integration data.

		Raises:
		    OICValidationError: If required fields are missing.

		"""
		# Validate required fields
		required_fields = ['name', 'identifier', 'integrationType']
		for field in required_fields:
			if field not in data:
				raise OICValidationError(
					f'Missing required field for integration creation: {field}'
				)

		return super().create(data, params)

	def update(
		self,
		integration_id: str,
		data: Dict[str, Any],
		params: Optional[Dict[str, Any]] = None,
		headers: Optional[Dict[str, Any]] = None,
	) -> Dict[str, Any]:
		"""
		Update a specific integration.

		Args:
		    integration_id: ID of the integration to update.
		    data: Updated integration data.
		    params: Optional query parameters.

		Returns:
		    Dict: The updated integration data.

		"""
		return super().update(integration_id, data=data, params=params, headers=headers)

	def delete(
		self, integration_id: str, params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Delete a specific integration.

		Args:
		    integration_id: ID of the integration to delete.
		    params: Optional query parameters.

		Returns:
		    Dict: The response data.

		"""
		return super().delete(integration_id, params)

	def activate(
		self, integration_id: str, params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Activate a specific integration.

		Args:
		    integration_id: ID of the integration to activate.
		    data: Optional activation data.
		    params: Optional query parameters.

		Returns:
		    Dict: The activation result data.

		"""
		headers = {'X-HTTP-Method-Override': 'PATCH'}

		body = {'status': 'ACTIVATED'}

		return self.update(integration_id, data=body, params=params, headers=headers)

	def resume_schedule(self, integration_id: str) -> Dict[str, Any]:
		"""
		Resume a specific integration schedule.

		Args:
		    integration_id: ID of the integration to activate.

		Returns:
		    Dict: The activation result data.

		"""
		return self.client.request(
			'POST',
			self._get_endpoint(resource_id=integration_id, action='schedule/resume'),
		)

	def deactivate(
		self,
		integration_id: str,
		stop_schedular: False,
		params: Optional[Dict[str, Any]] = None,
	) -> Dict[str, Any]:
		"""
		Deactivate a specific integration.

		Args:
		    integration_id: ID of the integration to deactivate.
		    stop_schedular: stops an integration's schedule
		    params: Optional query parameters.

		Returns:
		    Dict: The deactivation result data.

		"""
		headers = {'X-HTTP-Method-Override': 'PATCH'}

		body = {'status': 'CONFIGURED'}

		if stop_schedular:
			body['stopScheduleForDeactivation'] = 'true'

		return self.update(
			integration_id=integration_id, data=body, params=params, headers=headers
		)

	def export(
		self,
		integration_id: str,
		file_path: str,
		params: Optional[Dict[str, Any]] = None,
	) -> str:
		"""
		Export a specific integration to a file.

		Args:
		    integration_id: ID of the integration to export.
		    file_path: Path to save the exported integration file.
		    params: Optional query parameters.

		Returns:
		    str: Path to the exported integration file.

		Raises:
		    OICAPIError: If the export fails.

		"""
		# Set custom headers for binary content
		headers = {'Accept': 'application/octet-stream'}

		# Make the export request
		response = self.client.request(
			'GET',
			self._get_endpoint(integration_id, 'archive'),
			params=params,
			headers=headers,
		)

		# Check if the response contains binary content
		if 'content' in response and isinstance(response['content'], bytes):
			# Write the content to the file

			file_path = file_path.replace('|', '-')
			if not file_path.endswith('.zip'):
				file_path = file_path + '.zip'

			try:
				with open(file_path, 'wb') as f:
					f.write(response['content'])
				self.logger.info(f'Integration exported to {file_path}')
				return file_path
			except Exception as e:
				raise OICAPIError(f'Failed to write export file: {e!s}')
		else:
			raise OICAPIError('Export response did not contain binary content')

	def import_integration(
		self,
		file_path: str,
		data: Optional[Dict[str, Any]] = None,
		params: Optional[Dict[str, Any]] = None,
	) -> Dict[str, Any]:
		"""
		Import an integration from a file.

		Args:
		    file_path: Path to the integration file to import.
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
			raise OICValidationError(f'Integration file not found: {file_path}')

		# Set up the file for upload
		try:
			with open(file_path, 'rb') as f:
				files = {
					'file': (os.path.basename(file_path), f, 'application/octet-stream')
				}

				# Make the import request with data as form fields
				headers = {'Accept': 'application/json'}

				# Make a custom request that includes both files and form data
				return self.client.request(
					'POST',
					self._get_endpoint(action='import'),
					data=data,
					params=params,
					files=files,
					headers=headers,
				)
		except Exception as e:
			raise OICAPIError(f'Failed to import integration: {e!s}')

	def clone(
		self,
		integration_id: str,
		data: Dict[str, Any],
		params: Optional[Dict[str, Any]] = None,
	) -> Dict[str, Any]:
		"""
		Clone a specific integration.

		Args:
		    integration_id: ID of the integration to clone.
		    data: Data for the cloned integration, including:
		        - name: Name of the cloned integration.
		        - identifier: Unique identifier for the cloned integration.
		    params: Optional query parameters.

		Returns:
		    Dict: The cloned integration data.

		Raises:
		    OICValidationError: If required fields are missing.

		"""
		# Validate required fields
		required_fields = ['name', 'identifier']
		for field in required_fields:
			if field not in data:
				raise OICValidationError(
					f'Missing required field for integration cloning: {field}'
				)

		return self.execute_action(
			'clone', integration_id, data=data, params=params, method='POST'
		)

	def get_types(
		self, params: Optional[Dict[str, Any]] = None
	) -> List[Dict[str, Any]]:
		"""
		Get all available integration types.

		Args:
		    params: Optional query parameters.

		Returns:
		    List[Dict]: List of integration types.

		"""
		response = self.client.get(f'{self.base_path}/types', params=params)

		if 'items' in response:
			return response['items']
		if 'elements' in response:
			return response['elements']
		if isinstance(response, list):
			return response
		self.logger.warning(
			f'Unexpected response format from get_types endpoint: {response.keys() if isinstance(response, dict) else type(response)}'
		)
		return []

	def get_type(
		self, type_id: str, params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Get a specific integration type by ID.

		Args:
		    type_id: ID of the integration type to retrieve.
		    params: Optional query parameters.

		Returns:
		    Dict: The integration type data.

		"""
		return self.client.get(f'{self.base_path}/types/{type_id}', params=params)
