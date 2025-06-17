"""
Libraries resource module for the OIC DevOps package.

This module provides functionality for managing OIC libraries.
"""

import os
from typing import Any, Dict, List, Optional

from oic_devops.exceptions import OICAPIError, OICValidationError
from oic_devops.resources.base import BaseResource


class LibrariesResource(BaseResource):
	"""
	Class for managing OIC libraries.

	Provides methods for listing, retrieving, creating, updating, and
	deleting libraries, as well as importing and exporting libraries.
	"""

	def __init__(self, client):
		"""
		Initialize the libraries resource client.

		Args:
		    client: The parent OICClient instance.

		"""
		super().__init__(client)
		self.base_path = '/ic/api/integration/v1/libraries'

	def list(self, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
		"""
		List all libraries.

		Args:
		    params: Optional query parameters such as:
		        - limit: Maximum number of items to return.
		        - offset: Number of items to skip.
		        - fields: Comma-separated list of fields to include.
		        - q: Search query.
		        - orderBy: Field to order by.

		Returns:
		    List[Dict]: List of libraries.

		"""
		return super().list(params)

	def get(
		self, library_id: str, params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Get a specific library by ID.

		Args:
		    library_id: ID of the library to retrieve.
		    params: Optional query parameters.

		Returns:
		    Dict: The library data.

		"""
		return super().get(library_id, params)

	def create(
		self, data: Dict[str, Any], params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Create a new library.

		Args:
		    data: Library data, including:
		        - name: Name of the library.
		        - identifier: Unique identifier for the library.
		        - ... and other library-specific properties.
		    params: Optional query parameters.

		Returns:
		    Dict: The created library data.

		Raises:
		    OICValidationError: If required fields are missing.

		"""
		# Validate required fields
		required_fields = ['name', 'identifier']
		for field in required_fields:
			if field not in data:
				raise OICValidationError(
					f'Missing required field for library creation: {field}'
				)

		return super().create(data, params)

	def update(
		self,
		library_id: str,
		data: Dict[str, Any],
		params: Optional[Dict[str, Any]] = None,
	) -> Dict[str, Any]:
		"""
		Update a specific library.

		Args:
		    library_id: ID of the library to update.
		    data: Updated library data.
		    params: Optional query parameters.

		Returns:
		    Dict: The updated library data.

		"""
		return super().update(library_id, data, params)

	def delete(
		self, library_id: str, params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Delete a specific library.

		Args:
		    library_id: ID of the library to delete.
		    params: Optional query parameters.

		Returns:
		    Dict: The response data.

		"""
		return super().delete(library_id, params)

	def export(
		self, library_id: str, file_path: str, params: Optional[Dict[str, Any]] = None
	) -> str:
		"""
		Export a specific library to a file.

		Args:
		    library_id: ID of the library to export.
		    file_path: Path to save the exported library file.
		    params: Optional query parameters.

		Returns:
		    str: Path to the exported library file.

		Raises:
		    OICAPIError: If the export fails.

		"""
		# Set custom headers for binary content
		headers = {'Accept': 'application/octet-stream'}

		# Make the export request
		response = self.client.request(
			'GET',
			self._get_endpoint(library_id, 'export'),
			params=params,
			headers=headers,
		)

		# Check if the response contains binary content
		if 'content' in response and isinstance(response['content'], bytes):
			# Write the content to the file
			try:
				with open(file_path, 'wb') as f:
					f.write(response['content'])
				self.logger.info(f'Library exported to {file_path}')
				return file_path
			except Exception as e:
				raise OICAPIError(f'Failed to write export file: {e!s}')
		else:
			raise OICAPIError('Export response did not contain binary content')

	def import_library(
		self,
		file_path: str,
		data: Optional[Dict[str, Any]] = None,
		params: Optional[Dict[str, Any]] = None,
	) -> Dict[str, Any]:
		"""
		Import a library from a file.

		Args:
		    file_path: Path to the library file to import.
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
			raise OICValidationError(f'Library file not found: {file_path}')

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
			raise OICAPIError(f'Failed to import library: {e!s}')

	def get_types(
		self, params: Optional[Dict[str, Any]] = None
	) -> List[Dict[str, Any]]:
		"""
		Get all available library types.

		Args:
		    params: Optional query parameters.

		Returns:
		    List[Dict]: List of library types.

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
		Get a specific library type by ID.

		Args:
		    type_id: ID of the library type to retrieve.
		    params: Optional query parameters.

		Returns:
		    Dict: The library type data.

		"""
		return self.client.get(f'{self.base_path}/types/{type_id}', params=params)
