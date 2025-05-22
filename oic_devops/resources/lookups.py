"""
Lookups resource module for the OIC DevOps package.

This module provides functionality for managing OIC lookups.
"""

import os
from typing import Any, Dict, List, Optional

from oic_devops.exceptions import OICAPIError, OICValidationError
from oic_devops.resources.base import BaseResource


class LookupsResource(BaseResource):
	"""
	Class for managing OIC lookups.

	Provides methods for listing, retrieving, creating, updating, and
	deleting lookups, as well as importing and exporting lookups.
	"""

	def __init__(self, client):
		"""
		Initialize the lookups resource client.

		Args:
		    client: The parent OICClient instance.

		"""
		super().__init__(client)
		self.base_path = '/ic/api/integration/v1/lookups'

	def list(self, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
		"""
		List all lookups.

		Args:
		    params: Optional query parameters such as:
		        - limit: Maximum number of items to return.
		        - offset: Number of items to skip.
		        - fields: Comma-separated list of fields to include.
		        - q: Search query.
		        - orderBy: Field to order by.

		Returns:
		    List[Dict]: List of lookups.

		"""
		return super().list(params)

	def get(
		self, lookup_id: str, params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Get a specific lookup by ID.

		Args:
		    lookup_id: ID of the lookup to retrieve.
		    params: Optional query parameters.

		Returns:
		    Dict: The lookup data.

		"""
		return super().get(lookup_id, params)

	def create(
		self, data: Dict[str, Any], params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Create a new lookup.

		Args:
		    data: Lookup data, including:
		        - name: Name of the lookup.
		        - identifier: Unique identifier for the lookup.
		        - columns: List of column definitions.
		        - rows: List of row data.
		        - ... and other lookup-specific properties.
		    params: Optional query parameters.

		Returns:
		    Dict: The created lookup data.

		Raises:
		    OICValidationError: If required fields are missing.

		"""
		# Validate required fields
		required_fields = ['name', 'identifier', 'columns']
		for field in required_fields:
			if field not in data:
				raise OICValidationError(
					f'Missing required field for lookup creation: {field}'
				)

		return super().create(data, params)

	def update(
		self,
		lookup_id: str,
		data: Dict[str, Any],
		params: Optional[Dict[str, Any]] = None,
	) -> Dict[str, Any]:
		"""
		Update a specific lookup.

		Args:
		    lookup_id: ID of the lookup to update.
		    data: Updated lookup data.
		    params: Optional query parameters.

		Returns:
		    Dict: The updated lookup data.

		"""
		return super().update(lookup_id, data, params)

	def delete(
		self, lookup_id: str, params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Delete a specific lookup.

		Args:
		    lookup_id: ID of the lookup to delete.
		    params: Optional query parameters.

		Returns:
		    Dict: The response data.

		"""
		return super().delete(lookup_id, params)

	def export(
		self, lookup_id: str, file_path: str, params: Optional[Dict[str, Any]] = None
	) -> str:
		"""
		Export a specific lookup to a file.

		Args:
		    lookup_id: ID of the lookup to export.
		    file_path: Path to save the exported lookup file.
		    params: Optional query parameters.

		Returns:
		    str: Path to the exported lookup file.

		Raises:
		    OICAPIError: If the export fails.

		"""
		# Set custom headers for binary content
		headers = {'Accept': 'application/octet-stream'}

		# Make the export request
		response = self.client.request(
			'GET',
			self._get_endpoint(lookup_id, 'archive'),
			params=params,
			headers=headers,
		)

		# Check if the response contains binary content
		if 'content' in response and isinstance(response['content'], bytes):
			# Write the content to the file
			try:
				with open(file_path, 'wb') as f:
					f.write(response['content'])
				self.logger.info(f'Lookup exported to {file_path}')
				return file_path
			except Exception as e:
				raise OICAPIError(f'Failed to write export file: {e!s}')
		else:
			raise OICAPIError('Export response did not contain binary content')

	def import_lookup(
		self,
		file_path: str,
		data: Optional[Dict[str, Any]] = None,
		params: Optional[Dict[str, Any]] = None,
	) -> Dict[str, Any]:
		"""
		Import a lookup from a file.

		Args:
		    file_path: Path to the lookup file to import.
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
			raise OICValidationError(f'Lookup file not found: {file_path}')

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
			raise OICAPIError(f'Failed to import lookup: {e!s}')

	def get_data(
		self, lookup_id: str, params: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		Get the data for a specific lookup.

		Args:
		    lookup_id: ID of the lookup to get data for.
		    params: Optional query parameters.

		Returns:
		    Dict: The lookup data.

		"""
		return self.execute_action('data', lookup_id, params=params, method='GET')

	def update_data(
		self,
		lookup_id: str,
		data: Dict[str, Any],
		params: Optional[Dict[str, Any]] = None,
	) -> Dict[str, Any]:
		"""
		Update the data for a specific lookup.

		Args:
		    lookup_id: ID of the lookup to update data for.
		    data: Updated lookup data, including:
		        - rows: List of row data.
		    params: Optional query parameters.

		Returns:
		    Dict: The updated lookup data.

		Raises:
		    OICValidationError: If required fields are missing.

		"""
		# Validate required fields
		if 'rows' not in data:
			raise OICValidationError(
				'Missing required field for lookup data update: rows'
			)

		return self.execute_action(
			'data', lookup_id, data=data, params=params, method='PUT'
		)

	def export_all(
		self, directory_path: str, params: Optional[Dict[str, Any]] = None
	) -> List[str]:
		"""
		Export all lookup tables to a specified directory.

		Args:
		    directory_path: Path to the directory where lookup files will be saved.
		    params: Optional query parameters for listing and exporting lookups.

		Returns:
		    List[str]: List of paths to the exported lookup files.

		Raises:
		    OICValidationError: If the directory does not exist or is not writable.
		    OICAPIError: If any export operation fails.

		"""
		# Validate the directory
		if not os.path.exists(directory_path):
			try:
				os.makedirs(directory_path)
			except Exception as e:
				raise OICValidationError(
					f'Failed to create directory {directory_path}: {e!s}'
				)

		if not os.access(directory_path, os.W_OK):
			raise OICValidationError(f'Directory {directory_path} is not writable')

		# Get the list of lookups
		lookups = self.list(params)
		exported_files = []

		# Export each lookup
		for lookup in lookups:
			lookup_id = lookup.get('id')
			if not lookup_id:
				self.logger.warning(
					f'Skipping lookup with missing identifier: {lookup}'
				)
				continue

			# Create a safe filename from the lookup identifier
			safe_filename = ''.join(
				c if c.isalnum() or c in '-_.' else '_' for c in lookup_id
			)
			file_path = os.path.join(directory_path, f'{safe_filename}.csv')

			try:
				exported_path = self.export(lookup_id, file_path, params)
				exported_files.append(exported_path)
				self.logger.info(
					f'Successfully exported lookup {lookup_id} to {exported_path}'
				)
			except OICAPIError as e:
				self.logger.error(f'Failed to export lookup {lookup_id}: {e!s}')
				raise

		return exported_files
