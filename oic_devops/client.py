"""
OIC REST API client module for the OIC DevOps package.

This module provides the main client class for interacting with the OIC REST API.
"""

import logging
from typing import Any, Dict, Optional

import requests
from requests.exceptions import RequestException

from oic_devops.config import OICConfig
from oic_devops.exceptions import (
	OICAPIError,
	OICAuthenticationError,
	OICResourceNotFoundError,
)
from oic_devops.resources.connections import ConnectionsResource
from oic_devops.resources.integrations import IntegrationsResource
from oic_devops.resources.libraries import LibrariesResource
from oic_devops.resources.lookups import LookupsResource
from oic_devops.resources.monitoring import MonitoringResource
from oic_devops.resources.packages import PackagesResource

# Set up logging
logger = logging.getLogger(__name__)


class OICClient:
	"""
	Main client class for interacting with the OIC REST API.

	This class handles authentication, request handling, and provides
	access to all resource-specific clients.
	"""

	def __init__(
		self,
		config_file: Optional[str] = None,
		profile: str = 'default',
		log_level: int = logging.INFO,
	):
		"""
		Initialize the OIC client.

		Args:
		    config_file: Path to the configuration file. If None, will look in default locations.
		    profile: The profile to use from the configuration file.
		    log_level: Logging level to use.

		"""
		# Set up logging
		logging.basicConfig(level=log_level)
		self.logger = logging.getLogger('oic_devops')
		self.logger.setLevel(log_level)

		# Load configuration
		self.config = OICConfig(config_file=config_file, profile=profile)
		self.logger.info(f'Initialized OIC client with profile: {profile}')

		# Prepare session
		self.session = requests.Session()
		self.session.verify = self.config.verify_ssl
		self.authenticate()

		# Initialize resources
		self._init_resources()

	def _init_resources(self):
		"""Initialize resource-specific clients."""
		self.connections = ConnectionsResource(self)
		self.integrations = IntegrationsResource(self)
		self.libraries = LibrariesResource(self)
		self.lookups = LookupsResource(self)
		self.monitoring = MonitoringResource(self)
		self.packages = PackagesResource(self)

	def authenticate(self) -> str:
		"""
		Authenticate with the OIC REST API and get an access token.

		Returns:
		    str: The access token.

		Raises:
		    OICAuthenticationError: If authentication fails.

		"""
		self.logger.debug('Authenticating with OIC REST API')

		# Prepare the request body
		data = {
			'grant_type': 'client_credentials',
			'scope': self.config.scope if self.config.scope else '',
		}

		try:
			response = self.session.post(
				self.config.auth_url,
				data=data,
				auth=(self.config.username, self.config.password),
				timeout=self.config.timeout,
			)

			if response.status_code == 200:
				auth_data = response.json()
				self._auth_token = auth_data.get('access_token')
				self.auth_expiration_time = auth_data.get('expires_in')

				if not self._auth_token:
					raise OICAuthenticationError(
						'No access token in authentication response'
					)

				self.session.headers.update(
					{
						'Authorization': f'Bearer {self._auth_token}',
						'Content-Type': 'application/json',
						'Accept': 'application/json',
					}
				)
				self.logger.debug('Authentication successful')
				return
			error_msg = f'Authentication failed with status code {response.status_code}'
			try:
				error_data = response.json()
				if 'detail' in error_data:
					error_msg += f': {error_data["detail"]}'
				else:
					error_msg += f': {response.text}'
			except ValueError:
				error_msg += f': {response.text}'

			raise OICAuthenticationError(error_msg)

		except RequestException as e:
			raise OICAuthenticationError(f'Authentication request failed: {e!s}')

	def get_auth_token(self) -> str:
		"""
		Get the authentication token, authenticating if necessary.

		Returns:
		    str: The authentication token.

		Raises:
		    OICAuthenticationError: If authentication fails.

		"""
		if not self._auth_token:
			return self.authenticate()
		return self._auth_token

	def _prepare_headers(
		self, custom_headers: Optional[Dict[str, str]] = None
	) -> Dict[str, str]:
		"""
		Prepare request headers, including authentication.

		Args:
		    custom_headers: Additional headers to include.

		Returns:
		    Dict: Prepared headers.

		"""
		headers = {
			'Authorization': f'Bearer {self.get_auth_token()}',
			'Content-Type': 'application/json',
			'Accept': 'application/json',
		}

		if custom_headers:
			headers.update(custom_headers)

		return headers

	def request(
		self,
		method: str,
		endpoint: str,
		params: Optional[Dict[str, Any]] = None,
		data: Optional[Dict[str, Any]] = None,
		files: Optional[Dict[str, Any]] = None,
		headers: Optional[Dict[str, str]] = None,
		retry_auth: bool = True,
	) -> Dict[str, Any]:
		"""
		Make a request to the OIC REST API.

		Args:
		    method: HTTP method to use (GET, POST, PUT, DELETE, etc).
		    endpoint: API endpoint to call.
		    params: Query parameters to include.
		    data: Data to send in the request body.
		    files: Files to send in the request.
		    headers: Additional headers to include.
		    retry_auth: Whether to retry the request if authentication fails.

		Returns:
		    Dict: Response data.

		Raises:
		    OICResourceNotFoundError: If the resource is not found.
		    OICAPIError: If the API returns an error.

		"""
		# Prepare URL
		if endpoint.startswith('http'):
			url = endpoint
		else:
			url = f'{self.config.instance_url}{endpoint}'

		# Prepare headers
		request_headers = self._prepare_headers(headers)

		# Extend Params for constant values
		if not params:
			params = dict()
		params['integrationInstance'] = self.config.identity_domain

		# Prepare request
		try:
			self.logger.debug(f'Making {method} request to {url}')

			# Prepare the data
			json_data = None
			if data is not None:
				json_data = data

			# Make the request
			response = self.session.request(
				method,
				url,
				params=params,
				json=json_data,
				files=files,
				headers=request_headers,
				timeout=self.config.timeout,
			)

			# Handle authentication errors
			if response.status_code == 401 and retry_auth:
				self.logger.debug('Authentication token expired, refreshing...')
				self._auth_token = None
				return self.request(
					method=method,
					endpoint=endpoint,
					params=params,
					data=data,
					files=files,
					headers=headers,
					retry_auth=False,  # Prevent infinite recursion
				)

			# Handle response
			if response.status_code == 404:
				raise OICResourceNotFoundError(
					f'Resource not found: {response.request.url}'
				)

			if response.status_code >= 400:
				error_msg = (
					f'API request failed with status code {response.status_code}'
				)
				try:
					error_data = response.json()
					if 'detail' in error_data:
						error_msg += f': {error_data["detail"]}'
					elif 'message' in error_data:
						error_msg += f': {error_data["message"]}'
					elif 'title' in error_data:
						error_msg += f': {error_data["title"]}'
					error_msg += f'\n\n{response.text}'
				except ValueError:
					error_msg += f': {response.text}'

				raise OICAPIError(
					message=error_msg,
					status_code=response.status_code,
					response=response,
				)

			# Return response data
			if response.status_code == 204 or not response.content:
				return {}

			try:
				return response.json()
			except ValueError:
				return {'content': response.content}

		except RequestException as e:
			raise OICAPIError(f'Request failed: {e!s}')

	def get(
		self,
		endpoint: str,
		params: Optional[Dict[str, Any]] = None,
		headers: Optional[Dict[str, str]] = None,
	) -> Dict[str, Any]:
		"""
		Make a GET request to the OIC REST API.

		Args:
		    endpoint: API endpoint to call.
		    params: Query parameters to include.
		    headers: Additional headers to include.

		Returns:
		    Dict: Response data.

		"""
		return self.request('GET', endpoint, params=params, headers=headers)

	def post(
		self,
		endpoint: str,
		data: Optional[Dict[str, Any]] = None,
		params: Optional[Dict[str, Any]] = None,
		files: Optional[Dict[str, Any]] = None,
		headers: Optional[Dict[str, str]] = None,
	) -> Dict[str, Any]:
		"""
		Make a POST request to the OIC REST API.

		Args:
		    endpoint: API endpoint to call.
		    data: Data to send in the request body.
		    params: Query parameters to include.
		    files: Files to send in the request.
		    headers: Additional headers to include.

		Returns:
		    Dict: Response data.

		"""
		return self.request(
			'POST', endpoint, params=params, data=data, files=files, headers=headers
		)

	def put(
		self,
		endpoint: str,
		data: Optional[Dict[str, Any]] = None,
		params: Optional[Dict[str, Any]] = None,
		headers: Optional[Dict[str, str]] = None,
	) -> Dict[str, Any]:
		"""
		Make a PUT request to the OIC REST API.

		Args:
		    endpoint: API endpoint to call.
		    data: Data to send in the request body.
		    params: Query parameters to include.
		    headers: Additional headers to include.

		Returns:
		    Dict: Response data.

		"""
		return self.request('PUT', endpoint, params=params, data=data, headers=headers)

	def delete(
		self,
		endpoint: str,
		params: Optional[Dict[str, Any]] = None,
		headers: Optional[Dict[str, str]] = None,
	) -> Dict[str, Any]:
		"""
		Make a DELETE request to the OIC REST API.

		Args:
		    endpoint: API endpoint to call.
		    params: Query parameters to include.
		    headers: Additional headers to include.

		Returns:
		    Dict: Response data.

		"""
		return self.request('DELETE', endpoint, params=params, headers=headers)

	def patch(
		self,
		endpoint: str,
		data: Optional[Dict[str, Any]] = None,
		params: Optional[Dict[str, Any]] = None,
		headers: Optional[Dict[str, str]] = None,
	) -> Dict[str, Any]:
		"""
		Make a PATCH request to the OIC REST API.

		Args:
		    endpoint: API endpoint to call.
		    data: Data to send in the request body.
		    params: Query parameters to include.
		    headers: Additional headers to include.

		Returns:
		    Dict: Response data.

		"""
		return self.request(
			'PATCH', endpoint, params=params, data=data, headers=headers
		)
