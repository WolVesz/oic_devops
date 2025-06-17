"""
Exceptions module for the OIC DevOps package.

This module defines custom exceptions used throughout the package.
"""


class OICError(Exception):
	"""Base exception for all OIC DevOps errors."""


class OICAuthenticationError(OICError):
	"""Exception raised for authentication failures."""


class OICResourceNotFoundError(OICError):
	"""Exception raised when a resource is not found."""


class OICConfigurationError(OICError):
	"""Exception raised for configuration errors."""


class OICValidationError(OICError):
	"""Exception raised for validation errors."""


class OICAPIError(OICError):
	"""Exception raised for API errors."""

	def __init__(self, message, status_code=None, response=None):
		"""
		Initialize OICAPIError.

		Args:
		    message: Error message.
		    status_code: HTTP status code of the error.
		    response: Full response object.

		"""
		self.status_code = status_code
		self.response = response
		super().__init__(message)
