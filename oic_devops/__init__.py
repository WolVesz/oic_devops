"""
OIC DevOps - A production-grade package for Oracle Integration Cloud DevOps.

This package provides functionality to interact with the Oracle Integration Cloud REST API
for managing integration tasks including Connections, Integrations, Libraries, Lookups,
Monitoring, and Packages.
"""

__version__ = '0.1.0'
__author__ = 'Claude & WolVesz'
__email__ = 's.com'

from oic_devops.client import OICClient
from oic_devops.exceptions import (
	OICAPIError,
	OICAuthenticationError,
	OICConfigurationError,
	OICError,
	OICResourceNotFoundError,
	OICValidationError,
)

__all__ = [
	'OICAPIError',
	'OICAuthenticationError',
	'OICClient',
	'OICConfigurationError',
	'OICError',
	'OICResourceNotFoundError',
	'OICValidationError',
]
