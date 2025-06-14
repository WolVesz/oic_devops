"""
OIC DevOps - A production-grade package for Oracle Integration Cloud DevOps.

This package provides functionality to interact with the Oracle Integration Cloud REST API
for managing integration tasks including Connections, Integrations, Libraries, Lookups,
Monitoring, and Packages.
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from oic_devops.client import OICClient
from oic_devops.exceptions import (
    OICError,
    OICAuthenticationError,
    OICResourceNotFoundError,
    OICConfigurationError,
    OICValidationError,
    OICAPIError,
)

__all__ = [
    "OICClient",
    "OICError",
    "OICAuthenticationError",
    "OICResourceNotFoundError",
    "OICConfigurationError",
    "OICValidationError",
    "OICAPIError",
]
