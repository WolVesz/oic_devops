"""
Resources package for the OIC DevOps package.

This package contains resource-specific classes for interacting with
different types of OIC resources.
"""

from oic_devops.resources.connections import ConnectionsResource
from oic_devops.resources.integrations import IntegrationsResource
from oic_devops.resources.libraries import LibrariesResource
from oic_devops.resources.lookups import LookupsResource
from oic_devops.resources.monitoring import MonitoringResource
from oic_devops.resources.packages import PackagesResource

__all__ = [
	'ConnectionsResource',
	'IntegrationsResource',
	'LibrariesResource',
	'LookupsResource',
	'MonitoringResource',
	'PackagesResource',
]
