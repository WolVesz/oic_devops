"""
Workflows package for the OIC DevOps package.

This package provides higher-level workflow operations built on top of
the core API client to handle common OIC DevOps tasks.
"""

from oic_devops.workflows.base import BaseWorkflow, WorkflowResult
from oic_devops.workflows.connection import ConnectionWorkflows
from oic_devops.workflows.integration import IntegrationWorkflows
from oic_devops.workflows.deployment import DeploymentWorkflows
from oic_devops.workflows.monitoring import MonitoringWorkflows
from oic_devops.workflows.schedule import ScheduleWorkflows
from oic_devops.workflows.validation import ValidationWorkflows
from oic_devops.workflows.backup import BackupWorkflows

__all__ = [
    "BaseWorkflow",
    "WorkflowResult",
    "ConnectionWorkflows",
    "IntegrationWorkflows",
    "DeploymentWorkflows",
    "MonitoringWorkflows",
    "ScheduleWorkflows", 
    "ValidationWorkflows",
    "BackupWorkflows",
]
