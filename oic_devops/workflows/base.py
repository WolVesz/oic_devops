"""
Base workflow module for the OIC DevOps package.

This module provides the base classes for all workflow operations.
"""

import logging
import time
import os
import json
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Union, Callable
from abc import ABC, abstractmethod

from oic_devops.client import OICClient
from oic_devops.exceptions import OICError


@dataclass
class WorkflowResult:
    """
    Data class for storing workflow execution results.
    
    Attributes:
        success: Whether the workflow was successful.
        message: A message describing the result.
        details: Detailed information about the execution.
        resources: Resources affected by the workflow.
        errors: Any errors that occurred during execution.
    """
    success: bool = True
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    resources: Dict[str, Any] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    
    def add_error(self, message: str, error: Optional[Exception] = None, resource_id: Optional[str] = None) -> None:
        """
        Add an error to the workflow result.
        
        Args:
            message: Error message.
            error: Optional exception that caused the error.
            resource_id: Optional ID of the resource that caused the error.
        """
        error_info = {
            "message": message,
            "timestamp": time.time(),
        }
        
        if error:
            error_info["exception"] = str(error)
            error_info["exception_type"] = error.__class__.__name__
        
        if resource_id:
            error_info["resource_id"] = resource_id
            
        self.errors.append(error_info)
        self.success = False
    
    def add_resource(self, resource_type: str, resource_id: str, data: Dict[str, Any]) -> None:
        """
        Add a resource to the workflow result.
        
        Args:
            resource_type: Type of the resource.
            resource_id: ID of the resource.
            data: Resource data.
        """
        if resource_type not in self.resources:
            self.resources[resource_type] = {}
            
        self.resources[resource_type][resource_id] = data
    
    def merge(self, other: 'WorkflowResult') -> 'WorkflowResult':
        """
        Merge another workflow result into this one.
        
        Args:
            other: The other workflow result to merge.
            
        Returns:
            WorkflowResult: The merged workflow result.
        """
        # Update success status - if either failed, the merged result is a failure
        if not other.success:
            self.success = False
            
        # Append messages if both have messages
        if self.message and other.message:
            self.message = f"{self.message}; {other.message}"
        elif other.message:
            self.message = other.message
            
        # Merge details
        self.details.update(other.details)
        
        # Merge resources
        for resource_type, resources in other.resources.items():
            if resource_type not in self.resources:
                self.resources[resource_type] = {}
                
            self.resources[resource_type].update(resources)
            
        # Merge errors
        self.errors.extend(other.errors)
        
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the workflow result to a dictionary.
        
        Returns:
            Dict: The workflow result as a dictionary.
        """
        return {
            "success": self.success,
            "message": self.message,
            "details": self.details,
            "resources": self.resources,
            "errors": self.errors,
        }
    
    def to_json(self, pretty: bool = False) -> str:
        """
        Convert the workflow result to a JSON string.
        
        Args:
            pretty: Whether to format the JSON with indentation.
            
        Returns:
            str: The workflow result as a JSON string.
        """
        indent = 2 if pretty else None
        return json.dumps(self.to_dict(), indent=indent, default=str)
    
    def save_to_file(self, file_path: str, pretty: bool = True) -> str:
        """
        Save the workflow result to a file.
        
        Args:
            file_path: Path to save the workflow result.
            pretty: Whether to format the JSON with indentation.
            
        Returns:
            str: The file path.
        """
        with open(file_path, "w") as f:
            f.write(self.to_json(pretty=pretty))
            
        return file_path
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowResult':
        """
        Create a workflow result from a dictionary.
        
        Args:
            data: The dictionary to create the workflow result from.
            
        Returns:
            WorkflowResult: The created workflow result.
        """
        return cls(
            success=data.get("success", True),
            message=data.get("message", ""),
            details=data.get("details", {}),
            resources=data.get("resources", {}),
            errors=data.get("errors", []),
        )
    
    @classmethod
    def from_json(cls, json_str: str) -> 'WorkflowResult':
        """
        Create a workflow result from a JSON string.
        
        Args:
            json_str: The JSON string to create the workflow result from.
            
        Returns:
            WorkflowResult: The created workflow result.
        """
        return cls.from_dict(json.loads(json_str))
    
    @classmethod
    def from_file(cls, file_path: str) -> 'WorkflowResult':
        """
        Create a workflow result from a file.
        
        Args:
            file_path: Path to the file containing the workflow result.
            
        Returns:
            WorkflowResult: The created workflow result.
        """
        with open(file_path, "r") as f:
            return cls.from_json(f.read())
    
    @classmethod
    def create_error(cls, message: str, error: Optional[Exception] = None) -> 'WorkflowResult':
        """
        Create a workflow result for an error.
        
        Args:
            message: Error message.
            error: Optional exception that caused the error.
            
        Returns:
            WorkflowResult: The created workflow result.
        """
        result = cls(success=False, message=message)
        result.add_error(message, error)
        return result


class BaseWorkflow(ABC):
    """
    Base class for all workflow operations.
    
    This class provides common functionality for all workflows, such as
    error handling, logging, and result formatting.
    """
    
    def __init__(self, client: OICClient, logger: Optional[logging.Logger] = None):
        """
        Initialize the workflow.
        
        Args:
            client: The OIC client to use for API operations.
            logger: Optional logger to use for logging.
        """
        self.client = client
        self.logger = logger or logging.getLogger(self.__class__.__name__)
    
    def execute_safely(
        self,
        operation: Callable,
        error_message: str,
        resource_id: Optional[str] = None,
        result: Optional[WorkflowResult] = None,
        **kwargs
    ) -> WorkflowResult:
        """
        Execute an operation safely, handling any errors.
        
        Args:
            operation: The operation to execute.
            error_message: The error message to use if the operation fails.
            resource_id: Optional ID of the resource being operated on.
            result: Optional workflow result to update with the execution result.
            **kwargs: Additional arguments to pass to the operation.
            
        Returns:
            WorkflowResult: The execution result.
        """
        if result is None:
            result = WorkflowResult()
            
        try:
            operation_result = operation(**kwargs)
            
            if isinstance(operation_result, WorkflowResult):
                result.merge(operation_result)
            elif isinstance(operation_result, dict):
                result.details.update(operation_result)
                
            return result
            
        except OICError as e:
            self.logger.error(f"{error_message}: {str(e)}")
            result.add_error(error_message, e, resource_id)
            return result
            
        except Exception as e:
            self.logger.exception(f"Unexpected error during {error_message}")
            result.add_error(f"Unexpected error during {error_message}", e, resource_id)
            return result
    
    def wait_for_operation(
        self,
        check_operation: Callable,
        check_result: Callable,
        max_attempts: int = 30,
        interval_seconds: int = 10,
        **kwargs
    ) -> WorkflowResult:
        """
        Wait for an operation to complete.
        
        Args:
            check_operation: The operation to execute to check status.
            check_result: A function that takes the result of check_operation
                and returns True if the operation is complete.
            max_attempts: Maximum number of attempts.
            interval_seconds: Interval between attempts in seconds.
            **kwargs: Additional arguments to pass to check_operation.
            
        Returns:
            WorkflowResult: The execution result.
        """
        result = WorkflowResult()
        
        for attempt in range(max_attempts):
            try:
                check_result_data = check_operation(**kwargs)
                
                if check_result(check_result_data):
                    result.details["attempts"] = attempt + 1
                    result.details["completed"] = True
                    return result
                    
            except OICError as e:
                self.logger.warning(f"Error checking operation status: {str(e)}")
                # Continue trying rather than failing immediately
                
            # Not complete yet, wait and try again
            self.logger.debug(f"Operation not complete, waiting {interval_seconds}s (attempt {attempt + 1}/{max_attempts})")
            time.sleep(interval_seconds)
        
        # If we get here, the operation didn't complete in time
        result.success = False
        result.message = "Operation did not complete in the allotted time"
        result.details["attempts"] = max_attempts
        result.details["completed"] = False
        result.add_error("Timeout waiting for operation to complete")
        return result
    
    @abstractmethod
    def execute(self, *args, **kwargs) -> WorkflowResult:
        """
        Execute the workflow.
        
        This method must be implemented by all workflow classes.
        
        Returns:
            WorkflowResult: The workflow execution result.
        """
        pass
