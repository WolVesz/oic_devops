"""
Monitoring workflows module for the OIC DevOps package.

This module provides workflow operations for monitoring OIC resources,
including error tracking, performance metrics, and health checks.
"""

import os
import logging
import time
import datetime
import json
import csv
from typing import Dict, Any, List, Optional, Union, Tuple

from oic_devops.client import OICClient
from oic_devops.exceptions import OICError
from oic_devops.workflows.base import BaseWorkflow, WorkflowResult


class MonitoringWorkflows(BaseWorkflow):
    """
    Workflow operations for monitoring OIC resources.
    
    This class provides higher-level operations for monitoring OIC resources,
    including error tracking, performance metrics, and health checks.
    """
    
    def execute(self, *args, **kwargs) -> WorkflowResult:
        """
        Execute the specified monitoring workflow.
        
        This is a dispatcher method that calls the appropriate workflow
        based on the operation argument.
        
        Args:
            operation: The workflow operation to execute.
            **kwargs: Additional arguments specific to the workflow.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        operation = kwargs.pop("operation", None)
        
        if operation == "health_check":
            return self.perform_health_check(**kwargs)
        elif operation == "error_analysis":
            return self.analyze_errors(**kwargs)
        elif operation == "performance_metrics":
            return self.collect_performance_metrics(**kwargs)
        elif operation == "purge_instances":
            return self.purge_integration_instances(**kwargs)
        elif operation == "generate_report":
            return self.generate_monitoring_report(**kwargs)
        else:
            result = WorkflowResult(success=False, message=f"Unknown monitoring workflow operation: {operation}")
            result.add_error(f"Unknown operation: {operation}")
            return result
    
    def perform_health_check(
        self,
        check_integrations: bool = True,
        check_connections: bool = True,
        test_connections: bool = False,
        integration_filter: Optional[str] = None,
        connection_filter: Optional[str] = None
    ) -> WorkflowResult:
        """
        Perform a health check of the OIC instance.
        
        This workflow:
        1. Checks overall instance health
        2. Checks integration status
        3. Checks connection status
        4. Optionally tests connections
        
        Args:
            check_integrations: Whether to check integration health.
            check_connections: Whether to check connection health.
            test_connections: Whether to test connections (more intensive).
            integration_filter: Optional filter to apply to integration checks.
            connection_filter: Optional filter to apply to connection checks.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        result.message = "Performing OIC health check"
        
        # Initialize health check summary
        summary = {
            "timestamp": datetime.datetime.now().isoformat(),
            "instance": {},
            "integrations": {},
            "connections": {}
        }
        
        # Check instance health
        try:
            self.logger.info("Checking instance health")
            instance_stats = self.client.monitoring.get_instance_stats()
            
            # Extract key instance metrics
            summary["instance"] = {
                "status": "healthy",  # Default to healthy
                "stats": instance_stats
            }
            
            # Look for critical metrics that would indicate unhealthy instance
            if "stats" in instance_stats:
                instance_stats = instance_stats["stats"]
                
                # Check for high error rates
                if "errors" in instance_stats and "total" in instance_stats:
                    total = instance_stats.get("total", 0)
                    errors = instance_stats.get("errors", 0)
                    
                    if total > 0 and errors / total > 0.2:  # If error rate > 20%
                        summary["instance"]["status"] = "degraded"
                        summary["instance"]["issues"] = ["High error rate"]
            
            result.details["instance_health"] = summary["instance"]
            
        except OICError as e:
            self.logger.error(f"Failed to check instance health: {str(e)}")
            summary["instance"]["status"] = "unknown"
            summary["instance"]["error"] = str(e)
            result.add_error("Failed to check instance health", e)
        
        # Check integration health
        if check_integrations:
            try:
                self.logger.info("Checking integration health")
                params = {}
                if integration_filter:
                    params["q"] = integration_filter
                
                integrations = self.client.integrations.list(params=params)
                
                # Process integration status
                status_counts = {
                    "ACTIVATED": 0,
                    "CONFIGURED": 0,
                    "ERROR": 0,
                    "other": 0
                }
                
                problematic_integrations = []
                
                for integration in integrations:
                    integration_id = integration.get("id")
                    integration_name = integration.get("name", "Unknown")
                    status = integration.get("status", "UNKNOWN")
                    
                    # Count status
                    if status in status_counts:
                        status_counts[status] += 1
                    else:
                        status_counts["other"] += 1
                    
                    # Add to problematic integrations if in error state
                    if status == "ERROR":
                        problematic_integrations.append({
                            "id": integration_id,
                            "name": integration_name,
                            "status": status
                        })
                        
                        result.add_resource("integration", integration_id, {
                            "name": integration_name,
                            "status": status,
                            "health": "error"
                        })
                
                # Determine overall integration health
                integration_health = "healthy"
                if problematic_integrations:
                    integration_health = "degraded"
                    
                summary["integrations"] = {
                    "status": integration_health,
                    "counts": status_counts,
                    "total": len(integrations),
                    "problematic": problematic_integrations
                }
                
                result.details["integration_health"] = summary["integrations"]
                
            except OICError as e:
                self.logger.error(f"Failed to check integration health: {str(e)}")
                summary["integrations"]["status"] = "unknown"
                summary["integrations"]["error"] = str(e)
                result.add_error("Failed to check integration health", e)
        
        # Check connection health
        if check_connections:
            try:
                self.logger.info("Checking connection health")
                params = {}
                if connection_filter:
                    params["q"] = connection_filter
                
                connections = self.client.connections.list(params=params)
                
                # Process connection status
                connection_health = "healthy"
                problematic_connections = []
                
                for connection in connections:
                    connection_id = connection.get("id")
                    connection_name = connection.get("name", "Unknown")
                    status = connection.get("status", "UNKNOWN")
                    
                    # If we're testing connections, actually test them
                    if test_connections and connection_id:
                        try:
                            self.logger.info(f"Testing connection: {connection_name}")
                            test_result = self.client.connections.test(connection_id)
                            
                            test_status = test_result.get("status", "UNKNOWN")
                            if test_status != "SUCCESS":
                                connection_health = "degraded"
                                problematic_connections.append({
                                    "id": connection_id,
                                    "name": connection_name,
                                    "status": status,
                                    "test_result": test_result
                                })
                                
                                result.add_resource("connection", connection_id, {
                                    "name": connection_name,
                                    "status": status,
                                    "test_status": test_status,
                                    "health": "error"
                                })
                            else:
                                result.add_resource("connection", connection_id, {
                                    "name": connection_name,
                                    "status": status,
                                    "test_status": test_status,
                                    "health": "healthy"
                                })
                                
                        except OICError as e:
                            self.logger.warning(f"Failed to test connection {connection_name}: {str(e)}")
                            connection_health = "degraded"
                            problematic_connections.append({
                                "id": connection_id,
                                "name": connection_name,
                                "status": status,
                                "error": str(e)
                            })
                            
                            result.add_resource("connection", connection_id, {
                                "name": connection_name,
                                "status": status,
                                "error": str(e),
                                "health": "error"
                            })
                    
                    # Otherwise just record the status
                    elif status != "CONFIGURED" and status != "ACTIVATED":
                        connection_health = "degraded"
                        problematic_connections.append({
                            "id": connection_id,
                            "name": connection_name,
                            "status": status
                        })
                        
                        result.add_resource("connection", connection_id, {
                            "name": connection_name,
                            "status": status,
                            "health": "error"
                        })
                
                summary["connections"] = {
                    "status": connection_health,
                    "total": len(connections),
                    "problematic": problematic_connections,
                    "tested": test_connections
                }
                
                result.details["connection_health"] = summary["connections"]
                
            except OICError as e:
                self.logger.error(f"Failed to check connection health: {str(e)}")
                summary["connections"]["status"] = "unknown"
                summary["connections"]["error"] = str(e)
                result.add_error("Failed to check connection health", e)
        
        # Determine overall health
        overall_health = "healthy"
        health_parts = []
        
        if summary["instance"]["status"] != "healthy":
            overall_health = "degraded"
            health_parts.append("instance")
        
        if check_integrations and summary["integrations"].get("status") != "healthy":
            overall_health = "degraded"
            health_parts.append("integrations")
        
        if check_connections and summary["connections"].get("status") != "healthy":
            overall_health = "degraded"
            health_parts.append("connections")
        
        # Update result
        result.details["overall_health"] = overall_health
        result.details["health_summary"] = summary
        
        # Build result message
        if overall_health == "healthy":
            result.message = "OIC instance is healthy"
        else:
            result.success = False
            result.message = f"OIC health check found issues with: {', '.join(health_parts)}"
        
        return result
    
    def analyze_errors(
        self,
        start_time: Optional[Union[str, datetime.datetime]] = None,
        end_time: Optional[Union[str, datetime.datetime]] = None,
        integration_id: Optional[str] = None,
        error_type: Optional[str] = None,
        group_by: str = "integration",  # "integration", "error_type", "time"
        generate_report: bool = False,
        report_file: Optional[str] = None
    ) -> WorkflowResult:
        """
        Analyze errors in OIC.
        
        This workflow:
        1. Gathers error information from OIC
        2. Analyzes and categorizes errors
        3. Optionally generates a report
        
        Args:
            start_time: Optional start time for the analysis window.
            end_time: Optional end time for the analysis window.
            integration_id: Optional specific integration to analyze.
            error_type: Optional specific error type to focus on.
            group_by: How to group errors in the analysis.
            generate_report: Whether to generate an error report file.
            report_file: Path to save the error report if generated.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        
        # Format the time range for messaging
        time_range_str = ""
        if start_time:
            if isinstance(start_time, datetime.datetime):
                start_time_str = start_time.isoformat()
            else:
                start_time_str = start_time
            time_range_str = f"from {start_time_str} "
        
        if end_time:
            if isinstance(end_time, datetime.datetime):
                end_time_str = end_time.isoformat()
            else:
                end_time_str = end_time
            if time_range_str:
                time_range_str += f"to {end_time_str}"
            else:
                time_range_str = f"until {end_time_str}"
        
        integration_str = f" for integration {integration_id}" if integration_id else ""
        error_str = f" with error type {error_type}" if error_type else ""
        
        result.message = f"Analyzing errors{integration_str}{error_str} {time_range_str}"
        
        # Convert datetime objects to strings for API calls
        api_start_time = None
        api_end_time = None
        
        if start_time:
            if isinstance(start_time, datetime.datetime):
                api_start_time = start_time.isoformat()
            else:
                api_start_time = start_time
        
        if end_time:
            if isinstance(end_time, datetime.datetime):
                api_end_time = end_time.isoformat()
            else:
                api_end_time = end_time
        
        # Get error information
        try:
            self.logger.info("Getting error information")
            
            # Get errors from monitoring API
            monitoring_errors = self.client.monitoring.get_errors(
                start_time=api_start_time,
                end_time=api_end_time
            )
            
            # Get failed integration instances
            params = {
                "status": "FAILED"
            }
            
            if integration_id:
                params["integrationId"] = integration_id
                
            if api_start_time:
                params["startTime"] = api_start_time
                
            if api_end_time:
                params["endTime"] = api_end_time
                
            failed_instances = self.client.monitoring.get_instances(**params)
            
            if not monitoring_errors and not failed_instances:
                result.message = f"No errors found {time_range_str}"
                return result
            
            # Process errors and instances to build a comprehensive error analysis
            error_analysis = {
                "by_integration": {},
                "by_error_type": {},
                "by_time": {},
                "total_errors": 0,
                "total_failed_instances": len(failed_instances)
            }
            
            # Process monitoring errors
            for error in monitoring_errors:
                # Extract error details
                error_type = error.get("type", "Unknown")
                if error_type_filter and error_type != error_type_filter:
                    continue
                
                error_time = error.get("timestamp", "Unknown")
                error_message = error.get("message", "Unknown")
                error_integration = error.get("integrationId", "Unknown")
                error_integration_name = error.get("integrationName", "Unknown")
                
                # Skip if not matching integration filter
                if integration_id and error_integration != integration_id:
                    continue
                
                # Add to total count
                error_analysis["total_errors"] += 1
                
                # Group by integration
                if error_integration not in error_analysis["by_integration"]:
                    error_analysis["by_integration"][error_integration] = {
                        "name": error_integration_name,
                        "count": 0,
                        "error_types": {},
                        "recent_errors": []
                    }
                
                integration_data = error_analysis["by_integration"][error_integration]
                integration_data["count"] += 1
                
                # Track error types within integration
                if error_type not in integration_data["error_types"]:
                    integration_data["error_types"][error_type] = 0
                integration_data["error_types"][error_type] += 1
                
                # Add to recent errors (limit to 5)
                if len(integration_data["recent_errors"]) < 5:
                    integration_data["recent_errors"].append({
                        "time": error_time,
                        "message": error_message,
                        "type": error_type
                    })
                
                # Group by error type
                if error_type not in error_analysis["by_error_type"]:
                    error_analysis["by_error_type"][error_type] = {
                        "count": 0,
                        "integrations": {},
                        "recent_errors": []
                    }
                
                error_type_data = error_analysis["by_error_type"][error_type]
                error_type_data["count"] += 1
                
                # Track integrations with this error type
                if error_integration not in error_type_data["integrations"]:
                    error_type_data["integrations"][error_integration] = {
                        "name": error_integration_name,
                        "count": 0
                    }
                error_type_data["integrations"][error_integration]["count"] += 1
                
                # Add to recent errors (limit to 5)
                if len(error_type_data["recent_errors"]) < 5:
                    error_type_data["recent_errors"].append({
                        "time": error_time,
                        "message": error_message,
                        "integration": error_integration_name
                    })
                
                # Group by time (daily buckets)
                if error_time != "Unknown":
                    try:
                        # Parse the timestamp and get date part
                        if isinstance(error_time, str):
                            date_part = error_time.split("T")[0]
                        else:
                            date_part = error_time.strftime("%Y-%m-%d")
                            
                        if date_part not in error_analysis["by_time"]:
                            error_analysis["by_time"][date_part] = {
                                "count": 0,
                                "error_types": {},
                                "integrations": {}
                            }
                            
                        time_data = error_analysis["by_time"][date_part]
                        time_data["count"] += 1
                        
                        # Track error types by day
                        if error_type not in time_data["error_types"]:
                            time_data["error_types"][error_type] = 0
                        time_data["error_types"][error_type] += 1
                        
                        # Track integrations by day
                        if error_integration not in time_data["integrations"]:
                            time_data["integrations"][error_integration] = {
                                "name": error_integration_name,
                                "count": 0
                            }
                        time_data["integrations"][error_integration]["count"] += 1
                    except:
                        # Just skip time grouping if timestamp is invalid
                        pass
            
            # Process failed instances for more details
            for instance in failed_instances:
                instance_id = instance.get("id")
                integration_id_from_instance = instance.get("integrationId", "Unknown")
                integration_name = instance.get("integrationName", "Unknown")
                failure_message = instance.get("message", "Unknown")
                
                # Extract error type from failure message (simplistic approach)
                instance_error_type = "Unknown"
                if "connection" in failure_message.lower():
                    instance_error_type = "CONNECTION_ERROR"
                elif "timeout" in failure_message.lower():
                    instance_error_type = "TIMEOUT_ERROR"
                elif "validation" in failure_message.lower():
                    instance_error_type = "VALIDATION_ERROR"
                else:
                    instance_error_type = "RUNTIME_ERROR"
                
                # Skip if not matching error type filter
                if error_type and instance_error_type != error_type:
                    continue
                
                # Add instance to resource
                result.add_resource("instance", instance_id, {
                    "integrationId": integration_id_from_instance,
                    "integrationName": integration_name,
                    "status": instance.get("status"),
                    "startTime": instance.get("startTime"),
                    "endTime": instance.get("endTime"),
                    "message": failure_message,
                    "error_type": instance_error_type
                })
            
            # Determine most common errors
            top_integrations = sorted(
                list(error_analysis["by_integration"].items()),
                key=lambda x: x[1]["count"],
                reverse=True
            )[:5]
            
            top_error_types = sorted(
                list(error_analysis["by_error_type"].items()),
                key=lambda x: x[1]["count"],
                reverse=True
            )[:5]
            
            # Build error summary based on grouping
            if group_by == "integration":
                error_summary = {
                    "top_errors_by_integration": [
                        {
                            "id": integration_id,
                            "name": data["name"],
                            "count": data["count"],
                            "top_error_types": sorted(
                                list(data["error_types"].items()),
                                key=lambda x: x[1],
                                reverse=True
                            )[:3]
                        }
                        for integration_id, data in top_integrations
                    ]
                }
            elif group_by == "error_type":
                error_summary = {
                    "top_error_types": [
                        {
                            "type": error_type,
                            "count": data["count"],
                            "top_integrations": sorted(
                                list(data["integrations"].items()),
                                key=lambda x: x[1]["count"],
                                reverse=True
                            )[:3]
                        }
                        for error_type, data in top_error_types
                    ]
                }
            elif group_by == "time":
                # Sort dates chronologically
                sorted_dates = sorted(error_analysis["by_time"].keys())
                error_summary = {
                    "errors_by_date": [
                        {
                            "date": date,
                            "count": error_analysis["by_time"][date]["count"],
                            "top_error_types": sorted(
                                list(error_analysis["by_time"][date]["error_types"].items()),
                                key=lambda x: x[1],
                                reverse=True
                            )[:3]
                        }
                        for date in sorted_dates
                    ]
                }
            
            # Add summary to result
            result.details["error_analysis"] = error_analysis
            result.details["error_summary"] = error_summary
            result.details["grouping"] = group_by
            
            # Generate error report if requested
            if generate_report:
                if not report_file:
                    # Create default report name
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    report_file = f"oic_error_report_{timestamp}.json"
                
                try:
                    # Ensure directory exists
                    report_dir = os.path.dirname(report_file)
                    if report_dir and not os.path.exists(report_dir):
                        os.makedirs(report_dir)
                    
                    # Write report
                    with open(report_file, 'w') as f:
                        json.dump({
                            "error_analysis": error_analysis,
                            "error_summary": error_summary,
                            "metadata": {
                                "timestamp": datetime.datetime.now().isoformat(),
                                "time_range": time_range_str,
                                "integration_id": integration_id,
                                "error_type": error_type,
                                "group_by": group_by
                            }
                        }, f, indent=2)
                    
                    result.details["report_file"] = report_file
                    self.logger.info(f"Error report generated: {report_file}")
                except Exception as e:
                    self.logger.error(f"Failed to generate error report: {str(e)}")
                    result.add_error("Failed to generate error report", e)
            
            # Build result message
            total_errors = error_analysis["total_errors"]
            total_failed = error_analysis["total_failed_instances"]
            
            if integration_id:
                result.message = f"Found {total_errors} errors and {total_failed} failed instances for integration {integration_id} {time_range_str}"
            else:
                result.message = f"Found {total_errors} errors and {total_failed} failed instances {time_range_str}"
                
            # Add top error info to message if available
            if top_integrations and top_error_types:
                top_integration = top_integrations[0]
                top_error = top_error_types[0]
                
                result.message += f". Most errors: {top_integration[1]['count']} in {top_integration[1]['name']} integration, {top_error[1]['count']} of type {top_error[0]}."
                
            if generate_report and "report_file" in result.details:
                result.message += f" Report generated: {result.details['report_file']}"
                
        except OICError as e:
            self.logger.error(f"Failed to analyze errors: {str(e)}")
            result.add_error("Failed to analyze errors", e)
            result.success = False
            result.message = f"Failed to analyze errors: {str(e)}"
            
        return result
    
    def collect_performance_metrics(
        self,
        start_time: Optional[Union[str, datetime.datetime]] = None,
        end_time: Optional[Union[str, datetime.datetime]] = None,
        integration_id: Optional[str] = None,
        interval: str = "day",  # "hour", "day", "week", "month"
        metrics: List[str] = ["counts", "durations", "errors"],
        generate_report: bool = False,
        report_file: Optional[str] = None
    ) -> WorkflowResult:
        """
        Collect performance metrics for OIC.
        
        This workflow:
        1. Gathers performance metrics from OIC
        2. Analyzes and summarizes the metrics
        3. Optionally generates a performance report
        
        Args:
            start_time: Optional start time for metrics collection.
            end_time: Optional end time for metrics collection.
            integration_id: Optional specific integration to collect metrics for.
            interval: Time interval for grouping metrics.
            metrics: List of metric types to collect.
            generate_report: Whether to generate a performance report.
            report_file: Path to save the performance report if generated.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        
        # Format the time range for messaging
        time_range_str = ""
        if start_time:
            if isinstance(start_time, datetime.datetime):
                start_time_str = start_time.isoformat()
            else:
                start_time_str = start_time
            time_range_str = f"from {start_time_str} "
        
        if end_time:
            if isinstance(end_time, datetime.datetime):
                end_time_str = end_time.isoformat()
            else:
                end_time_str = end_time
            if time_range_str:
                time_range_str += f"to {end_time_str}"
            else:
                time_range_str = f"until {end_time_str}"
        
        integration_str = f" for integration {integration_id}" if integration_id else ""
        
        result.message = f"Collecting performance metrics{integration_str} {time_range_str}"
        
        # Convert datetime objects to strings for API calls
        api_start_time = None
        api_end_time = None
        
        if start_time:
            if isinstance(start_time, datetime.datetime):
                api_start_time = start_time.isoformat()
            else:
                api_start_time = start_time
        
        if end_time:
            if isinstance(end_time, datetime.datetime):
                api_end_time = end_time.isoformat()
            else:
                api_end_time = end_time
        
        # Collect performance metrics
        try:
            self.logger.info("Collecting performance metrics")
            
            # Get integration stats
            integration_stats = self.client.monitoring.get_integration_stats(
                integration_id=integration_id,
                start_time=api_start_time,
                end_time=api_end_time,
                interval=interval
            )
            
            # Get instance details for durations
            params = {}
            if integration_id:
                params["integrationId"] = integration_id
                
            if api_start_time:
                params["startTime"] = api_start_time
                
            if api_end_time:
                params["endTime"] = api_end_time
                
            instances = self.client.monitoring.get_instances(**params)
            
            # Process metrics
            perf_metrics = {
                "counts": {},
                "durations": {},
                "errors": {},
                "summary": {}
            }
            
            # Process integration stats for counts and errors
            if "status" in integration_stats and integration_stats["status"] == "SUCCESS":
                stats_data = integration_stats.get("stats", {})
                
                # Process counts
                if "counts" in metrics and "counts" in stats_data:
                    perf_metrics["counts"] = stats_data["counts"]
                    
                    # Calculate summary for counts
                    count_summary = {
                        "total": 0,
                        "by_status": {},
                        "by_integration": {}
                    }
                    
                    for period, period_data in stats_data["counts"].items():
                        for status, status_count in period_data.items():
                            if status == "timestamp":
                                continue
                                
                            # Add to total
                            count_summary["total"] += status_count
                            
                            # Add to status counts
                            if status not in count_summary["by_status"]:
                                count_summary["by_status"][status] = 0
                            count_summary["by_status"][status] += status_count
                            
                            # Add to integration counts if available
                            if "integrationId" in period_data:
                                integration_id = period_data["integrationId"]
                                integration_name = period_data.get("integrationName", "Unknown")
                                
                                if integration_id not in count_summary["by_integration"]:
                                    count_summary["by_integration"][integration_id] = {
                                        "name": integration_name,
                                        "total": 0,
                                        "by_status": {}
                                    }
                                    
                                int_summary = count_summary["by_integration"][integration_id]
                                int_summary["total"] += status_count
                                
                                if status not in int_summary["by_status"]:
                                    int_summary["by_status"][status] = 0
                                int_summary["by_status"][status] += status_count
                    
                    perf_metrics["summary"]["counts"] = count_summary
                
                # Process errors
                if "errors" in metrics and "errors" in stats_data:
                    perf_metrics["errors"] = stats_data["errors"]
                    
                    # Calculate summary for errors
                    error_summary = {
                        "total": 0,
                        "by_type": {},
                        "by_integration": {}
                    }
                    
                    for period, period_data in stats_data["errors"].items():
                        for error_type, error_count in period_data.items():
                            if error_type == "timestamp":
                                continue
                                
                            # Add to total
                            error_summary["total"] += error_count
                            
                            # Add to type counts
                            if error_type not in error_summary["by_type"]:
                                error_summary["by_type"][error_type] = 0
                            error_summary["by_type"][error_type] += error_count
                            
                            # Add to integration counts if available
                            if "integrationId" in period_data:
                                integration_id = period_data["integrationId"]
                                integration_name = period_data.get("integrationName", "Unknown")
                                
                                if integration_id not in error_summary["by_integration"]:
                                    error_summary["by_integration"][integration_id] = {
                                        "name": integration_name,
                                        "total": 0,
                                        "by_type": {}
                                    }
                                    
                                int_summary = error_summary["by_integration"][integration_id]
                                int_summary["total"] += error_count
                                
                                if error_type not in int_summary["by_type"]:
                                    int_summary["by_type"][error_type] = 0
                                int_summary["by_type"][error_type] += error_count
                    
                    perf_metrics["summary"]["errors"] = error_summary
            
            # Process instances for durations
            if "durations" in metrics:
                durations_by_integration = {}
                durations_by_period = {}
                
                # Calculate durations for all completed instances
                for instance in instances:
                    if instance.get("status") != "COMPLETED":
                        continue
                        
                    instance_id = instance.get("id")
                    start_time = instance.get("startTime")
                    end_time = instance.get("endTime")
                    integration_id = instance.get("integrationId", "Unknown")
                    integration_name = instance.get("integrationName", "Unknown")
                    
                    # Skip if missing required data
                    if not start_time or not end_time:
                        continue
                        
                    # Parse timestamps and calculate duration
                    try:
                        if isinstance(start_time, str):
                            start_dt = datetime.datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                        else:
                            start_dt = start_time
                            
                        if isinstance(end_time, str):
                            end_dt = datetime.datetime.fromisoformat(end_time.replace("Z", "+00:00"))
                        else:
                            end_dt = end_time
                            
                        duration_seconds = (end_dt - start_dt).total_seconds()
                        
                        # Get period key based on interval
                        if interval == "hour":
                            period_key = start_dt.strftime("%Y-%m-%d %H:00")
                        elif interval == "day":
                            period_key = start_dt.strftime("%Y-%m-%d")
                        elif interval == "week":
                            # Get start of week (Monday)
                            week_start = start_dt - datetime.timedelta(days=start_dt.weekday())
                            period_key = week_start.strftime("%Y-%m-%d")
                        elif interval == "month":
                            period_key = start_dt.strftime("%Y-%m")
                        else:
                            # Default to day
                            period_key = start_dt.strftime("%Y-%m-%d")
                        
                        # Add to integration durations
                        if integration_id not in durations_by_integration:
                            durations_by_integration[integration_id] = {
                                "name": integration_name,
                                "durations": [],
                                "total_seconds": 0,
                                "count": 0
                            }
                            
                        int_data = durations_by_integration[integration_id]
                        int_data["durations"].append(duration_seconds)
                        int_data["total_seconds"] += duration_seconds
                        int_data["count"] += 1
                        
                        # Add to period durations
                        if period_key not in durations_by_period:
                            durations_by_period[period_key] = {
                                "durations": [],
                                "total_seconds": 0,
                                "count": 0,
                                "by_integration": {}
                            }
                            
                        period_data = durations_by_period[period_key]
                        period_data["durations"].append(duration_seconds)
                        period_data["total_seconds"] += duration_seconds
                        period_data["count"] += 1
                        
                        # Add to integration within period
                        if integration_id not in period_data["by_integration"]:
                            period_data["by_integration"][integration_id] = {
                                "name": integration_name,
                                "durations": [],
                                "total_seconds": 0,
                                "count": 0
                            }
                            
                        period_int_data = period_data["by_integration"][integration_id]
                        period_int_data["durations"].append(duration_seconds)
                        period_int_data["total_seconds"] += duration_seconds
                        period_int_data["count"] += 1
                        
                    except:
                        # Skip on any parsing error
                        continue
                
                # Calculate statistics for durations
                duration_summary = {
                    "total_instances": 0,
                    "total_duration_seconds": 0,
                    "average_duration_seconds": 0,
                    "min_duration_seconds": 0,
                    "max_duration_seconds": 0,
                    "by_integration": {}
                }
                
                all_durations = []
                
                # Process integration durations
                for integration_id, int_data in durations_by_integration.items():
                    if int_data["count"] > 0:
                        # Calculate statistics
                        int_data["average_seconds"] = int_data["total_seconds"] / int_data["count"]
                        int_data["min_seconds"] = min(int_data["durations"])
                        int_data["max_seconds"] = max(int_data["durations"])
                        
                        # Add to summary
                        duration_summary["total_instances"] += int_data["count"]
                        duration_summary["total_duration_seconds"] += int_data["total_seconds"]
                        all_durations.extend(int_data["durations"])
                        
                        # Add to integration summary
                        duration_summary["by_integration"][integration_id] = {
                            "name": int_data["name"],
                            "count": int_data["count"],
                            "total_seconds": int_data["total_seconds"],
                            "average_seconds": int_data["average_seconds"],
                            "min_seconds": int_data["min_seconds"],
                            "max_seconds": int_data["max_seconds"]
                        }
                
                # Process period durations
                for period_key, period_data in durations_by_period.items():
                    if period_data["count"] > 0:
                        # Calculate statistics
                        period_data["average_seconds"] = period_data["total_seconds"] / period_data["count"]
                        period_data["min_seconds"] = min(period_data["durations"])
                        period_data["max_seconds"] = max(period_data["durations"])
                        
                        # Process integrations within period
                        for integration_id, int_data in period_data["by_integration"].items():
                            if int_data["count"] > 0:
                                int_data["average_seconds"] = int_data["total_seconds"] / int_data["count"]
                                int_data["min_seconds"] = min(int_data["durations"])
                                int_data["max_seconds"] = max(int_data["durations"])
                
                # Calculate overall statistics
                if all_durations:
                    duration_summary["average_duration_seconds"] = sum(all_durations) / len(all_durations)
                    duration_summary["min_duration_seconds"] = min(all_durations)
                    duration_summary["max_duration_seconds"] = max(all_durations)
                
                # Store durations data
                perf_metrics["durations"] = {
                    "by_integration": durations_by_integration,
                    "by_period": durations_by_period
                }
                
                perf_metrics["summary"]["durations"] = duration_summary
            
            # Update result with metrics
            result.details["performance_metrics"] = perf_metrics
            
            # Generate performance report if requested
            if generate_report:
                if not report_file:
                    # Create default report name
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    report_file = f"oic_performance_report_{timestamp}.json"
                
                try:
                    # Ensure directory exists
                    report_dir = os.path.dirname(report_file)
                    if report_dir and not os.path.exists(report_dir):
                        os.makedirs(report_dir)
                    
                    # Write report
                    with open(report_file, 'w') as f:
                        json.dump({
                            "performance_metrics": perf_metrics,
                            "metadata": {
                                "timestamp": datetime.datetime.now().isoformat(),
                                "time_range": time_range_str,
                                "integration_id": integration_id,
                                "interval": interval,
                                "metrics": metrics
                            }
                        }, f, indent=2)
                    
                    result.details["report_file"] = report_file
                    self.logger.info(f"Performance report generated: {report_file}")
                    
                    # Generate CSV report for durations if applicable
                    if "durations" in metrics and perf_metrics["durations"]["by_period"]:
                        csv_file = report_file.replace(".json", "_durations.csv")
                        with open(csv_file, 'w', newline='') as f:
                            writer = csv.writer(f)
                            # Write header
                            writer.writerow(["Period", "Integration", "Count", "Total (s)", "Average (s)", "Min (s)", "Max (s)"])
                            
                            # Write data
                            for period, period_data in sorted(perf_metrics["durations"]["by_period"].items()):
                                if period_data["count"] > 0:
                                    # Write period total
                                    writer.writerow([
                                        period,
                                        "ALL",
                                        period_data["count"],
                                        period_data["total_seconds"],
                                        period_data["average_seconds"],
                                        period_data["min_seconds"],
                                        period_data["max_seconds"]
                                    ])
                                    
                                    # Write integration breakdowns
                                    for int_id, int_data in sorted(period_data["by_integration"].items(), key=lambda x: x[1]["count"], reverse=True):
                                        if int_data["count"] > 0:
                                            writer.writerow([
                                                period,
                                                int_data["name"],
                                                int_data["count"],
                                                int_data["total_seconds"],
                                                int_data["average_seconds"],
                                                int_data["min_seconds"],
                                                int_data["max_seconds"]
                                            ])
                        
                        result.details["csv_durations_file"] = csv_file
                        
                except Exception as e:
                    self.logger.error(f"Failed to generate performance report: {str(e)}")
                    result.add_error("Failed to generate performance report", e)
                    
            # Build result message
            metrics_summary = []
            
            # Add counts to summary
            if "counts" in metrics and "counts" in perf_metrics["summary"]:
                count_summary = perf_metrics["summary"]["counts"]
                metrics_summary.append(f"{count_summary['total']} total instances")
            
            # Add durations to summary
            if "durations" in metrics and "durations" in perf_metrics["summary"]:
                duration_summary = perf_metrics["summary"]["durations"]
                if duration_summary["total_instances"] > 0:
                    avg_duration = duration_summary["average_duration_seconds"]
                    metrics_summary.append(f"{avg_duration:.2f}s average duration")
            
            # Add errors to summary
            if "errors" in metrics and "errors" in perf_metrics["summary"]:
                error_summary = perf_metrics["summary"]["errors"]
                metrics_summary.append(f"{error_summary['total']} errors")
            
            # Update result message
            if metrics_summary:
                if integration_id:
                    result.message = f"Collected metrics for integration {integration_id} {time_range_str}: " + ", ".join(metrics_summary)
                else:
                    result.message = f"Collected metrics {time_range_str}: " + ", ".join(metrics_summary)
                    
                if generate_report and "report_file" in result.details:
                    result.message += f". Report generated: {result.details['report_file']}"
            else:
                result.message = f"No metrics data available {time_range_str}"
            
        except OICError as e:
            self.logger.error(f"Failed to collect performance metrics: {str(e)}")
            result.add_error("Failed to collect performance metrics", e)
            result.success = False
            result.message = f"Failed to collect performance metrics: {str(e)}"
            
        return result
    
    def purge_integration_instances(
        self,
        integration_id: Optional[str] = None,
        status: Optional[str] = None,
        start_time: Optional[Union[str, datetime.datetime]] = None,
        end_time: Optional[Union[str, datetime.datetime]] = None,
        dry_run: bool = True,
        batch_size: int = 100
    ) -> WorkflowResult:
        """
        Purge integration instances from OIC.
        
        This workflow:
        1. Finds instances matching the criteria
        2. Purges instances in batches
        
        Args:
            integration_id: Optional specific integration to purge instances for.
            status: Optional status to filter instances by.
            start_time: Optional start time for instances to purge.
            end_time: Optional end time for instances to purge.
            dry_run: Whether to perform a dry run (don't actually purge).
            batch_size: Number of instances to purge in each batch.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        
        # Format the time range for messaging
        time_range_str = ""
        if start_time:
            if isinstance(start_time, datetime.datetime):
                start_time_str = start_time.isoformat()
            else:
                start_time_str = start_time
            time_range_str = f"from {start_time_str} "
        
        if end_time:
            if isinstance(end_time, datetime.datetime):
                end_time_str = end_time.isoformat()
            else:
                end_time_str = end_time
            if time_range_str:
                time_range_str += f"to {end_time_str}"
            else:
                time_range_str = f"until {end_time_str}"
        
        integration_str = f" for integration {integration_id}" if integration_id else ""
        status_str = f" with status {status}" if status else ""
        
        dry_run_str = " (DRY RUN)" if dry_run else ""
        
        result.message = f"Purging integration instances{integration_str}{status_str} {time_range_str}{dry_run_str}"
        
        # Convert datetime objects to strings for API calls
        api_start_time = None
        api_end_time = None
        
        if start_time:
            if isinstance(start_time, datetime.datetime):
                api_start_time = start_time.isoformat()
            else:
                api_start_time = start_time
        
        if end_time:
            if isinstance(end_time, datetime.datetime):
                api_end_time = end_time.isoformat()
            else:
                api_end_time = end_time
        
        # Get instances to purge
        try:
            self.logger.info("Getting instances to purge")
            
            # Set up query parameters
            params = {}
            if integration_id:
                params["integrationId"] = integration_id
                
            if status:
                params["status"] = status
                
            if api_start_time:
                params["startTime"] = api_start_time
                
            if api_end_time:
                params["endTime"] = api_end_time
            
            instances = self.client.monitoring.get_instances(**params)
            
            if not instances:
                result.message = f"No instances found to purge{integration_str}{status_str} {time_range_str}"
                return result
            
            instance_count = len(instances)
            self.logger.info(f"Found {instance_count} instances to purge")
            
            # Group instances by integration for better purging
            instances_by_integration = {}
            
            for instance in instances:
                instance_id = instance.get("id")
                instance_integration_id = instance.get("integrationId", "Unknown")
                instance_integration_name = instance.get("integrationName", "Unknown")
                instance_status = instance.get("status", "Unknown")
                
                if instance_integration_id not in instances_by_integration:
                    instances_by_integration[instance_integration_id] = {
                        "name": instance_integration_name,
                        "instances": []
                    }
                    
                instances_by_integration[instance_integration_id]["instances"].append({
                    "id": instance_id,
                    "status": instance_status,
                    "startTime": instance.get("startTime"),
                    "endTime": instance.get("endTime")
                })
            
            # Update result with instance data
            result.details["total_instances"] = instance_count
            result.details["instances_by_integration"] = instances_by_integration
            
            # If dry run, we're done
            if dry_run:
                result.message = f"Found {instance_count} instances that would be purged{integration_str}{status_str} {time_range_str} (DRY RUN)"
                return result
            
            # Purge instances in batches
            purged_counts = {}
            purge_results = {}
            total_purged = 0
            
            for integration_id, integration_data in instances_by_integration.items():
                integration_name = integration_data["name"]
                integration_instances = integration_data["instances"]
                
                # Process in batches
                for i in range(0, len(integration_instances), batch_size):
                    batch = integration_instances[i:i + batch_size]
                    batch_size_actual = len(batch)
                    
                    self.logger.info(f"Purging batch of {batch_size_actual} instances for integration {integration_name}")
                    
                    try:
                        # Prepare purge data
                        purge_data = {
                            "integrationId": integration_id
                        }
                        
                        if api_start_time:
                            purge_data["startTime"] = api_start_time
                            
                        if api_end_time:
                            purge_data["endTime"] = api_end_time
                            
                        if status:
                            purge_data["status"] = status
                        
                        # Perform purge
                        purge_result = self.client.monitoring.purge_instances(purge_data)
                        
                        # Update purge counts
                        purged_count = purge_result.get("count", 0)
                        total_purged += purged_count
                        
                        if integration_id not in purged_counts:
                            purged_counts[integration_id] = 0
                        purged_counts[integration_id] += purged_count
                        
                        # Store purge result
                        if integration_id not in purge_results:
                            purge_results[integration_id] = []
                        purge_results[integration_id].append(purge_result)
                        
                        self.logger.info(f"Purged {purged_count} instances for integration {integration_name}")
                        
                        # Resource tracking
                        result.add_resource("integration", integration_id, {
                            "name": integration_name,
                            "purged_count": purged_count
                        })
                        
                    except OICError as e:
                        self.logger.error(f"Failed to purge instances for integration {integration_name}: {str(e)}")
                        result.add_error(f"Failed to purge instances for integration {integration_name}", e, integration_id)
                        result.success = False
                        
                        # Continue with next integration
                        break
            
            # Update result with purge data
            result.details["purged_counts"] = purged_counts
            result.details["purge_results"] = purge_results
            result.details["total_purged"] = total_purged
            
            # Update result message
            if result.success:
                result.message = f"Successfully purged {total_purged} of {instance_count} instances{integration_str}{status_str} {time_range_str}"
            else:
                result.message = f"Partially purged {total_purged} of {instance_count} instances{integration_str}{status_str} {time_range_str}, but encountered errors"
                
        except OICError as e:
            self.logger.error(f"Failed to purge instances: {str(e)}")
            result.add_error("Failed to purge instances", e)
            result.success = False
            result.message = f"Failed to purge instances: {str(e)}"
            
        return result
    
    def generate_monitoring_report(
        self,
        report_type: str = "full",  # "full", "errors", "performance", "usage"
        start_time: Optional[Union[str, datetime.datetime]] = None,
        end_time: Optional[Union[str, datetime.datetime]] = None,
        output_format: str = "json",  # "json", "csv", "html"
        report_file: Optional[str] = None
    ) -> WorkflowResult:
        """
        Generate a comprehensive monitoring report.
        
        This workflow:
        1. Collects data for the requested report type
        2. Processes and formats the data
        3. Generates a report file
        
        Args:
            report_type: Type of report to generate.
            start_time: Optional start time for the report window.
            end_time: Optional end time for the report window.
            output_format: Output format for the report.
            report_file: Path to save the report.
            
        Returns:
            WorkflowResult: The workflow execution result.
        """
        result = WorkflowResult()
        
        # Format the time range for messaging
        time_range_str = ""
        if start_time:
            if isinstance(start_time, datetime.datetime):
                start_time_str = start_time.isoformat()
            else:
                start_time_str = start_time
            time_range_str = f"from {start_time_str} "
        
        if end_time:
            if isinstance(end_time, datetime.datetime):
                end_time_str = end_time.isoformat()
            else:
                end_time_str = end_time
            if time_range_str:
                time_range_str += f"to {end_time_str}"
            else:
                time_range_str = f"until {end_time_str}"
        
        result.message = f"Generating {report_type} monitoring report {time_range_str}"
        
        # Create default report file name if not provided
        if not report_file:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = f"oic_{report_type}_report_{timestamp}.{output_format}"
        
        # Ensure directory exists
        report_dir = os.path.dirname(report_file)
        if report_dir and not os.path.exists(report_dir):
            try:
                os.makedirs(report_dir)
            except Exception as e:
                self.logger.error(f"Failed to create report directory {report_dir}: {str(e)}")
                result.add_error("Failed to create report directory", e)
                result.success = False
                result.message = f"Failed to generate report: {str(e)}"
                return result
        
        # Collect report data based on report type
        report_data = {
            "metadata": {
                "timestamp": datetime.datetime.now().isoformat(),
                "report_type": report_type,
                "time_range": time_range_str
            }
        }
        
        try:
            if report_type == "errors" or report_type == "full":
                # Collect error data
                error_workflow = self.analyze_errors(
                    start_time=start_time,
                    end_time=end_time,
                    generate_report=False
                )
                
                if error_workflow.success:
                    report_data["errors"] = error_workflow.details.get("error_analysis", {})
                    report_data["error_summary"] = error_workflow.details.get("error_summary", {})
                else:
                    self.logger.warning("Failed to collect error data for report")
                    report_data["errors"] = {"error": "Failed to collect error data"}
            
            if report_type == "performance" or report_type == "full":
                # Collect performance data
                perf_workflow = self.collect_performance_metrics(
                    start_time=start_time,
                    end_time=end_time,
                    interval="day",
                    metrics=["counts", "durations", "errors"],
                    generate_report=False
                )
                
                if perf_workflow.success:
                    report_data["performance"] = perf_workflow.details.get("performance_metrics", {})
                else:
                    self.logger.warning("Failed to collect performance data for report")
                    report_data["performance"] = {"error": "Failed to collect performance data"}
            
            if report_type == "usage" or report_type == "full":
                # Collect usage data (instance counts, integration activity)
                try:
                    # Get instance stats
                    instance_stats = self.client.monitoring.get_instance_stats()
                    
                    # Get all integrations
                    integrations = self.client.integrations.list()
                    
                    # Get active integrations
                    active_integrations = [i for i in integrations if i.get("status") == "ACTIVATED"]
                    
                    # Build usage data
                    usage_data = {
                        "instance_stats": instance_stats,
                        "integration_counts": {
                            "total": len(integrations),
                            "active": len(active_integrations),
                            "inactive": len(integrations) - len(active_integrations)
                        },
                        "active_integrations": [
                            {
                                "id": i.get("id"),
                                "name": i.get("name", "Unknown"),
                                "type": i.get("integrationType", "Unknown")
                            }
                            for i in active_integrations
                        ]
                    }
                    
                    report_data["usage"] = usage_data
                    
                except OICError as e:
                    self.logger.warning(f"Failed to collect usage data for report: {str(e)}")
                    report_data["usage"] = {"error": "Failed to collect usage data"}
            
            if report_type == "health" or report_type == "full":
                # Collect health check data
                health_workflow = self.perform_health_check(
                    check_integrations=True,
                    check_connections=True,
                    test_connections=False
                )
                
                if health_workflow.success:
                    report_data["health"] = {
                        "overall_health": health_workflow.details.get("overall_health", "unknown"),
                        "instance_health": health_workflow.details.get("instance_health", {}),
                        "integration_health": health_workflow.details.get("integration_health", {}),
                        "connection_health": health_workflow.details.get("connection_health", {})
                    }
                else:
                    self.logger.warning("Failed to collect health data for report")
                    report_data["health"] = {"error": "Failed to collect health data"}
            
            # Generate report in the requested format
            if output_format == "json":
                try:
                    with open(report_file, 'w') as f:
                        json.dump(report_data, f, indent=2, default=str)
                    
                    self.logger.info(f"Generated JSON report: {report_file}")
                    result.details["report_file"] = report_file
                    
                except Exception as e:
                    self.logger.error(f"Failed to write JSON report: {str(e)}")
                    result.add_error("Failed to write JSON report", e)
                    result.success = False
                    
            elif output_format == "csv":
                try:
                    # For CSV, we need to create separate files for different sections
                    base_file = os.path.splitext(report_file)[0]
                    
                    csv_files = []
                    
                    # Generate metadata CSV
                    metadata_file = f"{base_file}_metadata.csv"
                    with open(metadata_file, 'w', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(["Key", "Value"])
                        for key, value in report_data["metadata"].items():
                            writer.writerow([key, value])
                    
                    csv_files.append(metadata_file)
                    
                    # Generate error summary CSV if available
                    if "error_summary" in report_data:
                        errors_file = f"{base_file}_errors.csv"
                        with open(errors_file, 'w', newline='') as f:
                            writer = csv.writer(f)
                            
                            if "top_errors_by_integration" in report_data["error_summary"]:
                                writer.writerow(["Integration", "Error Count", "Top Error Types"])
                                for integration in report_data["error_summary"]["top_errors_by_integration"]:
                                    writer.writerow([
                                        integration["name"],
                                        integration["count"],
                                        ", ".join([f"{err_type}: {err_count}" for err_type, err_count in integration["top_error_types"]])
                                    ])
                                    
                            elif "top_error_types" in report_data["error_summary"]:
                                writer.writerow(["Error Type", "Error Count", "Top Integrations"])
                                for error in report_data["error_summary"]["top_error_types"]:
                                    top_integrations = [f"{int_id}: {int_data['count']}" for int_id, int_data in error["top_integrations"][:3]]
                                    writer.writerow([
                                        error["type"],
                                        error["count"],
                                        ", ".join(top_integrations)
                                    ])
                        
                        csv_files.append(errors_file)
                    
                    # Generate performance CSV if available
                    if "performance" in report_data and "summary" in report_data["performance"]:
                        perf_summary = report_data["performance"]["summary"]
                        
                        if "durations" in perf_summary:
                            durations_file = f"{base_file}_durations.csv"
                            with open(durations_file, 'w', newline='') as f:
                                writer = csv.writer(f)
                                writer.writerow(["Integration", "Instances", "Total Duration (s)", "Average Duration (s)", "Min Duration (s)", "Max Duration (s)"])
                                
                                # Write overall stats
                                writer.writerow([
                                    "ALL",
                                    perf_summary["durations"]["total_instances"],
                                    perf_summary["durations"]["total_duration_seconds"],
                                    perf_summary["durations"]["average_duration_seconds"],
                                    perf_summary["durations"]["min_duration_seconds"],
                                    perf_summary["durations"]["max_duration_seconds"]
                                ])
                                
                                # Write integration breakdown
                                for int_id, int_data in perf_summary["durations"]["by_integration"].items():
                                    writer.writerow([
                                        int_data["name"],
                                        int_data["count"],
                                        int_data["total_seconds"],
                                        int_data["average_seconds"],
                                        int_data["min_seconds"],
                                        int_data["max_seconds"]
                                    ])
                            
                            csv_files.append(durations_file)
                    
                    # Generate health CSV if available
                    if "health" in report_data:
                        health_file = f"{base_file}_health.csv"
                        with open(health_file, 'w', newline='') as f:
                            writer = csv.writer(f)
                            writer.writerow(["Component", "Status"])
                            writer.writerow(["Overall", report_data["health"]["overall_health"]])
                            writer.writerow(["Instance", report_data["health"]["instance_health"].get("status", "unknown")])
                            writer.writerow(["Integrations", report_data["health"]["integration_health"].get("status", "unknown")])
                            writer.writerow(["Connections", report_data["health"]["connection_health"].get("status", "unknown")])
                        
                        csv_files.append(health_file)
                    
                    # Generate usage CSV if available
                    if "usage" in report_data:
                        usage_file = f"{base_file}_usage.csv"
                        with open(usage_file, 'w', newline='') as f:
                            writer = csv.writer(f)
                            writer.writerow(["Metric", "Value"])
                            writer.writerow(["Total Integrations", report_data["usage"]["integration_counts"]["total"]])
                            writer.writerow(["Active Integrations", report_data["usage"]["integration_counts"]["active"]])
                            writer.writerow(["Inactive Integrations", report_data["usage"]["integration_counts"]["inactive"]])
                        
                        csv_files.append(usage_file)
                    
                    self.logger.info(f"Generated CSV reports: {', '.join(csv_files)}")
                    result.details["report_files"] = csv_files
                    
                except Exception as e:
                    self.logger.error(f"Failed to write CSV report: {str(e)}")
                    result.add_error("Failed to write CSV report", e)
                    result.success = False
                    
            elif output_format == "html":
                try:
                    # Simple HTML report template
                    html_content = f"""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <title>OIC {report_type.capitalize()} Report</title>
                        <style>
                            body {{ font-family: Arial, sans-serif; margin: 20px; }}
                            h1, h2, h3 {{ color: #333; }}
                            table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                            th {{ background-color: #f2f2f2; }}
                            tr:nth-child(even) {{ background-color: #f9f9f9; }}
                            .healthy {{ color: green; }}
                            .degraded {{ color: orange; }}
                            .error {{ color: red; }}
                            .unknown {{ color: gray; }}
                        </style>
                    </head>
                    <body>
                        <h1>OIC {report_type.capitalize()} Report</h1>
                        <p><strong>Generated:</strong> {report_data["metadata"]["timestamp"]}</p>
                        <p><strong>Time Range:</strong> {time_range_str if time_range_str else "All time"}</p>
                    """
                    
                    # Add health section if available
                    if "health" in report_data:
                        health_data = report_data["health"]
                        health_status = health_data["overall_health"]
                        html_content += f"""
                        <h2>Health Status</h2>
                        <p><strong>Overall Health:</strong> <span class="{health_status}">{health_status.upper()}</span></p>
                        <table>
                            <tr>
                                <th>Component</th>
                                <th>Status</th>
                            </tr>
                            <tr>
                                <td>Instance</td>
                                <td class="{health_data["instance_health"].get("status", "unknown")}">{health_data["instance_health"].get("status", "unknown").upper()}</td>
                            </tr>
                            <tr>
                                <td>Integrations</td>
                                <td class="{health_data["integration_health"].get("status", "unknown")}">{health_data["integration_health"].get("status", "unknown").upper()}</td>
                            </tr>
                            <tr>
                                <td>Connections</td>
                                <td class="{health_data["connection_health"].get("status", "unknown")}">{health_data["connection_health"].get("status", "unknown").upper()}</td>
                            </tr>
                        </table>
                        """
                        
                        # Add problematic integrations if available
                        if "integration_health" in health_data and "problematic" in health_data["integration_health"] and health_data["integration_health"]["problematic"]:
                            html_content += """
                            <h3>Problematic Integrations</h3>
                            <table>
                                <tr>
                                    <th>Integration</th>
                                    <th>Status</th>
                                </tr>
                            """
                            
                            for integration in health_data["integration_health"]["problematic"]:
                                html_content += f"""
                                <tr>
                                    <td>{integration.get("name", "Unknown")}</td>
                                    <td class="error">{integration.get("status", "ERROR")}</td>
                                </tr>
                                """
                                
                            html_content += "</table>"
                            
                        # Add problematic connections if available
                        if "connection_health" in health_data and "problematic" in health_data["connection_health"] and health_data["connection_health"]["problematic"]:
                            html_content += """
                            <h3>Problematic Connections</h3>
                            <table>
                                <tr>
                                    <th>Connection</th>
                                    <th>Status</th>
                                </tr>
                            """
                            
                            for connection in health_data["connection_health"]["problematic"]:
                                html_content += f"""
                                <tr>
                                    <td>{connection.get("name", "Unknown")}</td>
                                    <td class="error">{connection.get("status", "ERROR")}</td>
                                </tr>
                                """
                                
                            html_content += "</table>"
                    
                    # Add error section if available
                    if "error_summary" in report_data:
                        html_content += "<h2>Error Summary</h2>"
                        
                        if "top_errors_by_integration" in report_data["error_summary"]:
                            html_content += """
                            <h3>Top Errors by Integration</h3>
                            <table>
                                <tr>
                                    <th>Integration</th>
                                    <th>Error Count</th>
                                    <th>Top Error Types</th>
                                </tr>
                            """
                            
                            for integration in report_data["error_summary"]["top_errors_by_integration"]:
                                html_content += f"""
                                <tr>
                                    <td>{integration["name"]}</td>
                                    <td>{integration["count"]}</td>
                                    <td>{"<br>".join([f"{err_type}: {err_count}" for err_type, err_count in integration["top_error_types"]])}</td>
                                </tr>
                                """
                                
                            html_content += "</table>"
                            
                        elif "top_error_types" in report_data["error_summary"]:
                            html_content += """
                            <h3>Top Error Types</h3>
                            <table>
                                <tr>
                                    <th>Error Type</th>
                                    <th>Error Count</th>
                                    <th>Top Integrations</th>
                                </tr>
                            """
                            
                            for error in report_data["error_summary"]["top_error_types"]:
                                top_integrations = [f"{int_data['name']}: {int_data['count']}" for int_id, int_data in error["top_integrations"][:3]]
                                html_content += f"""
                                <tr>
                                    <td>{error["type"]}</td>
                                    <td>{error["count"]}</td>
                                    <td>{"<br>".join(top_integrations)}</td>
                                </tr>
                                """
                                
                            html_content += "</table>"
                    
                    # Add performance section if available
                    if "performance" in report_data and "summary" in report_data["performance"]:
                        perf_summary = report_data["performance"]["summary"]
                        html_content += "<h2>Performance Summary</h2>"
                        
                        if "durations" in perf_summary:
                            durations = perf_summary["durations"]
                            html_content += f"""
                            <h3>Execution Durations</h3>
                            <p><strong>Total Instances:</strong> {durations["total_instances"]}</p>
                            <p><strong>Average Duration:</strong> {durations["average_duration_seconds"]:.2f} seconds</p>
                            <p><strong>Min Duration:</strong> {durations["min_duration_seconds"]:.2f} seconds</p>
                            <p><strong>Max Duration:</strong> {durations["max_duration_seconds"]:.2f} seconds</p>
                            
                            <h4>Durations by Integration</h4>
                            <table>
                                <tr>
                                    <th>Integration</th>
                                    <th>Instances</th>
                                    <th>Average Duration (s)</th>
                                    <th>Min Duration (s)</th>
                                    <th>Max Duration (s)</th>
                                </tr>
                            """
                            
                            for int_id, int_data in sorted(durations["by_integration"].items(), key=lambda x: x[1]["average_seconds"], reverse=True):
                                html_content += f"""
                                <tr>
                                    <td>{int_data["name"]}</td>
                                    <td>{int_data["count"]}</td>
                                    <td>{int_data["average_seconds"]:.2f}</td>
                                    <td>{int_data["min_seconds"]:.2f}</td>
                                    <td>{int_data["max_seconds"]:.2f}</td>
                                </tr>
                                """
                                
                            html_content += "</table>"
                    
                    # Add usage section if available
                    if "usage" in report_data:
                        usage_data = report_data["usage"]
                        html_content += """
                        <h2>Usage Summary</h2>
                        <table>
                            <tr>
                                <th>Metric</th>
                                <th>Value</th>
                            </tr>
                        """
                        
                        html_content += f"""
                        <tr>
                            <td>Total Integrations</td>
                            <td>{usage_data["integration_counts"]["total"]}</td>
                        </tr>
                        <tr>
                            <td>Active Integrations</td>
                            <td>{usage_data["integration_counts"]["active"]}</td>
                        </tr>
                        <tr>
                            <td>Inactive Integrations</td>
                            <td>{usage_data["integration_counts"]["inactive"]}</td>
                        </tr>
                        """
                        
                        html_content += """
                        </table>
                        
                        <h3>Active Integrations</h3>
                        <table>
                            <tr>
                                <th>Integration</th>
                                <th>Type</th>
                            </tr>
                        """
                        
                        for integration in usage_data["active_integrations"]:
                            html_content += f"""
                            <tr>
                                <td>{integration["name"]}</td>
                                <td>{integration["type"]}</td>
                            </tr>
                            """
                            
                        html_content += "</table>"
                    
                    # Close HTML
                    html_content += """
                    </body>
                    </html>
                    """
                    
                    # Write HTML to file
                    with open(report_file, 'w') as f:
                        f.write(html_content)
                    
                    self.logger.info(f"Generated HTML report: {report_file}")
                    result.details["report_file"] = report_file
                    
                except Exception as e:
                    self.logger.error(f"Failed to write HTML report: {str(e)}")
                    result.add_error("Failed to write HTML report", e)
                    result.success = False
            
            # Update result message
            if result.success:
                if output_format == "csv" and "report_files" in result.details:
                    result.message = f"Successfully generated {report_type} report in {len(result.details['report_files'])} CSV files"
                else:
                    result.message = f"Successfully generated {report_type} report: {report_file}"
            else:
                result.message = f"Failed to generate complete {report_type} report"
                
        except Exception as e:
            self.logger.error(f"Failed to generate report: {str(e)}")
            result.add_error("Failed to generate report", e)
            result.success = False
            result.message = f"Failed to generate {report_type} report: {str(e)}"
            
        return result
