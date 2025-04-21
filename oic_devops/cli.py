"""
Command-line interface for the OIC DevOps package.

This module provides the command-line interface for interacting with the
Oracle Integration Cloud REST API.
"""

import os
import sys
import json
import logging
import click
from typing import Dict, Any, Optional, List

from oic_devops.client import OICClient
from oic_devops.config import OICConfig
from oic_devops.exceptions import OICError


def configure_logging(verbosity: int) -> None:
    """
    Configure logging based on verbosity level.
    
    Args:
        verbosity: The verbosity level (0-3).
    """
    log_levels = {
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
        3: logging.DEBUG,
    }
    
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    if verbosity >= 3:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    
    logging.basicConfig(
        level=log_levels.get(verbosity, logging.INFO),
        format=log_format,
    )


def output_json(data: Any, pretty: bool = False) -> None:
    """
    Output data as JSON.
    
    Args:
        data: The data to output.
        pretty: Whether to pretty-print the JSON.
    """
    indent = 2 if pretty else None
    click.echo(json.dumps(data, indent=indent, default=str))


def output_table(data: List[Dict[str, Any]], fields: Optional[List[str]] = None) -> None:
    """
    Output data as a table.
    
    Args:
        data: The data to output.
        fields: The fields to include in the table.
    """
    if not data:
        click.echo("No data found.")
        return
    
    # If fields not specified, use all fields from the first item
    if not fields:
        fields = list(data[0].keys())
    
    # Get max width for each field
    widths = {field: max(len(str(item.get(field, ""))) for item in data) for field in fields}
    widths = {field: max(widths[field], len(field)) for field in fields}
    
    # Print header
    header = " | ".join(f"{field:{widths[field]}}" for field in fields)
    click.echo(header)
    click.echo("-" * len(header))
    
    # Print data
    for item in data:
        row = " | ".join(f"{str(item.get(field, '')):{widths[field]}}" for field in fields)
        click.echo(row)


# Define common options
def common_options(func):
    """Decorator to add common options to commands."""
    func = click.option(
        "--config-file", "-c",
        help="Path to the configuration file.",
    )(func)
    func = click.option(
        "--profile", "-p",
        default="default",
        help="Profile to use from the configuration file.",
    )(func)
    func = click.option(
        "--verbose", "-v",
        count=True,
        help="Increase verbosity (can be used multiple times).",
    )(func)
    func = click.option(
        "--output", "-o",
        type=click.Choice(["json", "table", "pretty"]),
        default="pretty",
        help="Output format.",
    )(func)
    return func


# Define the CLI
@click.group()
@click.version_option()
def cli():
    """
    OIC DevOps - Command-line tool for Oracle Integration Cloud DevOps.
    
    This tool provides functionality for managing Oracle Integration Cloud
    resources using the OIC REST API.
    """
    pass


# Config commands
@cli.group()
def config():
    """Manage configuration."""
    pass


@config.command("list-profiles")
@common_options
def config_list_profiles(config_file, profile, verbose, output):
    """List available profiles in the configuration file."""
    configure_logging(verbose)
    
    try:
        config = OICConfig(config_file=config_file, profile=profile)
        profiles = config.get_available_profiles()
        
        if output == "json":
            output_json(profiles)
        elif output == "table":
            output_table([{"profile": p} for p in profiles], ["profile"])
        else:
            output_json(profiles, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@config.command("get-profile")
@click.argument("profile_name")
@common_options
def config_get_profile(profile_name, config_file, profile, verbose, output):
    """Get details for a specific profile."""
    configure_logging(verbose)
    
    try:
        config = OICConfig(config_file=config_file, profile=profile_name)
        
        # Remove sensitive information
        profile_data = config.profile_config.copy()
        if "password" in profile_data:
            profile_data["password"] = "********"
        
        if output == "json":
            output_json(profile_data)
        elif output == "table":
            output_table([profile_data])
        else:
            output_json(profile_data, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


# Connections commands
@cli.group()
def connections():
    """Manage connections."""
    pass


@connections.command("list")
@click.option("--limit", type=int, help="Maximum number of items to return.")
@click.option("--offset", type=int, help="Number of items to skip.")
@click.option("--fields", help="Comma-separated list of fields to include.")
@click.option("--query", "-q", help="Search query.")
@click.option("--order-by", help="Field to order by.")
@common_options
def connections_list(limit, offset, fields, query, order_by, config_file, profile, verbose, output):
    """List all connections."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if fields:
            params["fields"] = fields
        if query:
            params["q"] = query
        if order_by:
            params["orderBy"] = order_by
        
        connections = client.connections.list(params=params)
        
        if output == "json":
            output_json(connections)
        elif output == "table":
            fields_list = ["id", "name", "connectionType", "status"] 
            output_table(connections, fields_list)
        else:
            output_json(connections, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@connections.command("get")
@click.argument("connection_id")
@common_options
def connections_get(connection_id, config_file, profile, verbose, output):
    """Get a specific connection by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        connection = client.connections.get(connection_id)
        
        if output == "json":
            output_json(connection)
        elif output == "table":
            output_table([connection])
        else:
            output_json(connection, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@connections.command("test")
@click.argument("connection_id")
@common_options
def connections_test(connection_id, config_file, profile, verbose, output):
    """Test a specific connection by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.connections.test(connection_id)
        
        if output == "json":
            output_json(result)
        elif output == "table":
            output_table([result])
        else:
            output_json(result, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


# Integrations commands
@cli.group()
def integrations():
    """Manage integrations."""
    pass


@integrations.command("list")
@click.option("--limit", type=int, help="Maximum number of items to return.")
@click.option("--offset", type=int, help="Number of items to skip.")
@click.option("--fields", help="Comma-separated list of fields to include.")
@click.option("--query", "-q", help="Search query.")
@click.option("--order-by", help="Field to order by.")
@click.option("--status", help="Filter by status (e.g., 'ACTIVATED', 'CONFIGURED').")
@common_options
def integrations_list(limit, offset, fields, query, order_by, status, config_file, profile, verbose, output):
    """List all integrations."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if fields:
            params["fields"] = fields
        if query:
            params["q"] = query
        if order_by:
            params["orderBy"] = order_by
        if status:
            params["status"] = status
        
        integrations = client.integrations.list(params=params)
        
        if output == "json":
            output_json(integrations)
        elif output == "table":
            fields_list = ["id", "name", "integrationType", "status"] 
            output_table(integrations, fields_list)
        else:
            output_json(integrations, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@integrations.command("get")
@click.argument("integration_id")
@common_options
def integrations_get(integration_id, config_file, profile, verbose, output):
    """Get a specific integration by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        integration = client.integrations.get(integration_id)
        
        if output == "json":
            output_json(integration)
        elif output == "table":
            output_table([integration])
        else:
            output_json(integration, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@integrations.command("activate")
@click.argument("integration_id")
@common_options
def integrations_activate(integration_id, config_file, profile, verbose, output):
    """Activate a specific integration by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.integrations.activate(integration_id)
        
        if output == "json":
            output_json(result)
        elif output == "table":
            output_table([result])
        else:
            output_json(result, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@integrations.command("deactivate")
@click.argument("integration_id")
@common_options
def integrations_deactivate(integration_id, config_file, profile, verbose, output):
    """Deactivate a specific integration by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.integrations.deactivate(integration_id)
        
        if output == "json":
            output_json(result)
        elif output == "table":
            output_table([result])
        else:
            output_json(result, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@integrations.command("export")
@click.argument("integration_id")
@click.option("--output-file", "-o", required=True, help="Path to save the exported integration file.")
@common_options
def integrations_export(integration_id, output_file, config_file, profile, verbose, output):
    """Export a specific integration by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.integrations.export(integration_id, output_file)
        
        click.echo(f"Integration exported to {result}")
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@integrations.command("import")
@click.argument("file_path")
@common_options
def integrations_import(file_path, config_file, profile, verbose, output):
    """Import an integration from a file."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.integrations.import_integration(file_path)
        
        if output == "json":
            output_json(result)
        elif output == "table":
            output_table([result])
        else:
            output_json(result, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


# Libraries commands
@cli.group()
def libraries():
    """Manage libraries."""
    pass


@libraries.command("list")
@click.option("--limit", type=int, help="Maximum number of items to return.")
@click.option("--offset", type=int, help="Number of items to skip.")
@click.option("--fields", help="Comma-separated list of fields to include.")
@click.option("--query", "-q", help="Search query.")
@click.option("--order-by", help="Field to order by.")
@common_options
def libraries_list(limit, offset, fields, query, order_by, config_file, profile, verbose, output):
    """List all libraries."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if fields:
            params["fields"] = fields
        if query:
            params["q"] = query
        if order_by:
            params["orderBy"] = order_by
        
        libraries = client.libraries.list(params=params)
        
        if output == "json":
            output_json(libraries)
        elif output == "table":
            fields_list = ["id", "name", "type"] 
            output_table(libraries, fields_list)
        else:
            output_json(libraries, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@libraries.command("get")
@click.argument("library_id")
@common_options
def libraries_get(library_id, config_file, profile, verbose, output):
    """Get a specific library by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        library = client.libraries.get(library_id)
        
        if output == "json":
            output_json(library)
        elif output == "table":
            output_table([library])
        else:
            output_json(library, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@libraries.command("export")
@click.argument("library_id")
@click.option("--output-file", "-o", required=True, help="Path to save the exported library file.")
@common_options
def libraries_export(library_id, output_file, config_file, profile, verbose, output):
    """Export a specific library by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.libraries.export(library_id, output_file)
        
        click.echo(f"Library exported to {result}")
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@libraries.command("import")
@click.argument("file_path")
@common_options
def libraries_import(file_path, config_file, profile, verbose, output):
    """Import a library from a file."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.libraries.import_library(file_path)
        
        if output == "json":
            output_json(result)
        elif output == "table":
            output_table([result])
        else:
            output_json(result, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


# Lookups commands
@cli.group()
def lookups():
    """Manage lookups."""
    pass


@lookups.command("list")
@click.option("--limit", type=int, help="Maximum number of items to return.")
@click.option("--offset", type=int, help="Number of items to skip.")
@click.option("--fields", help="Comma-separated list of fields to include.")
@click.option("--query", "-q", help="Search query.")
@click.option("--order-by", help="Field to order by.")
@common_options
def lookups_list(limit, offset, fields, query, order_by, config_file, profile, verbose, output):
    """List all lookups."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if fields:
            params["fields"] = fields
        if query:
            params["q"] = query
        if order_by:
            params["orderBy"] = order_by
        
        lookups = client.lookups.list(params=params)
        
        if output == "json":
            output_json(lookups)
        elif output == "table":
            fields_list = ["id", "name"] 
            output_table(lookups, fields_list)
        else:
            output_json(lookups, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@lookups.command("get")
@click.argument("lookup_id")
@common_options
def lookups_get(lookup_id, config_file, profile, verbose, output):
    """Get a specific lookup by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        lookup = client.lookups.get(lookup_id)
        
        if output == "json":
            output_json(lookup)
        elif output == "table":
            output_table([lookup])
        else:
            output_json(lookup, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@lookups.command("get-data")
@click.argument("lookup_id")
@common_options
def lookups_get_data(lookup_id, config_file, profile, verbose, output):
    """Get data for a specific lookup by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        lookup_data = client.lookups.get_data(lookup_id)
        
        if output == "json":
            output_json(lookup_data)
        elif output == "table":
            if "rows" in lookup_data and lookup_data["rows"]:
                output_table(lookup_data["rows"])
            else:
                click.echo("No data found.")
        else:
            output_json(lookup_data, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@lookups.command("export")
@click.argument("lookup_id")
@click.option("--output-file", "-o", required=True, help="Path to save the exported lookup file.")
@common_options
def lookups_export(lookup_id, output_file, config_file, profile, verbose, output):
    """Export a specific lookup by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.lookups.export(lookup_id, output_file)
        
        click.echo(f"Lookup exported to {result}")
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@lookups.command("import")
@click.argument("file_path")
@common_options
def lookups_import(file_path, config_file, profile, verbose, output):
    """Import a lookup from a file."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.lookups.import_lookup(file_path)
        
        if output == "json":
            output_json(result)
        elif output == "table":
            output_table([result])
        else:
            output_json(result, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


# Monitoring commands
@cli.group()
def monitoring():
    """Manage monitoring."""
    pass


@monitoring.command("instance-stats")
@common_options
def monitoring_instance_stats(config_file, profile, verbose, output):
    """Get instance statistics."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        stats = client.monitoring.get_instance_stats()
        
        if output == "json":
            output_json(stats)
        elif output == "table":
            if isinstance(stats, dict):
                output_table([stats])
            else:
                output_table(stats)
        else:
            output_json(stats, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@monitoring.command("instances")
@click.option("--integration-id", help="Filter by integration ID.")
@click.option("--status", help="Filter by status.")
@click.option("--start-time", help="Filter by start time (ISO format).")
@click.option("--end-time", help="Filter by end time (ISO format).")
@common_options
def monitoring_instances(integration_id, status, start_time, end_time, config_file, profile, verbose, output):
    """Get integration instances."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        instances = client.monitoring.get_instances(
            integration_id=integration_id,
            status=status,
            start_time=start_time,
            end_time=end_time,
        )
        
        if output == "json":
            output_json(instances)
        elif output == "table":
            fields_list = ["id", "integrationId", "status", "startTime", "endTime"] 
            output_table(instances, fields_list)
        else:
            output_json(instances, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@monitoring.command("instance")
@click.argument("instance_id")
@common_options
def monitoring_instance(instance_id, config_file, profile, verbose, output):
    """Get a specific integration instance by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        instance = client.monitoring.get_instance(instance_id)
        
        if output == "json":
            output_json(instance)
        elif output == "table":
            output_table([instance])
        else:
            output_json(instance, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@monitoring.command("instance-activities")
@click.argument("instance_id")
@common_options
def monitoring_instance_activities(instance_id, config_file, profile, verbose, output):
    """Get activities for a specific integration instance."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        activities = client.monitoring.get_instance_activities(instance_id)
        
        if output == "json":
            output_json(activities)
        elif output == "table":
            fields_list = ["id", "activityName", "status", "startTime", "endTime"] 
            output_table(activities, fields_list)
        else:
            output_json(activities, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@monitoring.command("resubmit-instance")
@click.argument("instance_id")
@common_options
def monitoring_resubmit_instance(instance_id, config_file, profile, verbose, output):
    """Resubmit a specific integration instance."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.monitoring.resubmit_instance(instance_id)
        
        if output == "json":
            output_json(result)
        elif output == "table":
            output_table([result])
        else:
            output_json(result, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


# Packages commands
@cli.group()
def packages():
    """Manage packages."""
    pass


@packages.command("list")
@click.option("--limit", type=int, help="Maximum number of items to return.")
@click.option("--offset", type=int, help="Number of items to skip.")
@click.option("--fields", help="Comma-separated list of fields to include.")
@click.option("--query", "-q", help="Search query.")
@click.option("--order-by", help="Field to order by.")
@common_options
def packages_list(limit, offset, fields, query, order_by, config_file, profile, verbose, output):
    """List all packages."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if fields:
            params["fields"] = fields
        if query:
            params["q"] = query
        if order_by:
            params["orderBy"] = order_by
        
        packages = client.packages.list(params=params)
        
        if output == "json":
            output_json(packages)
        elif output == "table":
            fields_list = ["id", "name"] 
            output_table(packages, fields_list)
        else:
            output_json(packages, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@packages.command("get")
@click.argument("package_id")
@common_options
def packages_get(package_id, config_file, profile, verbose, output):
    """Get a specific package by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        package = client.packages.get(package_id)
        
        if output == "json":
            output_json(package)
        elif output == "table":
            output_table([package])
        else:
            output_json(package, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@packages.command("export")
@click.argument("package_id")
@click.option("--output-file", "-o", required=True, help="Path to save the exported package file.")
@common_options
def packages_export(package_id, output_file, config_file, profile, verbose, output):
    """Export a specific package by ID."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.packages.export(package_id, output_file)
        
        click.echo(f"Package exported to {result}")
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@packages.command("import")
@click.argument("file_path")
@common_options
def packages_import(file_path, config_file, profile, verbose, output):
    """Import a package from a file."""
    configure_logging(verbose)
    
    try:
        client = OICClient(config_file=config_file, profile=profile, log_level=logging.DEBUG if verbose > 1 else None)
        
        result = client.packages.import_package(file_path)
        
        if output == "json":
            output_json(result)
        elif output == "table":
            output_table([result])
        else:
            output_json(result, pretty=True)
            
    except OICError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


# Entry point
def main():
    """Entry point for the CLI."""
    try:
        cli()
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
