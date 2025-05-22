"""
Backup workflows module for the OIC DevOps package.

This module provides workflow operations for backing up OIC resources,
including automated backups, bulk exports, and restoration.
"""

import datetime
import glob
import json
import os
import shutil
from typing import List, Optional

from oic_devops.client import OICClient
from oic_devops.exceptions import OICError
from oic_devops.workflows.base import BaseWorkflow, WorkflowResult


class BackupWorkflows(BaseWorkflow):
	"""
	Workflow operations for backing up OIC resources.

	This class provides higher-level operations for backing up OIC resources,
	including full instance backups, resource-specific backups, and restoration.
	"""

	def execute(self, *args, **kwargs) -> WorkflowResult:
		"""
		Execute the specified backup workflow.

		This is a dispatcher method that calls the appropriate workflow
		based on the operation argument.

		Args:
		    operation: The workflow operation to execute.
		    **kwargs: Additional arguments specific to the workflow.

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		operation = kwargs.pop('operation', None)

		if operation == 'full_backup':
			return self.backup_all_resources(**kwargs)
		if operation == 'selective_backup':
			return self.backup_selected_resources(**kwargs)
		if operation == 'backup_integrations':
			return self.backup_integrations(**kwargs)
		if operation == 'backup_lookups':
			return self.backup_lookups(**kwargs)
		if operation == 'backup_connections':
			return self.backup_connections(**kwargs)
		if operation == 'restore_backup':
			return self.restore_from_backup(**kwargs)
		if operation == 'prune_backups':
			return self.prune_old_backups(**kwargs)
		result = WorkflowResult(
			success=False, message=f'Unknown backup workflow operation: {operation}'
		)
		result.add_error(f'Unknown operation: {operation}')
		return result

	def backup_all_resources(
		self,
		backup_dir: str,
		include_packages: bool = True,
		include_metadata: bool = True,
		compress: bool = True,
		continue_on_error: bool = True,
	) -> WorkflowResult:
		"""
		Perform a full backup of all OIC resources.

		This workflow:
		1. Creates a backup directory structure
		2. Backs up all resource types
		3. Optionally compresses the backup

		Args:
		    backup_dir: Base directory for the backup.
		    include_packages: Whether to also back up packages.
		    include_metadata: Whether to include metadata about the backup.
		    compress: Whether to compress the backup into a single archive.
		    continue_on_error: Whether to continue if some backups fail.

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		result = WorkflowResult()

		# Create timestamp for backup
		backup_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
		backup_path = os.path.join(backup_dir, f'oic_backup_{backup_timestamp}')

		# Create backup directories
		try:
			os.makedirs(backup_path, exist_ok=True)
			os.makedirs(os.path.join(backup_path, 'integrations'), exist_ok=True)
			os.makedirs(os.path.join(backup_path, 'connections'), exist_ok=True)
			os.makedirs(os.path.join(backup_path, 'lookups'), exist_ok=True)
			os.makedirs(os.path.join(backup_path, 'libraries'), exist_ok=True)

			if include_packages:
				os.makedirs(os.path.join(backup_path, 'packages'), exist_ok=True)

			self.logger.info(f'Created backup directory structure at {backup_path}')

		except Exception as e:
			result.success = False
			result.message = f'Failed to create backup directories: {e!s}'
			result.add_error('Failed to create backup directories', e)
			return result

		# Initialize backup stats
		backup_stats = {
			'timestamp': backup_timestamp,
			'integrations': {'total': 0, 'successful': 0, 'failed': 0},
			'connections': {'total': 0, 'successful': 0, 'failed': 0},
			'lookups': {'total': 0, 'successful': 0, 'failed': 0},
			'libraries': {'total': 0, 'successful': 0, 'failed': 0},
			'packages': {'total': 0, 'successful': 0, 'failed': 0},
		}

		# Back up integrations
		try:
			self.logger.info('Backing up integrations')

			# Get all integrations
			integrations = self.client.integrations.list()
			backup_stats['integrations']['total'] = len(integrations)

			for integration in integrations:
				integration_id = integration.get('id')
				integration_name = integration.get('name', 'Unknown')

				if not integration_id:
					continue

				# Create integration export file path
				export_file = os.path.join(
					backup_path,
					'integrations',
					f'{integration_id}_{self._sanitize_filename(integration_name)}.iar',
				)

				try:
					# Export integration
					self.logger.info(f'Exporting integration {integration_name}')
					self.client.integrations.export(integration_id, export_file)

					# Update stats
					backup_stats['integrations']['successful'] += 1

					# Add to resources
					result.add_resource(
						'integration',
						integration_id,
						{
							'name': integration_name,
							'backup_file': export_file,
							'backup_successful': True,
						},
					)

				except OICError as e:
					self.logger.error(
						f'Failed to export integration {integration_name}: {e!s}'
					)
					backup_stats['integrations']['failed'] += 1

					# Add to resources
					result.add_resource(
						'integration',
						integration_id,
						{
							'name': integration_name,
							'backup_successful': False,
							'error': str(e),
						},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = f'Backup failed when exporting integration {integration_name}'
						result.add_error(
							'Failed to export integration', e, integration_id
						)
						break

		except OICError as e:
			self.logger.error(f'Failed to get integrations list: {e!s}')
			result.add_error('Failed to back up integrations', e)
			if not continue_on_error:
				result.success = False
				result.message = 'Backup failed when retrieving integrations'
				result.details['backup_stats'] = backup_stats
				result.details['backup_path'] = backup_path
				return result

		# Back up connections
		try:
			self.logger.info('Backing up connections')

			# Get all connections
			connections = self.client.connections.list()
			backup_stats['connections']['total'] = len(connections)

			for connection in connections:
				connection_id = connection.get('id')
				connection_name = connection.get('name', 'Unknown')

				if not connection_id:
					continue

				# Create connection export file path
				export_file = os.path.join(
					backup_path,
					'connections',
					f'{connection_id}_{self._sanitize_filename(connection_name)}.json',
				)

				try:
					# Get connection details for export
					connection_details = self.client.connections.get(connection_id)

					# Save to file
					with open(export_file, 'w') as f:
						json.dump(connection_details, f, indent=2)

					# Update stats
					backup_stats['connections']['successful'] += 1

					# Add to resources
					result.add_resource(
						'connection',
						connection_id,
						{
							'name': connection_name,
							'backup_file': export_file,
							'backup_successful': True,
						},
					)

				except OICError as e:
					self.logger.error(
						f'Failed to export connection {connection_name}: {e!s}'
					)
					backup_stats['connections']['failed'] += 1

					# Add to resources
					result.add_resource(
						'connection',
						connection_id,
						{
							'name': connection_name,
							'backup_successful': False,
							'error': str(e),
						},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = (
							f'Backup failed when exporting connection {connection_name}'
						)
						result.add_error(
							'Failed to export connection', e, connection_id
						)
						break

		except OICError as e:
			self.logger.error(f'Failed to get connections list: {e!s}')
			result.add_error('Failed to back up connections', e)
			if not continue_on_error:
				result.success = False
				result.message = 'Backup failed when retrieving connections'
				result.details['backup_stats'] = backup_stats
				result.details['backup_path'] = backup_path
				return result

		# Back up lookups
		try:
			self.logger.info('Backing up lookups')

			# Get all lookups
			lookups = self.client.lookups.list()
			backup_stats['lookups']['total'] = len(lookups)

			for lookup in lookups:
				lookup_id = lookup.get('id')
				lookup_name = lookup.get('name', 'Unknown')

				if not lookup_id:
					continue

				# Create lookup export file path
				export_file = os.path.join(
					backup_path,
					'lookups',
					f'{lookup_id}_{self._sanitize_filename(lookup_name)}.csv',
				)

				try:
					# Export lookup
					self.logger.info(f'Exporting lookup {lookup_name}')
					self.client.lookups.export(lookup_id, export_file)

					# Update stats
					backup_stats['lookups']['successful'] += 1

					# Add to resources
					result.add_resource(
						'lookup',
						lookup_id,
						{
							'name': lookup_name,
							'backup_file': export_file,
							'backup_successful': True,
						},
					)

				except OICError as e:
					self.logger.error(f'Failed to export lookup {lookup_name}: {e!s}')
					backup_stats['lookups']['failed'] += 1

					# Add to resources
					result.add_resource(
						'lookup',
						lookup_id,
						{
							'name': lookup_name,
							'backup_successful': False,
							'error': str(e),
						},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = (
							f'Backup failed when exporting lookup {lookup_name}'
						)
						result.add_error('Failed to export lookup', e, lookup_id)
						break

		except OICError as e:
			self.logger.error(f'Failed to get lookups list: {e!s}')
			result.add_error('Failed to back up lookups', e)
			if not continue_on_error:
				result.success = False
				result.message = 'Backup failed when retrieving lookups'
				result.details['backup_stats'] = backup_stats
				result.details['backup_path'] = backup_path
				return result

		# Back up libraries
		try:
			self.logger.info('Backing up libraries')

			# Get all libraries
			libraries = self.client.libraries.list()
			backup_stats['libraries']['total'] = len(libraries)

			for library in libraries:
				library_id = library.get('id')
				library_name = library.get('name', 'Unknown')

				if not library_id:
					continue

				# Create library export file path
				export_file = os.path.join(
					backup_path,
					'libraries',
					f'{library_id}_{self._sanitize_filename(library_name)}.jar',
				)

				try:
					# Export library
					self.logger.info(f'Exporting library {library_name}')
					self.client.libraries.export(library_id, export_file)

					# Update stats
					backup_stats['libraries']['successful'] += 1

					# Add to resources
					result.add_resource(
						'library',
						library_id,
						{
							'name': library_name,
							'backup_file': export_file,
							'backup_successful': True,
						},
					)

				except OICError as e:
					self.logger.error(f'Failed to export library {library_name}: {e!s}')
					backup_stats['libraries']['failed'] += 1

					# Add to resources
					result.add_resource(
						'library',
						library_id,
						{
							'name': library_name,
							'backup_successful': False,
							'error': str(e),
						},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = (
							f'Backup failed when exporting library {library_name}'
						)
						result.add_error('Failed to export library', e, library_id)
						break

		except OICError as e:
			self.logger.error(f'Failed to get libraries list: {e!s}')
			result.add_error('Failed to back up libraries', e)
			if not continue_on_error:
				result.success = False
				result.message = 'Backup failed when retrieving libraries'
				result.details['backup_stats'] = backup_stats
				result.details['backup_path'] = backup_path
				return result

		# Back up packages if requested
		if include_packages:
			try:
				self.logger.info('Backing up packages')

				# Get all packages
				packages = self.client.packages.list()
				backup_stats['packages']['total'] = len(packages)

				for package in packages:
					package_id = package.get('id')
					package_name = package.get('name', 'Unknown')

					if not package_id:
						continue

					# Create package export file path
					export_file = os.path.join(
						backup_path,
						'packages',
						f'{package_id}_{self._sanitize_filename(package_name)}.par',
					)

					try:
						# Export package
						self.logger.info(f'Exporting package {package_name}')
						self.client.packages.export(package_id, export_file)

						# Update stats
						backup_stats['packages']['successful'] += 1

						# Add to resources
						result.add_resource(
							'package',
							package_id,
							{
								'name': package_name,
								'backup_file': export_file,
								'backup_successful': True,
							},
						)

					except OICError as e:
						self.logger.error(
							f'Failed to export package {package_name}: {e!s}'
						)
						backup_stats['packages']['failed'] += 1

						# Add to resources
						result.add_resource(
							'package',
							package_id,
							{
								'name': package_name,
								'backup_successful': False,
								'error': str(e),
							},
						)

						# Stop if continue_on_error is False
						if not continue_on_error:
							result.success = False
							result.message = (
								f'Backup failed when exporting package {package_name}'
							)
							result.add_error('Failed to export package', e, package_id)
							break

			except OICError as e:
				self.logger.error(f'Failed to get packages list: {e!s}')
				result.add_error('Failed to back up packages', e)
				if not continue_on_error:
					result.success = False
					result.message = 'Backup failed when retrieving packages'
					result.details['backup_stats'] = backup_stats
					result.details['backup_path'] = backup_path
					return result

		# Create metadata file if requested
		if include_metadata:
			try:
				self.logger.info('Creating backup metadata')

				# Get instance info and other metadata
				instance_info = {}

				try:
					# Get instance stats
					instance_stats = self.client.monitoring.get_instance_stats()
					instance_info['stats'] = instance_stats
				except:
					# Continue even if we can't get instance stats
					pass

				# Create metadata
				metadata = {
					'backup_timestamp': backup_timestamp,
					'backup_stats': backup_stats,
					'instance_info': instance_info,
				}

				# Save metadata
				metadata_file = os.path.join(backup_path, 'backup_metadata.json')
				with open(metadata_file, 'w') as f:
					json.dump(metadata, f, indent=2)

				self.logger.info(f'Created backup metadata at {metadata_file}')

			except Exception as e:
				self.logger.error(f'Failed to create backup metadata: {e!s}')
				# Continue with backup even if metadata creation fails

		# Compress backup if requested
		if compress:
			try:
				self.logger.info('Compressing backup')

				# Create archive name
				archive_path = f'{backup_path}.zip'

				# Create archive
				shutil.make_archive(backup_path, 'zip', backup_path)

				# Remove original directory if archive was created successfully
				if os.path.exists(archive_path):
					shutil.rmtree(backup_path)

				self.logger.info(f'Compressed backup to {archive_path}')

				# Update backup path to archive
				backup_path = archive_path

			except Exception as e:
				self.logger.error(f'Failed to compress backup: {e!s}')
				# Continue even if compression fails

		# Calculate total backup statistics
		total_resources = sum(
			backup_stats[resource_type]['total']
			for resource_type in backup_stats
			if resource_type != 'timestamp'
		)
		successful_resources = sum(
			backup_stats[resource_type]['successful']
			for resource_type in backup_stats
			if resource_type != 'timestamp'
		)
		failed_resources = sum(
			backup_stats[resource_type]['failed']
			for resource_type in backup_stats
			if resource_type != 'timestamp'
		)

		# Update result details
		result.details['backup_stats'] = backup_stats
		result.details['backup_path'] = backup_path
		result.details['total_resources'] = total_resources
		result.details['successful_resources'] = successful_resources
		result.details['failed_resources'] = failed_resources

		# Update result success and message
		if failed_resources > 0:
			result.success = False
			result.message = f'Backup completed with {failed_resources} of {total_resources} resources failed'
		else:
			result.message = f'Successfully backed up all {total_resources} resources to {backup_path}'

		return result

	def backup_selected_resources(
		self,
		backup_dir: str,
		integration_ids: Optional[List[str]] = None,
		connection_ids: Optional[List[str]] = None,
		lookup_ids: Optional[List[str]] = None,
		library_ids: Optional[List[str]] = None,
		package_ids: Optional[List[str]] = None,
		compress: bool = True,
		continue_on_error: bool = True,
	) -> WorkflowResult:
		"""
		Perform a selective backup of specified OIC resources.

		This workflow:
		1. Creates a backup directory structure
		2. Backs up only the specified resources
		3. Optionally compresses the backup

		Args:
		    backup_dir: Base directory for the backup.
		    integration_ids: Optional list of integration IDs to back up.
		    connection_ids: Optional list of connection IDs to back up.
		    lookup_ids: Optional list of lookup IDs to back up.
		    library_ids: Optional list of library IDs to back up.
		    package_ids: Optional list of package IDs to back up.
		    compress: Whether to compress the backup into a single archive.
		    continue_on_error: Whether to continue if some backups fail.

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		result = WorkflowResult()

		# Check if any resources were specified
		has_resources = bool(
			integration_ids
			or connection_ids
			or lookup_ids
			or library_ids
			or package_ids
		)

		if not has_resources:
			result.message = 'No resources specified for backup'
			return result

		# Create timestamp for backup
		backup_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
		backup_path = os.path.join(
			backup_dir, f'oic_selective_backup_{backup_timestamp}'
		)

		# Create backup directories as needed
		try:
			os.makedirs(backup_path, exist_ok=True)

			if integration_ids:
				os.makedirs(os.path.join(backup_path, 'integrations'), exist_ok=True)

			if connection_ids:
				os.makedirs(os.path.join(backup_path, 'connections'), exist_ok=True)

			if lookup_ids:
				os.makedirs(os.path.join(backup_path, 'lookups'), exist_ok=True)

			if library_ids:
				os.makedirs(os.path.join(backup_path, 'libraries'), exist_ok=True)

			if package_ids:
				os.makedirs(os.path.join(backup_path, 'packages'), exist_ok=True)

			self.logger.info(f'Created backup directory structure at {backup_path}')

		except Exception as e:
			result.success = False
			result.message = f'Failed to create backup directories: {e!s}'
			result.add_error('Failed to create backup directories', e)
			return result

		# Initialize backup stats
		backup_stats = {
			'timestamp': backup_timestamp,
			'integrations': {'total': 0, 'successful': 0, 'failed': 0},
			'connections': {'total': 0, 'successful': 0, 'failed': 0},
			'lookups': {'total': 0, 'successful': 0, 'failed': 0},
			'libraries': {'total': 0, 'successful': 0, 'failed': 0},
			'packages': {'total': 0, 'successful': 0, 'failed': 0},
		}

		# Back up integrations
		if integration_ids:
			backup_stats['integrations']['total'] = len(integration_ids)

			for integration_id in integration_ids:
				try:
					# Get integration details
					integration = self.client.integrations.get(integration_id)
					integration_name = integration.get('name', 'Unknown')

					# Create integration export file path
					export_file = os.path.join(
						backup_path,
						'integrations',
						f'{integration_id}_{self._sanitize_filename(integration_name)}.iar',
					)

					# Export integration
					self.logger.info(f'Exporting integration {integration_name}')
					self.client.integrations.export(integration_id, export_file)

					# Update stats
					backup_stats['integrations']['successful'] += 1

					# Add to resources
					result.add_resource(
						'integration',
						integration_id,
						{
							'name': integration_name,
							'backup_file': export_file,
							'backup_successful': True,
						},
					)

				except OICError as e:
					self.logger.error(
						f'Failed to export integration {integration_id}: {e!s}'
					)
					backup_stats['integrations']['failed'] += 1

					# Add to resources
					result.add_resource(
						'integration',
						integration_id,
						{'backup_successful': False, 'error': str(e)},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = (
							f'Backup failed when exporting integration {integration_id}'
						)
						result.add_error(
							'Failed to export integration', e, integration_id
						)
						break

		# Back up connections
		if connection_ids and (result.success or continue_on_error):
			backup_stats['connections']['total'] = len(connection_ids)

			for connection_id in connection_ids:
				try:
					# Get connection details
					connection = self.client.connections.get(connection_id)
					connection_name = connection.get('name', 'Unknown')

					# Create connection export file path
					export_file = os.path.join(
						backup_path,
						'connections',
						f'{connection_id}_{self._sanitize_filename(connection_name)}.json',
					)

					# Save to file
					with open(export_file, 'w') as f:
						json.dump(connection, f, indent=2)

					# Update stats
					backup_stats['connections']['successful'] += 1

					# Add to resources
					result.add_resource(
						'connection',
						connection_id,
						{
							'name': connection_name,
							'backup_file': export_file,
							'backup_successful': True,
						},
					)

				except OICError as e:
					self.logger.error(
						f'Failed to export connection {connection_id}: {e!s}'
					)
					backup_stats['connections']['failed'] += 1

					# Add to resources
					result.add_resource(
						'connection',
						connection_id,
						{'backup_successful': False, 'error': str(e)},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = (
							f'Backup failed when exporting connection {connection_id}'
						)
						result.add_error(
							'Failed to export connection', e, connection_id
						)
						break

		# Back up lookups
		if lookup_ids and (result.success or continue_on_error):
			backup_stats['lookups']['total'] = len(lookup_ids)

			for lookup_id in lookup_ids:
				try:
					# Get lookup details
					lookup = self.client.lookups.get(lookup_id)
					lookup_name = lookup.get('name', 'Unknown')

					# Create lookup export file path
					export_file = os.path.join(
						backup_path,
						'lookups',
						f'{lookup_id}_{self._sanitize_filename(lookup_name)}.csv',
					)

					# Export lookup
					self.logger.info(f'Exporting lookup {lookup_name}')
					self.client.lookups.export(lookup_id, export_file)

					# Update stats
					backup_stats['lookups']['successful'] += 1

					# Add to resources
					result.add_resource(
						'lookup',
						lookup_id,
						{
							'name': lookup_name,
							'backup_file': export_file,
							'backup_successful': True,
						},
					)

				except OICError as e:
					self.logger.error(f'Failed to export lookup {lookup_id}: {e!s}')
					backup_stats['lookups']['failed'] += 1

					# Add to resources
					result.add_resource(
						'lookup',
						lookup_id,
						{'backup_successful': False, 'error': str(e)},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = (
							f'Backup failed when exporting lookup {lookup_id}'
						)
						result.add_error('Failed to export lookup', e, lookup_id)
						break

		# Back up libraries
		if library_ids and (result.success or continue_on_error):
			backup_stats['libraries']['total'] = len(library_ids)

			for library_id in library_ids:
				try:
					# Get library details
					library = self.client.libraries.get(library_id)
					library_name = library.get('name', 'Unknown')

					# Create library export file path
					export_file = os.path.join(
						backup_path,
						'libraries',
						f'{library_id}_{self._sanitize_filename(library_name)}.jar',
					)

					# Export library
					self.logger.info(f'Exporting library {library_name}')
					self.client.libraries.export(library_id, export_file)

					# Update stats
					backup_stats['libraries']['successful'] += 1

					# Add to resources
					result.add_resource(
						'library',
						library_id,
						{
							'name': library_name,
							'backup_file': export_file,
							'backup_successful': True,
						},
					)

				except OICError as e:
					self.logger.error(f'Failed to export library {library_id}: {e!s}')
					backup_stats['libraries']['failed'] += 1

					# Add to resources
					result.add_resource(
						'library',
						library_id,
						{'backup_successful': False, 'error': str(e)},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = (
							f'Backup failed when exporting library {library_id}'
						)
						result.add_error('Failed to export library', e, library_id)
						break

		# Back up packages
		if package_ids and (result.success or continue_on_error):
			backup_stats['packages']['total'] = len(package_ids)

			for package_id in package_ids:
				try:
					# Get package details
					package = self.client.packages.get(package_id)
					package_name = package.get('name', 'Unknown')

					# Create package export file path
					export_file = os.path.join(
						backup_path,
						'packages',
						f'{package_id}_{self._sanitize_filename(package_name)}.par',
					)

					# Export package
					self.logger.info(f'Exporting package {package_name}')
					self.client.packages.export(package_id, export_file)

					# Update stats
					backup_stats['packages']['successful'] += 1

					# Add to resources
					result.add_resource(
						'package',
						package_id,
						{
							'name': package_name,
							'backup_file': export_file,
							'backup_successful': True,
						},
					)

				except OICError as e:
					self.logger.error(f'Failed to export package {package_id}: {e!s}')
					backup_stats['packages']['failed'] += 1

					# Add to resources
					result.add_resource(
						'package',
						package_id,
						{'backup_successful': False, 'error': str(e)},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = (
							f'Backup failed when exporting package {package_id}'
						)
						result.add_error('Failed to export package', e, package_id)
						break

		# Create metadata file
		try:
			self.logger.info('Creating backup metadata')

			# Create metadata
			metadata = {
				'backup_timestamp': backup_timestamp,
				'backup_stats': backup_stats,
				'selective_backup': True,
				'resource_counts': {
					'integrations': len(integration_ids) if integration_ids else 0,
					'connections': len(connection_ids) if connection_ids else 0,
					'lookups': len(lookup_ids) if lookup_ids else 0,
					'libraries': len(library_ids) if library_ids else 0,
					'packages': len(package_ids) if package_ids else 0,
				},
			}

			# Save metadata
			metadata_file = os.path.join(backup_path, 'backup_metadata.json')
			with open(metadata_file, 'w') as f:
				json.dump(metadata, f, indent=2)

			self.logger.info(f'Created backup metadata at {metadata_file}')

		except Exception as e:
			self.logger.error(f'Failed to create backup metadata: {e!s}')
			# Continue with backup even if metadata creation fails

		# Compress backup if requested
		if compress:
			try:
				self.logger.info('Compressing backup')

				# Create archive name
				archive_path = f'{backup_path}.zip'

				# Create archive
				shutil.make_archive(backup_path, 'zip', backup_path)

				# Remove original directory if archive was created successfully
				if os.path.exists(archive_path):
					shutil.rmtree(backup_path)

				self.logger.info(f'Compressed backup to {archive_path}')

				# Update backup path to archive
				backup_path = archive_path

			except Exception as e:
				self.logger.error(f'Failed to compress backup: {e!s}')
				# Continue even if compression fails

		# Calculate total backup statistics
		total_resources = sum(
			backup_stats[resource_type]['total']
			for resource_type in backup_stats
			if resource_type != 'timestamp'
		)
		successful_resources = sum(
			backup_stats[resource_type]['successful']
			for resource_type in backup_stats
			if resource_type != 'timestamp'
		)
		failed_resources = sum(
			backup_stats[resource_type]['failed']
			for resource_type in backup_stats
			if resource_type != 'timestamp'
		)

		# Update result details
		result.details['backup_stats'] = backup_stats
		result.details['backup_path'] = backup_path
		result.details['total_resources'] = total_resources
		result.details['successful_resources'] = successful_resources
		result.details['failed_resources'] = failed_resources

		# Update result success and message
		if failed_resources > 0:
			result.success = False
			result.message = f'Selective backup completed with {failed_resources} of {total_resources} resources failed'
		else:
			result.message = f'Successfully backed up all {total_resources} selected resources to {backup_path}'

		return result

	def backup_integrations(
		self,
		backup_dir: str,
		filter_query: Optional[str] = None,
		include_dependencies: bool = False,
		compress: bool = True,
		continue_on_error: bool = True,
	) -> WorkflowResult:
		"""
		Back up integrations with optional filtering.

		This workflow:
		1. Gets integrations based on filter criteria
		2. Backs up the integrations
		3. Optionally backs up dependencies

		Args:
		    backup_dir: Base directory for the backup.
		    filter_query: Optional query to filter integrations.
		    include_dependencies: Whether to include dependencies (connections, lookups).
		    compress: Whether to compress the backup into a single archive.
		    continue_on_error: Whether to continue if some backups fail.

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		result = WorkflowResult()

		# Create timestamp for backup
		backup_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
		backup_path = os.path.join(
			backup_dir, f'oic_integrations_backup_{backup_timestamp}'
		)

		# Create backup directories
		try:
			os.makedirs(backup_path, exist_ok=True)
			os.makedirs(os.path.join(backup_path, 'integrations'), exist_ok=True)

			if include_dependencies:
				os.makedirs(os.path.join(backup_path, 'connections'), exist_ok=True)
				os.makedirs(os.path.join(backup_path, 'lookups'), exist_ok=True)

			self.logger.info(f'Created backup directory structure at {backup_path}')

		except Exception as e:
			result.success = False
			result.message = f'Failed to create backup directories: {e!s}'
			result.add_error('Failed to create backup directories', e)
			return result

		# Initialize backup stats
		backup_stats = {
			'timestamp': backup_timestamp,
			'integrations': {'total': 0, 'successful': 0, 'failed': 0},
			'connections': {'total': 0, 'successful': 0, 'failed': 0},
			'lookups': {'total': 0, 'successful': 0, 'failed': 0},
		}

		# Get integrations based on filter
		try:
			params = {}
			if filter_query:
				params['q'] = filter_query

			integrations = self.client.integrations.list(params=params)
			backup_stats['integrations']['total'] = len(integrations)

			if not integrations:
				result.message = 'No integrations found matching the filter criteria'
				return result

			# Track dependencies
			dependencies = {'connections': set(), 'lookups': set()}

			# Back up each integration
			for integration in integrations:
				integration_id = integration.get('id')
				integration_name = integration.get('name', 'Unknown')

				if not integration_id:
					continue

				# Create integration export file path
				export_file = os.path.join(
					backup_path,
					'integrations',
					f'{integration_id}_{self._sanitize_filename(integration_name)}.iar',
				)

				try:
					# Export integration
					self.logger.info(f'Exporting integration {integration_name}')
					self.client.integrations.export(integration_id, export_file)

					# Update stats
					backup_stats['integrations']['successful'] += 1

					# Add to resources
					result.add_resource(
						'integration',
						integration_id,
						{
							'name': integration_name,
							'backup_file': export_file,
							'backup_successful': True,
						},
					)

					# Find dependencies if requested
					if include_dependencies:
						try:
							# Get detailed integration to find dependencies
							integration_detail = self.client.integrations.get(
								integration_id
							)

							# Look for connection references
							if 'references' in integration_detail:
								for ref in integration_detail['references']:
									if isinstance(ref, dict):
										ref_type = ref.get('type')
										ref_id = ref.get('id')

										if ref_type == 'CONNECTION' and ref_id:
											dependencies['connections'].add(ref_id)
										elif ref_type == 'LOOKUP' and ref_id:
											dependencies['lookups'].add(ref_id)

							# Look for connection references in other sections
							for section in ['triggers', 'invokes']:
								if section in integration_detail:
									for item in integration_detail[section]:
										if (
											isinstance(item, dict)
											and 'connectionId' in item
										):
											conn_id = item['connectionId']
											if conn_id:
												dependencies['connections'].add(conn_id)

						except OICError as e:
							self.logger.warning(
								f'Failed to get dependencies for integration {integration_name}: {e!s}'
							)
							# Continue even if we can't get dependencies

				except OICError as e:
					self.logger.error(
						f'Failed to export integration {integration_name}: {e!s}'
					)
					backup_stats['integrations']['failed'] += 1

					# Add to resources
					result.add_resource(
						'integration',
						integration_id,
						{
							'name': integration_name,
							'backup_successful': False,
							'error': str(e),
						},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = f'Backup failed when exporting integration {integration_name}'
						result.add_error(
							'Failed to export integration', e, integration_id
						)
						break

			# Back up dependencies if requested and any integrations were backed up successfully
			if include_dependencies and backup_stats['integrations']['successful'] > 0:
				# Back up connections
				if dependencies['connections']:
					backup_stats['connections']['total'] = len(
						dependencies['connections']
					)

					for connection_id in dependencies['connections']:
						try:
							# Get connection details
							connection = self.client.connections.get(connection_id)
							connection_name = connection.get('name', 'Unknown')

							# Create connection export file path
							export_file = os.path.join(
								backup_path,
								'connections',
								f'{connection_id}_{self._sanitize_filename(connection_name)}.json',
							)

							# Save to file
							with open(export_file, 'w') as f:
								json.dump(connection, f, indent=2)

							# Update stats
							backup_stats['connections']['successful'] += 1

							# Add to resources
							result.add_resource(
								'connection',
								connection_id,
								{
									'name': connection_name,
									'backup_file': export_file,
									'backup_successful': True,
								},
							)

						except OICError as e:
							self.logger.error(
								f'Failed to export connection {connection_id}: {e!s}'
							)
							backup_stats['connections']['failed'] += 1

							# Add to resources
							result.add_resource(
								'connection',
								connection_id,
								{'backup_successful': False, 'error': str(e)},
							)

							# Continue even if one dependency fails

				# Back up lookups
				if dependencies['lookups']:
					backup_stats['lookups']['total'] = len(dependencies['lookups'])

					for lookup_id in dependencies['lookups']:
						try:
							# Get lookup details
							lookup = self.client.lookups.get(lookup_id)
							lookup_name = lookup.get('name', 'Unknown')

							# Create lookup export file path
							export_file = os.path.join(
								backup_path,
								'lookups',
								f'{lookup_id}_{self._sanitize_filename(lookup_name)}.csv',
							)

							# Export lookup
							self.logger.info(f'Exporting lookup {lookup_name}')
							self.client.lookups.export(lookup_id, export_file)

							# Update stats
							backup_stats['lookups']['successful'] += 1

							# Add to resources
							result.add_resource(
								'lookup',
								lookup_id,
								{
									'name': lookup_name,
									'backup_file': export_file,
									'backup_successful': True,
								},
							)

						except OICError as e:
							self.logger.error(
								f'Failed to export lookup {lookup_id}: {e!s}'
							)
							backup_stats['lookups']['failed'] += 1

							# Add to resources
							result.add_resource(
								'lookup',
								lookup_id,
								{'backup_successful': False, 'error': str(e)},
							)

							# Continue even if one dependency fails

		except OICError as e:
			self.logger.error(f'Failed to get integrations: {e!s}')
			result.add_error('Failed to get integrations', e)
			result.success = False
			result.message = 'Backup failed when retrieving integrations'
			return result

		# Create metadata file
		try:
			self.logger.info('Creating backup metadata')

			# Create metadata
			metadata = {
				'backup_timestamp': backup_timestamp,
				'backup_stats': backup_stats,
				'filter_query': filter_query,
				'include_dependencies': include_dependencies,
			}

			# Save metadata
			metadata_file = os.path.join(backup_path, 'backup_metadata.json')
			with open(metadata_file, 'w') as f:
				json.dump(metadata, f, indent=2)

			self.logger.info(f'Created backup metadata at {metadata_file}')

		except Exception as e:
			self.logger.error(f'Failed to create backup metadata: {e!s}')
			# Continue with backup even if metadata creation fails

		# Compress backup if requested
		if compress:
			try:
				self.logger.info('Compressing backup')

				# Create archive name
				archive_path = f'{backup_path}.zip'

				# Create archive
				shutil.make_archive(backup_path, 'zip', backup_path)

				# Remove original directory if archive was created successfully
				if os.path.exists(archive_path):
					shutil.rmtree(backup_path)

				self.logger.info(f'Compressed backup to {archive_path}')

				# Update backup path to archive
				backup_path = archive_path

			except Exception as e:
				self.logger.error(f'Failed to compress backup: {e!s}')
				# Continue even if compression fails

		# Calculate total backup statistics
		total_resources = sum(
			backup_stats[resource_type]['total']
			for resource_type in backup_stats
			if resource_type != 'timestamp'
		)
		successful_resources = sum(
			backup_stats[resource_type]['successful']
			for resource_type in backup_stats
			if resource_type != 'timestamp'
		)
		failed_resources = sum(
			backup_stats[resource_type]['failed']
			for resource_type in backup_stats
			if resource_type != 'timestamp'
		)

		# Update result details
		result.details['backup_stats'] = backup_stats
		result.details['backup_path'] = backup_path
		result.details['total_resources'] = total_resources
		result.details['successful_resources'] = successful_resources
		result.details['failed_resources'] = failed_resources

		# Update result success and message
		if failed_resources > 0:
			result.success = False
			result.message = f'Integration backup completed with {failed_resources} of {total_resources} resources failed'
		else:
			result.message = f'Successfully backed up {backup_stats["integrations"]["successful"]} integrations'

			if include_dependencies:
				result.message += f' and {backup_stats["connections"]["successful"] + backup_stats["lookups"]["successful"]} dependencies'

			result.message += f' to {backup_path}'

		return result

	def backup_lookups(
		self,
		backup_dir: str,
		filter_query: Optional[str] = None,
		include_data: bool = True,
		compress: bool = True,
		continue_on_error: bool = True,
	) -> WorkflowResult:
		"""
		Back up lookups with optional filtering.

		This workflow:
		1. Gets lookups based on filter criteria
		2. Backs up the lookups and optionally their data

		Args:
		    backup_dir: Base directory for the backup.
		    filter_query: Optional query to filter lookups.
		    include_data: Whether to include lookup data.
		    compress: Whether to compress the backup into a single archive.
		    continue_on_error: Whether to continue if some backups fail.

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		result = WorkflowResult()

		# Create timestamp for backup
		backup_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
		backup_path = os.path.join(backup_dir, f'oic_lookups_backup_{backup_timestamp}')

		# Create backup directories
		try:
			os.makedirs(backup_path, exist_ok=True)
			os.makedirs(os.path.join(backup_path, 'lookups'), exist_ok=True)

			if include_data:
				os.makedirs(os.path.join(backup_path, 'lookup_data'), exist_ok=True)

			self.logger.info(f'Created backup directory structure at {backup_path}')

		except Exception as e:
			result.success = False
			result.message = f'Failed to create backup directories: {e!s}'
			result.add_error('Failed to create backup directories', e)
			return result

		# Initialize backup stats
		backup_stats = {
			'timestamp': backup_timestamp,
			'lookups': {'total': 0, 'successful': 0, 'failed': 0},
			'lookup_data': {'total': 0, 'successful': 0, 'failed': 0},
		}

		# Get lookups based on filter
		try:
			params = {}
			if filter_query:
				params['q'] = filter_query

			lookups = self.client.lookups.list(params=params)
			backup_stats['lookups']['total'] = len(lookups)

			if not lookups:
				result.message = 'No lookups found matching the filter criteria'
				return result

			# Back up each lookup
			for lookup in lookups:
				lookup_id = lookup.get('id')
				lookup_name = lookup.get('name', 'Unknown')

				if not lookup_id:
					continue

				# Create lookup export file path
				export_file = os.path.join(
					backup_path,
					'lookups',
					f'{lookup_id}_{self._sanitize_filename(lookup_name)}.csv',
				)

				try:
					# Export lookup
					self.logger.info(f'Exporting lookup {lookup_name}')
					self.client.lookups.export(lookup_id, export_file)

					# Update stats
					backup_stats['lookups']['successful'] += 1

					# Add to resources
					result.add_resource(
						'lookup',
						lookup_id,
						{
							'name': lookup_name,
							'backup_file': export_file,
							'backup_successful': True,
						},
					)

					# Backup lookup data if requested
					if include_data:
						try:
							# Get lookup data
							lookup_data = self.client.lookups.get_data(lookup_id)

							# Create data export file
							data_file = os.path.join(
								backup_path,
								'lookup_data',
								f'{lookup_id}_{self._sanitize_filename(lookup_name)}_data.json',
							)

							# Save data to file
							with open(data_file, 'w') as f:
								json.dump(lookup_data, f, indent=2)

							# Update stats
							backup_stats['lookup_data']['successful'] += 1
							backup_stats['lookup_data']['total'] += 1

						except OICError as e:
							self.logger.warning(
								f'Failed to get data for lookup {lookup_name}: {e!s}'
							)
							backup_stats['lookup_data']['failed'] += 1
							backup_stats['lookup_data']['total'] += 1

							# Continue even if we can't get data for one lookup

				except OICError as e:
					self.logger.error(f'Failed to export lookup {lookup_name}: {e!s}')
					backup_stats['lookups']['failed'] += 1

					# Add to resources
					result.add_resource(
						'lookup',
						lookup_id,
						{
							'name': lookup_name,
							'backup_successful': False,
							'error': str(e),
						},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = (
							f'Backup failed when exporting lookup {lookup_name}'
						)
						result.add_error('Failed to export lookup', e, lookup_id)
						break

		except OICError as e:
			self.logger.error(f'Failed to get lookups: {e!s}')
			result.add_error('Failed to get lookups', e)
			result.success = False
			result.message = 'Backup failed when retrieving lookups'
			return result

		# Create metadata file
		try:
			self.logger.info('Creating backup metadata')

			# Create metadata
			metadata = {
				'backup_timestamp': backup_timestamp,
				'backup_stats': backup_stats,
				'filter_query': filter_query,
				'include_data': include_data,
			}

			# Save metadata
			metadata_file = os.path.join(backup_path, 'backup_metadata.json')
			with open(metadata_file, 'w') as f:
				json.dump(metadata, f, indent=2)

			self.logger.info(f'Created backup metadata at {metadata_file}')

		except Exception as e:
			self.logger.error(f'Failed to create backup metadata: {e!s}')
			# Continue with backup even if metadata creation fails

		# Compress backup if requested
		if compress:
			try:
				self.logger.info('Compressing backup')

				# Create archive name
				archive_path = f'{backup_path}.zip'

				# Create archive
				shutil.make_archive(backup_path, 'zip', backup_path)

				# Remove original directory if archive was created successfully
				if os.path.exists(archive_path):
					shutil.rmtree(backup_path)

				self.logger.info(f'Compressed backup to {archive_path}')

				# Update backup path to archive
				backup_path = archive_path

			except Exception as e:
				self.logger.error(f'Failed to compress backup: {e!s}')
				# Continue even if compression fails

		# Calculate total backup statistics
		total_resources = backup_stats['lookups']['total'] + (
			backup_stats['lookup_data']['total'] if include_data else 0
		)
		successful_resources = backup_stats['lookups']['successful'] + (
			backup_stats['lookup_data']['successful'] if include_data else 0
		)
		failed_resources = backup_stats['lookups']['failed'] + (
			backup_stats['lookup_data']['failed'] if include_data else 0
		)

		# Update result details
		result.details['backup_stats'] = backup_stats
		result.details['backup_path'] = backup_path
		result.details['total_resources'] = total_resources
		result.details['successful_resources'] = successful_resources
		result.details['failed_resources'] = failed_resources

		# Update result success and message
		if failed_resources > 0:
			result.success = False
			result.message = f'Lookup backup completed with {failed_resources} of {total_resources} resources failed'
		else:
			result.message = f'Successfully backed up {backup_stats["lookups"]["successful"]} lookups'

			if include_data:
				result.message += ' with data'

			result.message += f' to {backup_path}'

		return result

	def backup_connections(
		self,
		backup_dir: str,
		filter_query: Optional[str] = None,
		include_credentials: bool = False,
		compress: bool = True,
		continue_on_error: bool = True,
	) -> WorkflowResult:
		"""
		Back up connections with optional filtering.

		This workflow:
		1. Gets connections based on filter criteria
		2. Backs up the connections
		3. Optionally includes or redacts credentials

		Args:
		    backup_dir: Base directory for the backup.
		    filter_query: Optional query to filter connections.
		    include_credentials: Whether to include sensitive credentials.
		    compress: Whether to compress the backup into a single archive.
		    continue_on_error: Whether to continue if some backups fail.

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		result = WorkflowResult()

		# Create timestamp for backup
		backup_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
		backup_path = os.path.join(
			backup_dir, f'oic_connections_backup_{backup_timestamp}'
		)

		# Create backup directories
		try:
			os.makedirs(backup_path, exist_ok=True)
			os.makedirs(os.path.join(backup_path, 'connections'), exist_ok=True)

			self.logger.info(f'Created backup directory structure at {backup_path}')

		except Exception as e:
			result.success = False
			result.message = f'Failed to create backup directories: {e!s}'
			result.add_error('Failed to create backup directories', e)
			return result

		# Initialize backup stats
		backup_stats = {
			'timestamp': backup_timestamp,
			'connections': {'total': 0, 'successful': 0, 'failed': 0},
		}

		# Get connections based on filter
		try:
			params = {}
			if filter_query:
				params['q'] = filter_query

			connections = self.client.connections.list(params=params)
			backup_stats['connections']['total'] = len(connections)

			if not connections:
				result.message = 'No connections found matching the filter criteria'
				return result

			# Back up each connection
			for connection in connections:
				connection_id = connection.get('id')
				connection_name = connection.get('name', 'Unknown')
				connection_type = connection.get('connectionType', 'Unknown')

				if not connection_id:
					continue

				try:
					# Get connection details
					connection_details = self.client.connections.get(connection_id)

					# Redact credentials if requested
					if not include_credentials:
						# Look for credential fields in different properties sections
						for section in [
							'securityProperties',
							'connectionProperties',
							'properties',
						]:
							if section in connection_details:
								props = connection_details[section]

								# Redact common credential fields
								for field in [
									'password',
									'apiKey',
									'secretKey',
									'secret',
									'token',
									'accessToken',
									'refreshToken',
								]:
									if field in props:
										props[field] = '**REDACTED**'

					# Create connection export file path
					export_file = os.path.join(
						backup_path,
						'connections',
						f'{connection_id}_{self._sanitize_filename(connection_name)}.json',
					)

					# Save to file
					with open(export_file, 'w') as f:
						json.dump(connection_details, f, indent=2)

					# Update stats
					backup_stats['connections']['successful'] += 1

					# Add to resources
					result.add_resource(
						'connection',
						connection_id,
						{
							'name': connection_name,
							'type': connection_type,
							'backup_file': export_file,
							'backup_successful': True,
							'credentials_included': include_credentials,
						},
					)

				except OICError as e:
					self.logger.error(
						f'Failed to export connection {connection_name}: {e!s}'
					)
					backup_stats['connections']['failed'] += 1

					# Add to resources
					result.add_resource(
						'connection',
						connection_id,
						{
							'name': connection_name,
							'type': connection_type,
							'backup_successful': False,
							'error': str(e),
						},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = (
							f'Backup failed when exporting connection {connection_name}'
						)
						result.add_error(
							'Failed to export connection', e, connection_id
						)
						break

		except OICError as e:
			self.logger.error(f'Failed to get connections: {e!s}')
			result.add_error('Failed to get connections', e)
			result.success = False
			result.message = 'Backup failed when retrieving connections'
			return result

		# Create metadata file
		try:
			self.logger.info('Creating backup metadata')

			# Create metadata
			metadata = {
				'backup_timestamp': backup_timestamp,
				'backup_stats': backup_stats,
				'filter_query': filter_query,
				'include_credentials': include_credentials,
			}

			# Save metadata
			metadata_file = os.path.join(backup_path, 'backup_metadata.json')
			with open(metadata_file, 'w') as f:
				json.dump(metadata, f, indent=2)

			self.logger.info(f'Created backup metadata at {metadata_file}')

		except Exception as e:
			self.logger.error(f'Failed to create backup metadata: {e!s}')
			# Continue with backup even if metadata creation fails

		# Compress backup if requested
		if compress:
			try:
				self.logger.info('Compressing backup')

				# Create archive name
				archive_path = f'{backup_path}.zip'

				# Create archive
				shutil.make_archive(backup_path, 'zip', backup_path)

				# Remove original directory if archive was created successfully
				if os.path.exists(archive_path):
					shutil.rmtree(backup_path)

				self.logger.info(f'Compressed backup to {archive_path}')

				# Update backup path to archive
				backup_path = archive_path

			except Exception as e:
				self.logger.error(f'Failed to compress backup: {e!s}')
				# Continue even if compression fails

		# Update result details
		result.details['backup_stats'] = backup_stats
		result.details['backup_path'] = backup_path
		result.details['total_resources'] = backup_stats['connections']['total']
		result.details['successful_resources'] = backup_stats['connections'][
			'successful'
		]
		result.details['failed_resources'] = backup_stats['connections']['failed']

		# Update result success and message
		if backup_stats['connections']['failed'] > 0:
			result.success = False
			result.message = f'Connection backup completed with {backup_stats["connections"]["failed"]} of {backup_stats["connections"]["total"]} connections failed'
		else:
			credentials_str = (
				' with credentials'
				if include_credentials
				else ' with redacted credentials'
			)
			result.message = f'Successfully backed up {backup_stats["connections"]["successful"]} connections{credentials_str} to {backup_path}'

		return result

	def restore_from_backup(
		self,
		backup_path: str,
		target_client: Optional[OICClient] = None,
		resource_types: List[str] = [
			'integrations',
			'connections',
			'lookups',
			'libraries',
			'packages',
		],
		filter_pattern: Optional[str] = None,
		overwrite_existing: bool = True,
		continue_on_error: bool = True,
	) -> WorkflowResult:
		"""
		Restore resources from a backup.

		This workflow:
		1. Extracts the backup if compressed
		2. Reads the backup metadata
		3. Restores specified resource types

		Args:
		    backup_path: Path to the backup file or directory.
		    target_client: Optional target client for cross-instance restore.
		    resource_types: Types of resources to restore.
		    filter_pattern: Optional regex pattern to filter resources to restore.
		    overwrite_existing: Whether to overwrite existing resources.
		    continue_on_error: Whether to continue if some restores fail.

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		result = WorkflowResult()

		# Check if target client is provided, otherwise use the current client
		if target_client is None:
			target_client = self.client

		# Handle compressed backup
		is_compressed = backup_path.endswith('.zip')
		backup_dir = backup_path
		temp_dir = None

		if is_compressed:
			try:
				self.logger.info(f'Extracting backup {backup_path}')

				# Create temporary directory
				import tempfile

				temp_dir = tempfile.mkdtemp(prefix='oic_restore_')

				# Extract backup
				shutil.unpack_archive(backup_path, temp_dir, format='zip')

				# Update backup directory
				backup_dir = temp_dir
				self.logger.info(f'Extracted backup to {backup_dir}')

			except Exception as e:
				result.success = False
				result.message = f'Failed to extract backup: {e!s}'
				result.add_error('Failed to extract backup', e)
				return result

		# Initialize restore stats
		restore_stats = {
			'integrations': {'total': 0, 'successful': 0, 'failed': 0},
			'connections': {'total': 0, 'successful': 0, 'failed': 0},
			'lookups': {'total': 0, 'successful': 0, 'failed': 0},
			'libraries': {'total': 0, 'successful': 0, 'failed': 0},
			'packages': {'total': 0, 'successful': 0, 'failed': 0},
		}

		# Read backup metadata if available
		backup_metadata = None
		metadata_file = os.path.join(backup_dir, 'backup_metadata.json')

		if os.path.exists(metadata_file):
			try:
				with open(metadata_file) as f:
					backup_metadata = json.load(f)

				self.logger.info(f'Read backup metadata from {metadata_file}')

			except Exception as e:
				self.logger.warning(f'Failed to read backup metadata: {e!s}')
				# Continue even if we can't read metadata

		# Restore each resource type
		for resource_type in resource_types:
			# Check if resource directory exists
			resource_dir = os.path.join(backup_dir, resource_type)
			if not os.path.exists(resource_dir) or not os.path.isdir(resource_dir):
				self.logger.warning(
					f'Backup does not contain {resource_type} directory'
				)
				continue

			self.logger.info(f'Restoring {resource_type}')

			# Get list of resource files
			resource_files = glob.glob(os.path.join(resource_dir, '*.*'))

			# Filter resources if pattern provided
			if filter_pattern:
				pattern = re.compile(filter_pattern)
				resource_files = [
					f for f in resource_files if pattern.search(os.path.basename(f))
				]

			restore_stats[resource_type]['total'] = len(resource_files)

			# Import each resource
			for resource_file in resource_files:
				file_name = os.path.basename(resource_file)

				try:
					# Extract resource ID and name from filename
					# Format: id_name.ext
					match = re.match(r'^([^_]+)_(.+)\.(.+)$', file_name)
					if match:
						resource_id = match.group(1)
						resource_name = match.group(2)
						file_ext = match.group(3)
					else:
						# If filename doesn't match pattern, use the whole filename
						resource_id = 'unknown'
						resource_name = file_name
						file_ext = os.path.splitext(file_name)[1][1:]

					self.logger.info(f'Restoring {resource_type} from {file_name}')

					# Perform import based on resource type
					if resource_type == 'integrations':
						# Import integration
						import_result = target_client.integrations.import_integration(
							resource_file, {'overwrite': overwrite_existing}
						)

						# Extract imported resource details
						imported_id = import_result.get('id')
						imported_name = import_result.get('name', 'Unknown')

						# Update stats
						restore_stats[resource_type]['successful'] += 1

						# Add to resources
						result.add_resource(
							'integration',
							imported_id,
							{
								'name': imported_name,
								'source_file': resource_file,
								'restore_successful': True,
							},
						)

					elif resource_type == 'connections':
						# Import connection from JSON
						with open(resource_file) as f:
							connection_data = json.load(f)

						# Check if connection exists
						try:
							# Try to find existing connection by identifier
							identifier = connection_data.get('identifier')
							existing = None

							if identifier:
								params = {'q': f'identifier:{identifier}'}
								existing_connections = target_client.connections.list(
									params=params
								)

								if existing_connections:
									existing = existing_connections[0]
									existing_id = existing['id']

									if overwrite_existing:
										# Update existing connection
										import_result = (
											target_client.connections.update(
												existing_id, connection_data
											)
										)
									else:
										# Skip without error
										self.logger.info(
											f'Skipping existing connection: {existing.get("name", "Unknown")}'
										)
										import_result = existing
								else:
									# Create new connection
									import_result = target_client.connections.create(
										connection_data
									)
							else:
								# Create new connection
								import_result = target_client.connections.create(
									connection_data
								)

							# Extract imported resource details
							imported_id = import_result.get('id')
							imported_name = import_result.get('name', 'Unknown')

							# Update stats
							restore_stats[resource_type]['successful'] += 1

							# Add to resources
							result.add_resource(
								'connection',
								imported_id,
								{
									'name': imported_name,
									'source_file': resource_file,
									'restore_successful': True,
								},
							)

						except OICError as e:
							self.logger.error(
								f'Failed to import connection {resource_name}: {e!s}'
							)
							restore_stats[resource_type]['failed'] += 1

							# Add to resources
							result.add_resource(
								'connection',
								resource_id,
								{
									'name': resource_name,
									'source_file': resource_file,
									'restore_successful': False,
									'error': str(e),
								},
							)

							# Stop if continue_on_error is False
							if not continue_on_error:
								result.success = False
								result.message = f'Restore failed when importing connection {resource_name}'
								result.add_error(
									'Failed to import connection', e, resource_id
								)
								break

					elif resource_type == 'lookups':
						# Import lookup
						import_result = target_client.lookups.import_lookup(
							resource_file, {'overwrite': overwrite_existing}
						)

						# Extract imported resource details
						imported_id = import_result.get('id')
						imported_name = import_result.get('name', 'Unknown')

						# Update stats
						restore_stats[resource_type]['successful'] += 1

						# Add to resources
						result.add_resource(
							'lookup',
							imported_id,
							{
								'name': imported_name,
								'source_file': resource_file,
								'restore_successful': True,
							},
						)

					elif resource_type == 'libraries':
						# Import library
						import_result = target_client.libraries.import_library(
							resource_file, {'overwrite': overwrite_existing}
						)

						# Extract imported resource details
						imported_id = import_result.get('id')
						imported_name = import_result.get('name', 'Unknown')

						# Update stats
						restore_stats[resource_type]['successful'] += 1

						# Add to resources
						result.add_resource(
							'library',
							imported_id,
							{
								'name': imported_name,
								'source_file': resource_file,
								'restore_successful': True,
							},
						)

					elif resource_type == 'packages':
						# Import package
						import_result = target_client.packages.import_package(
							resource_file, {'overwrite': overwrite_existing}
						)

						# Extract imported resource details
						imported_id = import_result.get('id')
						imported_name = import_result.get('name', 'Unknown')

						# Update stats
						restore_stats[resource_type]['successful'] += 1

						# Add to resources
						result.add_resource(
							'package',
							imported_id,
							{
								'name': imported_name,
								'source_file': resource_file,
								'restore_successful': True,
							},
						)

				except OICError as e:
					self.logger.error(
						f'Failed to import {resource_type} {resource_name}: {e!s}'
					)
					restore_stats[resource_type]['failed'] += 1

					# Add to resources
					result.add_resource(
						resource_type[:-1],
						resource_id,
						{
							'name': resource_name,
							'source_file': resource_file,
							'restore_successful': False,
							'error': str(e),
						},
					)

					# Stop if continue_on_error is False
					if not continue_on_error:
						result.success = False
						result.message = f'Restore failed when importing {resource_type} {resource_name}'
						result.add_error(
							f'Failed to import {resource_type}', e, resource_id
						)
						break

		# Clean up temporary directory if used
		if temp_dir:
			try:
				shutil.rmtree(temp_dir)
				self.logger.info(f'Cleaned up temporary directory {temp_dir}')
			except Exception as e:
				self.logger.warning(f'Failed to clean up temporary directory: {e!s}')

		# Calculate total restore statistics
		total_resources = sum(
			restore_stats[resource_type]['total'] for resource_type in restore_stats
		)
		successful_resources = sum(
			restore_stats[resource_type]['successful']
			for resource_type in restore_stats
		)
		failed_resources = sum(
			restore_stats[resource_type]['failed'] for resource_type in restore_stats
		)

		# Update result details
		result.details['restore_stats'] = restore_stats
		result.details['backup_path'] = backup_path
		result.details['backup_metadata'] = backup_metadata
		result.details['total_resources'] = total_resources
		result.details['successful_resources'] = successful_resources
		result.details['failed_resources'] = failed_resources

		# Update result success and message
		if failed_resources > 0:
			result.success = False
			result.message = f'Restore completed with {failed_resources} of {total_resources} resources failed'
		else:
			result.message = f'Successfully restored all {total_resources} resources from {backup_path}'

		return result

	def prune_old_backups(
		self,
		backup_dir: str,
		retention_days: int = 30,
		retention_count: int = 10,
		dry_run: bool = True,
	) -> WorkflowResult:
		"""
		Prune old backups based on retention policy.

		This workflow:
		1. Finds all backups in the backup directory
		2. Prunes backups based on age and count retention policies

		Args:
		    backup_dir: Directory containing backups.
		    retention_days: How many days to keep backups.
		    retention_count: Minimum number of backups to keep regardless of age.
		    dry_run: Whether to perform a dry run (don't actually delete).

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		result = WorkflowResult()

		dry_run_str = ' (DRY RUN)' if dry_run else ''
		result.message = f'Pruning old backups{dry_run_str}'

		# Check if backup directory exists
		if not os.path.exists(backup_dir) or not os.path.isdir(backup_dir):
			result.success = False
			result.message = f'Backup directory {backup_dir} does not exist'
			result.add_error('Backup directory does not exist')
			return result

		# Find all backup files and directories
		backup_files = []

		# Look for zip files
		zip_files = glob.glob(os.path.join(backup_dir, 'oic_*_backup_*.zip'))
		backup_files.extend(zip_files)

		# Look for backup directories
		backup_dirs = [
			d
			for d in glob.glob(os.path.join(backup_dir, 'oic_*_backup_*'))
			if os.path.isdir(d)
		]
		backup_files.extend(backup_dirs)

		if not backup_files:
			result.message = f'No backups found in {backup_dir}'
			return result

		self.logger.info(f'Found {len(backup_files)} backups in {backup_dir}')

		# Parse backup information
		backups_info = []

		for backup_path in backup_files:
			try:
				# Extract timestamp from filename
				filename = os.path.basename(backup_path)
				timestamp_match = re.search(r'_(\d{8}_\d{6})', filename)

				if timestamp_match:
					timestamp_str = timestamp_match.group(1)
					timestamp = datetime.datetime.strptime(
						timestamp_str, '%Y%m%d_%H%M%S'
					)
				else:
					# If can't extract from filename, use file modification time
					file_mtime = os.path.getmtime(backup_path)
					timestamp = datetime.datetime.fromtimestamp(file_mtime)

				# Get file size
				if os.path.isdir(backup_path):
					# Calculate directory size
					total_size = 0
					for dirpath, dirnames, filenames in os.walk(backup_path):
						for f in filenames:
							fp = os.path.join(dirpath, f)
							total_size += os.path.getsize(fp)
				else:
					total_size = os.path.getsize(backup_path)

				# Add to backups info
				backups_info.append(
					{
						'path': backup_path,
						'timestamp': timestamp,
						'size': total_size,
						'is_dir': os.path.isdir(backup_path),
					}
				)

			except Exception as e:
				self.logger.warning(
					f'Failed to parse backup info for {backup_path}: {e!s}'
				)
				# Skip this backup

		# Sort backups by timestamp, newest first
		backups_info.sort(key=lambda x: x['timestamp'], reverse=True)

		# Determine which backups to keep and which to delete
		backups_to_keep = []
		backups_to_delete = []

		# Keep at least retention_count backups
		if len(backups_info) <= retention_count:
			backups_to_keep = backups_info
		else:
			# Keep the newest retention_count backups
			backups_to_keep = backups_info[:retention_count]

			# Check age for the rest
			cutoff_date = datetime.datetime.now() - datetime.timedelta(
				days=retention_days
			)

			for backup in backups_info[retention_count:]:
				if backup['timestamp'] >= cutoff_date:
					backups_to_keep.append(backup)
				else:
					backups_to_delete.append(backup)

		# Delete backups if not a dry run
		deleted_backups = []

		if not dry_run:
			for backup in backups_to_delete:
				try:
					backup_path = backup['path']
					self.logger.info(f'Deleting backup: {backup_path}')

					if os.path.isdir(backup_path):
						shutil.rmtree(backup_path)
					else:
						os.remove(backup_path)

					deleted_backups.append(backup)

				except Exception as e:
					self.logger.error(f'Failed to delete backup {backup_path}: {e!s}')
					result.add_error('Failed to delete backup', e)
		else:
			# In dry run, just log
			for backup in backups_to_delete:
				self.logger.info(f'Would delete backup: {backup["path"]} (DRY RUN)')

			deleted_backups = backups_to_delete

		# Calculate sizes
		keep_size = sum(b['size'] for b in backups_to_keep)
		delete_size = sum(b['size'] for b in backups_to_delete)

		# Update result details
		result.details['retention_days'] = retention_days
		result.details['retention_count'] = retention_count
		result.details['total_backups'] = len(backups_info)
		result.details['backups_to_keep'] = len(backups_to_keep)
		result.details['backups_to_delete'] = len(backups_to_delete)
		result.details['keep_size_bytes'] = keep_size
		result.details['delete_size_bytes'] = delete_size
		result.details['keep_size_mb'] = round(keep_size / (1024 * 1024), 2)
		result.details['delete_size_mb'] = round(delete_size / (1024 * 1024), 2)
		result.details['dry_run'] = dry_run

		# Format sizes for message
		keep_size_str = f'{result.details["keep_size_mb"]} MB'
		delete_size_str = f'{result.details["delete_size_mb"]} MB'

		# Update result message
		if dry_run:
			result.message = f'Dry run: Would keep {len(backups_to_keep)} backups ({keep_size_str}) and delete {len(backups_to_delete)} old backups ({delete_size_str})'
		else:
			result.message = f'Kept {len(backups_to_keep)} backups ({keep_size_str}) and deleted {len(deleted_backups)} old backups ({delete_size_str})'

		return result

	def _sanitize_filename(self, name: str) -> str:
		"""
		Sanitize a name for use in a filename.

		Args:
		    name: The name to sanitize.

		Returns:
		    str: The sanitized name.

		"""
		# Replace invalid characters with underscores
		sanitized = re.sub(r'[\\/*?:"<>|]', '_', name)

		# Truncate if too long
		if len(sanitized) > 50:
			sanitized = sanitized[:47] + '...'

		return sanitized
