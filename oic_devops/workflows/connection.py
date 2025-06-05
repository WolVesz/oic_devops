"""
Connection workflows module for the OIC DevOps package.

This module provides workflow operations for managing connections.
"""

import time
from typing import Any, Dict

from oic_devops.exceptions import OICError
from oic_devops.workflows.base import BaseWorkflow, WorkflowResult


class ConnectionWorkflows(BaseWorkflow):
	"""
	Workflow operations for managing connections.

	This class provides higher-level operations for working with connections,
	such as updating credentials, testing all connections, and finding dependent
	integrations.
	"""

	def execute(self, *args, **kwargs) -> WorkflowResult:
		"""
		Execute the specified connection workflow.

		This is a dispatcher method that calls the appropriate workflow
		based on the operation argument for CLI purposes.

		Args:
		    operation: The workflow operation to execute.
		    **kwargs: Additional arguments specific to the workflow.

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		operation = kwargs.pop('operation', None)

		if operation == 'update_credentials':
			return self.update_credentials(**kwargs)
		if operation == 'test_all':
			return self.test_all_connections(**kwargs)
		if operation == 'find_dependents':
			return self.find_dependent_integrations(**kwargs)
		if operation == 'update_and_restart':
			return self.update_credentials_and_restart_integrations(**kwargs)
		result = WorkflowResult(
			success=False, message=f'Unknown connection workflow operation: {operation}'
		)
		result.add_error(f'Unknown operation: {operation}')
		return result

	def update_credentials(
		self,
		connection_id: str,
		security_properties: dict,
		test_connection: bool = True,
		**kwargs,
	) -> WorkflowResult:
		"""
		Update credentials for a connection.Generally from securityProperties in
		the connection object.

		This workflow:
		1. Gets the current connection configuration
		2. Updates the credentials - must contain at least the propertyName and propertyValue
		3. Updates the connection
		4. Optionally tests the connection

		Example: Basic Auth
		{"securityProperties":[
		        {"propertyName":"username",
		         "propertyValue":"new_username"},
		        {"propertyName":"password",
		         "propertyValue":"new_password"}
		         ]
		}


		Args:
		    connection_id: ID of the connection to update.
		    security_properties: Dict containing credential fields to update.
		        Keys depend on the connection type but typically include
		        'password', 'securityToken', etc.
		    test_connection: Whether to test the connection after updating.
		    **kwargs: connected to update function

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		result = WorkflowResult()
		result.message = f'Updating credentials for connection {connection_id}'
		self.logger.info(f'Updating {connection_id} security properties')

		try:
			self.client.connections.update(connection_id, security_properties, **kwargs)
			result.add_resource(
				'connection',
				connection_id,
				{'updated_information': security_properties},
			)
			self.logger.info(f'Updated {connection_id} security properties')
			result.success = True
		except Exception as e:
			self.logger.error(f'Failed to update credential values: {e!s}')
			result.add_error('Failed to update credential values', e, connection_id)
			return result

		if test_connection:
			try:
				self.logger.info(f'Testing connection {connection_id}')
				test_result = self.client.connections.test(connection_id)

				# Check test result
				if (
					test_result.get('status') == 'SUCCESS'
					or test_result.get('state') == 'SUCCESS'
				):
					result.details['test_result'] = {'status': 'success'}
					self.logger.info(f'Connection test successful for {connection_id}')
				else:
					error_msg = test_result.get('message', 'Unknown test failure')
					self.logger.error(f'Connection test failed: {error_msg}')
					result.success = False
					result.message = f'Credentials updated but test failed: {error_msg}'
					result.add_error(
						'Connection test failed', resource_id=connection_id
					)
					result.details['test_result'] = {
						'status': 'failure',
						'message': error_msg,
					}

			except OICError as e:
				self.logger.error(f'Failed to test connection {connection_id}: {e!s}')
				result.success = False
				result.message = 'Credentials updated but test failed'
				result.add_error('Failed to test connection', e, connection_id)
				result.details['test_result'] = {'status': 'error', 'message': str(e)}

		return result

	def test_all_connections(
		self, continue_on_error: bool = True, **kwargs
	) -> WorkflowResult:
		"""
		Test all connections or a filtered subset of connections.

		Args:
		    continue_on_error: Whether to continue testing if some tests fail.
		    **kwargs are for list_all

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		result = WorkflowResult()
		result.message = 'Testing connections'

		# Get list of connections
		try:
			connections = self.client.connections.list_all(**kwargs)
			result.details['connection_count'] = len(connections)
			self.logger.info(f'Found {len(connections)} connections to test')

		except OICError as e:
			self.logger.error(f'Failed to get connections list: {e!s}')
			result.add_error('Failed to get connections list', e)
			return result

		# Test each connection
		success_count = 0
		failed_connections = []

		for connection in connections:
			connection_id = connection.get('id')
			connection_name = connection.get('name', 'Unknown')

			if not connection_id:
				self.logger.warning(
					f'Skipping connection with no ID: {connection_name}'
				)
				continue

			self.logger.info(f'Testing connection {connection_name} ({connection_id})')

			# test
			try:
				test_result = self.client.connections.test(connection_id)

				# Check if test was successful
				if (
					test_result.get('status') == 'SUCCESS'
					or test_result.get('state') == 'SUCCESS'
				):
					self.logger.info(
						f'Connection test successful for {connection_name}'
					)
					success_count += 1
					result.add_resource(
						'connection',
						connection_id,
						{'name': connection_name, 'test_result': 'success'},
					)
				else:
					error_msg = test_result.get('message', 'Unknown test failure')
					self.logger.error(
						f'Connection test failed for {connection_name}: {error_msg}'
					)
					failed_connections.append(
						{
							'id': connection_id,
							'name': connection_name,
							'error': error_msg,
						}
					)
					result.add_resource(
						'connection',
						connection_id,
						{
							'name': connection_name,
							'test_result': 'failure',
							'error': error_msg,
						},
					)

					if not continue_on_error:
						result.success = False
						result.message = f'Connection test failed for {connection_name}'
						result.add_error(
							f'Connection test failed: {error_msg}',
							resource_id=connection_id,
						)
						break

			except OICError as e:
				self.logger.error(f'Error testing connection {connection_name}: {e!s}')
				failed_connections.append(
					{'id': connection_id, 'name': connection_name, 'error': str(e)}
				)
				result.add_resource(
					'connection',
					connection_id,
					{'name': connection_name, 'test_result': 'error', 'error': str(e)},
				)

				if not continue_on_error:
					result.success = False
					result.message = f'Error testing connection {connection_name}'
					result.add_error('Error testing connection', e, connection_id)
					break

		# Update result message and details
		if result.success:
			if not failed_connections:
				result.message = f'All {success_count} connections tested successfully'
			else:
				result.success = False
				result.message = f'{success_count} connections tested successfully, {len(failed_connections)} failed'

		result.details['success_count'] = success_count
		result.details['failed_count'] = len(failed_connections)
		result.details['failed_connections'] = failed_connections

		return result

	def find_dependent_integrations(
		self, connection_id: str, check_active_only: bool = False
	) -> WorkflowResult:
		"""
		Find all integrations that depend on a specific connection.

		This workflow:
		1. Gets all integrations
		2. Checks each integration for references to the connection
		3. Returns a list of dependent integrations

		Args:
		    connection_id: ID of the connection to check for dependencies.
		    check_active_only: Whether to check only active integrations.

		Returns:
		    WorkflowResult: The workflow execution result with dependent integrations.

		"""
		result = WorkflowResult()
		result.success = False

		params = {}
		if check_active_only:
			params['status'] = 'ACTIVATED'

		try:
			integrations = self.client.integrations.df(params=params)
			integrations.columns = [
				f'integration_{x}'
				if 'connection' not in x and 'integration' not in x
				else x
				for x in integrations.columns
			]

			integrations = integrations[integrations['connection_id'] == connection_id]

			result.success = True
			result.add_resource('connection', connection_id, integrations)

			if len(integrations) == 0:
				result.message = f'No integrations found that could depend on connection {connection_id}'

			return result
		except OICError as e:
			self.logger.error(f'Failed to identify integrations: {e}')
			result.add_error(f'Failed to identify integrations: {e}')
			return result

	def update_credentials_and_restart_integrations(
		self,
		connection_id: str,
		credentials: Dict[str, Any],
		restart_scope: str = 'all',  # "all", "active", "none"
		sequential_restart: bool = True,
		verify_restart: bool = True,
		wait_time: int = 20,
	) -> WorkflowResult:
		"""
		Update connection credentials and restart dependent integrations.

		This workflow:
		1. Updates credentials for the connection
		2. Finds all dependent integrations
		3. Deactivates and reactivates each integration to restart them with new credentials

		Args:
		    connection_id: ID of the connection to update.
		    credentials: Dict containing credential fields to update.
		    restart_scope: Which integrations to restart: "all", "active", or "none".
		    sequential_restart: Whether to restart integrations one at a time.
		    verify_restart: Whether to verify integrations are active after restart.
		    wait_time: Time to wait between operations in seconds.

		Returns:
		    WorkflowResult: The workflow execution result.

		"""
		result = WorkflowResult()
		result.message = f'Updating credentials and restarting integrations for connection {connection_id}'

		# Step 1: Update credentials
		self.logger.info(f'Updating credentials for connection {connection_id}')
		update_result = self.update_credentials(
			connection_id=connection_id,
			security_properties=credentials,
			test_connection=True,
		)

		# Merge results
		result.merge(update_result)

		# If credential update failed, stop
		if not update_result.success:
			self.logger.error('Credential update failed, stopping workflow')
			result.message = 'Failed to update credentials, integrations not restarted'
			return result

		# Get connection name for better logging
		connection_name = 'Unknown'
		if (
			'connection' in update_result.resources
			and connection_id in update_result.resources['connection']
		):
			connection_name = update_result.resources['connection'][connection_id].get(
				'name', 'Unknown'
			)

		# Step 2: If no restart needed, we're done
		if restart_scope == 'none':
			result.message = f'Successfully updated credentials for connection {connection_name}, no integrations restarted'
			return result

		# Step 3: Find dependent integrations
		self.logger.info(
			f'Finding integrations dependent on connection {connection_id}'
		)
		dependent_result = self.find_dependent_integrations(
			connection_id=connection_id, check_active_only=(restart_scope == 'active')
		)

		dependent_result_output = dependent_result.resources['connection'][
			connection_id
		]
		dependent_result_output = dependent_result_output[
			dependent_result_output['connection_id'] == connection_id
		]
		current_versions = (
			dependent_result_output.sort_values(
				by='integration_version', ascending=False
			)
			.groupby('integration_name')[['integration_id', 'integration_version']]
			.first()
			.reset_index()
		)
		dependent_result_output = dependent_result_output[
			dependent_result_output['integration_id'].isin(
				current_versions['integration_id']
			)
		][
			[
				'integration_name',
				'integration_id',
				'integration_status',
				'integration_pattern',
				'integration_version',
			]
		].drop_duplicates()

		# Merge results
		result.merge(dependent_result)

		# If finding dependents failed, report but continue
		if not dependent_result.success:
			self.logger.warning('Error finding dependent integrations, but continuing')

		# If no integrations to restart, we're done
		if len(dependent_result_output) == 0:
			self.logger.info('No integrations to restart')
			result.message = f'Successfully updated credentials for connection {connection_name}, no integrations to restart'
			return result

		# Step 4: Restart each integration
		successful_restarts = []
		failed_restarts = []

		for index, integration in dependent_result_output.iterrows():
			self.logger.info(
				f'Restarting {len(integration["integration_id"])} integrations'
			)

			integration_id = integration['integration_id']
			integration_name = integration['integration_name']
			current_status = integration['integration_status']
			integration_scheduled = integration['integration_pattern'] == 'Scheduled'

			self.logger.info(
				f'Processing integration: {integration_name} (current status: {current_status})'
			)

			# Only deactivate if already activated
			deactivate_needed = current_status == 'ACTIVATED'
			restart_success = True
			restart_error = None

			# Step 4a: Deactivate if needed
			if deactivate_needed:
				try:
					self.logger.info(f'Deactivating integration: {integration_name}')

					if integration_scheduled:
						self.client.integrations.deactivate(
							integration_id, stop_schedular=True
						)
					else:
						self.client.integrations.deactivate(
							integration_id, stop_schedular=False
						)

					# Wait for deactivation to complete if sequential restart
					if sequential_restart:
						self.logger.info(
							f'Waiting {wait_time}s for deactivation to complete'
						)
						time.sleep(wait_time)

						# Verify deactivation if requested
						if verify_restart:
							integration_status = self.client.integrations.get(
								integration_id
							)
							if integration_status.get('status') != 'CONFIGURED':
								self.logger.warning(
									f'Integration {integration_name} not fully deactivated, status: {integration_status.get("status")}'
								)

				except OICError as e:
					self.logger.error(
						f'Failed to deactivate integration {integration_name}: {e!s}'
					)
					restart_success = False
					restart_error = f'Deactivation failed: {e!s}'

			# Step 4b: Activate the integration
			if restart_success:  # Only if deactivation succeeded or wasn't needed
				try:
					self.logger.info(f'Activating integration: {integration_name}')
					self.client.integrations.activate(integration_id)

					# Wait for activation to complete if sequential restart
					if sequential_restart:
						self.logger.info(
							f'Waiting {wait_time}s for activation to complete'
						)
						time.sleep(wait_time)

						# Verify activation if requested
						if verify_restart:
							integration_status = self.client.integrations.get(
								integration_id
							)
							if integration_status.get('status') != 'ACTIVATED':
								self.logger.warning(
									f'Integration {integration_name} not fully activated, status: {integration_status.get("status")}'
								)
								restart_success = False
								restart_error = f'Activation verification failed, status: {integration_status.get("status")}'

						if integration_scheduled:
							self.logger.info(
								f'Integration {integration_name} - Resuming Schedule'
							)
							schedule_status = self.client.integrations.resume_schedule(
								integration_id
							)
							self.logger.info(
								f'Integration {integration_name} - Schedule Activated'
							)

				except OICError as e:
					self.logger.error(
						f'Failed to activate integration {integration_name}: {e!s}'
					)
					restart_success = False
					restart_error = f'Activation failed: {e!s}'

			# Record the result
			if restart_success:
				successful_restarts.append(
					{'id': integration_id, 'name': integration_name}
				)
				result.add_resource(
					'restarted_integration',
					integration_id,
					{'name': integration_name, 'result': 'success'},
				)
			else:
				failed_restarts.append(
					{
						'id': integration_id,
						'name': integration_name,
						'error': restart_error,
					}
				)
				result.add_error(
					f'Failed to restart integration {integration_name}',
					resource_id=integration_id,
				)
				result.add_resource(
					'restarted_integration',
					integration_id,
					{
						'name': integration_name,
						'result': 'failure',
						'error': restart_error,
					},
				)

		# Update overall workflow status
		if failed_restarts:
			result.success = False
			result.message = (
				f'Updated credentials for connection {connection_name}, '
				f'but {len(failed_restarts)} of {len(dependent_result_output)} integration restarts failed'
			)
		else:
			result.message = (
				f'Successfully updated credentials for connection {connection_name} '
				f'and restarted {len(successful_restarts)} integrations'
			)

		# Add details to the result
		result.details['restart_results'] = {
			'successful_count': len(successful_restarts),
			'failed_count': len(failed_restarts),
			'successful_restarts': successful_restarts,
			'failed_restarts': failed_restarts,
		}

		return result
