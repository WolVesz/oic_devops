"""
Connection workflows module for the OIC DevOps package.

This module provides workflow operations for managing connections.
"""
import time
from datetime import datetime
from typing import Any, Dict, List

from oic_devops.exceptions import OICError, OICAPIError, OICResourceNotFoundError
from oic_devops.workflows.base import BaseWorkflow, WorkflowResult


class ConnectionWorkflows(BaseWorkflow):
    """
    Workflow operations for managing connections.

    This class provides higher-level operations for working with connections,
    such as updating credentials, testing all connections, and finding dependent
    integrations.
    """
    connections_configured_enriched : Dict[str,Dict[str,Any]]= None
    connections_dictionary: Dict[str, Dict[str, Any]] = None


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

    def get_connection_ids_using_username(self, target_username: str)-> List[str]:
        # Get the connections info
        if not self.connections_configured_enriched:
            params = {'status': 'CONFIGURED'}
            self.connections_configured_enriched = self.client.connections.list_enriched()
        connections = self.connections_configured_enriched

        # Find the connections mat
        matching_connection_ids = [
            conn_key
            for conn_key, conn in connections.items()
            for p in conn.get('securityProperties', [])
            if p.get('propertyName') == 'username'
               and p.get('propertyValue') == target_username
        ]
        unique_list = list(set(matching_connection_ids))
        return unique_list

    def update_password_basic_authentication(self, target_usernames: Dict[str, str], refresh_xref: bool= False) -> WorkflowResult:
        result = WorkflowResult()
        result.message = f'Updating password and restarting integrations'
        # Clean staged data
        if refresh_xref:
            self.connections_dictionary = Dict[str, Dict[str, Any]] = {}

        # TODO: 1. Loop through the target_usernames[username, password]
        #  1. For each user, get connection Ids  calling  _get_connection_ids_using_username(). Concentrate in a connections_dictionary: Dictionary[connection_id, {username:str, integrations:List[integration_id:str]]
        connections_dictionary: Dict[str, Dict[str, Any]] = {}
        integrations_original_status: Dict[str, Dict[str, Any]] = {}
        if  self.connections_dictionary:
            connections_dictionary = self.connections_dictionary
        else:
            for target_username in target_usernames.keys():
                connection_ids =  self.get_connection_ids_using_username(target_username)
            # 1.1 For each connection Id, get the list of integrations using that connection.
                for connection_id in connection_ids:
                    connections_dictionary[connection_id] = {"username":target_username, "integrations":[]}
                    #  1.1.1 Call client.connection.usage(connection_id=connection_id, raw=False).
                    connection_usage_df = self.client.connections.usage(connection_id=connection_id, raw=False)
                    #  1.1.2 Add integration_id to connections_dictionary if the status == "ACTIVATED"
                    integration_ids = []
                    for integration in connection_usage_df:
                        if integration["status"] == "ACTIVATED":
                            integration_ids.append(integration['integration_id'])
                            integrations_original_status[integration['integration_id']] = {}
                            # print(f"Connection {connection_id} uses integration: {integration['integration_id']}")
                    connections_dictionary[connection_id]["integrations"] = integration_ids
                    # print(f"Connections: {connection_id} \tIntegrations: {integration_ids}")
        self.connections_dictionary = connections_dictionary

        # Get integration original status
        for integration_id in integrations_original_status.keys():
            try:
                integrations_original_status[integration_id] = self.client.integrations.get(integration_id=integration_id)
            except OICAPIError as exc:
                message = f"FAILED to fetch integration {integration_id}: {exc.title}"
                print(f"\t -{message}")
                result.success = False
                result.add_error(resource_id=integration_id, message=exc.title, error=exc)
                result.add_resource(resource_type='integration', resource_id=integration_id, data={"status":message})
            except OICResourceNotFoundError as not_found_error:
                print(f"Error getting {integration_id} integration original status: {not_found_error}")
                exit(0)

        #  1.2 Get list of integrations with active schedulers:
        schedule_integrations = [
            integration_id
            for integration_id, info in integrations_original_status.items()
            if info.get("pattern") == "Scheduled"
        ]
        schedule_integrations = list(set(schedule_integrations)) # remove duplicates
        schedule_integrations_without_schedule = []
        for integration_id in schedule_integrations:
            try:
                integration_schedule = self.client.integrations.get_schedule(integration_id=integration_id)
                integrations_original_status[integration_id]={"SCHEDULE":integration_schedule}
            except OICAPIError as exc:
                message = f"FAILED to fetch Schedule for {integration_id}: {exc.title}"
                print(f"\t -{message}")
                result.success = False
                result.add_error(resource_id=integration_id, message=exc.title, error=exc)
                result.add_resource(resource_type='integration', resource_id=integration_id, data={"status":message})
            except OICResourceNotFoundError as not_found_error:
                print(f"INFO - {integration_id}  - Schedule not found: {not_found_error}")
                schedule_integrations_without_schedule.append(integration_id)
        # Remove integrations without schedule from list
        schedule_integrations = [
            int_id for int_id in schedule_integrations
            if int_id not in schedule_integrations_without_schedule
        ]

        # # TODO: 2. Prompt user for proceed confirmation showing the schedule integrations with their running state and next run date
        # # 2.1 upon negative answer exit
        #
        # 3. Stop schedules of schedule integrations. Keep track of them because they will need to be re-started
        # # TODO: 3.1 upon failure, re-start the stopped schedulers
        filtered = [s for s in schedule_integrations if s.startswith("PSSWRD")] # TODO: remove
        result_stop_shc = self._stop_schedulers(filtered, integrations_original_status)
        result.merge(result_stop_shc)
        data = {"action": "STOP_SCHEDULE", "datetime": datetime.now().isoformat(), "success": True,
                "schedule_state": "STOPPED"}
        for integration_id in schedule_integrations:
            result.add_resource(resource_type='integration', resource_id=integration_id, data=data)

        # # TODO: 4. Deactivate integrations.
        # # 4.1 Deactivate
        filtered = [s for s in integrations_original_status.keys() if s.startswith("PSSWRD")]  # TODO: remove
        response_wf:WorkflowResult = self.deactivate_integrations(integration_ids=filtered)
        result.merge(response_wf)
        if not response_wf.success:
            print("Failed: ", response_wf.message, " ", result.message)
            exit(1)

        # #  4.2 Ensure all integrations go into CONFIGURED state. Time out after 5 minutes, include the integrations de-activated in response
        #
        # Update the connections passwords
        print(f"\n======================================================")

        for connection_id in connections_dictionary.keys():
            username = connections_dictionary[connection_id]['username']
            # print(f"\tUpdating {username} password for: {connection_id} ")
            password = target_usernames[username]
            params = {'patchAttachments':True}
            data = {
               "securityProperties": [
                    {
                        "propertyName": "username",
                        "propertyValue": username,
                        "requiredFlag": True
                    },
                    {
                        "propertyName": "password",
                        "propertyValue": password,
                        "requiredFlag": True
                    }
                ]}
            if connection_id.startswith('Psswrd SKIP') :
                try:
                    response = self.client.connections.update(connection_id=connection_id,params=params, data=data)
                    ## make sure the update succeeded
                    if response["status"] == "CONFIGURED":
                        result.add_resource(resource_type='connection', resource_id=connection_id,data={"status":"password successfully updated"})
                        print(f"\t\t- Updated password in: {connection_id}")
                    else:
                        message =f"{connection_id}/{username}  password updated FAILED"
                        print(message)
                        # track errors
                        result.success = False
                        result.add_error(resource_id=connection_id,message=message)
                        result.add_resource(resource_type='connection', resource_id=connection_id,
                                            data={"status": "Error: password updated, but it did not get CONFIGURED status."})
                except OICAPIError as exc:
                    message = f"{connection_id}/{username}  password updated FAILED: {exc.title}"
                    print(f"\t -{message}")
                    result.success = False
                    result.add_error(resource_id=connection_id, message=exc.title, error=exc)
                    result.add_resource(resource_type='connection', resource_id=connection_id,data={"status":message})


        return result

    def _stop_schedulers(self, schedule_integrations: list[str], integrations_original_status: dict[str, dict[str, Any]]
                         ) -> WorkflowResult:
        schedule_integration_stopped: list[Any] = []
        result= WorkflowResult()
        success = True
        for integration_id in schedule_integrations:
            try:
                print(integration_id, "state", integrations_original_status[integration_id]['SCHEDULE']['state'] )
                if integrations_original_status[integration_id]['SCHEDULE']['state'] == "ACTIVE":
                    print("Stopping Scheduler for: ", integration_id)
                    params = {'asynch': True}
                    self.client.integrations.stop_schedule(integration_id=integration_id, params=params)
                    data = {"action": "STOP_SCHEDULE", "datetime": datetime.now().isoformat(), "success": True,
                            'schedule_state': 'STOPPED'}
                    result.add_resource(resource_type='integration', resource_id=integration_id,
                                        data=data)
                    schedule_integration_stopped.append(integration_id)

            except OICAPIError as exc:
                message = f"FAILED to STOP Schedule for {integration_id}: {exc.title}"
                print(f"\t -{message}")
                success = success if exc.status_code != "412" else False # Other than 412 is fatal error
                result.add_error(resource_id=integration_id, message=exc.title, error=exc)
                data = {"action": "STOP_SCHEDULE", "datetime": datetime.now().isoformat(), "success": False,
                        "message": message}
                result.add_resource(resource_type='integration', resource_id=integration_id,
                                    data=data)
            except OICResourceNotFoundError as not_found_error:
                print(f"\n\t{integration_id} not found: {not_found_error}")
                success = False
                message = f'Error Stopping Schedule: {not_found_error}'
                result.message = "Internal coding error: Schedule is not found by OIC API."
                result.add_error(resource_id=integration_id, error=not_found_error, message=message)
                data = {"action": "STOP_SCHEDULE", "datetime": datetime.now().isoformat(), "success": False,
                        "message": message}
                result.add_resource(resource_type='integration', resource_id=integration_id,
                                    data=data)
        #  Wait until all schedules go into CANCELLED state. Time out after 5 minutes, include the integrations de-activated in response
        #
        all_stopped = False   #guilty till proven innocent
        iteration = 0
        while iteration < 5 and not all_stopped:
            iteration += 1

            # Assume all are stopped until we find one that isn't
            all_stopped = True

            for integration_id in schedule_integrations:
                try:
                    schedule = self.client.integrations.get_schedule(integration_id)
                    is_cancelled = schedule.get("state") == "CANCELLED"
                    if not is_cancelled:
                        all_stopped = False
                        # No need to keep checking others in this iteration
                        break
                except OICError as oic_error:
                    print(f"Error validating scheduler is deactivated for {integration_id}: {oic_error}")
                    # If we can't validate one, be conservative and keep waiting
                    all_stopped = False
                    break

            if not all_stopped and iteration < 5:
                time.sleep(5)

        result.success = success and all_stopped

        return result

    def deactivate_integrations(self, integration_ids: List[str]) -> WorkflowResult:
        """
        Deactivates the integrations that are active

        Returns WorkflowResult:
            Already inactive integrations will append a resource having the details in the "data" field
            Success == False terminal error happened.  Failing to de-active because it wasn't active does not count as terminal fail.
        """
        result = WorkflowResult()
        success = True
        for integration_id in integration_ids:
            try:
                self.client.integrations.deactivate(integration_id=integration_id, delete_event_subscription_flag=False)
                data = {"action":"DEACTIVATE", "datetime": datetime.now().isoformat(), "success":True}
                result.add_resource(resource_id=integration_id, resource_type="integration",data=data)
            except OICAPIError as exc:
                message = f"FAILED to DEACTIVATE {integration_id}: {exc.title}"
                success = success if exc.status_code != "412" else False # 412 means "Integration xxx is not active". Do not fail on 412
                if not success:
                    result.add_error(resource_id=integration_id, message=exc.title, error=exc)
                data = {"action": "DEACTIVATE", "datetime": datetime.now().isoformat(), "success": False, "message": message}
                result.add_resource(resource_type='integration', resource_id=integration_id,
                                    data=data)
            except OICResourceNotFoundError as not_found_error:
                print(f"\n\t{integration_id} not found: {not_found_error}")
                result.success = False
                result.add_error(resource_id=integration_id, error=not_found_error, message=f'Error Deactivating: {not_found_error}')
                result.message ="Internal coding error: Integrations are not found by OIC API."
                data = {"action": "DEACTIVATE", "datetime": datetime.now().isoformat(), "success": False,
                        "message": "404 - Not Found"}
                result.add_resource(resource_type='integration', resource_id=integration_id,
                                    data=data)
        result.success = success
        return result
