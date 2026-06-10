"""
Refresh Environment workflow for the OIC DevOps package.

This module provides the RefreshEnvironment workflow to synchronize active
integrations from a source environment to the target environment.
"""
import logging
import os
import tempfile
import time
import yaml
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .base import BaseWorkflow, WorkflowResult
from oic_devops.client import OICClient
from .. import OICAPIError, OICResourceNotFoundError
from enum import Enum

## OIC Constant values
SCHEDULED = 'Scheduled'
SCHEDULE_ACTIVE_STATE = 'ACTIVE'

##
PLAN_STEPS_NAMES = [
    'BACKUP', 'EXPORT_FROM_SOURCE', 'DEACTIVATE_INTEGRATIONS',
    'IMPORT_INTEGRATIONS', 'UPDATE_INTEGRATIONS_PROPERTIES',
    'ACTIVATE_INTEGRATIONS', 'GENERATE_REPORT'
]

PlanSteps = Enum('PlanSteps', {state: state for state in PLAN_STEPS_NAMES})

def is_plan_step_requested(plan_steps_to_execute: List[PlanSteps], step: PlanSteps):
    return step.name in plan_steps_to_execute


class RefreshEnvironment(BaseWorkflow):
    """
    Workflow class for refreshing an OIC environment by copying active integrations
    from a source environment to the target environment.

    Major versions (e.g. 1.00.0000 vs 2.00.0000) are treated as distinct integrations.
    """

    def __init__(
        self,
        target_client: OICClient,
        source_client: OICClient,
        refresh_plan_dir: Optional[str] = None, # Main directory. Defaults to OS tempfile.gettempdir()
        backup_dir: Optional[str] = None, #defauls to refresh_plan_path/backup_dir


    ):
        super().__init__(target_client)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.target_client = target_client
        self.source_client = source_client
        self.target_integrations = target_client.integrations
        self.source_integrations = source_client.integrations
        self.refresh_plan_dir = refresh_plan_dir or os.path.join(tempfile.gettempdir(), f'refresh_plan_{ts}')
        self.refresh_plan_path = os.path.join(self.refresh_plan_dir, f'refresh_plan.yaml')

        self.backup_dir = backup_dir or os.path.join(refresh_plan_dir, f'refresh_backup')
        self.backup_integrations_dir = os.path.join(self.backup_dir, 'integrations_backup')
        self.original_state_path = os.path.join(self.backup_dir,
                                                f'original_state_{target_client.config.profile}_{ts}.yaml')
        self.source_export_dir = os.path.join(self.refresh_plan_dir, 'integrations_source')

        os.makedirs(self.refresh_plan_dir, exist_ok=True)
        os.makedirs(self.backup_integrations_dir, exist_ok=True)
        os.makedirs(self.source_export_dir, exist_ok=True)
        self.logger.info(f"Refresh Plan directory: {self.refresh_plan_dir}")
        self.logger.info(f"Backup directory: {self.backup_dir}")

    @staticmethod
    def _get_major_version(version: str) -> str:
        """Extract major version from version string (e.g. '2.00.0000' -> '2')."""
        if not version:
            return "1"
        try:
            # Handle formats like 1.00.0000, 2.0, v1.0, etc.
            major = version.split('.')[0].replace('v', '').replace('V', '').strip()
            return major
        except Exception:
            return version

    @staticmethod
    def _get_composite_key(integ: Dict[str, Any]) -> str:
        """Create unique key for major version as distinct integration."""
        code = integ.get('code', '')
        version = integ.get('version', '')
        major = RefreshEnvironment._get_major_version(version)
        return f"{code}|{major}"

    def _get_active_integrations(self, client: OICClient, integration_name_filter: str = None) -> List[Dict[str, Any]]:
        """Get list of active integrations from the given client."""
        if not integration_name_filter:
            params = {"q":"{status:'ACTIVATED'}"}
        else:
            params ={"q":"{status:'ACTIVATED', name:" +integration_name_filter +"}"}

        integrations = client.integrations.list_all(params=params)
        self.logger.info(f"Found {len(integrations)} integrations")

        ## For Scheduled integrations add the schedule state
        for integration in integrations:
            if integration.get("pattern") == SCHEDULED and integration.get('scheduleDefinedFlag'):
                try:
                    schedule_resp = client.integrations.get_schedule(integration.get('id'))
                    schedule = {key: schedule_resp.get(key) for key in [
                        "id", "description", "frequency", "name",
                        "scheduleTZ", "scheduleTZDisplayName",
                        "scheduleType", "startDate", "state"
                    ]}
                    integration['schedule'] = schedule

                except Exception as e:
                    self.logger.error(
                        f"Failed to fetch schedule for {integration.get('id')}: {e}"
                    )
                    raise

        return integrations

    def _export_integration(self, integration_id: str, client: OICClient, file_path: str) -> str:
        """Export integration from client to file_path."""
        try:
            return client.integrations.export(integration_id, file_path)
        except Exception as e:
            self.logger.error(f"Failed to export {integration_id}: {e}")
            raise

    def _delete_integration(self, integration_id: str, client: OICClient)-> Dict[str, Any]:
        try:
            return client.integrations.delete(integration_id=integration_id)
        except Exception as e:
            self.logger.error(f"Failed to delete {integration_id} from {client.config.profile}: {e}")
            raise
    def _import_integration(self, file_path: str, client: OICClient) -> Dict[str, Any]:
        """Import integration with replace option."""

        file_path = file_path.replace('|', '_')
        try:
            return client.integrations.import_integration(file_path)
        except Exception as e:
            self.logger.error(f"Failed to import from {file_path}: {e}")
            raise

    def _stop_scheduler(self,integration_id: str, client: OICClient) -> Tuple[bool, Optional[str]]:
        """Stops the Scheduler. Returns True when stopped or was not ACTIVE. False when schedule does not exist or api exception  """
        try:
            schedule = client.integrations.get_schedule(integration_id)
            state = schedule.get('state', 'CANCELLED')

            if state == "ACTIVE":
                self.logger.info(f'Stopping schedule for {integration_id}')
                params = {"asynch": False}
                self.client.integrations.stop_schedule(integration_id=integration_id, params=params)
                msg = f'Stopped schedule for {integration_id}'
                self.logger.info(msg)
                return True, msg
            else:
                return True, f'Schedule not active for {integration_id}'
        except OICAPIError as exc:
            # Consider 412 (precondition) as non-fatal; others fatal
            is_fatal = str(getattr(exc, "status_code", "")) != "412"
            msg = f"FAILED to STOP Schedule: {exc.title}"
            self.logger.error("%s for %s", msg, integration_id)
            return is_fatal, msg
        except OICResourceNotFoundError as nf:
            msg = f"Schedule not found: {nf}"
            self.logger.error("%s for %s", msg, integration_id)
            return False, msg

    def _prepare_integration_for_import(self, integration_id: str,is_scheduler: bool, client: OICClient) -> Tuple[bool, Optional[str]]:
        """Stop schedule and deactivate if necessary before import."""
        try:
            success = True
            msg = f'Deactivated {integration_id}' # optimistic approach
            if is_scheduler:
                success, msg = self._stop_scheduler(integration_id, client)

            if success:
                self.logger.info(f"Deactivating {integration_id}")
                client.integrations.deactivate(integration_id)
                deactivated = self._wait_for_integration_status(integration_id, 'CONFIGURED', max_attempts=10)
                msg += f' Deactivated {integration_id}' if deactivated else f' {integration_id} failed to deactivate. Reason: time out.'
                self.logger.info(msg)
                return deactivated, msg
            else:
                return False, f'{msg} when DEACTIVATE_INTEGRATIONS (self._prepare_integration_for_import)'
        except Exception as e:
            msg = f'⚠️ Exception when _prepare_integration_for_import( {integration_id} ): {str(e)}'
            self.logger.warning(msg,e)
            return False, msg

    def _wait_for_integration_status(
        self,
        integration_id: str,
        desired_status: str = 'ACTIVATED',
        max_attempts: int = 30,
        interval_seconds: int = 5
    ) -> bool:
        """Wait for integration to reach desired status."""
        for attempt in range(max_attempts):
            try:
                integ = self.target_integrations.get(integration_id)
                current_status = integ.get('status')
                self.logger.info(f"Integration {integration_id} status: {current_status} (attempt {attempt+1}/{max_attempts})")

                if current_status == desired_status:
                    return True
            except Exception as e:
                self.logger.warning(f"⚠️ Error checking status for {integration_id}: {e}")

            if attempt < max_attempts - 1:
                time.sleep(interval_seconds + (attempt * interval_seconds - interval_seconds))

        self.logger.error(f"Timeout waiting for {integration_id} to reach status {desired_status}")
        return False

    def _create_original_state(self, active_integrations: List[Dict[str, Any]], original_state_path: str = None) -> str:
        """Create YAML file with original state for rollback."""

        if not original_state_path:
            original_state_path = self.original_state_path

        state = {
            'timestamp': datetime.now().isoformat(),
            'integrations': [
                {
                    'id': integ.get('id'),
                    'name': integ.get('name'),
                    'code': integ.get('code'),
                    'version': integ.get('version'),
                    'major_version': self._get_major_version(integ.get('version')),
                    'status': integ.get('status'),
                    'pattern': integ.get('pattern'),
                    'schedule': integ.get('schedule', None)
                    ## TODO: add integration properties values
                }
                for integ in active_integrations
            ]
        }
        with open(original_state_path, 'w') as f:
            yaml.safe_dump(state, f, default_flow_style=False)
        self.logger.info(f"Original state saved to {self.original_state_path}")
        return original_state_path

    def _create_refresh_plan(self, source_active: List[Dict], target_active: List[Dict]) -> Dict:
        """Create detailed refresh plan using composite key (code + major_version)."""
        plan = {
            'timestamp': datetime.now().isoformat(),
            'source_environment': getattr(self.source_client.config, 'identity_domain', 'SOURCE'),
            'target_environment': getattr(self.target_client.config, 'identity_domain', 'TARGET'),
            'skip_backup': False,
            'integrations_to_refresh': [],
            'target_integrations_to_deactivate': [],
            'schedulers_to_start': [],
            'plan_steps_to_execute': [step.name for step in PlanSteps],
        }

        # Use composite key: code|major_version
        source_map = {self._get_composite_key(i): i for i in source_active}
        target_map = {self._get_composite_key(i): i for i in target_active}

         #  integrations_to_refresh
        for composite_key, source_integ in source_map.items():
            target_integ = target_map.get(composite_key)
            code = source_integ.get('code')
            source_version = source_integ.get('version')
            major = self._get_major_version(source_version)
            integration_id = source_integ.get("id")
            source_pattern = source_integ.get('pattern')
            is_schedule_active_in_source = source_integ.get('schedule', {}).get('state', '') == SCHEDULE_ACTIVE_STATE
            plan['integrations_to_refresh'].append({
                'integration_id': integration_id,
                'composite_key': composite_key,
                'code': code,
                'major_version': major,
                'name': source_integ.get('name'),
                'source_version': source_version,
                'target_version': target_integ.get('version') if target_integ else None,
                'is_active_in_target': target_integ is not None, # True when the integration same major version is active in target
                'is_scheduler': source_pattern == SCHEDULED,
                'is_schedule_active_in_source': is_schedule_active_in_source,
            })
            if is_schedule_active_in_source:
                plan['schedulers_to_start'].append({
                    'integration_id': integration_id,
                })

            if target_integ:
                is_scheduler = target_integ.get('pattern') == SCHEDULED
                is_scheduler_active = is_scheduler and target_integ.get('schedule', {}).get('state', '') == SCHEDULE_ACTIVE_STATE
                plan['target_integrations_to_deactivate'].append({
                    'integration_id': integration_id,
                    'name': target_integ.get('name'),
                    'target_version': target_integ.get('version'),
                    'major_version': major,
                    'code': code,
                    'is_scheduler': is_scheduler,
                    'is_scheduler_active': is_scheduler_active,
                })
            
        with open(self.refresh_plan_path, 'w') as f:
            yaml.safe_dump(plan, f, default_flow_style=False)

        return plan



    def _backup_target_environment(self) -> str:
        """Backup active integrations in target environment."""
        active = self._get_active_integrations(self.target_client)
        for integ in active:
            integ_id = integ.get('id')
            backup_path = os.path.join(self.backup_integrations_dir, f"{integ_id}.iar")
            self._export_integration(integ_id, self.target_client, backup_path)
        self.logger.info(f"Target environment backed up to {self.backup_integrations_dir}")
        return self.backup_integrations_dir

    def _approve_plan(self, plan: Dict) -> bool:
        """Display plan and ask for user approval."""
        self._print_plan(plan)
        response = input("Approve and proceed? (yes/no): ").strip().lower()
        return response in ('yes', 'y')

    def _print_plan(self, plan: dict):
        print("\n=== REFRESH ENVIRONMENT PLAN ===")
        print(f"Source: {plan['source_environment']}")
        print(f"Target: {plan['target_environment']}")
        print(f"\nIntegrations to import/activate ({len(plan['integrations_to_refresh'])}):")
        for item in plan['integrations_to_refresh']:
            replace_str = f" (replacing v{item['target_version']})" if item['is_active_in_target'] else ""
            sched_str = " [SCHEDULER ACTIVE]" if item.get(
                'is_schedule_active_in_source') else " [SCHEDULER]" if item.get('is_scheduler') else ""
            print(f"  - {item['name']} v{item['source_version']} \t\t{replace_str}{sched_str}")

        if plan['target_integrations_to_deactivate']:
            print(f"\nSchedulers to stop ({len(plan['target_integrations_to_deactivate'])}):")
            for sched in plan['target_integrations_to_deactivate']:
                print(f"  - {sched['name']} ({sched.get('version')})")

        active_schedulers_count = sum(
            1 for i in plan['integrations_to_refresh'] if i.get('is_schedule_active_in_source'))

        print(f"\nSchedulers to start ({active_schedulers_count}):")
        for sched in plan['integrations_to_refresh']:
            if sched.get('is_scheduler', False):
                print(f"  - {sched['name']} ({sched.get('source_version')})")

        print(f"\nBackup dir: {self.backup_dir}")
        print(f"Original state backup: {self.original_state_path}")
        print(f"Refresh plan: {self.refresh_plan_dir}")
        skip_bk_msg = f"⚠️ Skip target environment backup: {plan['skip_backup']}" if plan['skip_backup'] else f"Skip target environment backup: {plan['skip_backup']}"
        print(skip_bk_msg)

    def _retrieve_plan(self, refresh_plan_file_path: str) -> Dict:
        """
        Load and validate the refresh plan from a YAML file.

        Args:
            refresh_plan_file_path (str): Path to the refresh_plan.yaml file

        Returns:
            Dict: Parsed refresh plan
        """
        if not refresh_plan_file_path:
            raise ValueError("refresh_plan_file_path is required")

        if not os.path.exists(refresh_plan_file_path):
            raise FileNotFoundError(f"Refresh plan not found: {refresh_plan_file_path}")

        try:
            with open(refresh_plan_file_path, "r") as f:
                plan = yaml.safe_load(f) or {}

            if not isinstance(plan, dict):
                raise ValueError("Refresh plan must be a valid YAML dictionary")

            # Basic structure validation (defensive)
            required_keys = [
                "integrations_to_refresh",
                "target_integrations_to_deactivate",
            ]

            for key in required_keys:
                if key not in plan:
                    self.logger.warning(f"⚠️ Missing key in refresh plan: {key}")
                    plan[key] = []

            # Optional keys normalization
            plan.setdefault("schedulers_to_start", [])

            self.logger.info(f"Refresh plan loaded from {refresh_plan_file_path}")
            return plan

        except Exception as e:
            self.logger.error(f"Failed to load refresh plan: {e}")
            raise
    def create_plan(self, integration_name_filter: str = None,
                    backup_dir: Optional[str] = None,
                    refresh_plan_path: Optional[str] = None,
                    original_state_path: Optional[str] = None
                    ) -> WorkflowResult:
        """
        Create a refresh environment workflow.
        Major versions are treated as distinct integrations.
        """
        result = WorkflowResult()

        # Override backup_dir
        if backup_dir:
            self.backup_dir = backup_dir
        else:
            backup_dir = self.backup_dir

        result.details['backup_dir'] = self.backup_dir

        # Override refresh plan path
        if refresh_plan_path:
            self.refresh_plan_dir = refresh_plan_path
        else:
            refresh_plan_path = self.refresh_plan_dir

        result.details['refresh_plan_path'] = refresh_plan_path

        # Override original_state_path
        if original_state_path:
            self.original_state_path = original_state_path
        else:
            original_state_path = self.original_state_path

        result.details['original_state_path'] = self.original_state_path

        # Rip it!
        try:

            # Step 1: Get active integrations
            source_active = self._get_active_integrations(self.source_client, integration_name_filter)
            target_active = self._get_active_integrations(self.target_client, integration_name_filter)

            # Step 2: Create original state for rollback
            self._create_original_state(target_active)
            result.add_resource('original_state', 'yaml', {'path': self.original_state_path})

            # Step 3: Create refresh plan (major version aware)
            plan = self._create_refresh_plan(source_active, target_active)
            self._print_plan(plan)

            result.details['refresh_plan'] = self.refresh_plan_path

        except Exception as e:
            result.success = False
            result.message = f"Refresh workflow failed: {e}"
            result.add_error("❌ Workflow execution failed", e)
            print(f"❌ Workflow execution failed: {str(e)}")

        return result


    def execute(self, skip_backup:bool=False, refresh_plan_path: Optional[str] = None,) -> WorkflowResult:
        """
        Applies refresh Environment Plan
        """
        result = WorkflowResult()
        try:
            if refresh_plan_path:
                refresh_plan = self._retrieve_plan(refresh_plan_path)
            else:
                refresh_plan = self._retrieve_plan(self.refresh_plan_path)

            if not self._approve_plan(refresh_plan):
                result.success = False
                result.message = "User declined the refresh plan"
                return result

            # Step 4: Backup target environment. Aborts on any Exception
            backup_requested = is_plan_step_requested(
                refresh_plan['plan_steps_to_execute'],
                PlanSteps.BACKUP
            )

            if skip_backup or not backup_requested:
                self.logger.info("🚫 Not Requested: Backing up target environment...")
            else:
                self.logger.info("Backing up target environment...")
                self._backup_target_environment()
                bk_msg = result.add_resource('backup', 'target_environment', {'path': self.backup_integrations_dir})
                self.logger.info(f"✅ Complete backing up target environment. {bk_msg}")

            # Step 5: Perform the refresh

            success_count = 0
            failures = []
            result.details['failures'] = failures
            actions_performed = [{}]
            result.details['actions_performed'] = actions_performed

            # 5.1 Export from Source
            export_requested = is_plan_step_requested(
                refresh_plan['plan_steps_to_execute'],
                PlanSteps.EXPORT_FROM_SOURCE
            )
            if not export_requested:
                self.logger.info("🚫 Not Requested: Export integrations...")
            else:
                self.logger.info(
                    f"(↓) Start exporting integrations from source environment {refresh_plan['source_environment']}...")
                export_count = 0
                for integration_to_refresh in refresh_plan['integrations_to_refresh']:
                    integration_id = integration_to_refresh.get('integration_id')                    # version-specific ID
                    source_version = integration_to_refresh.get('source_version')
                    integration_name = integration_to_refresh.get('name')

                    try:
                        # Export from sourcecode using version-specific ID
                        export_file_name = f"{integration_id}.iar"
                        export_path = os.path.join(self.source_export_dir, export_file_name)
                        self._export_integration(integration_id,  self.source_client, export_path)
                        actions_performed.append({integration_id: f'Exported from {self.source_client.config.profile} to {export_path}'})
                        export_count +=1
                    except Exception as exception:
                        failures.append({'integration_id': integration_id, 'error': str(exception)})
                        error_msg = f"❌ Failed to export {integration_name} v{source_version}"
                        result.add_error(error_msg, exception, integration_id)
                        logging.error(f'{error_msg}. Error: {str(exception)}')
                        print(f'{error_msg}. Error: {str(exception)}')
                        return result

                self.logger.info(
                    f"✅(↓) Ended exporting integrations from source environment {refresh_plan['source_environment']}: {export_count}")

            #5.2 Stop Schedulers and inactivate integrations
            stop_sched_requested = is_plan_step_requested(
                refresh_plan['plan_steps_to_execute'],
                PlanSteps.DEACTIVATE_INTEGRATIONS
            )
            if not stop_sched_requested:
                self.logger.info("🚫 Not Requested: Deactivate integrations.")
            else:
                self.logger.info(
                    f" Deactivating integrations from target environment {refresh_plan['target_environment']}...")
                stopped_sched_count = 0
                for integration_to_import in refresh_plan['target_integrations_to_deactivate']:
                    integration_id = integration_to_import.get('integration_id')  # version-specific ID
                    source_version = integration_to_import.get('source_version')
                    is_scheduler = integration_to_import.get('is_scheduler')
                    integration_name = integration_to_import.get('name')

                    # Prepare target (deactivate matching major version if exists)
                    if integration_to_import.get("is_active_in_target"):
                        prepared_for_import_successful, msg = self._prepare_integration_for_import(integration_id,is_scheduler, self.target_client)  # Use specific ID
                        if not prepared_for_import_successful:
                            failures.append({'integration_id': integration_id, 'error': msg})
                            error_msg = f"❌ Failed to export {integration_name} v{source_version}"
                            exception = Exception(error_msg)
                            result.add_error(error_msg, exception, integration_id)
                            return result

                        actions_performed.append({integration_id: msg})
                        stopped_sched_count +=1

                self.logger.info(
                    f"✅ Ended deactivating integrations: {stopped_sched_count}")

            #5.3 Import integrations
            activate_integrations_requested = is_plan_step_requested(
                refresh_plan['plan_steps_to_execute'],
                PlanSteps.IMPORT_INTEGRATIONS
            )
            if not activate_integrations_requested:
                self.logger.info("🚫 Not Requested: Import integrations.")
            else:
                self.logger.info(
                    f" Importing integrations to target environment {refresh_plan['target_environment']}...")
                for integration_to_import in refresh_plan['integrations_to_refresh']:
                    integration_id = integration_to_import.get('integration_id')  # version-specific ID
                    source_version = integration_to_import.get('source_version')
                    integration_name = integration_to_import.get('name')

                    # Delete existing in target prior to import, otherwise import fails.
                    if integration_to_import.get('source_version') == integration_to_import.get('target_version'):
                        try:
                            delete_result = self._delete_integration(integration_id=integration_id)
                        except Exception as exception:
                            self.logger.error(f"❌  Failed to delete {integration_name} v{source_version}", exception,
                                              integration_id)
                            failures.append({'integration_id': integration_id, 'error': str(exception)})
                            result.add_error(f"❌  Failed to delete {integration_name} v{source_version}", exception,
                                             integration_id)
                            return result
                    try:
                        # Import to target
                        export_file_name = f"{integration_id}.iar"
                        export_path = os.path.join(self.source_export_dir, export_file_name)
                        import_result = self._import_integration(export_path, self.target_client)
                        ## TODO: remove
                        print(f'{integration_id} import result: {str(import_result)}')

                    except Exception as exception:
                        self.logger.error(f"❌  Failed to import {integration_name} v{source_version}", exception, integration_id)
                        failures.append({'integration_id': integration_id, 'error': str(exception)})
                        result.add_error(f"❌  Failed to import {integration_name} v{source_version}", exception, integration_id)
                        return result
                self.logger.info(
                    f"✅ Ended importing integrations:  TODO of {len(refresh_plan['integrations_to_refresh'])}")

            # Step 5.4 Update Integration Configuration Properties:  UPDATE_INTEGRATIONS_PROPERTIES
            activate_integrations_requested = is_plan_step_requested(
                refresh_plan['plan_steps_to_execute'],
                PlanSteps.UPDATE_INTEGRATIONS_PROPERTIES
            )
            if not activate_integrations_requested:
                self.logger.info("🚫 Not Requested: Update integrations Properties.")
            else:
                self.logger.info(
                    f" Updating integrations properties in target environment: {refresh_plan['target_environment']}...")
                for integration_to_refresh in refresh_plan['integrations_to_refresh']:
                    integration_id = integration_to_refresh.get('integration_id')                    # version-specific ID
                    source_version = integration_to_refresh.get('source_version')
                    integration_name = integration_to_refresh.get('name')

                    try:
                        print('TODO: Not implemented yet')
                        ## TODO: implement
                        return result
                    except Exception as exception:
                        failures.append({'integration_id': integration_id, 'error': str(exception)})
                        result.add_error(f"❌  Failed to refresh {integration_name} v{source_version}", exception,
                                             integration_id)

            # STEP 4.6 Activate Integrations
            activate_integrations_requested = is_plan_step_requested(
                refresh_plan['plan_steps_to_execute'],
                PlanSteps.ACTIVATE_INTEGRATIONS
            )
            if not activate_integrations_requested:
                self.logger.info("🚫 Not Requested: Activate integrations Properties.")
            else:
                self.logger.info(
                    f" Activating integrations in target environment {refresh_plan['target_environment']}...")
                for integration_to_refresh in refresh_plan['integrations_to_refresh']:
                    integration_id = integration_to_refresh.get('integration_id')                    # version-specific ID
                    source_version = integration_to_refresh.get('source_version')
                    integration_name = integration_to_refresh.get('name')
                    composite_key = integration_to_refresh.get('composite_key')
                    is_scheduler = integration_to_refresh.get('is_scheduler')
                    print('TODO: Not implemented yet')
                    return result

                    try:

                        # Activate
                        self.logger.info(
                            f"Activating {integration_name} v{source_version} (major {self._get_major_version(source_version)})")
                        self.target_integrations.activate(integration_id)  # Use version-specific ID

                        # Wait for activation
                        activated = self._wait_for_integration_status(integration_id, 'ACTIVATED', max_attempts=25)

                        # Start scheduler if applicable
                        if is_scheduler and activated:
                            self.logger.info(f"Starting scheduler for {code} v{source_version}")
                            start_data = {"parameters": []}
                            self.target_integrations.start_schedule(integration_id, data=start_data)

                        success_count += 1
                        result.add_resource('refreshed_integration', composite_key, {
                            'status': 'success',
                            'import_result': import_result,
                            'is_scheduler': is_scheduler,
                            'version': source_version
                        })

                    except Exception as exception:
                        failures.append({'composite_key': composite_key, 'error': str(exception)})
                        result.add_error(f"❌  Failed to refresh {integration_name} v{source_version}", exception,
                                             integration_id)

            # Step 6: Generate report
            result.details['refreshed_count'] = success_count

            result.message = f"Refresh completed: {success_count} integrations refreshed, {len(failures)} failures."

            if failures:
                result.success = False
                print("❌ Some Failures")
            else:
                print("✅ Success")

            return result

        except Exception as e:
            result.success = False
            result.message = f"Refresh workflow failed: {e}"
            result.add_error("Workflow execution failed", e)
            print('======================================================')
            print(f'❌ Workflow execution failed: {str(e)}')
            print('======================================================')
            return result

    def rollback(self) -> WorkflowResult:
        """Rollback to original state using the saved original_state.yaml."""
        result = WorkflowResult()
        if not os.path.exists(self.original_state_path):
            result.success = False
            result.message = "No original state file found for rollback"
            return result

        try:
            with open(self.original_state_path) as f:
                original_state = yaml.safe_load(f)

            self.logger.info("Starting rollback to original state...")
            # TODO: Implement full restore from backup using original_state

            result.message = "Rollback completed (placeholder - full restore logic needed)"
            result.details['original_state'] = original_state
            return result

        except Exception as e:
            result.success = False
            result.message = f"Rollback failed: {e}"
            result.add_error("Rollback failed", e)
            return result

