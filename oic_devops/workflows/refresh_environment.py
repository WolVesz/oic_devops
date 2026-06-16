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
    'BACKUP', 'BACKUP_LIBRARIES', 'EXPORT_FROM_SOURCE', 'EXPORT_LIBRARIES_FROM_SOURCE','DEACTIVATE_INTEGRATIONS',
    'IMPORT_LIBRARIES', 'IMPORT_INTEGRATIONS', 'UPDATE_INTEGRATIONS_PROPERTIES',
    'ACTIVATE_INTEGRATIONS','START_SCHEDULER', 'GENERATE_REPORT',

]
PlanSteps = Enum('PlanSteps', {state: state for state in PLAN_STEPS_NAMES})

# Utility Steps can be manually added to plan. Become handy for reprocessing and debugging
UTILITY_STEPS_NAMES = [
    # Delete integrations is a convenient step that will delete the integrations listed in the plan from the target environment
    'DELETE_INTEGRATIONS',
    'SKIP_VALIDATION'
]
UtilitySteps = Enum('UtilitySteps', {state: state for state in UTILITY_STEPS_NAMES})

def is_plan_step_requested(plan_steps_to_execute: List[PlanSteps], step: PlanSteps):
    return step.name in plan_steps_to_execute

IGNORE_ERRORS = ['is not active', 'resource not found']

def should_ignore_error(e: Exception) -> bool:
    msg = str(e).lower()
    return any(p in msg for p in IGNORE_ERRORS)



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
        self.backup_libraries_dir = os.path.join(self.backup_dir, 'libraries_backup')
        self.original_state_path = os.path.join(self.backup_dir,
                                                f'original_state_{target_client.config.profile}_{ts}.yaml')
        self.source_export_dir = os.path.join(self.refresh_plan_dir, 'integrations_source')
        self.source_export_lib_dir = os.path.join(self.refresh_plan_dir, 'library_source')

        os.makedirs(self.refresh_plan_dir, exist_ok=True)
        os.makedirs(self.backup_integrations_dir, exist_ok=True)
        os.makedirs(self.backup_libraries_dir, exist_ok=True)
        os.makedirs(self.source_export_dir, exist_ok=True)
        os.makedirs(self.source_export_lib_dir, exist_ok=True)
        self.logger.info(f"Refresh Plan directory: {self.refresh_plan_dir}")
        self.logger.info(f"Backup directory: {self.backup_dir}")

        self.actions_performed: Dict[str, List[Dict[str, Any]]] = {}

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
    def _export_library(self, lib_id: str, client: OICClient, file_path: str) -> str:
        """Export library from client to file_path."""
        try:
            file_path = file_path.replace('|', '_')
            return client.libraries.export(library_id=lib_id, file_path=file_path)
        except Exception as e:
            self.logger.error(f"Failed to export {lib_id}: {e}")
            raise

    def _delete_integration(self, integration_id: str, is_scheduler: bool, client: OICClient)->bool:

        try:
            self._stop_and_deactivate_integrations(integration_id, is_scheduler, client)
            client.integrations.delete(integration_id=integration_id)
        except OICResourceNotFoundError as oicExc:
            # Not a problem. keep moving
            return
        except Exception as e:
            self.logger.error(f"Failed to delete {integration_id} from {client.config.profile}: {e}")
            raise
        return True

    def _import_integration(self, file_path: str, client: OICClient) -> Dict[str, Any]:
        """Import integration with replace option."""

        file_path = file_path.replace('|', '_')
        try:
            return client.integrations.import_integration(file_path)
        except Exception as e:
            self.logger.error(f"Failed to import from {file_path}: {e}")
            raise

    def _import_library(self, file_path: str, client: OICClient )->Dict[str,Any]:
        """ import libraries """
        try:
            return client.libraries.import_library(file_path)
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
            self.logger.info("%s for %s", msg, integration_id)
            return True, msg

    def _stop_and_deactivate_integrations(self, integration_id: str, is_scheduler: bool, client: OICClient) -> Tuple[bool, Optional[str]]:
        """Stop schedule and deactivate if necessary before import."""
        try:
            success = True
            msg = f'Deactivated {integration_id}' # optimistic approach
            if is_scheduler:
                success, msg = self._stop_scheduler(integration_id, client)

            if success:
                self.logger.info(f"Deactivating {integration_id}")
                params = {"deleteEventSubscriptionFlag": True}
                client.integrations.deactivate(integration_id=integration_id, params=params)
                deactivated = self._wait_for_integration_status(integration_id, 'CONFIGURED', max_attempts=5)
                msg += f' Deactivated {integration_id}' if deactivated else f' {integration_id} failed to deactivate. Reason: time out.'
                self.logger.info(msg)
                return deactivated, msg
            else:
                return False, f'{msg} when DEACTIVATE_INTEGRATIONS (self._prepare_integration_for_import)'
        except OICResourceNotFoundError:
            msg = f'{integration_id} not found when DEACTIVATE_INTEGRATIONS (self._prepare_integration_for_import())'
            self.logger.info(msg)
            return True, msg
        except Exception as e:
            msg = f'⚠️ Exception when _prepare_integration_for_import( {integration_id} ): {str(e)}'
            self.logger.warning(msg,e)
            ignore_it = should_ignore_error(e)
            return ignore_it, msg

    def _wait_for_integration_status(
        self,
        integration_id: str,
        desired_status: str = 'ACTIVATED',
        max_attempts: int = 5,
        interval_seconds: int = 5
    ) -> bool:
        """Wait for integration to reach desired status."""
        for attempt in range(max_attempts):
            try:
                integ = self.target_integrations.get(integration_id)
                current_status = integ.get('status')
                self.logger.info(f"Waiting for Integration {integration_id} to be at {desired_status} status. Current: {current_status} (attempt {attempt+1}/{max_attempts})")

                if current_status == desired_status:
                    return True
            except Exception as e:
                self.logger.warning(f"⚠️ Error checking status for {integration_id}: {e}")

            if attempt < max_attempts - 1:
                time.sleep(interval_seconds + (attempt * interval_seconds - interval_seconds))

        self.logger.error(f"Timeout waiting for {integration_id} to reach status {desired_status}")
        return False

    def _create_original_state(self, active_integrations: List[Dict[str, Any]], client: OICClient, original_state_path: str = None) -> str:
        """Create YAML file with original state for rollback."""

        if not original_state_path:
            original_state_path = self.original_state_path

        libraries = client.libraries.list_all()
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
                    'schedule': integ.get('schedule', None),
                    'lockedFlag': integ.get('lockedFlag')
                    ## TODO: add integration properties values
                }
                for integ in active_integrations
            ],
            'libraries':[
                {
                    'id': library.get('id'),
                    'code': library.get('code'),
                    'displayName': library.get('displayName'),
                    'version': library.get('version'),
                    'status': library.get('status'),
                    'lockedFlag': library.get('lockedFlag'),
                    'usage': library.get('usage'),
                }
                for library in libraries
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
            'integrations_to_refresh': [],
            'libraries_to_refresh':[],
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
            action = "Replace"
            if not target_integ:
                action = "New Integration"
            else:
                if target_integ.get('version') != source_version:
                    action = "New Version"

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
                'fyi_action': action,
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
        # get the libraries from the source
        libraries = self.source_client.libraries.list_all()
        for library in libraries:
            plan['libraries_to_refresh'].append(
                {
                    'id': library.get('id'),
                    'code': library.get('code'),
                    'displayName': library.get('displayName'),
                    'version': library.get('version'),
                    'status': library.get('status'),
                    'lockedFlag': library.get('lockedFlag'),
                    'usage': library.get('usage'),
                }

            )

        with open(self.refresh_plan_path, 'w') as f:
            yaml.safe_dump(plan, f, default_flow_style=False)

        return plan


    def _backup_libraries_target_environment(self)->str:
        """Backup libraries """
        libraries = self.target_client.libraries.list_all()
        for lib in libraries:
            lib_id = lib.get('id')
            backup_path = os.path.join(self.backup_libraries_dir, f"{lib_id}.zip")
            self._export_library(lib_id=lib_id, file_path=backup_path, client=self.target_client)

        self.logger.info(f"Target environment libraries backed up to {self.backup_libraries_dir}")
        return self.backup_integrations_dir

    def _backup_target_environment(self) -> str:
        """Backup active integrations in target environment."""
        active = self._get_active_integrations(self.target_client)
        for integ in active:
            integ_id = integ.get('id')
            backup_path = os.path.join(self.backup_integrations_dir, f"{integ_id}.iar")
            self._export_integration(integ_id, self.target_client, backup_path)
        libraries = self.target_client.libraries.list_all()
        for lib in libraries:
            lib_id = lib.get('id')
            backup_path = os.path.join(self.backup_libraries_dir, f"{lib_id}.zip")
            self._export_library(lib_id=lib_id, file_path=backup_path, client=self.target_client)

        self.logger.info(f"Target environment backed up to {self.backup_integrations_dir}")
        return self.backup_integrations_dir


    def _approve_plan(self, plan: Dict) -> bool:
        """Display plan and ask for user approval."""
        self._print_plan(plan)
        response = input("Approve and proceed? (yes/no): ").strip().lower()
        return response in ('yes', 'y')

    def _print_plan(self, plan: dict):
        print("\n=== REFRESH ENVIRONMENT PLAN ===")

        print(f"\nIntegrations to import/activate ({len(plan['integrations_to_refresh'])}):")
        for item in plan['integrations_to_refresh']:
            replace_str = f" (replacing active target v{item['target_version']})" if item['is_active_in_target'] else ""
            sched_str = " [SCHEDULER ACTIVE]" if item.get(
                'is_schedule_active_in_source') else " [SCHEDULER]" if item.get('is_scheduler') else ""
            print(f"  - {item['name']} v{item['source_version']} \t\t{replace_str}{sched_str}")

        if plan['target_integrations_to_deactivate']:
            print(f"\nIntegrations to deactivate ({len(plan['target_integrations_to_deactivate'])}):")
            for sched in plan['target_integrations_to_deactivate']:
                print(f"  - {sched['name']} ({sched.get('version')})")

        active_schedulers_count = sum(
            1 for i in plan['integrations_to_refresh'] if i.get('is_schedule_active_in_source'))

        print(f"\nSchedulers to start ({active_schedulers_count}):")
        for sched in plan['integrations_to_refresh']:
            if sched.get('is_scheduler', False) and sched.get('is_schedule_active_in_source', False):
                print(f"  - {sched['name']} ({sched.get(
                    'source_version')})")

        print(f"\nLibraries to refresh ({len(plan['libraries_to_refresh'])}):")
        for lib in plan['libraries_to_refresh']:
            print(f"  - {lib['displayName']} ({lib.get('version')})")

        print(f'\nSteps to execute: ')
        for action in plan['plan_steps_to_execute']:
            print(f"   - {action}")

        print(f"\nBackup dir: {self.backup_dir}")
        print(f"Refresh plan: {self.refresh_plan_path}\n")

        print(f"\nTotal Libraries to import: ({len(plan['libraries_to_refresh'])})")
        print(f"Total Integrations to deactivate: ({len(plan['target_integrations_to_deactivate'])})")
        print(f"Total Integrations to import and activate: ({len(plan['integrations_to_refresh'])})")
        print(f"Total Schedulers to start: ({active_schedulers_count})")
        print(f"Source: {plan['source_environment']}")
        print(f"Target: {plan['target_environment']}")

    def _retrieve_plan(self, refresh_plan_file_path: str) -> Dict:
        """
        Load and validate the refresh plan from a YAML file.

        Args:
            refresh_plan_file_path (str): Path to the refresh_plan_dependencies.yaml file

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
                "plan_steps_to_execute",
                "schedulers_to_start",
                "source_environment",
                "target_environment"
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

    def _persist_result(self, result: WorkflowResult) -> str:
        filename = ''
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(self.refresh_plan_dir, f'result_workflow_{ts}.json')
            result.save_to_file(filename, True)
            return filename
        except Exception as ex:
            self.logger.error(f'Failed to persist WorkflowResult: {str(ex)}', ex)
            filename = str(ex)

        return filename


    def create_plan(self, integration_name_filter: str = None,
                    backup_dir: Optional[str] = None,
                    refresh_plan_dir: Optional[str] = None,
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
        if refresh_plan_dir:
            self.refresh_plan_dir = refresh_plan_dir
        else:
            refresh_plan_dir = self.refresh_plan_dir

        result.details['refresh_plan_path'] = refresh_plan_dir

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
            self._create_original_state(active_integrations=target_active, client=self.target_client, original_state_path=self.original_state_path)
            result.add_resource('original_state', 'yaml', {'path': self.original_state_path})

            # Step 3: Create refresh plan (major version aware)
            plan = self._create_refresh_plan(source_active, target_active)
            self._print_plan(plan)
            print(f"Original state backup: {self.original_state_path}")

            result.details['refresh_plan'] = self.refresh_plan_path

            print(f"✅ Success. Plan create: {self.refresh_plan_path}")

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
        abort = False
        failures = []
        result.details['failures'] = failures
        self.actions_performed.clear()
        result.details['actions_performed'] = self.actions_performed

        try:
            if refresh_plan_path:
                refresh_plan = self._retrieve_plan(refresh_plan_path)
            else:
                refresh_plan = self._retrieve_plan(self.refresh_plan_path)

            ## Request plan approval
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
                self.logger.info("▶️ Backing up target environment...")
                self._backup_target_environment()
                result.add_resource('backup', 'backup_integrations_dir', {'path': self.backup_integrations_dir})
                self._append_action_performed(step=PlanSteps.BACKUP,integration_id=None, env= self.backup_integrations_dir)
                self.logger.info(f"✅ Complete backing up target environment.")

            step_requested = is_plan_step_requested(
                refresh_plan['plan_steps_to_execute'],
                PlanSteps.BACKUP_LIBRARIES
            )
            if skip_backup or not step_requested:
                self.logger.info("🚫 Not Requested: Backing up Libraries for target environment...")
            else:
                self.logger.info("▶️ Backing up libraries for target environment...")
                self._backup_libraries_target_environment()
                result.add_resource('backup_library', 'backup_libraries_dir',
                                             {'path': self.backup_libraries_dir})
                self._append_action_performed(step=PlanSteps.BACKUP, integration_id=None,
                                              env=self.backup_libraries_dir)
                self.logger.info(f"✅ Complete backing up Libraries  for target environment.")

            if skip_backup or not backup_requested:
                self.logger.info("🚫 Not Requested: Backing up target environment...")
            else:
                self.logger.info("▶️ Backing up target environment...")
            # Step Verify realtime there are no locked integrations, libraries in target environment
            step_requested = is_plan_step_requested(
                refresh_plan['plan_steps_to_execute'],
                UtilitySteps.SKIP_VALIDATION
            )
            if step_requested:
                self.logger.info("🚫 Skip Lock Verification in target environment...")
            else:
                self.logger.info("▶️ Verifying no locks in target environment...")
                locked_list = self.validate_any_locks(refresh_plan, self.target_client)

                if len(locked_list) > 0:
                    result.success = False
                    message = f'LOCKED artifacts to refresh prevent plan execution: {len(locked_list)}'
                    result.message = message
                    print(f"\n❌ {message}")
                    for obj_id in locked_list:
                        print(f'\t🔒 {obj_id}')

                    return result

            # Step 5: Perform the refresh
            activated_success_count = 0

            # 5.1 Export Integrations from Source
            if not abort:
                step_requested = is_plan_step_requested(
                    refresh_plan['plan_steps_to_execute'],
                    PlanSteps.EXPORT_FROM_SOURCE
                )
                if not step_requested:
                    self.logger.info("🚫 Not Requested: Export from source...")
                else:
                    self.logger.info(
                        f"▶️(↓) Start exporting integrations from source environment {refresh_plan['source_environment']}...")
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
                            self._append_action_performed(PlanSteps.EXPORT_FROM_SOURCE, integration_id, self.source_client.config.profile)
                            export_count +=1
                        except Exception as exception:
                            failures.append({'integration_id': integration_id, 'error': str(exception)})
                            error_msg = f"❌ Failed to export integration: {integration_name} v{source_version}"
                            result.add_error(error_msg, exception, integration_id)
                            logging.error(f'{error_msg}. Error: {str(exception)}')
                            print(f'{error_msg}. Error: {str(exception)}')
                            abort = True
                            # No break needed
                    self.logger.info(
                        f"✅(↓) Ended exporting integrations from source environment {refresh_plan['source_environment']}: "
                        f"{export_count} of {len(refresh_plan.get('integrations_to_refresh', []))}")
                    self.logger.info(
                        f"▶️(↓) Start exporting integrations from source environment {refresh_plan['source_environment']}...")

            # 5.1 Export Integrations from Source
            if not abort:
                step_requested = is_plan_step_requested(
                    refresh_plan['plan_steps_to_execute'],
                    PlanSteps.EXPORT_LIBRARIES_FROM_SOURCE
                )
                if not step_requested:
                    self.logger.info("🚫 Not Requested: Export Library from source...")
                else:
                    self.logger.info(
                        f"▶️(↓) Start exporting Libraries from source environment {refresh_plan['source_environment']}...")
                    export_count = 0

                    for library_to_refresh in refresh_plan['libraries_to_refresh']:
                        lib_id = library_to_refresh['id']
                        export_file_name = library_to_refresh['displayName'] + '_' +  library_to_refresh['version'] + '.zip'
                        export_path = os.path.join(self.source_export_lib_dir, export_file_name)
                        try:
                            self._export_library(lib_id, self.source_client, export_path)
                            self._append_action_performed(PlanSteps.EXPORT_FROM_SOURCE, lib_id, self.source_client.config.profile)
                            export_count += 1
                        except Exception as exception:
                            failures.append({'library_id': lib_id, 'error': str(exception)})
                            error_msg = f"❌ Failed to export library: {export_file_name} "
                            result.add_error(error_msg, exception, lib_id)
                            logging.error(f'{error_msg}. Error: {str(exception)}')
                            print(f'{error_msg}. Error: {str(exception)}')
                            abort = True
                            # No break needed
                    self.logger.info(
                        f"✅(↓) Ended exporting libraries from source environment {refresh_plan['source_environment']}: "
                        f"{export_count} of {len(refresh_plan.get('libraries_to_refresh',[]))}")

            #5.2 Stop Schedulers and inactivate integrations
            if not abort:
                step_requested = is_plan_step_requested(
                    refresh_plan['plan_steps_to_execute'],
                    PlanSteps.DEACTIVATE_INTEGRATIONS
                )
                if not step_requested:
                    self.logger.info("🚫 Not Requested: Deactivate integrations.")
                else:
                    self.logger.info(
                        f"▶️ Deactivating integrations from target environment {refresh_plan['target_environment']}...")
                    done_count = 0
                    for integration_to_import in refresh_plan['target_integrations_to_deactivate']:
                        integration_id = integration_to_import.get('integration_id')  # version-specific ID
                        target_version = integration_to_import.get('target_version')
                        code = integration_to_import.get('code')
                        is_scheduler = integration_to_import.get('is_scheduler')
                        integration_name = integration_to_import.get('name')

                        # Prepare target (deactivate matching major version if exists)
                        target_integration_id =  f'{code}|{target_version}'
                        prepared_for_import_successful, msg = self._stop_and_deactivate_integrations(
                            integration_id=target_integration_id,
                            is_scheduler=is_scheduler,
                            client=self.target_client)  # Use specific ID
                        if not prepared_for_import_successful:
                            failures.append({'integration_id':target_integration_id, 'error': msg})
                            error_msg = f"❌ Failed to deactivate  {integration_name} v{target_version}"
                            exception = Exception(error_msg)
                            result.add_error(error_msg, exception, integration_id)
                            abort = True
                            break # stop processing
                        else:
                            self._append_action_performed(PlanSteps.DEACTIVATE_INTEGRATIONS, integration_id,
                                                          self.target_client.config.profile)
                            done_count +=1

                    self.logger.info(
                        f"✅ Ended deactivating integrations: {done_count} of {len(refresh_plan.get('target_integrations_to_deactivate', []))}")

            # Delete integration to import
            action_requested = is_plan_step_requested(
                refresh_plan['plan_steps_to_execute'],
                UtilitySteps.DELETE_INTEGRATIONS
            )
            count = 0
            if action_requested and not abort:
                self.logger.info(
                    f" ▶️ Deleting integrations in target environment: {refresh_plan['target_environment']}...")
                count = 0
                for integration_to_refresh in refresh_plan['integrations_to_refresh']:
                    integration_id = integration_to_refresh.get('integration_id')  # version-specific ID
                    integration_name = integration_to_refresh.get('name')
                    source_version = integration_to_refresh.get('source_version')
                    is_scheduler = integration_to_refresh.get('is_scheduler', False)

                    try:
                        real_delete = self._delete_integration(integration_id=integration_id, is_scheduler= is_scheduler, client=self.target_client)
                        if real_delete:
                            count += 1
                            self._append_action_performed(UtilitySteps.DELETE_INTEGRATIONS, integration_id,
                                                           self.target_client.config.profile)
                    except Exception as exception:
                        failures.append({'integration_id': integration_id, 'error': str(exception)})
                        result.add_error(f"❌  Failed to delete {integration_name} v{source_version}", exception,
                                         integration_id)
                        abort = True
                self.logger.info(
                    f"✅ Ended deleting integrations. {count} of {len(refresh_plan.get('integrations_to_refresh', []))}")

            #5 Import integrations
            if not abort:
                step_requested = is_plan_step_requested(
                    refresh_plan['plan_steps_to_execute'],
                    PlanSteps.IMPORT_INTEGRATIONS
                )
                integrations_imported_count = 0
                if not step_requested:
                    self.logger.info("🚫 Not Requested: Import integrations.")
                else:
                    self.logger.info(
                        f"▶️ Importing integrations to target environment {refresh_plan['target_environment']}...")
                    for integration_to_import in refresh_plan['integrations_to_refresh']:
                        integration_id = integration_to_import.get('integration_id')  # version-specific ID
                        source_version = integration_to_import.get('source_version')
                        integration_name = integration_to_import.get('name')
                        is_scheduler = integration_to_import.get('is_scheduler', False)

                        # Delete existing in target prior to import, otherwise import fails.
                        try:
                             real_delete = self._delete_integration(integration_id=integration_id, is_scheduler=is_scheduler, client=self.target_client)
                             if real_delete:
                                 self._append_action_performed(UtilitySteps.DELETE_INTEGRATIONS, integration_id,
                                                               self.target_client.config.profile)

                        except Exception as exception:
                            self.logger.error(f"❌  Failed to delete {integration_name} v{source_version}. Error: {str(exception)}", exception,
                                              integration_id)
                            failures.append({'integration_id': integration_id, 'error': str(exception)})
                            result.add_error(f"❌  Failed to delete {integration_name} v{source_version}", exception,
                                             integration_id)
                            abort = True
                            break
                        try:
                            # Import to target
                            export_file_name = f"{integration_id}.iar"
                            export_path = os.path.join(self.source_export_dir, export_file_name)
                            self._import_integration(export_path, self.target_client)
                            self._append_action_performed(PlanSteps.IMPORT_INTEGRATIONS, integration_id,
                                                          self.target_client.config.profile)
                            integrations_imported_count +=1
                            self.logger.info(f'Imported: {integration_id}')
                        except Exception as exception:
                            self.logger.error(f"❌  Failed to import {integration_name} v{source_version}", exception, integration_id)
                            failures.append({'integration_id': integration_id, 'error': str(exception)})
                            result.add_error(f"❌  Failed to import {integration_name} v{source_version}", exception, integration_id)
                            abort = True
                    self.logger.info(
                        f"✅ Ended importing integrations. {integrations_imported_count} of {len(refresh_plan.get('integrations_to_refresh',[]))}")

            # Step  Import Libraries
            if not abort:
                step_requested = is_plan_step_requested(
                    refresh_plan['plan_steps_to_execute'],
                    PlanSteps.IMPORT_LIBRARIES
                )
                if not step_requested:
                    self.logger.info("🚫 Not Requested: Import Libraries.")
                else:
                    self.logger.info(
                        f"▶️ Importing Libraries in target environment {refresh_plan['target_environment']}...")
                    for lib_to_import in refresh_plan['libraries_to_refresh']:
                        lib_name = lib_to_import.get("displayName")
                        liv_version = lib_to_import.get("version")
                        lib_file_name = f"{lib_name}_{liv_version}.zip"
                        export_path = os.path.join(self.source_export_lib_dir, lib_file_name)
                        try:
                            self._import_library(file_path=export_path, client=self.target_client)
                        except Exception as exception:
                            lib_id = lib_to_import.get('id')
                            failures.append({'library_id': lib_id, 'error': str(exception)})
                            result.add_error(f"❌  Failed to refresh library: {lib_name} v{liv_version}", exception, lib_id)
                            abort = True
                    self.logger.info(
                        f"✅ Ended importing libraries: {len(refresh_plan['libraries_to_refresh'])}")

            # Step 5.4 Update Integration Configuration Properties:  UPDATE_INTEGRATIONS_PROPERTIES
            # TODO: implement
            if True == False and not abort:
                action_requested = is_plan_step_requested(
                    refresh_plan['plan_steps_to_execute'],
                    PlanSteps.UPDATE_INTEGRATIONS_PROPERTIES
                )
                if not action_requested:
                    self.logger.info("🚫 Not Requested: Update integrations Properties.")
                else:
                    self.logger.info(
                        f" ▶️ Updating integrations properties in target environment: {refresh_plan['target_environment']}...")
                    count = 0
                    for integration_to_refresh in refresh_plan['integrations_to_refresh']:
                        integration_id = integration_to_refresh.get('integration_id')                    # version-specific ID
                        source_version = integration_to_refresh.get('source_version')
                        integration_name = integration_to_refresh.get('name')

                        try:
                            print(f'TODO: UPDATE_INTEGRATIONS_PROPERTIES Not implemented yet! {integration_id}')
                            ## TODO: implement
                            # self._append_action_performed(PlanSteps.UPDATE_INTEGRATIONS_PROPERTIES, integration_id,
                            #                               self.target_client.config.profile)
                        except Exception as exception:
                            failures.append({'integration_id': integration_id, 'error': str(exception)})
                            result.add_error(f"❌  Failed to refresh {integration_name} v{source_version}", exception,
                                                 integration_id)
                            abort = True
                    self.logger.info(
                        f"✅ Ended updating properties for integrations. {count} of {len(refresh_plan.get('integrations_to_refresh', []))}")


            # STEP 4.6 Activate Integrations
            if not abort:
                step_requested = is_plan_step_requested(
                    refresh_plan['plan_steps_to_execute'],
                    PlanSteps.ACTIVATE_INTEGRATIONS
                )
                if not step_requested:
                    self.logger.info("🚫 Not Requested: Activate integrations Properties.")
                else:
                        self.logger.info(
                            f"▶️ Activating integrations in target environment {refresh_plan['target_environment']}...")

                        for integration_to_refresh in refresh_plan['integrations_to_refresh']:
                            integration_id = integration_to_refresh.get('integration_id')                    # version-specific ID
                            source_version = integration_to_refresh.get('source_version')
                            integration_name = integration_to_refresh.get('name')
                            composite_key = integration_to_refresh.get('composite_key')
                            is_scheduler = integration_to_refresh.get('is_scheduler')

                            try:
                                # Activate
                                self.logger.info(
                                    f"🚀 Activating {integration_name} v{source_version} (major {self._get_major_version(source_version)})")
                                json_data = {
                                    "tracingEnabledFlag":True,
                                    "payloadTracingEnabledFlag":False,
                                    "recordEnabledFlag":False,
                                    "replay":{"canReplay":False},"payload":{"validate":False},"softDeactivate":False}
                                self.target_integrations.activate(integration_id, json_data=json_data)  # Use version-specific ID


                                # Wait for activation
                                activated = self._wait_for_integration_status(integration_id, 'ACTIVATED', max_attempts=5)
                                self._append_action_performed(PlanSteps.ACTIVATE_INTEGRATIONS, integration_id,
                                                              self.target_client.config.profile)
                                self.logger.info(f'     🟢 Activated integration {integration_id}')

                                # Start scheduler if applicable
                                if is_scheduler and activated:
                                    self.logger.info(f"Starting scheduler for {integration_name} v{source_version}")
                                    start_data = {"parameters": []}
                                    self.target_integrations.start_schedule(integration_id, data=start_data)
                                    self._append_action_performed(PlanSteps.START_SCHEDULER, integration_id,
                                                                  self.target_client.config.profile)
                                    self.logger.info(
                                        f'⏲ Started Scheduler for integration {integration_id}')

                                activated_success_count += 1
                                result.add_resource('activated_integration', composite_key, {
                                    'status': 'success',
                                    'name': integration_name,
                                    'is_scheduler': is_scheduler,
                                    'version': source_version
                                })

                            except Exception as exception:
                                failures.append({'integration_id': integration_id, 'error': str(exception)})
                                result.add_error(f"❌  Failed to activate {integration_name} v{source_version}", exception,
                                                     integration_id)
                                abort = True
                        self.logger.info(
                            f"✅ Ended activating integrations. {activated_success_count} of {len(refresh_plan.get('integrations_to_refresh', []))}")

            # Step 6: Generate report
            if not abort:
                step_requested = is_plan_step_requested(
                    refresh_plan['plan_steps_to_execute'],
                    PlanSteps.GENERATE_REPORT
                )
                if not step_requested:
                    self.logger.info("🚫 Not Requested: Generate Report.")
                else:
                    self.logger.info(
                        f"▶️ Generating report...")

                    # for integration_to_refresh in refresh_plan['integrations_to_refresh']:
                    #     integration_id = integration_to_refresh.get('integration_id')  # version-specific ID
                    #     activated_integration = result.resources.get('activated_integration',{}).get('integration_id',{})
                    #
                self.logger.info(
                    f"✅ Ended Generating report.")

            result.details['refreshed_count'] = activated_success_count

            result.message = f"Refresh completed: {activated_success_count} integrations refreshed, {len(failures)} failures."

            response_filename = self._persist_result(result)

            if len(failures) > 0:
                result.success = False
                print(f"❌ Some Failures ({len(failures)})")
                for failure in failures:
                    print(f"\t💥 {str(failure)}")
            else:
                print(f"✅ Success. \nResult persisted at {response_filename}")


            return result

        except Exception as e:
            result.success = False
            result.message = f"Refresh workflow failed: {e}"
            result.add_error("Workflow execution failed", e)
            print('======================================================')
            print(f'❌ Workflow execution failed: {str(e)}')
            print('======================================================')
            self._persist_result(result)
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

    def _append_action_performed(
            self,
            step: PlanSteps,
            integration_id: str = None,
            env: str = None,
    ):

        if step.name not in self.actions_performed:
            self.actions_performed[step.name] = []

        action = self.actions_performed[step.name]
        action.append({"integration_id":integration_id, "environment":env})

    def _is_any_integration_locked(self, refresh_plan: Dict, client: OICClient) -> Tuple[int, List]:
        locked = []
        for integration in refresh_plan.get('integrations_to_refresh', []):
            integration_id = integration.get('integration_id')
            try:
                integration_now = client.integrations.get(integration_id=integration_id)
                if integration_now.get('lockedFlag'):
                    locked.append(integration_id)
                    self.logger.warning(f'Locked integration in {client.config.profile}: {integration_id}')
            except OICResourceNotFoundError as e:
                self.logger.debug(f'str{e}')  # Eat the exception

        return len(locked), locked

    def _is_any_library_locked(self, refresh_plan: Dict, client: OICClient) -> Tuple[int, List]:

        locked = []
        for library in refresh_plan.get('libraries_to_refresh', []):
            library_id = library.get('id')
            try:
                library_now = client.libraries.get(library_id=library_id)
                if library_now.get('lockedFlag'):
                    locked.append(library_id)
                    self.logger.warning(f'️Locked library in {client.config.profile}: {library_id}')
            except OICResourceNotFoundError as e:
                self.logger.debug(f'str{e}')  # Eat the exception
        return len(locked), locked

    def validate_any_locks(self, refresh_plan, target_client)-> List[str]:
        message = ''
        locked_list = []
        ## Verify realtime there are no integrations locked
        locked_count, integrations_locked = self._is_any_integration_locked(refresh_plan, self.target_client)
        if locked_count > 0:
            for locked_id in integrations_locked:
                locked_list.append(f'integration_id: {locked_id}')

        ## TODO: Verify any Connection is locked

        ## Verify realtime there are no libraries locked
        locked_count, libraries_locked = self._is_any_library_locked(refresh_plan, self.target_client)

        if locked_count > 0:
            for locked_id in libraries_locked:
                locked_list.append(f'library_id: {locked_id}')


        return locked_list






