"""
Refresh Environment workflow for the OIC DevOps package.

This module provides the RefreshEnvironment workflow to synchronize active
integrations from a source environment to the target environment.
"""

import os
import tempfile
import time
import yaml
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from colorama.ansi import clear_line
from numpy.f2py.auxfuncs import throw_error

from .base import BaseWorkflow, WorkflowResult
from oic_devops.client import OICClient

SCHEDULED = 'Scheduled'
SCHEDULE_ACTIVE_STATE = 'ACTIVE'


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
        backup_dir: Optional[str] = None,
        refresh_plan_path: Optional[str] = None,

    ):
        super().__init__(target_client)
        self.target_client = target_client
        self.source_client = source_client
        self.target_integrations = target_client.integrations
        self.source_integrations = source_client.integrations
        self.backup_dir = backup_dir or os.path.join(tempfile.gettempdir(), f'oic_refresh_backup_{int(time.time())}')
        self.refresh_plan_path = refresh_plan_path or os.path.join(self.backup_dir, 'refresh_plan.yaml')
        self.original_state_path = os.path.join(self.backup_dir, f'original_state{target_client.config.profile}.yaml')
        self.backup_integrations_dir = os.path.join(self.backup_dir, 'integrations_backup')
        os.makedirs(self.backup_integrations_dir, exist_ok=True)
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

    def _get_active_integrations(self, client: OICClient) -> List[Dict[str, Any]]:
        """Get list of active integrations from the given client."""
        params = {"q":"{status:'ACTIVATED'}"}
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

    def _import_integration(self, file_path: str, client: OICClient, replace: bool = True) -> Dict[str, Any]:
        """Import integration with replace option."""
        data = {'importMode': 'REPLACE'} if replace else {}
        try:
            return client.integrations.import_integration(file_path, data=data)
        except Exception as e:
            self.logger.error(f"Failed to import from {file_path}: {e}")
            raise

    def _prepare_integration_for_import(self, integration_id: str, client: OICClient) -> Tuple[bool, Optional[str]]:
        """Stop schedule and deactivate if necessary before import."""
        try:
            integ = client.integrations.get(integration_id)
            is_scheduler = integ.get('pattern') == SCHEDULED

            if is_scheduler:
                self.logger.info(f"Stopping schedule for {integration_id}")
                client.integrations.stop_schedule(integration_id)

            self.logger.info(f"Deactivating {integration_id}")
            client.integrations.deactivate(integration_id)
            return True, None
        except Exception as e:
            return False, str(e)

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
                self.logger.warning(f"Error checking status for {integration_id}: {e}")

            if attempt < max_attempts - 1:
                time.sleep(interval_seconds)

        self.logger.error(f"Timeout waiting for {integration_id} to reach status {desired_status}")
        return False

    def _create_original_state(self, active_integrations: List[Dict[str, Any]]) -> str:
        """Create YAML file with original state for rollback."""
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
                    ## TODO: add integration properties values -- maybe nots
                }
                for integ in active_integrations
            ]
        }
        with open(self.original_state_path, 'w') as f:
            yaml.safe_dump(state, f, default_flow_style=False)
        self.logger.info(f"Original state saved to {self.original_state_path}")
        return self.original_state_path

    def _create_refresh_plan(self, source_active: List[Dict], target_active: List[Dict]) -> Dict:
        """Create detailed refresh plan using composite key (code + major_version)."""
        plan = {
            'timestamp': datetime.now().isoformat(),
            'source_environment': getattr(self.source_client.config, 'identity_domain', 'SOURCE'),
            'target_environment': getattr(self.target_client.config, 'identity_domain', 'TARGET'),
            'integrations_to_refresh': [],
            'schedulers_to_stop': [],
            'schedulers_to_start': [],
            'target_integrations_original_state':[],
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
                'id': integration_id,
                'composite_key': composite_key,
                'code': code,
                'major_version': major,
                'name': source_integ.get('name'),
                'source_version': source_version,
                'target_version': target_integ.get('version') if target_integ else None,
                'will_replace': target_integ is not None,
                'is_scheduler': source_pattern == SCHEDULED,
                'is_schedule_active_in_source': is_schedule_active_in_source,
            })

            if target_integ and target_integ.get('pattern') == SCHEDULED and target_integ.get('schedule', {}).get('state', '') == SCHEDULE_ACTIVE_STATE:
                plan['schedulers_to_stop'].append({
                    'integration_id': integration_id,
                    'name': target_integ.get('name'),
                    'target_version': target_integ.get('version'),
                    'major_version': major,
                    'code': code,
                })

            if source_pattern == SCHEDULED and is_schedule_active_in_source:
                plan['schedulers_to_start'].append({
                    'integration_id': integration_id,
                    'name': source_integ.get('name'),
                    'version': source_version,
                    'major_version': major,
                    'code': code,
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
        print("\n=== REFRESH ENVIRONMENT PLAN ===")
        print(f"Source: {plan['source_environment']}")
        print(f"Target: {plan['target_environment']}")
        print(f"\nIntegrations to import/activate ({len(plan['integrations_to_refresh'])}):")
        for item in plan['integrations_to_refresh']:
            replace_str = f" (replacing v{item['target_version']})" if item['will_replace'] else ""
            sched_str = " [SCHEDULER]" if item.get('is_scheduler') else ""
            print(f"  - {item['name']} v{item['source_version']} \t\t{replace_str}{sched_str}")

        if plan['schedulers_to_stop']:
            print(f"\nSchedulers to stop ({len(plan['schedulers_to_stop'])}):")
            for sched in plan['schedulers_to_stop']:
                print(f"  - {sched['name']} ({sched.get('version')})")

        if plan['schedulers_to_start']:
            print(f"\nSchedulers to start ({len(plan['schedulers_to_start'])}):")
            for sched in plan['schedulers_to_start']:
                print(f"  - {sched['name']} ({sched.get('version')})")

        print(f"\nBackup dir: {self.backup_dir}")
        print(f"Original state backup: {self.original_state_path}")
        print(f"Refresh plan: {self.refresh_plan_path}")
        print("\nWARNING: This will replace matching major version integrations in the target environment.")

        response = input("Approve and proceed? (yes/no): ").strip().lower()
        return response in ('yes', 'y')

    def execute(self, dry_run: bool = True) -> WorkflowResult:
        """
        Execute the refresh environment workflow.
        Major versions are treated as distinct integrations.
        """
        result = WorkflowResult()
        result.details['dry_run'] = dry_run
        result.details['backup_dir'] = self.backup_dir

        try:

            # Step 1: Get active integrations
            source_active = self._get_active_integrations(self.source_client)
            target_active = self._get_active_integrations(self.target_client)

            # Step 2: Create original state for rollback
            self._create_original_state(target_active)
            result.add_resource('original_state', 'yaml', {'path': self.original_state_path})

            # Step 3: Create and approve refresh plan (major version aware)
            refresh_plan = self._create_refresh_plan(source_active, target_active)
            result.details['refresh_plan'] = refresh_plan

            if not self._approve_plan(refresh_plan):
                result.success = False
                result.message = "User declined the refresh plan"
                return result

            if dry_run:
                result.message = "Dry run completed successfully. No changes made."
                return result

            # Step 4: Backup target environment
            self.logger.info("Backing up target environment...")
            self._backup_target_environment()
            result.add_resource('backup', 'target_environment', {'path': self.backup_integrations_dir})

            # Step 5: Perform the refresh
            success_count = 0
            failures = []

            for source_integ in source_active:
                integ_id = source_integ.get('id')                    # version-specific ID
                code = source_integ.get('code')
                source_version = source_integ.get('version')
                is_scheduler = source_integ.get('pattern') == SCHEDULED
                composite_key = self._get_composite_key(source_integ)

                try:
                    # Export from sourcecodez using version-specific ID
                    export_path = os.path.join(self.backup_dir, f"{code}_v{source_version}_from_source.iar")
                    self._export_integration(integ_id, self.source_client, export_path)

                    # Prepare target (deactivate matching major version if exists)
                    self._prepare_integration_for_import(integ_id, self.target_client)  # Use specific ID

                    # Import to target
                    import_result = self._import_integration(export_path, self.target_client)

                    # Activate
                    self.logger.info(f"Activating {code} v{source_version} (major {self._get_major_version(source_version)})")
                    self.target_integrations.activate(integ_id)   # Use version-specific ID

                    # Wait for activation
                    activated = self._wait_for_integration_status(integ_id, 'ACTIVATED', max_attempts=25)

                    # Start scheduler if applicable
                    if is_scheduler and activated:
                        self.logger.info(f"Starting scheduler for {code} v{source_version}")
                        start_data = {"parameters": []}
                        self.target_integrations.start_schedule(integ_id, data=start_data)

                    success_count += 1
                    result.add_resource('refreshed_integration', composite_key, {
                        'status': 'success',
                        'import_result': import_result,
                        'is_scheduler': is_scheduler,
                        'version': source_version
                    })

                except Exception as e:
                    failures.append({'composite_key': composite_key, 'error': str(e)})
                    result.add_error(f"Failed to refresh {code} v{source_version}", e, integ_id)

            # Step 6: Generate report
            result.details['refreshed_count'] = success_count
            result.details['failures'] = failures
            result.message = f"Refresh completed: {success_count} integrations refreshed, {len(failures)} failures."

            if failures:
                result.success = False

            return result

        except Exception as e:
            result.success = False
            result.message = f"Refresh workflow failed: {e}"
            result.add_error("Workflow execution failed", e)
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