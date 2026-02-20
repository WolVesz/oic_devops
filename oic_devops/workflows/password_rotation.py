
"""
Password rotation workflow OIC connections.
"""
from __future__ import annotations

import time
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple, Optional

from oic_devops.exceptions import (
    OICAPIError,
    OICError,
    OICResourceNotFoundError,
)
from oic_devops.workflows.base import BaseWorkflow, WorkflowResult


class PasswordRotationWorkflow(BaseWorkflow):
    """Rotate passwords for Basic Authentication connections.

    High-level orchestration:
      1) Build a connections cross-reference by username → connection IDs and dependent integrations
      2) Snapshot integration original state (and schedules if applicable)
      3) Stop active schedules and wait until they are cancelled
      4) Deactivate dependent integrations
      5) Update connection passwords

    All major steps are split into smaller methods that can be unit tested independently.
    """

    connections_configured_enriched: Dict[str, Dict[str, Any]] | None = None
    connections_dictionary: Dict[str, Dict[str, Any]] | None = None
    connectionFilter: Optional[str] | None = None

    RESOURCE_INTEGRATION = "integration"
    ACTION_INTEGRATION_IN_PROCESS = 'INTEGRATION_IN_PROCESS'
    ACTION_ACTIVATE = "ACTIVATE"
    ACTION_DEACTIVATE = "DEACTIVATE"
    ACTION_START_SCHEDULE = "START_SCHEDULE"


    # --------------------------- Public API ---------------------------
    def execute(self, *args, **kwargs) -> WorkflowResult:
        """Dispatcher-compatible entry point.

        Expected kwargs:
            target_usernames: Dict[str, str]  -> {username: new_password}
            refresh_xref: bool                -> rebuild the in-memory cross-reference
        """
        # target_usernames: Dict[str, str] = kwargs.get("target_usernames", {})
        # refresh_xref: bool = kwargs.get("refresh_xref", False)
        # return self.update_password_basic_authentication(target_usernames, refresh_xref)
        print("TODO: execute() method, not implemented yet!")
        return WorkflowResult()

    def update_password_basic_authentication(
            self,
            target_usernames: Dict[str, str],
            connection_filter: Optional[str] = None,
            refresh_xref: bool = False,
    ) -> WorkflowResult:
        result = WorkflowResult()
        result.message = "Updating passwords and restarting integrations"

        # 1) Build/reuse cross-reference: connection_id -> { username, integrations: [ids] }
        if refresh_xref or not self.connections_dictionary or not self.connectionFilter == connection_filter:
            try:
                self.connections_dictionary = self.build_connections_dictionary(target_usernames, connectionFilter=connection_filter)
                self.connectionFilter = connection_filter
            except Exception as exc:  # surface build errors
                self.logger.exception("Failed to build connections dictionary.")
                result.success = False
                result.add_error(message="Failed to build connections dictionary", error=exc)
                return result

        connections_dictionary = self.connections_dictionary or {}

        # 2) Snapshot original status for all dependent integrations
        integrations_original_status, snapshot_wf = self.snapshot_integrations_state(connections_dictionary)
        result.merge(snapshot_wf)
        if not snapshot_wf.success:
            return result

        # 3) Identify scheduled integrations and their schedules
        scheduled_ids = self.get_scheduled_integrations(integrations_original_status)
        schedules_wf = self.snapshot_schedules(scheduled_ids, integrations_original_status)
        result.merge(schedules_wf)
        if not schedules_wf.success:
            return result

        # 4) Stop schedules and wait until CANCELLED
        stop_sched_wf = self.stop_schedulers(scheduled_ids, integrations_original_status)
        result.merge(stop_sched_wf)
        if not stop_sched_wf.success:
            self.save_result(result)
            return result

        wait_sched_wf = self.wait_for_schedules_state(scheduled_ids, desired_state="CANCELLED", timeout_sec=300, poll_interval_sec=5)
        result.merge(wait_sched_wf)
        if not wait_sched_wf.success:
            self.save_result(result)
            return result

        # 5) Deactivate dependent integrations
        dependent_integration_ids = self.collect_all_dependent_integrations(connections_dictionary)
        deact_wf = self.deactivate_integrations(dependent_integration_ids)
        result.merge(deact_wf)
        if not deact_wf.success:
            self.save_result(result)
            return result

        # 6) Update connection passwords
        update_wf = self.update_connection_passwords(connections_dictionary, target_usernames)
        result.merge(update_wf)
        if not update_wf.success:
            self.save_result(result)
            return result

        # 7) Activate Integrations
        activate_wf = self.activate_integrations(dependent_integration_ids)
        result.merge(activate_wf)
        if not activate_wf.success:
            self.save_result(result)
            return result

        # 8) Start Schedulers
        schedulers_wf = self.start_schedulers(schedule_integrations=scheduled_ids, integrations_original_status= integrations_original_status)
        result.merge(schedulers_wf)
        if not schedules_wf.success:
            self.save_result(result)
            return result
        # 9) Wait for activated
        wait_sched_wf = self.wait_for_schedules_state(scheduled_ids, desired_state="ACTIVE", timeout_sec=300,
                                                      poll_interval_sec=5)
        result.merge(wait_sched_wf)
        if not wait_sched_wf.success:
            self.save_result(result)
            return result

        self.save_result(result)
        return result


    # ---------------------- Unit-testable helpers ----------------------
    def build_connections_dictionary(
            self,
            target_usernames: Dict[str, str],
            connectionFilter: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Build a dict mapping connection_id -> { 'username': str, 'integrations': [str] }.

        For each target username, fetch connections that use it and the activated
        integrations that depend on each connection.
        """
        self.logger.info("Building connections dictionary for usernames: %s", list(target_usernames.keys()))

        # Ensure we have enriched connections cached
        if not self.connections_configured_enriched:
            params = None
            if connectionFilter:
                params = {"q": "{"+f"name:{connectionFilter}"+"}"}

            self.logger.info("Fetching enriched connections with filter: %s", connectionFilter)
            self.connections_configured_enriched = self.client.connections.list_enriched(params=params)

        connections_dictionary: Dict[str, Dict[str, Any]] = {}

        for username in target_usernames.keys():
            connection_ids = self.get_connection_ids_using_username(username)
            for connection_id in connection_ids:
                connections_dictionary.setdefault(connection_id, {"username": username, "integrations": []})
                # Get dependent integrations (usage) for the connection
                usage = self.client.connections.usage(connection_id=connection_id, raw=False)
                active_integrations = [item["integration_id"] for item in usage if item.get("status") == "ACTIVATED"]
                connections_dictionary[connection_id]["integrations"] = list(sorted(set(active_integrations)))

        return connections_dictionary

    def get_connection_ids_using_username(self, target_username: str) -> List[str]:
        """Return a list of connection IDs that use the given username.

        Relies on the cached `connections_configured_enriched` populated by
        build_connections_dictionary().
        """
        if not self.connections_configured_enriched:
            self.connections_configured_enriched = self.client.connections.list_enriched()

        connections = self.connections_configured_enriched
        ids = [
            conn_key
            for conn_key, conn in connections.items()
            for p in conn.get("securityProperties", [])
            if p.get("propertyName") == "username" and p.get("propertyValue") == target_username
        ]
        return list(sorted(set(ids)))

    def snapshot_integrations_state(self, connections_dictionary: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], WorkflowResult]:
        """Read current state for all dependent integrations and return a map.

        Returns a tuple: (integrations_original_status, WorkflowResult)
        where the map is {integration_id: {...raw integration details...}}
        """
        result = WorkflowResult()
        info: Dict[str, Dict[str, Any]] = {}

        integration_ids = self.collect_all_dependent_integrations(connections_dictionary)
        for integration_id in integration_ids:
            try:
                info[integration_id] = self.client.integrations.get(integration_id=integration_id)
                result.add_resource(self.RESOURCE_INTEGRATION, integration_id, {"action": "SNAPSHOT", "success": True})
            except OICAPIError as exc:
                self.logger.error("Failed to fetch integration %s: %s", integration_id, exc.title)
                result.success = False
                result.add_error(resource_id=integration_id, message=exc.title, error=exc)
                result.add_resource(self.RESOURCE_INTEGRATION, integration_id, {"action": "SNAPSHOT", "success": False, "message": exc.title})
            except OICResourceNotFoundError as not_found:
                self.logger.error("Integration %s not found: %s", integration_id, not_found)
                result.success = False
                result.add_error(resource_id=integration_id, message=str(not_found), error=not_found)
                result.add_resource(self.RESOURCE_INTEGRATION, integration_id, {"action": "SNAPSHOT", "success": False, "message": "404 - Not Found"})

        return info, result

    @staticmethod
    def get_scheduled_integrations(integrations_original_status: Dict[str, Dict[str, Any]]) -> List[str]:
        """Return integration IDs that are Scheduled based on the snapshot."""
        return sorted({iid for iid, details in integrations_original_status.items() if details.get("pattern") == "Scheduled"})

    def snapshot_schedules(
        self,
        schedule_integrations: Iterable[str],
        integrations_original_status: Dict[str, Dict[str, Any]],
    ) -> WorkflowResult:
        """Fetch and record the schedule details for given integrations."""
        result = WorkflowResult()
        for integration_id in schedule_integrations:
            try:
                schedule = self.client.integrations.get_schedule(integration_id=integration_id)
                integrations_original_status[integration_id]["SCHEDULE"] = schedule
                result.add_resource(self.RESOURCE_INTEGRATION, integration_id, {"action": "FETCH_SCHEDULE", "success": True, "schedule_state": schedule.get("state")})
            except OICAPIError as exc:
                msg = f"FAILED to fetch schedule: {exc.title}"
                self.logger.error("%s for %s", msg, integration_id)
                result.success = False
                result.add_error(resource_id=integration_id, message=exc.title, error=exc)
                result.add_resource(self.RESOURCE_INTEGRATION, integration_id, {"action": "FETCH_SCHEDULE", "success": False, "message": msg})
            except OICResourceNotFoundError as nf:
                # Not a fatal error: schedule might not exist
                self.logger.info("Schedule not found for %s: %s", integration_id, nf)
                result.add_resource(self.RESOURCE_INTEGRATION, integration_id, {"action": "FETCH_SCHEDULE", "success": True, "schedule_state": "ABSENT"})
        return result

    def stop_schedulers(
        self, schedule_integrations: Iterable[str], integrations_original_status: Dict[str, Dict[str, Any]]
    ) -> WorkflowResult:
        """Stop ACTIVE schedules for the provided integrations."""
        result = WorkflowResult()
        overall_success = True

        for integration_id in schedule_integrations:
            try:
                state = integrations_original_status.get(integration_id, {}).get("SCHEDULE", {}).get("state")
                if state == "ACTIVE":
                    params = {"asynch": True}
                    self.client.integrations.stop_schedule(integration_id=integration_id, params=params)
                    data = {"action": "STOP_SCHEDULE", "datetime": datetime.now().isoformat(), "success": True, "schedule_state": "STOPPED"}
                    result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
                else:
                    # Already not active; record as noop
                    data = {"action": "STOP_SCHEDULE", "datetime": datetime.now().isoformat(), "success": True, "schedule_state": state or "UNKNOWN"}
                    result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
            except OICAPIError as exc:
                # Consider 412 (precondition) as non-fatal; others fatal
                is_fatal = str(getattr(exc, "status_code", "")) != "412"
                msg = f"FAILED to STOP Schedule: {exc.title}"
                self.logger.error("%s for %s", msg, integration_id)
                overall_success = overall_success and (not is_fatal)
                result.add_error(resource_id=integration_id, message=exc.title, error=exc)
                data = {"action": "STOP_SCHEDULE", "datetime": datetime.now().isoformat(), "success": False, "message": msg}
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
            except OICResourceNotFoundError as nf:
                msg = f"Schedule not found: {nf}"
                self.logger.error("%s for %s", msg, integration_id)
                overall_success = False
                result.add_error(resource_id=integration_id, message=str(nf), error=nf)
                data = {"action": "STOP_SCHEDULE", "datetime": datetime.now().isoformat(), "success": False, "message": msg}
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
        result.success = overall_success
        return result

    def start_schedulers(
        self, schedule_integrations: Iterable[str], integrations_original_status: Dict[str, Dict[str, Any]]
    ) -> WorkflowResult:
        """Stop ACTIVE schedules for the provided integrations."""
        result = WorkflowResult()
        overall_success = True

        for integration_id in schedule_integrations:
            try:
                state = integrations_original_status.get(integration_id, {}).get("SCHEDULE", {}).get("state")
                if state == "ACTIVE": # Originally active
                    # Activate the Scheduler for the integration
                    params = {'async': 'true'}
                    self.client.integrations.start_schedule(integration_id=integration_id, data=None, params=params)

                    data = {"action": self.ACTION_START_SCHEDULE, "datetime": datetime.now().isoformat(), "success": True, "schedule_state": "202 - Asked",
                            "message":"Start submitted"}
                    result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
                else:
                    # Originally not active; do nothing
                    data = {"action": self.ACTION_START_SCHEDULE, "datetime": datetime.now().isoformat(), "success": False, "schedule_state": state or "UNKNOWN",
                            "message":f"Schedule was not originally active. Original state: {state}" }
                    result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
                    self.logger.warning("%s was not originally active. No scheduler started.", integration_id)
            except OICAPIError as exc:
                # Consider 412 (precondition) as non-fatal; others fatal
                is_fatal = str(getattr(exc, "status_code", "")) != "412"
                msg = f"FAILED to Start Schedule: {exc.title}"
                self.logger.error("%s for %s", msg, integration_id)
                overall_success = overall_success and (not is_fatal)
                result.add_error(resource_id=integration_id, message=exc.title, error=exc)
                schedule_state = "UNKNOWN" if is_fatal else "ACTIVE"
                data = {"action": self.ACTION_START_SCHEDULE, "datetime": datetime.now().isoformat(), "success": False, "schedule_state": schedule_state,
                        "message": msg}
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
            except OICResourceNotFoundError as nf:
                msg = f"Schedule not found: {nf}"
                self.logger.error("%s for %s", msg, integration_id)
                overall_success = False
                result.add_error(resource_id=integration_id, message=str(nf), error=nf)
                data = {"action": self.ACTION_START_SCHEDULE, "datetime": datetime.now().isoformat(), "success": False, "schedule_state": "UNKNOWN", "message": msg}
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
        result.success = overall_success
        return result

    def wait_for_schedules_state(
        self,
        schedule_integrations: Iterable[str],
        desired_state: str = "CANCELLED",
        timeout_sec: int = 300,
        poll_interval_sec: int = 5,
    ) -> WorkflowResult:
        """Poll schedules until all reach the desired state (default CANCELLED)."""
        result = WorkflowResult()
        start = time.time()
        ids = list(schedule_integrations)
        if not ids:
            result.success = True
            result.message = "No scheduled integrations to wait for"
            return result

        while True:
            all_done = True
            for integration_id in ids:
                try:
                    schedule = self.client.integrations.get_schedule(integration_id)
                    if schedule.get("state") != desired_state:
                        all_done = False
                        break
                except OICError as exc:
                    # Conservative: keep waiting if a transient error occurs
                    all_done = False
                    break

            if all_done:
                result.success = True
                for integration_id in ids:
                    result.add_resource(self.RESOURCE_INTEGRATION, integration_id, {"action": "WAIT_SCHEDULE", "success": True, "state": desired_state})
                return result

            if time.time() - start >= timeout_sec:
                result.success = False
                result.message = f"Timeout while waiting for schedules to reach {desired_state}"
                for integration_id in ids:
                    result.add_resource(self.RESOURCE_INTEGRATION, integration_id, {"action": "WAIT_SCHEDULE", "success": False, "state": f"!= {desired_state}"})
                return result

            time.sleep(poll_interval_sec)

    @staticmethod
    def collect_all_dependent_integrations(connections_dictionary: Dict[str, Dict[str, Any]]) -> List[str]:
        """Flatten and de-duplicate integrations from the connections dictionary."""
        all_ids: List[str] = []
        for info in connections_dictionary.values():
            all_ids.extend(info.get("integrations", []))
        return sorted(set(all_ids))

    def get_in_progress_integrations(self, integration_ids: Iterable[str]) -> WorkflowResult:
        """
        {timewindow:'1h',code:'PSSWRD_SUBSCRIBER_B',version:'01.00.0000',status: 'IN_PROGRESS'}
        """
        result = WorkflowResult()
        overall_success = True
        all_in_progress = 0
        # check integration running instances
        for integration_id in integration_ids:
            code, version = integration_id.split("|", 1)
            try:
                response = self.client.monitoring.get_integration_instances(timewindow='1h', integration_code=code, integration_version=version, status="IN_PROGRESS")
                total_records_count = response['totalRecordsCount']
                if total_records_count > 0:
                    all_in_progress += total_records_count
                    result.message = f"There are integration instances running (in progress): {all_in_progress}"
                    data = {
                            "action": self.ACTION_INTEGRATION_IN_PROCESS, "datetime": datetime.now().isoformat(),
                            "message": f"In progress instances: {len(total_records_count)}",
                            "success" : True
                            }
                    result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)

            except OICAPIError as exc:
                is_fatal = str(getattr(exc, "status_code", "")) != "412"  # 412 = not active
                if is_fatal:
                    overall_success = False
                    result.add_error(resource_id=integration_id, message=exc.title, error=exc)
                data = {"action": self.ACTION_INTEGRATION_IN_PROCESS, "datetime": datetime.now().isoformat(), "success": not is_fatal,
                        "message": exc.title}
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
            except OICResourceNotFoundError as nf:
                overall_success = False
                result.add_error(resource_id=integration_id, message=str(nf), error=nf)
                data = {"action": self.ACTION_INTEGRATION_IN_PROCESS, "datetime": datetime.now().isoformat(), "success": False,
                        f"message": f"404 - Not Found: code: {code}, version: {version}"}
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)

        result.success = overall_success
        return result

    def _are_in_progress(self, in_progress_wf_response: WorkflowResult) -> bool:
        return len(in_progress_wf_response.resources[self.RESOURCE_INTEGRATION]) > 0 if in_progress_wf_response.resources[self.RESOURCE_INTEGRATION] else False

    def deactivate_integrations(self, integration_ids: Iterable[str]) -> WorkflowResult:
        """Deactivate the provided integrations (ignore 412 Precondition)."""
        result = WorkflowResult()
        overall_success = True

        # Verify no integrations are running
        in_progress_wf = self.get_in_progress_integrations(integration_ids)
        if self._are_in_progress(in_progress_wf_response=in_progress_wf):
            result.message = f"Cannot deactivate integrations because there are some running at this time: {datetime.now().isoformat()}"
            result.merge(in_progress_wf)
            result.success = False
            return result

        # Go for it, shut tehm down!
        for integration_id in integration_ids:
            try:
                self.client.integrations.deactivate(
                    integration_id=integration_id, delete_event_subscription_flag=False
                )
                data = {"action": self.ACTION_DEACTIVATE, "datetime": datetime.now().isoformat(), "success": True}
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
            except OICAPIError as exc:
                is_fatal = str(getattr(exc, "status_code", "")) != "412"  # 412 = not active
                if is_fatal:
                    overall_success = False
                    result.add_error(resource_id=integration_id, message=exc.title, error=exc)
                data = {"action": self.ACTION_DEACTIVATE, "datetime": datetime.now().isoformat(), "success": not is_fatal, "message": exc.title}
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
            except OICResourceNotFoundError as nf:
                overall_success = False
                result.add_error(resource_id=integration_id, message=str(nf), error=nf)
                data = {"action": self.ACTION_DEACTIVATE, "datetime": datetime.now().isoformat(), "success": False, "message": "404 - Not Found"}
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
        result.success = overall_success
        return result

    def activate_integrations(self, integration_ids: Iterable[str]) -> WorkflowResult:
        """Activate the provided integrations (ignore 412 Precondition)."""
        result = WorkflowResult()
        overall_success = True

        for integration_id in integration_ids:
            try:
                response = self.client.integrations.activate(
                    integration_id=integration_id
                )
                status = response['status'] if response['status'] else 'unknown'
                data = {"action": self.ACTION_ACTIVATE, "datetime": datetime.now().isoformat(), "success": True, "status": response['status'] }
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
            except OICAPIError as exc:
                is_fatal = str(getattr(exc, "status_code", "")) != "412"  # 412 = not active
                if is_fatal:
                    result.add_error(resource_id=integration_id, message=exc.title, error=exc)
                data = {"action": self.ACTION_ACTIVATE, "datetime": datetime.now().isoformat(), "success": not is_fatal, "message": exc.title, "status": ""}
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
                self.logger.warning("Activating %s has a fatal (%s) exception: %s", integration_id, is_fatal, exc.title)
            except OICResourceNotFoundError as nf:
                overall_success = False
                result.add_error(resource_id=integration_id, message=str(nf), error=nf)
                data = {"action": self.ACTION_ACTIVATE, "datetime": datetime.now().isoformat(), "success": False, "message": "404 - Not Found", "status": ""}
                result.add_resource(resource_type=self.RESOURCE_INTEGRATION, resource_id=integration_id, data=data)
        result.success = overall_success
        return result


    def update_connection_passwords(self, connections_dictionary: Dict[str, Dict[str, Any]], target_usernames: Dict[str, str]) -> WorkflowResult:
        """Patch username/password on each connection and verify CONFIGURED status."""
        result = WorkflowResult()
        overall_success = True

        for connection_id, info in connections_dictionary.items():
            username = info.get("username")
            if not username:
                self.logger.warning("Skipping %s: missing username in cross-ref", connection_id)
                continue
            new_password = target_usernames.get(username)
            if new_password is None:
                self.logger.warning("No target password provided for username '%s'; skipping %s", username, connection_id)
                continue

            try:
                params = {"patchAttachments": True}
                data = {
                    "securityProperties": [
                        {"propertyName": "username", "propertyValue": username, "requiredFlag": True},
                        {"propertyName": "password", "propertyValue": new_password, "requiredFlag": True},
                    ]
                }
                response = self.client.connections.update(connection_id=connection_id, params=params, data=data)
                configured = response.get("status") == "CONFIGURED"
                if configured:
                    result.add_resource("connection", connection_id, {"status": "password successfully updated"})
                    self.logger.info("Updated password for connection %s", connection_id)
                else:
                    overall_success = False
                    msg = "Password updated, but connection did not reach CONFIGURED status"
                    result.add_error(resource_id=connection_id, message=msg)
                    result.add_resource("connection", connection_id, {"status": "Error", "message": msg})
                    self.logger.error("%s for %s", msg, connection_id)
            except OICAPIError as exc:
                overall_success = False
                msg = f"{connection_id}/{username} password update FAILED: {exc.title}"
                self.logger.error(msg)
                result.add_error(resource_id=connection_id, message=exc.title, error=exc)
                result.add_resource("connection", connection_id, {"status": "Error", "message": msg})

        result.success = overall_success
        return result

    def save_result(self, wf: WorkflowResult):
        save_dir = self.client.config.save_results_directory

        # Ensure directory exists
        os.makedirs(save_dir, exist_ok=True)

        filename = os.path.join(save_dir, "password_rotation_result.json")
        wf.save_to_file(filename)
