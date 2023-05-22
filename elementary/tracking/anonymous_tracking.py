import hashlib
import json
import logging
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

import elementary.exceptions.exceptions
import elementary.tracking.runner
from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logging.getLogger("posthog").disabled = True
logger = get_logger(__name__)


class AnonymousWarehouse(BaseModel):
    id: str
    type: str


class AnonymousTracking(Tracking):
    _ANONYMOUS_USER_ID_FILE = ".user_id"
    _INTERNAL_EXCEPTIONS_LIMIT = 5

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self.anonymous_user_id = None
        self.anonymous_warehouse = None
        self._do_not_track = config.anonymous_tracking_enabled is False
        self._run_id = str(uuid.uuid4())

        # Exceptions that occurred during the run of the CLI, but don't fail the entire run.
        # We want to avoid sending an event for each one of these (as there might be many of them), so we will send
        # them as a part of the cli-end event.
        self.internal_exceptions: List[dict] = []
        self.internal_exceptions_count = 0

        if not self._do_not_track:
            self._init()

    def _init(self):
        try:
            self._props["env"] = elementary.tracking.runner.get_props()
            self.anonymous_user_id = self._get_anonymous_user_id()
            self.anonymous_warehouse = self._get_anonymous_warehouse()
        except Exception:
            logger.debug("Unable to initialize anonymous tracking.", exc_info=True)

    def _get_anonymous_user_id(self):
        user_id_path = Path().joinpath(
            self._config.config_dir, self._ANONYMOUS_USER_ID_FILE
        )
        try:
            return user_id_path.read_text()
        except OSError:
            pass
        user_id = str(uuid.uuid4())
        try:
            user_id_path.write_text(user_id)
        except OSError:
            pass
        return user_id

    def _send_anonymous_event(
        self, name: str, properties: Optional[dict] = None
    ) -> None:
        try:
            if self._do_not_track or self.anonymous_user_id is None:
                return

            if properties is None:
                properties = dict()

            self._send_event(
                distinct_id=self.anonymous_user_id,
                event_name=name,
                properties={
                    "run_id": self._run_id,
                    **self._props,
                    **properties,
                },
                groups={
                    "warehouse": self.anonymous_warehouse
                    and self.anonymous_warehouse.id
                },
            )
        except Exception:
            logger.debug("Unable to send tracking event.", exc_info=True)

    def record_internal_exception(self, exc: Exception):
        self.internal_exceptions_count += 1
        if len(self.internal_exceptions) < self._INTERNAL_EXCEPTIONS_LIMIT:
            self.internal_exceptions.append(self._get_exception_properties(exc))

    @staticmethod
    def _get_exception_properties(exc: Exception):
        props = {"exception_type": str(type(exc))}
        if isinstance(exc, elementary.exceptions.exceptions.Error):
            props.update(exc.anonymous_tracking_context)
        return props

    def _get_anonymous_warehouse(self) -> Optional[AnonymousWarehouse]:
        try:
            dbt_runner = DbtRunner(
                dbt_project_utils.PATH,
                self._config.profiles_dir,
                self._config.profile_target,
                dbt_env_vars=self._config.dbt_env_vars,
            )
            if not dbt_project_utils.is_dbt_package_up_to_date():
                logger.info("Downloading edr internal dbt package")
                dbt_runner.deps(quiet=True)

            adapter_type, adapter_unique_id = json.loads(
                dbt_runner.run_operation("get_adapter_type_and_unique_id", quiet=True)[
                    0
                ]
            )
            anonymous_warehouse_id = hashlib.sha256(
                adapter_unique_id.encode("utf-8")
            ).hexdigest()
            self._set_events_group(
                "warehouse",
                anonymous_warehouse_id,
                {"id": anonymous_warehouse_id, "type": adapter_type},
            )
            return AnonymousWarehouse(id=anonymous_warehouse_id, type=adapter_type)
        except Exception:
            return None


class AnonymousCommandLineTracking(AnonymousTracking):
    def track_cli_start(
        self,
        module_name: str,
        cli_properties: Optional[dict] = None,
        command: Optional[str] = None,
    ):
        props = {
            "cli_properties": cli_properties,
            "module_name": module_name,
            "command": command,
            "edr_env": self._config.env,
            "has_project_dir": bool(self._config.project_dir),
        }
        self._send_anonymous_event("cli-start", properties=props)

    def track_cli_end(
        self,
        module_name: str,
        execution_properties: Optional[dict],
        command: Optional[str] = None,
    ):
        props: Dict[str, Any] = {
            "execution_properties": execution_properties,
            "module_name": module_name,
            "command": command,
        }
        if self.internal_exceptions_count > 0:
            props["internal_exceptions"] = self.internal_exceptions
            props["internal_exceptions_count"] = self.internal_exceptions_count
        self._send_anonymous_event("cli-end", properties=props)

    def track_cli_exception(
        self, module_name: str, exc: Exception, command: Optional[str] = None
    ) -> None:
        props = {
            "module_name": module_name,
            "command": command,
        }
        props.update(self._get_exception_properties(exc))

        self._send_anonymous_event("cli-exception", properties=props)

    def track_cli_help(self):
        self._send_anonymous_event("cli-help")
