import hashlib
import json
import logging
import uuid
from pathlib import Path
from typing import Optional

import posthog
from pydantic import BaseModel

import elementary.tracking.env
from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils

logging.getLogger("posthog").disabled = True


class AnonymousWarehouse(BaseModel):
    id: str
    type: str


class AnonymousTracking:
    ANONYMOUS_USER_ID_FILE = ".user_id"
    POSTHOG_PROJECT_API_KEY = "phc_56XBEzZmh02mGkadqLiYW51eECyYKWPyecVwkGdGUfg"

    def __init__(self, config: Config) -> None:
        self._env_props = {}
        self.anonymous_user_id = None
        self.anonymous_warehouse = None
        self.config = config
        self.do_not_track = config.anonymous_tracking_enabled is False
        self.run_id = str(uuid.uuid4())
        self.init()

    def init(self):
        try:
            posthog.project_api_key = self.POSTHOG_PROJECT_API_KEY
            self._env_props.update(elementary.tracking.env.get_props())
            self.anonymous_user_id = self._get_anonymous_user_id()
            self.anonymous_warehouse = self._get_anonymous_warehouse()
        except Exception:
            pass

    def _get_anonymous_user_id(self):
        legacy_user_id_path = Path().joinpath(
            self.config.profiles_dir, self.ANONYMOUS_USER_ID_FILE
        )
        user_id_path = Path().joinpath(
            self.config.config_dir, self.ANONYMOUS_USER_ID_FILE
        )
        # First check legacy file path
        try:
            return legacy_user_id_path.read_text()
        except OSError:
            pass
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

    def send_event(self, name: str, properties: dict = None) -> None:
        try:
            if self.do_not_track:
                return

            if properties is None:
                properties = dict()

            posthog.capture(
                distinct_id=self.anonymous_user_id,
                event=name,
                properties={
                    "run_id": self.run_id,
                    "env": self._env_props,
                    **properties,
                },
                groups={
                    "warehouse": self.anonymous_warehouse.id
                    if self.anonymous_warehouse
                    else None
                },
            )
        except Exception:
            pass

    def track_cli_start(
        self, module_name: str, cli_properties: dict, command: str = None
    ):
        props = {
            "cli_properties": cli_properties,
            "module_name": module_name,
            "command": command,
        }
        self.send_event("cli-start", properties=props)

    def track_cli_end(
        self, module_name: str, execution_properties: dict, command: str = None
    ):
        props = {
            "execution_properties": execution_properties,
            "module_name": module_name,
            "command": command,
        }
        self.send_event("cli-end", properties=props)

    def track_cli_exception(
        self, module_name: str, exc: Exception, command: str = None
    ) -> None:
        props = {
            "exception_type": str(type(exc)),
            "module_name": module_name,
            "command": command,
        }
        self.send_event("cli-exception", properties=props)

    def track_cli_help(self):
        self.send_event("cli-help")

    def _get_anonymous_warehouse(self) -> Optional[AnonymousWarehouse]:
        try:
            dbt_runner = DbtRunner(
                dbt_project_utils.PATH,
                self.config.profiles_dir,
                self.config.profile_target,
            )
            if not dbt_project_utils.dbt_package_exists():
                dbt_runner.deps(quiet=True)

            adapter_type, adapter_unique_id = json.loads(
                dbt_runner.run_operation("get_adapter_type_and_unique_id", quiet=True)[
                    0
                ]
            )
            anonymous_warehouse_id = hashlib.sha256(
                adapter_unique_id.encode("utf-8")
            ).hexdigest()
            posthog.group_identify(
                "warehouse",
                anonymous_warehouse_id,
                {"id": anonymous_warehouse_id, "type": adapter_type},
            )
            return AnonymousWarehouse(id=anonymous_warehouse_id, type=adapter_type)
        except Exception:
            return None

    def set_env(self, key: str, value):
        self._env_props[key] = value
