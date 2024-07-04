import json
from typing import Any, Dict, Optional, cast

from packaging import version

from elementary.clients.dbt.factory import create_dbt_runner
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils
from elementary.monitor.data_monitoring.schema import FiltersSchema, WarehouseInfo
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.tracking.tracking_interface import Tracking
from elementary.utils import package
from elementary.utils.hash import hash
from elementary.utils.log import get_logger

logger = get_logger(__name__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class DataMonitoring:
    def __init__(
        self,
        config: Config,
        tracking: Optional[Tracking] = None,
        force_update_dbt_package: bool = False,
        disable_samples: bool = False,
        selector_filter: FiltersSchema = FiltersSchema(),
    ):
        self.execution_properties: Dict[str, Any] = {}
        self.config = config
        self.tracking = tracking
        self.force_update_dbt_package = force_update_dbt_package
        self.internal_dbt_runner = self._init_internal_dbt_runner()
        latest_invocation = self.get_latest_invocation()
        self.project_name = latest_invocation.get("project_name")
        dbt_pkg_version = latest_invocation.get("elementary_version")
        self.warehouse_info = self._get_warehouse_info(
            hash_id=isinstance(tracking, AnonymousTracking)
        )
        if tracking:
            if self.warehouse_info:
                tracking.register_group(
                    "warehouse",
                    self.warehouse_info.id,
                    self.warehouse_info.dict(),
                )
            tracking.set_env("target_name", latest_invocation.get("target_name"))
            tracking.set_env("dbt_orchestrator", latest_invocation.get("orchestrator"))
            tracking.set_env("dbt_version", latest_invocation.get("dbt_version"))
            tracking.set_env("dbt_pkg_version", dbt_pkg_version)
        if dbt_pkg_version:
            self._check_dbt_package_compatibility(dbt_pkg_version)
        self.elementary_database_and_schema = self.get_elementary_database_and_schema()
        self.success = True
        self.disable_samples = disable_samples
        self.selector_filter = selector_filter

    def _init_internal_dbt_runner(self):
        internal_dbt_runner = create_dbt_runner(
            dbt_project_utils.CLI_DBT_PROJECT_PATH,
            self.config.profiles_dir,
            self.config.profile_target,
            env_vars=self.config.env_vars,
            run_deps_if_needed=self.config.run_dbt_deps_if_needed,
            force_dbt_deps=self.force_update_dbt_package,
        )
        return internal_dbt_runner

    def properties(self):
        data_monitoring_properties = {
            "data_monitoring_properties": self.execution_properties
        }
        return data_monitoring_properties

    def get_elementary_database_and_schema(self):
        try:
            relation = self.internal_dbt_runner.run_operation(
                "elementary_cli.get_elementary_database_and_schema", quiet=True
            )[0]
            logger.info(f"Elementary's database and schema: '{relation}'")
            return relation
        except Exception as ex:
            logger.error("Failed to parse Elementary's database and schema.")
            if self.tracking:
                self.tracking.record_internal_exception(ex)
            return "<elementary_database>.<elementary_schema>"

    def get_latest_invocation(self) -> Dict[str, Any]:
        try:
            latest_invocation = self.internal_dbt_runner.run_operation(
                "elementary_cli.get_latest_invocation", quiet=True
            )[0]
            return json.loads(latest_invocation)[0] if latest_invocation else {}
        except Exception as err:
            logger.error(f"Unable to get the latest invocation: {err}")
            if self.tracking:
                self.tracking.record_internal_exception(err)
            return {}

    @staticmethod
    def _check_dbt_package_compatibility(dbt_pkg_ver_str: str) -> None:
        py_pkg_ver_str = package.get_package_version()
        if py_pkg_ver_str is None:
            logger.warning("Could not get package version!")
            return

        dbt_pkg_ver = cast(version.Version, version.parse(dbt_pkg_ver_str))
        py_pkg_ver = cast(version.Version, version.parse(py_pkg_ver_str))
        if dbt_pkg_ver.major > py_pkg_ver.major or (
            dbt_pkg_ver.major == py_pkg_ver.major
            and dbt_pkg_ver.minor > py_pkg_ver.minor
        ):
            logger.warning(
                f"You are using incompatible versions between edr ({py_pkg_ver}) and Elementary's dbt package ({dbt_pkg_ver}).\n "
                "To fix please run:\n"
                "pip install --upgrade elementary-data\n",
            )
            return

        if dbt_pkg_ver.major < py_pkg_ver.major or (
            dbt_pkg_ver.major == py_pkg_ver.major
            and dbt_pkg_ver.minor < py_pkg_ver.minor
        ):
            logger.warning(
                f"You are using incompatible versions between edr ({py_pkg_ver}) and Elementary's dbt package ({dbt_pkg_ver}).\n "
                "To fix please update your packages.yml, and run:\n"
                "dbt deps && dbt run --select elementary\n",
            )
            return

        logger.info(
            f"edr ({py_pkg_ver}) and Elementary's dbt package ({dbt_pkg_ver}) are compatible."
        )

    def _get_warehouse_info(self, hash_id: bool = False) -> Optional[WarehouseInfo]:
        try:
            warehouse_type, warehouse_unique_id = json.loads(
                self.internal_dbt_runner.run_operation(
                    "elementary_cli.get_adapter_type_and_unique_id", quiet=True
                )[0]
            )
            return WarehouseInfo(
                id=warehouse_unique_id if not hash_id else hash(warehouse_unique_id),
                type=warehouse_type,
            )
        except Exception:
            logger.debug("Could not get warehouse info.", exc_info=True)
            return None
