import json
from typing import Any, Dict, Optional, cast

from packaging import version

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.clients.slack.client import SlackClient
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils
from elementary.monitor.data_monitoring.schema import WarehouseInfo
from elementary.monitor.data_monitoring.selector_filter import SelectorFilter
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
        filter: Optional[str] = None,
    ):
        self.execution_properties: Dict[str, Any] = {}
        self.config = config
        self.tracking = tracking
        self.internal_dbt_runner = self._init_internal_dbt_runner()
        self.user_dbt_runner = self._init_user_dbt_runner()
        self._download_dbt_package_if_needed(force_update_dbt_package)
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
        # slack client is optional
        self.slack_client = SlackClient.create_client(
            self.config, tracking=self.tracking
        )
        self.elementary_database_and_schema = self.get_elementary_database_and_schema()
        self.success = True
        self.disable_samples = disable_samples
        self.raw_filter = filter
        self.filter = SelectorFilter(
            tracking=tracking,
            user_dbt_runner=self.user_dbt_runner,
            selector=self.raw_filter,
        )

    def _init_internal_dbt_runner(self):
        internal_dbt_runner = DbtRunner(
            dbt_project_utils.PATH,
            self.config.profiles_dir,
            self.config.profile_target,
            env_vars=self.config.env_vars,
        )
        return internal_dbt_runner

    def _init_user_dbt_runner(self):
        if self.config.project_dir:
            user_dbt_runner = DbtRunner(
                self.config.project_dir,
                self.config.profiles_dir,
                self.config.project_profile_target,
                env_vars=self.config.env_vars,
            )
        else:
            user_dbt_runner = None
        return user_dbt_runner

    def _download_dbt_package_if_needed(self, force_update_dbt_packages: bool):
        internal_dbt_package_up_to_date = dbt_project_utils.is_dbt_package_up_to_date()
        self.execution_properties[
            "dbt_package_up_to_date"
        ] = internal_dbt_package_up_to_date
        self.execution_properties[
            "force_update_dbt_packages"
        ] = force_update_dbt_packages
        if not internal_dbt_package_up_to_date or force_update_dbt_packages:
            logger.info("Downloading edr internal dbt package")
            package_downloaded = self.internal_dbt_runner.deps()
            self.execution_properties["package_downloaded"] = package_downloaded
            if not package_downloaded:
                logger.error("Could not download internal dbt package")
                self.success = False
                return

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
