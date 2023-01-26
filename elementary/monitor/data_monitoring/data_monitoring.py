import json
from typing import Any, Dict, Optional

from packaging import version

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.clients.slack.client import SlackClient
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.utils import package
from elementary.utils.log import get_logger

logger = get_logger(__name__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class DataMonitoring:
    def __init__(
        self,
        config: Config,
        tracking: AnonymousTracking,
        force_update_dbt_package: bool = False,
        disable_samples: bool = False,
        filter: Optional[str] = None,
    ):
        self.config = config
        self.tracking = tracking
        self.internal_dbt_runner = DbtRunner(
            dbt_project_utils.PATH,
            self.config.profiles_dir,
            self.config.profile_target,
            dbt_env_vars=self.config.dbt_env_vars,
        )
        if self.config.project_dir:
            self.user_dbt_runner = DbtRunner(
                self.config.project_dir,
                self.config.profiles_dir,
                self.config.project_profile_target,
                dbt_env_vars=self.config.dbt_env_vars,
            )
        else:
            self.user_dbt_runner = None
        if self.user_dbt_runner:
            self.run_elementary_models()
        self.execution_properties = {}
        latest_invocation = self.get_latest_invocation()
        self.project_name = latest_invocation.get("project_name")
        tracking.set_env("target_name", latest_invocation.get("target_name"))
        tracking.set_env("dbt_orchestrator", latest_invocation.get("orchestrator"))
        tracking.set_env("dbt_version", latest_invocation.get("dbt_version"))
        dbt_pkg_version = latest_invocation.get("elementary_version")
        tracking.set_env("dbt_pkg_version", dbt_pkg_version)
        if dbt_pkg_version:
            self._check_dbt_package_compatibility(dbt_pkg_version)
        # slack client is optional
        self.slack_client = SlackClient.create_client(
            self.config, tracking=self.tracking
        )
        self._download_dbt_package_if_needed(force_update_dbt_package)
        self.elementary_database_and_schema = self.get_elementary_database_and_schema()
        self.success = True
        self.disable_samples = disable_samples
        self.raw_filter = filter

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
            return self.internal_dbt_runner.run_operation(
                "get_elementary_database_and_schema", quiet=True
            )[0]
        except Exception as ex:
            logger.error("Failed to parse Elementary's database and schema.")
            self.tracking.record_cli_internal_exception(ex)
            return "<elementary_database>.<elementary_schema>"

    def get_latest_invocation(self) -> Dict[str, Any]:
        try:
            latest_invocation = self.internal_dbt_runner.run_operation(
                "get_latest_invocation", quiet=True
            )[0]
            return json.loads(latest_invocation)[0] if latest_invocation else {}
        except Exception as err:
            logger.error(f"Unable to get the latest invocation: {err}")
            self.tracking.record_cli_internal_exception(err)
            return {}

    @staticmethod
    def _check_dbt_package_compatibility(dbt_pkg_ver: str):
        dbt_pkg_ver = version.parse(dbt_pkg_ver)
        py_pkg_ver = version.parse(package.get_package_version())
        logger.info(
            f"Checking compatibility between edr ({py_pkg_ver}) and Elementary's dbt package ({dbt_pkg_ver})."
        )
        if (
            dbt_pkg_ver.major != py_pkg_ver.major
            or dbt_pkg_ver.minor != py_pkg_ver.minor
        ):
            logger.warning(
                f"You are using incompatible versions between edr ({py_pkg_ver}) and Elementary's dbt package ({dbt_pkg_ver}).\n "
                "Please upgrade the major and minor versions to align.\n",
            )

    def run_elementary_models(self):
        logger.info("Syncing with dbt project to its latest state.")
        self.user_dbt_runner.run("elementary")
