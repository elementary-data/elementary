from abc import ABC, abstractmethod
from click import Context
from typing import Optional

from config.config import Config
from tracking.anonymous_tracking import AnonymousTracking, track_cli_start, track_cli_exception, track_cli_end
from utils.package import get_package_version
from monitor.data_monitoring import DataMonitoring


class BaseWorkflow(ABC):
    def __init__(
        self,
        module_name: str,
        click_context: Context,
        config_dir: str,
        profiles_dir: str,
        profile_target: str,
        slack_webhook: Optional[str] = None,
        slack_token: Optional[str] = None,
        slack_channel_name: Optional[str] = None,
        update_dbt_package: Optional[bool] = None
    ) -> None:
        self.module_name = module_name
        self.config_dir = config_dir
        self.profiles_dir = profiles_dir
        self.profile_target = profile_target
        self.slack_webhook = slack_webhook
        self.slack_token = slack_token
        self.slack_channel_name = slack_channel_name
        self.update_dbt_package = update_dbt_package
        self.click_context = click_context
        self.cli_params = self.click_context.params if self.click_context.params else dict()
        self.command = self.click_context.command
        self.config = self._get_config()
        self.anonymous_tracking = self._initial_anonymous_tracking()
        self.data_monitoring = self._initial_data_monitoring()

    def run(self) -> None:
        try: 
            track_cli_start(
                anonymous_tracking=self.anonymous_tracking,
                module_name=self.module_name,
                cli_properties=self._get_cli_properties(),
                command=self.command.name
            )
            self.execute()
            track_cli_end(
                anonymous_tracking=self.anonymous_tracking,
                module_name=self.module_name,
                execution_properties=self._get_cli_properties(),
                command=self.command.name
            )
        except Exception as exc:
            track_cli_exception(
                anonymous_tracking=self.anonymous_tracking,
                module_name=self.module_name,
                exc=exc,
                command=self.command.name
            )
            raise

    @abstractmethod
    def execute(self) -> None:
        raise NotImplementedError

    def _get_cli_properties(self) -> dict:
        params = self.click_context.params if self.click_context.params else dict()

        reload_monitoring_configuration = params.get("reload_monitoring_configuration")
        update_dbt_package = params.get("update_dbt_package")
        full_refresh_dbt_package = params.get("full_refresh_dbt_package")

        return {
            "reload_monitoring_configuration": reload_monitoring_configuration,
            "update_dbt_package": update_dbt_package,
            "full_refresh_dbt_package": full_refresh_dbt_package,
            "version": get_package_version()
        }

    def _get_config(self) -> Config:
        return Config(
            config_dir=self.config_dir,
            profiles_dir=self.profiles_dir,
            profile_target=self.profile_target,
        )

    def _initial_anonymous_tracking(self) -> AnonymousTracking:
        return AnonymousTracking(self.config)

    def _initial_data_monitoring(self) -> DataMonitoring:
        return DataMonitoring(
            config=self.config,
            force_update_dbt_package=self.update_dbt_package or self.cli_params.get('update_dbt_package'),
            slack_webhook=self.slack_webhook or self.cli_params.get('slack_webhook'),
            slack_token=self.slack_token or self.cli_params.get('slack_token'),
            slack_channel_name=self.slack_channel_name or self.cli_params.get('slack_channel_name')
        )
