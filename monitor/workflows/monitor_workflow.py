from click import Context
from typing import Optional
from monitor.workflows.base_workflow import BaseWorkflow


class MonitorWorkflow(BaseWorkflow):
    def __init__(
        self,
        module_name: str,
        click_context: Context,
        config_dir: str,
        profiles_dir: str,
        profile_target: str,
        days_back: Optional[int] = None,
        full_refresh_dbt_package: Optional[bool] = None,
        slack_webhook: Optional[str] = None,
        slack_token: Optional[str] = None,
        slack_channel_name: Optional[str] = None,
        update_dbt_package: Optional[bool] = None
    ) -> None:
        super().__init__(
            module_name=module_name,
            click_context=click_context,
            config_dir=config_dir,
            profiles_dir=profiles_dir,
            profile_target=profile_target,
            slack_webhook=slack_webhook,
            slack_token=slack_token,
            slack_channel_name=slack_channel_name,
            update_dbt_package=update_dbt_package
        )
        self.days_back = days_back
        self.full_refresh_dbt_package = full_refresh_dbt_package
    
    def execute(self) -> None:
        self.data_monitoring.run(
            days_back=self.days_back,
            dbt_full_refresh=self.full_refresh_dbt_package
        )
