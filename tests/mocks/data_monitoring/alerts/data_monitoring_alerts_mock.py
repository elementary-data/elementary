from elementary.config.config import Config
from elementary.monitor.data_monitoring.alerts.data_monitoring_alerts import (
    DataMonitoringAlerts,
)
from elementary.monitor.data_monitoring.schema import SelectorFilterSchema
from tests.mocks.api.alerts_api_mock import MockAlertsAPI
from tests.mocks.dbt_runner_mock import MockDbtRunner


class DataMonitoringAlertsMock(DataMonitoringAlerts):
    def __init__(
        self,
        selector_filter: SelectorFilterSchema = SelectorFilterSchema(),
        force_update_dbt_package: bool = False,
        disable_samples: bool = False,
        send_test_message_on_success: bool = False,
        global_suppression_interval: int = 0,
        override_config: bool = False,
    ):
        config = Config(slack_token="mock", slack_channel_name="mock")
        tracking = None
        super().__init__(
            config,
            tracking,
            selector_filter,
            force_update_dbt_package,
            disable_samples,
            send_test_message_on_success,
            global_suppression_interval,
            override_config,
        )
        self.alerts_api = MockAlertsAPI()

    def _init_internal_dbt_runner(self):
        return MockDbtRunner()

    def _download_dbt_package_if_needed(self, *args, **kwargs):
        pass

    def get_latest_invocation(self):
        return dict()

    def _get_warehouse_info(self, *args, **kwargs):
        return None

    def get_elementary_database_and_schema(self):
        return "<elementary_database>.<elementary_schema>"
