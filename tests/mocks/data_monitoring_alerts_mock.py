from typing import Optional

from elementary.config.config import Config
from elementary.monitor.data_monitoring.data_monitoring_alerts import DataMonitoringAlerts
from elementary.tracking.anonymous_tracking import AnonymousTracking
from tests.mocks.api.alerts_api_mock import MockAlertsAPIReadOnly


class DataMonitoringAlertsMock(DataMonitoringAlerts):
    def __init__(
        self,
        config: Config,
        tracking: AnonymousTracking,
        filter: Optional[str] = None,
        force_update_dbt_package: bool = False,
        disable_samples: bool = False,
        send_test_message_on_success: bool = False,
    ):
        super().__init__(
            config, tracking, force_update_dbt_package, disable_samples, filter
        )

        self.alerts_api = MockAlertsAPIReadOnly(
            self.internal_dbt_runner,
            self.config,
            self.elementary_database_and_schema,
        )
        self.sent_alert_count = 0
        self.send_test_message_on_success = send_test_message_on_success
