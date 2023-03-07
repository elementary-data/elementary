from collections import defaultdict
from typing import Dict, Set, List, Union
from elementary.config.config import Config
from elementary.monitor.alerts.alerts import AlertType
from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.api.alerts.alerts import AlertsAPI
from tests.mocks.dbt_runner_mock import MockDbtRunner
from tests.mocks.fetchers.alerts_fetcher_mock import MockAlertsFetcher


class MockAlertsAPI(AlertsAPI):
    def __init__(self, *args, **kwargs):
        mock_dbt_runner = MockDbtRunner()
        config = Config()
        super().__init__(
            mock_dbt_runner, config, elementary_database_and_schema="test.test"
        )
        self.alerts_fetcher = MockAlertsFetcher()

class MockAlertsAPIReadOnly(AlertsAPI):
    def __init__(self, *args, **kwargs):
        self.sent_alerts: Dict[str, Set[str]] = defaultdict(set)  # map from table_name to alert IDs
        self.skipped_alerts: Dict[str, Set[str]] = defaultdict(set)  # map from table_name to alert IDs
        super().__init__(*args, **kwargs)

    def update_sent_alerts(self, alert_ids: List[str], table_name: str) -> None:
        self.sent_alerts[table_name].update(alert_ids)

    def skip_alerts(
        self, alerts_to_skip: List[Union[AlertType, MalformedAlert]], table_name: str
    ) -> None:
        self.skipped_alerts[table_name].update([alert.id for alert in alerts_to_skip])
