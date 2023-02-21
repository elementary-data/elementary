from elementary.config.config import Config
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
