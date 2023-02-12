from elementary.monitor.fetchers.tests.tests import TestsFetcher
from tests.mocks.dbt_runner_mock import MockDbtRunner


class MockTestsFetcher(TestsFetcher):
    def __init__(self):
        mock_dbt_runner = MockDbtRunner()
        super().__init__(mock_dbt_runner)
