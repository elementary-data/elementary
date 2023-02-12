from elementary.monitor.fetchers.selector.selector import SelectorFetcher
from tests.mocks.dbt_runner_mock import MockDbtRunner


class MockSelectorFetcher(SelectorFetcher):
    def __init__(self):
        mock_dbt_runner = MockDbtRunner()
        super().__init__(mock_dbt_runner)
