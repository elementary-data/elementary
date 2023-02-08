from elementary.monitor.api.selector.selector import SelectorAPI
from tests.mocks.dbt_runner_mock import MockDbtRunner


class MockSelectorAPI(SelectorAPI):
    def __init__(self):
        mock_dbt_runner = MockDbtRunner()
        super().__init__(mock_dbt_runner)
