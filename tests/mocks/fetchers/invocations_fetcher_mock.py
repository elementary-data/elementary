from elementary.monitor.fetchers.invocations.invocations import InvocationsFetcher
from tests.mocks.dbt_runner_mock import MockDbtRunner


class MockInvocationsFetcher(InvocationsFetcher):
    def __init__(self):
        mock_dbt_runner = MockDbtRunner()
        super().__init__(mock_dbt_runner)
