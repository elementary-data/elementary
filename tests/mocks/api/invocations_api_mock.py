from elementary.monitor.api.invocations.invocations import InvocationsAPI
from tests.mocks.dbt_runner_mock import MockDbtRunner
from tests.mocks.fetchers.invocations_fetcher_mock import MockInvocationsFetcher


class MockInvocationsAPI(InvocationsAPI):
    def __init__(self, *args, **kwargs):
        mock_dbt_runner = MockDbtRunner()
        super().__init__(mock_dbt_runner)
        self.invocations_fetcher = MockInvocationsFetcher()
