from typing import List

from elementary.monitor.api.tests.tests import TestsAPI
from elementary.monitor.fetchers.tests.schema import TestResultDBRowSchema
from tests.mocks.api.invocations_api_mock import MockInvocationsAPI
from tests.mocks.dbt_runner_mock import MockDbtRunner
from tests.mocks.fetchers.tests_fetcher_mock import MockTestsFetcher


class MockTestsAPI(TestsAPI):
    def __init__(self, *args, **kwargs):
        self.dbt_runner = MockDbtRunner
        self.tests_fetcher = MockTestsFetcher()
        self.invocations_api = MockInvocationsAPI()
        self.test_results_db_rows = self._get_test_results_db_rows()

    def _get_test_results_db_rows(self, *args, **kwargs) -> List[TestResultDBRowSchema]:
        return self.tests_fetcher.get_all_test_results_db_rows()
