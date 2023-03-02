import json
from typing import List, Optional

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.monitor.fetchers.tests.schema import TestResultDBRowSchema
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class TestsFetcher(FetcherClient):
    def __init__(self, dbt_runner: DbtRunner):
        super().__init__(dbt_runner)

    def get_all_test_results_db_rows(
        self,
        days_back: Optional[int] = 7,
        invocations_per_test: int = 720,
        metrics_sample_limit: int = 5,
        disable_passed_test_metrics: bool = False,
    ) -> List[TestResultDBRowSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="get_test_results",
            macro_args=dict(
                days_back=days_back,
                invocations_per_test=invocations_per_test,
                metrics_sample_limit=metrics_sample_limit,
                disable_passed_test_metrics=disable_passed_test_metrics,
            ),
        )
        test_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        test_results = [
            TestResultDBRowSchema(**test_result) for test_result in test_results
        ]
        return test_results
