import json
from typing import List, Optional

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.monitor.fetchers.tests.schema import (
    NormalizedTestSchema,
    TestDBRowSchema,
    TestResultDBRowSchema,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class TestsFetcher(FetcherClient):
    def __init__(self, dbt_runner: BaseDbtRunner):
        super().__init__(dbt_runner)

    def get_all_test_results_db_rows(
        self,
        days_back: Optional[int] = 7,
        invocations_per_test: int = 720,
        disable_passed_test_metrics: bool = False,
    ) -> List[TestResultDBRowSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_test_results",
            macro_args=dict(
                days_back=days_back,
                invocations_per_test=invocations_per_test,
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

    def get_singular_tests(self) -> List[NormalizedTestSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_singular_tests"
        )
        test_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        test_results = [
            NormalizedTestSchema(
                unique_id=test_result["unique_id"],
                model_name=test_result["name"],
                normalized_full_path=f"{test_result['package_name']}/{test_result['original_path']}",
                tags=test_result["tags"],
            )
            for test_result in test_results
        ]
        return test_results

    def get_tests(self) -> List[TestDBRowSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_tests"
        )
        return [
            TestDBRowSchema(**test) for test in json.loads(run_operation_response[0])
        ]
