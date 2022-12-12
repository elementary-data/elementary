import json
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional

from elementary.clients.api.api import APIClient
from elementary.monitor.api.tests.schema import (
    InvocationSchema,
    InvocationsSchema,
    TestMetadataSchema,
    TestUniqueIdType,
    TotalsSchema,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class TestsAPI(APIClient):
    @staticmethod
    def get_test_sub_type_unique_id(
        model_unique_id: str,
        test_unique_id: str,
        column_name: Optional[str] = None,
        test_sub_type: Optional[str] = None,
        **kwargs,
    ) -> str:
        return f"{model_unique_id}.{test_unique_id}.{column_name if column_name else 'None'}.{test_sub_type if test_sub_type else 'None'}"

    def get_tests_metadata(
        self, days_back: Optional[int] = 7
    ) -> List[TestMetadataSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="get_test_results", macro_args=dict(days_back=days_back)
        )
        tests_metadata = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        return [TestMetadataSchema(**test_metadata) for test_metadata in tests_metadata]

    def get_tests_sample_data(
        self,
        days_back: Optional[int] = 7,
        metrics_sample_limit: int = 5,
        disable_passed_test_metrics: bool = False,
    ) -> Dict[TestUniqueIdType, Dict[str, Any]]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="get_tests_sample_data",
            macro_args=dict(
                days_back=days_back,
                metrics_sample_limit=metrics_sample_limit,
                disable_passed_test_metrics=disable_passed_test_metrics,
            ),
        )
        tests_metrics = (
            json.loads(run_operation_response[0]) if run_operation_response else {}
        )
        return tests_metrics

    def get_invocations(
        self, invocations_per_test: int = 720, days_back: Optional[int] = 7
    ) -> Dict[TestUniqueIdType, InvocationsSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="get_tests_invocations",
            macro_args=dict(
                invocations_per_test=invocations_per_test, days_back=days_back
            ),
        )
        test_invocation_dicts = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        grouped_invocations = defaultdict(list)
        for test_invocation in test_invocation_dicts:
            try:
                sub_test_unique_id = self.get_test_sub_type_unique_id(**test_invocation)
                grouped_invocations[sub_test_unique_id].append(
                    InvocationSchema(
                        id=test_invocation["test_execution_id"],
                        time_utc=test_invocation["detected_at"],
                        status=test_invocation["status"],
                        affected_rows=self._parse_affected_row(
                            results_description=test_invocation[
                                "test_results_description"
                            ]
                        ),
                    )
                )
            except Exception:
                logger.error(
                    f"Could not parse test ({test_invocation.get('test_unique_id')}) invocation ({test_invocation.get('test_execution_id')})- continue to the next test"
                )
                continue

        tests_invocations = dict()
        for sub_test_unique_id, sub_test_invocations in grouped_invocations.items():
            totals = self._get_test_invocations_totals(sub_test_invocations)
            tests_invocations[sub_test_unique_id] = InvocationsSchema(
                fail_rate=round(totals.errors / len(sub_test_invocations), 2)
                if sub_test_invocations
                else 0,
                totals=totals,
                invocations=sub_test_invocations,
                description=self._get_invocations_description(totals),
            )
        return tests_invocations

    @staticmethod
    def _get_test_invocations_totals(
        invocations: List[InvocationSchema],
    ) -> TotalsSchema:
        totals = TotalsSchema()
        for invocation in invocations:
            totals.add_total(invocation.status)
        return totals

    @staticmethod
    def _get_invocations_description(
        invocations_totals: TotalsSchema,
    ) -> str:
        all_invocations_count = (
            invocations_totals.errors
            + invocations_totals.warnings
            + invocations_totals.passed
            + invocations_totals.failures
        )
        return f"There were {invocations_totals.failures or 'no'} failures, {invocations_totals.errors or 'no'} errors and {invocations_totals.warnings or 'no'} warnings on the last {all_invocations_count} test runs."

    @staticmethod
    def _parse_affected_row(results_description: str) -> Optional[int]:
        affected_rows_pattern = re.compile(r"^Got\s\d+\sresult")
        number_pattern = re.compile(r"\d+")
        try:
            matches_affected_rows_string = re.findall(
                affected_rows_pattern, results_description
            )[0]
            affected_rows = re.findall(number_pattern, matches_affected_rows_string)[0]
            return int(affected_rows)
        except Exception:
            return None

    def get_total_tests_results(
        self,
        tests_metadata: Optional[List[TestMetadataSchema]] = None,
        days_back: Optional[int] = None,
    ) -> Dict[str, TotalsSchema]:
        tests: List[TestMetadataSchema] = (
            tests_metadata
            if tests_metadata is not None
            else self.get_tests_metadata(days_back=days_back)
        )
        totals = dict()
        for test in tests:
            self._update_test_results_totals(
                totals_dict=totals,
                model_unique_id=test.model_unique_id,
                status=test.status,
            )
        return totals

    def get_total_tests_runs(
        self,
        tests_metadata: Optional[List[TestMetadataSchema]] = None,
        tests_invocations: Optional[Dict[TestUniqueIdType, InvocationsSchema]] = None,
        invocations_per_test: Optional[int] = None,
        days_back: Optional[int] = None,
    ) -> Dict[str, TotalsSchema]:
        tests: List[TestMetadataSchema] = (
            tests_metadata
            if tests_metadata is not None
            else self.get_tests_metadata(days_back=days_back)
        )
        invocations: Optional[Dict[TestUniqueIdType, InvocationsSchema]] = (
            tests_invocations
            if tests_invocations is not None
            else self.get_invocations(invocations_per_test=invocations_per_test)
        )
        totals = dict()
        for test in tests:
            test_sub_type_unique_id = self.get_test_sub_type_unique_id(**dict(test))
            test_invocations = invocations[test_sub_type_unique_id].invocations
            self._update_test_runs_totals(
                totals_dict=totals, test=test, test_invocations=test_invocations
            )
        return totals

    @staticmethod
    def _update_test_runs_totals(
        totals_dict: Dict[str, TotalsSchema],
        test: TestMetadataSchema,
        test_invocations: List[InvocationSchema],
    ):
        model_unique_id = test.model_unique_id

        if model_unique_id not in totals_dict:
            totals_dict[model_unique_id] = TotalsSchema()

        for test_invocation in test_invocations:
            totals_dict[model_unique_id].add_total(test_invocation.status)

    @staticmethod
    def _update_test_results_totals(
        totals_dict: Dict[str, TotalsSchema], model_unique_id: str, status: str
    ):
        if model_unique_id not in totals_dict:
            totals_dict[model_unique_id] = TotalsSchema()

        totals_dict[model_unique_id].add_total(status)
