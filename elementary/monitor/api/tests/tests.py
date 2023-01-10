import json
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from dateutil import tz

from elementary.clients.api.api import APIClient
from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.monitor.api.invocations.invocations import InvocationsAPI
from elementary.monitor.api.invocations.schema import DbtInvocationSchema
from elementary.monitor.api.tests.schema import (
    InvocationSchema,
    InvocationsSchema,
    ModelUniqueIdType,
    RawTestResultSchema,
    TestMetadataSchema,
    TestResultSchema,
    TestRunSchema,
    TestUniqueIdType,
    TotalsSchema,
)
from elementary.monitor.data_monitoring.schema import DataMonitoringReportFilter
from elementary.utils.log import get_logger
from elementary.utils.time import convert_utc_iso_format_to_datetime

logger = get_logger(__name__)


ALL_TEST_RESULTS = "all_test_results"
TEST_RESULTS = "test_results"
TEST_RUNS = "test_runs"
TESTS_METADATA = "tests_metadata"
TESTS_SAMPLE_DATA = "tests_sample_data"
INVOCATION = "invocation"
TEST_INVOCATIONS = "test_invocations"


class TestsAPI(APIClient):
    def __init__(self, dbt_runner: DbtRunner):
        super().__init__(dbt_runner)
        self.invocations_api = InvocationsAPI(dbt_runner)

    def _get_all_test_results(
        self,
        days_back: Optional[int] = 7,
        invocations_per_test: int = 720,
        metrics_sample_limit: int = 5,
        disable_passed_test_metrics: bool = False,
        should_cache: bool = True,
    ) -> List[RawTestResultSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="get_test_results_v2",
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
            RawTestResultSchema(**test_result) for test_result in test_results
        ]
        if should_cache:
            self.set_run_cache(key=ALL_TEST_RESULTS, value=test_results)
        return test_results

    def _get_invocation_from_filter(
        self, filter: DataMonitoringReportFilter
    ) -> Optional[DbtInvocationSchema]:
        # If none of the following filter options exists, the invocation is empty and there is no filter.
        invocation = DbtInvocationSchema()
        if filter.invocation_id:
            invocation = self.invocations_api.get_invocation_by_id(
                type="test", invocation_id=filter.invocation_id
            )
        elif filter.invocation_time:
            invocation = self.invocations_api.get_invocation_by_time(
                type="test", invocation_max_time=filter.invocation_time
            )
        elif filter.last_invocation:
            invocation = self.invocations_api.get_last_invocation(type="test")

        self.set_run_cache(key=INVOCATION, value=invocation)
        return invocation

    def get_test_metadata_from_raw_test_result(
        self,
        raw_test_result: RawTestResultSchema,
    ) -> TestMetadataSchema:
        test_display_name = (
            raw_test_result.test_name.replace("_", " ").title()
            if raw_test_result.test_name
            else ""
        )
        detected_at_datetime = convert_utc_iso_format_to_datetime(
            raw_test_result.detected_at
        )
        detected_at_utc = detected_at_datetime
        detected_at = detected_at_datetime.astimezone(tz.tzlocal())
        table_full_name_parts = [
            name
            for name in [
                raw_test_result.database_name,
                raw_test_result.schema_name,
                raw_test_result.table_name,
            ]
            if name
        ]
        table_full_name = ".".join(table_full_name_parts).lower()
        test_query = (
            raw_test_result.test_results_query.strip()
            if raw_test_result.test_results_query
            else None
        )

        result = dict(
            result_description=raw_test_result.test_results_description,
            result_query=test_query,
        )

        configuration = dict()

        if raw_test_result.test_type == "dbt_test":
            configuration = dict(
                test_name=raw_test_result.test_name,
                test_params=raw_test_result.test_params,
            )
        else:
            configuration = dict(
                test_name=raw_test_result.test_name,
                timestamp_column=raw_test_result.test_params.get("timestamp_column"),
                testing_timeframe=raw_test_result.test_params.get("timeframe"),
                anomaly_threshold=raw_test_result.test_params.get("sensitivity"),
            )

        return TestMetadataSchema(
            test_unique_id=raw_test_result.test_unique_id,
            elementary_unique_id=raw_test_result.elementary_unique_id,
            database_name=raw_test_result.database_name,
            schema_name=raw_test_result.schema_name,
            table_name=raw_test_result.table_name,
            column_name=raw_test_result.column_name,
            test_name=raw_test_result.test_name,
            test_display_name=test_display_name,
            latest_run_time=detected_at.isoformat(),
            latest_run_time_utc=detected_at_utc.isoformat(),
            latest_run_status=raw_test_result.status,
            model_unique_id=raw_test_result.model_unique_id,
            table_unique_id=table_full_name,
            test_type=raw_test_result.test_type,
            test_sub_type=raw_test_result.test_sub_type,
            test_query=test_query,
            test_params=raw_test_result.test_params,
            test_created_at=raw_test_result.test_created_at,
            description=raw_test_result.meta.get("description"),
            result=result,
            configuration=configuration,
        )

    def get_test_results(
        self,
        days_back: Optional[int] = 7,
        metrics_sample_limit: int = 5,
        invocations_per_test: int = 720,
        disable_passed_test_metrics: bool = False,
        disable_samples: bool = False,
        filter: Optional[DataMonitoringReportFilter] = None,
    ) -> Tuple[
        Dict[ModelUniqueIdType, List[TestResultSchema]], Optional[DbtInvocationSchema]
    ]:
        all_test_results = self.get_run_cache(ALL_TEST_RESULTS)
        if all_test_results is None:
            all_test_results = self._get_all_test_results(
                days_back=days_back,
                invocations_per_test=invocations_per_test,
                metrics_sample_limit=metrics_sample_limit,
                disable_passed_test_metrics=disable_passed_test_metrics,
            )

        invocation = self._get_invocation_from_filter(filter)
        if invocation.invocation_id:
            all_test_results = [
                test_result
                for test_result in all_test_results
                if test_result.invocation_id == invocation.invocation_id
            ]

        all_test_results = [
            test_result
            for test_result in all_test_results
            if test_result.invocations_order == 1
        ]

        test_results = defaultdict(list)
        for raw_test_result in all_test_results:
            test_result = TestResultSchema(
                metadata=self.get_test_metadata_from_raw_test_result(raw_test_result),
                test_results=raw_test_result.get_test_results(
                    disable_samples=disable_samples
                ),
            )
            test_results[raw_test_result.model_unique_id].append(test_result)

        self.set_run_cache(key=TEST_RESULTS, value=test_results)
        return test_results, invocation

    def get_test_runs(
        self,
        days_back: Optional[int] = 7,
        metrics_sample_limit: int = 5,
        invocations_per_test: int = 720,
    ) -> Dict[ModelUniqueIdType, List[TestRunSchema]]:
        all_test_results = self.get_run_cache(ALL_TEST_RESULTS)
        if all_test_results is None:
            all_test_results = self._get_all_test_results(
                days_back=days_back,
                invocations_per_test=invocations_per_test,
                metrics_sample_limit=metrics_sample_limit,
                disable_passed_test_metrics=True,
            )

        tests_invocations = self._get_invocations(all_test_results)
        latest_test_results = [
            test_result
            for test_result in all_test_results
            if test_result.invocations_order == 1
        ]

        test_runs = defaultdict(list)
        for raw_test_result in latest_test_results:
            test_invocations = tests_invocations.get(
                raw_test_result.elementary_unique_id
            )
            test_run = TestRunSchema(
                metadata=self.get_test_metadata_from_raw_test_result(raw_test_result),
                test_runs=test_invocations,
            )
            test_runs[raw_test_result.model_unique_id].append(test_run)

        self.set_run_cache(key=TEST_RUNS, value=test_runs)
        return test_runs

    def _get_invocations(
        self, raw_test_results: List[RawTestResultSchema]
    ) -> Dict[TestUniqueIdType, InvocationsSchema]:
        grouped_invocations = defaultdict(list)
        for raw_test_result in raw_test_results:
            try:
                elementary_unique_id = raw_test_result.elementary_unique_id
                grouped_invocations[elementary_unique_id].append(
                    InvocationSchema(
                        id=raw_test_result.invocation_id
                        or raw_test_result.test_execution_id,
                        time_utc=raw_test_result.detected_at,
                        status=raw_test_result.status,
                        affected_rows=self._parse_affected_row(
                            results_description=raw_test_result.test_results_description
                        ),
                    )
                )
            except Exception:
                logger.error(
                    f"Could not parse test ({raw_test_result.test_unique_id}) invocation ({raw_test_result.invocation_id or raw_test_result.test_execution_id}) - continue to the next test"
                )
                continue

        test_invocations = dict()
        for elementary_unique_id, invocations in grouped_invocations.items():
            totals = self._get_test_invocations_totals(invocations)
            test_invocations[elementary_unique_id] = InvocationsSchema(
                fail_rate=round((totals.errors + totals.failures) / len(invocations), 2)
                if invocations
                else 0,
                totals=totals,
                invocations=invocations,
                description=self._get_invocations_description(totals),
            )
        self.set_run_cache(key=TEST_INVOCATIONS, value=test_invocations)
        return test_invocations

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
    ) -> Dict[str, TotalsSchema]:
        totals = dict()
        for test in tests_metadata:
            self._update_test_results_totals(
                totals_dict=totals,
                model_unique_id=test.model_unique_id,
                status=test.latest_run_status,
            )
        return totals

    def get_total_tests_runs(
        self, tests_runs: Dict[ModelUniqueIdType, List[TestRunSchema]]
    ) -> Dict[str, TotalsSchema]:
        totals = dict()
        for test_runs in tests_runs.values():
            for test_run in test_runs:
                test_invocations = test_run.test_runs.invocations
                self._update_test_runs_totals(
                    totals_dict=totals,
                    test=test_run.metadata,
                    test_invocations=test_invocations,
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
