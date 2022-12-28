import json
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from dateutil import tz

from elementary.clients.api.api import APIClient
from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.monitor.api.invocations.invocations import InvocationsAPI
from elementary.monitor.api.invocations.schema import DbtInvocationSchema
from elementary.monitor.api.tests.schema import (
    InvocationSchema,
    InvocationsSchema,
    ModelUniqueIdType,
    TestInfoSchema,
    TestMetadataSchema,
    TestResultSchema,
    TestRunSchema,
    TestUniqueIdType,
    TotalsSchema,
)
from elementary.monitor.data_monitoring.schema import DataMonitoringFilter
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger
from elementary.utils.time import convert_utc_iso_format_to_datetime

logger = get_logger(__name__)


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

    def get_tests_metadata(
        self,
        days_back: Optional[int] = 7,
        invocation_id: str = None,
        should_cache: bool = True,
    ) -> List[TestMetadataSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="get_test_results",
            macro_args=dict(days_back=days_back, invocation_id=invocation_id),
        )
        tests_metadata = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        tests_metadata = [
            TestMetadataSchema(**test_metadata) for test_metadata in tests_metadata
        ]
        if should_cache:
            self.set_run_cache(key=TESTS_METADATA, value=tests_metadata)
        return tests_metadata

    def _get_invocation_from_filter(
        self, filter: DataMonitoringFilter
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

    def get_tests_sample_data(
        self,
        days_back: Optional[int] = 7,
        metrics_sample_limit: int = 5,
        disable_passed_test_metrics: bool = False,
        disable_samples: bool = False,
    ) -> Dict[TestUniqueIdType, Dict[str, Any]]:
        tests_metrics = {}
        if not disable_samples:
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

        self.set_run_cache(key=TESTS_SAMPLE_DATA, value=tests_metrics)
        return tests_metrics

    def get_test_info_from_test_metadata(
        self,
        metadata: TestMetadataSchema,
    ) -> TestInfoSchema:
        test_display_name = (
            metadata.test_name.replace("_", " ").title() if metadata.test_name else ""
        )
        detected_at_datetime = convert_utc_iso_format_to_datetime(metadata.detected_at)
        detected_at_utc = detected_at_datetime
        detected_at = detected_at_datetime.astimezone(tz.tzlocal())
        table_full_name_parts = [
            name
            for name in [
                metadata.database_name,
                metadata.schema_name,
                metadata.table_name,
            ]
            if name
        ]
        table_full_name = ".".join(table_full_name_parts).lower()
        test_params = try_load_json(metadata.test_params) or {}
        test_query = (
            metadata.test_results_query.strip() if metadata.test_results_query else None
        )

        result = dict(
            result_description=metadata.test_results_description,
            result_query=test_query,
        )

        configuration = dict()

        if metadata.test_type == "dbt_test":
            configuration = dict(
                test_name=metadata.test_name,
                test_params=try_load_json(metadata.test_params),
            )
        else:
            configuration = dict(
                test_name=metadata.test_name,
                timestamp_column=test_params.get("timestamp_column"),
                testing_timeframe=test_params.get("timeframe"),
                anomaly_threshold=test_params.get("sensitivity"),
            )

        return TestInfoSchema(
            test_unique_id=metadata.test_unique_id,
            test_sub_type_unique_id=metadata.test_sub_type_unique_id,
            database_name=metadata.database_name,
            schema_name=metadata.schema_name,
            table_name=metadata.table_name,
            column_name=metadata.column_name,
            test_name=metadata.test_name,
            test_display_name=test_display_name,
            latest_run_time=detected_at.isoformat(),
            latest_run_time_utc=detected_at_utc.isoformat(),
            latest_run_status=metadata.status,
            model_unique_id=metadata.model_unique_id,
            table_unique_id=table_full_name,
            test_type=metadata.test_type,
            test_sub_type=metadata.test_sub_type,
            test_query=test_query,
            test_params=test_params,
            test_created_at=metadata.test_created_at,
            description=metadata.meta.get("description"),
            result=result,
            configuration=configuration,
        )

    def get_test_results(
        self,
        days_back: Optional[int] = 7,
        metrics_sample_limit: int = 5,
        disable_passed_test_metrics: bool = False,
        disable_samples: bool = False,
        filter: Optional[DataMonitoringFilter] = None,
    ) -> Tuple[
        Dict[ModelUniqueIdType, TestResultSchema], Optional[DbtInvocationSchema]
    ]:
        test_results_metadata = self.get_run_cache(TESTS_METADATA)
        invocation = self._get_invocation_from_filter(filter)
        if invocation.invocation_id:
            test_results_metadata = self.get_tests_metadata(
                invocation_id=invocation.invocation_id, should_cache=False
            )
        elif test_results_metadata is None:
            test_results_metadata = self.get_tests_metadata(days_back=days_back)

        tests_sample_data = self.get_run_cache(TESTS_SAMPLE_DATA)
        if tests_sample_data is None:
            tests_sample_data = self.get_tests_sample_data(
                days_back=days_back,
                metrics_sample_limit=metrics_sample_limit,
                disable_passed_test_metrics=disable_passed_test_metrics,
                disable_samples=disable_samples,
            )

        test_results = defaultdict(list)
        for test_metadata in test_results_metadata:
            test_sample_data = tests_sample_data.get(
                test_metadata.test_sub_type_unique_id
            )
            test_result = TestResultSchema(
                metadata=self.get_test_info_from_test_metadata(test_metadata),
                test_results=test_metadata.get_test_results(test_sample_data).dict(),
            )
            test_results[test_metadata.model_unique_id].append(test_result)

        self.set_run_cache(key=TEST_RESULTS, value=test_results)
        return test_results, invocation

    def get_test_runs(
        self, days_back: Optional[int] = 7, invocations_per_test: int = 720
    ) -> Dict[ModelUniqueIdType, TestRunSchema]:
        test_results_metadata = self.get_run_cache(TESTS_METADATA)
        if test_results_metadata is None:
            test_results_metadata = self.get_tests_metadata(days_back=days_back)

        tests_invocations = self.get_run_cache(TEST_INVOCATIONS)
        if tests_invocations is None:
            tests_invocations = self.get_invocations(
                days_back=days_back, invocations_per_test=invocations_per_test
            )

        test_runs = defaultdict(list)
        for test_metadata in test_results_metadata:
            test_invocations = tests_invocations.get(
                test_metadata.test_sub_type_unique_id
            )
            test_run = TestRunSchema(
                metadata=self.get_test_info_from_test_metadata(test_metadata),
                test_runs=test_invocations,
            )
            test_runs[test_metadata.model_unique_id].append(test_run)

        self.set_run_cache(key=TEST_RUNS, value=test_runs)
        return test_runs

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
                sub_test_unique_id = test_invocation.get("test_sub_type_unique_id")
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
                    f"Could not parse test ({test_invocation.get('test_unique_id')}) invocation ({test_invocation.get('test_execution_id')}) - continue to the next test"
                )
                continue

        test_invocations = dict()
        for sub_test_unique_id, sub_test_invocations in grouped_invocations.items():
            totals = self._get_test_invocations_totals(sub_test_invocations)
            test_invocations[sub_test_unique_id] = InvocationsSchema(
                fail_rate=round(totals.errors / len(sub_test_invocations), 2)
                if sub_test_invocations
                else 0,
                totals=totals,
                invocations=sub_test_invocations,
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
        tests_info: Optional[List[TestInfoSchema]] = None,
    ) -> Dict[str, TotalsSchema]:
        totals = dict()
        for test in tests_info:
            self._update_test_results_totals(
                totals_dict=totals,
                model_unique_id=test.model_unique_id,
                status=test.latest_run_status,
            )
        return totals

    def get_total_tests_runs(
        self,
        tests_info: Optional[List[TestInfoSchema]] = None,
        tests_invocations: Optional[Dict[TestUniqueIdType, InvocationsSchema]] = None,
    ) -> Dict[str, TotalsSchema]:
        totals = dict()
        for test in tests_info:
            test_invocations = tests_invocations[
                test.test_sub_type_unique_id
            ].invocations
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
