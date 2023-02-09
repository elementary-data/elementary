from collections import defaultdict
from typing import Dict, List, Optional, Union

from dateutil import tz

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.monitor.api.invocations.invocations import InvocationsAPI
from elementary.monitor.api.invocations.schema import DbtInvocationSchema
from elementary.monitor.api.tests.schema import (
    DbtTestResultSchema,
    ElementaryTestResultSchema,
    InvocationSchema,
    ModelUniqueIdType,
    TestMetadataSchema,
    TestResultDBRowSchema,
    TestResultSchema,
    TestRunSchema,
    TotalsSchema,
)
from elementary.monitor.api.tests.tests import TestsAPI
from elementary.monitor.data_monitoring.schema import (
    DataMonitoringReportFilter,
    DataMonitoringReportTestResultsSchema,
)
from elementary.monitor.fetchers.base_fetcher import BaseFetcher
from elementary.utils.log import get_logger
from elementary.utils.time import convert_utc_iso_format_to_datetime

logger = get_logger(__name__)


class TestsFetcher(BaseFetcher):
    def __init__(
        self,
        dbt_runner: DbtRunner,
        days_back: Optional[int] = 7,
        invocations_per_test: int = 720,
        metrics_sample_limit: int = 5,
        disable_passed_test_metrics: bool = False,
    ):
        super().__init__(dbt_runner)
        self.tests_api = TestsAPI(dbt_runner=self.dbt_runner)
        self.invocations_api = InvocationsAPI(dbt_runner)
        self.test_results_db_rows = self._get_test_results_db_rows(
            days_back=days_back,
            invocations_per_test=invocations_per_test,
            metrics_sample_limit=metrics_sample_limit,
            disable_passed_test_metrics=disable_passed_test_metrics,
        )

    def _get_test_results_db_rows(
        self,
        days_back: Optional[int] = 7,
        invocations_per_test: int = 720,
        metrics_sample_limit: int = 5,
        disable_passed_test_metrics: bool = False,
    ) -> List[TestResultDBRowSchema]:
        return self.tests_api.get_all_test_results_db_rows(
            days_back=days_back,
            invocations_per_test=invocations_per_test,
            metrics_sample_limit=metrics_sample_limit,
            disable_passed_test_metrics=disable_passed_test_metrics,
        )

    def get_test_results(
        self,
        filter: Optional[DataMonitoringReportFilter],
        disable_samples: bool = False,
    ):
        try:
            filtered_test_results_db_rows = self.test_results_db_rows
            invocation = self._get_invocation_from_filter(filter)
            if invocation.invocation_id:
                filtered_test_results_db_rows = [
                    test_result
                    for test_result in filtered_test_results_db_rows
                    if test_result.invocation_id == invocation.invocation_id
                ]

            filtered_test_results_db_rows = [
                test_result
                for test_result in filtered_test_results_db_rows
                if test_result.invocations_rank_index == 1
            ]

            tests_results = defaultdict(list)
            for test_result_db_row in filtered_test_results_db_rows:
                test_result = TestResultSchema(
                    metadata=self._get_test_metadata_from_test_result_db_row(
                        test_result_db_row
                    ),
                    test_results=self._get_test_result_from_test_result_db_row(
                        test_result_db_row, disable_samples=disable_samples
                    ),
                )
                tests_results[test_result_db_row.model_unique_id].append(test_result)

            test_metadatas = []
            for test_results in tests_results.values():
                test_metadatas.extend([result.metadata for result in test_results])
            test_results_totals = self._get_total_tests_results(test_metadatas)
            return DataMonitoringReportTestResultsSchema(
                results=tests_results,
                totals=test_results_totals,
                invocation=invocation,
            )
        except Exception as e:
            logger.exception(f"Could not get test results and totals - Error: {e}")
            self.tracking.record_cli_internal_exception(e)
            self.success = False
            return DataMonitoringReportTestResultsSchema()

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

        return invocation

    @staticmethod
    def _get_test_metadata_from_test_result_db_row(
        test_result_db_row: TestResultDBRowSchema,
    ) -> TestMetadataSchema:
        test_display_name = (
            test_result_db_row.test_name.replace("_", " ").title()
            if test_result_db_row.test_name
            else ""
        )
        detected_at_datetime = convert_utc_iso_format_to_datetime(
            test_result_db_row.detected_at
        )
        detected_at_utc = detected_at_datetime
        detected_at = detected_at_datetime.astimezone(tz.tzlocal())
        table_full_name_parts = [
            name
            for name in [
                test_result_db_row.database_name,
                test_result_db_row.schema_name,
                test_result_db_row.table_name,
            ]
            if name
        ]
        table_full_name = ".".join(table_full_name_parts).lower()
        test_params = test_result_db_row.test_params
        test_query = (
            test_result_db_row.test_results_query.strip()
            if test_result_db_row.test_results_query
            else None
        )

        result = dict(
            result_description=test_result_db_row.test_results_description,
            result_query=test_query,
        )

        if test_result_db_row.test_type == "dbt_test":
            configuration = dict(
                test_name=test_result_db_row.test_name,
                test_params=test_params,
            )
        else:
            time_bucket_configuration = test_params.get("time_bucket", {})
            time_bucket_count = time_bucket_configuration.get("count", 1)
            time_bucket_period = time_bucket_configuration.get("period", "day")
            configuration = dict(
                test_name=test_result_db_row.test_name,
                timestamp_column=test_params.get("timestamp_column"),
                testing_timeframe=f"{time_bucket_count} {time_bucket_period}{'s' if time_bucket_count > 1 else ''}",
                anomaly_threshold=test_params.get("sensitivity"),
            )

        return TestMetadataSchema(
            test_unique_id=test_result_db_row.test_unique_id,
            elementary_unique_id=test_result_db_row.elementary_unique_id,
            database_name=test_result_db_row.database_name,
            schema_name=test_result_db_row.schema_name,
            table_name=test_result_db_row.table_name,
            column_name=test_result_db_row.column_name,
            test_name=test_result_db_row.test_name,
            test_display_name=test_display_name,
            latest_run_time=detected_at.isoformat(),
            latest_run_time_utc=detected_at_utc.isoformat(),
            latest_run_status=test_result_db_row.status,
            model_unique_id=test_result_db_row.model_unique_id,
            table_unique_id=table_full_name,
            test_type=test_result_db_row.test_type,
            test_sub_type=test_result_db_row.test_sub_type,
            test_query=test_query,
            test_params=test_result_db_row.test_params,
            test_created_at=test_result_db_row.test_created_at,
            description=test_result_db_row.meta.get("description"),
            result=result,
            configuration=configuration,
        )

    @staticmethod
    def _get_test_result_from_test_result_db_row(
        test_result_db_row: TestResultDBRowSchema,
        disable_samples: bool = False,
    ) -> Union[DbtTestResultSchema, ElementaryTestResultSchema]:
        test_results = None
        sample_data = test_result_db_row.sample_data if not disable_samples else None
        if test_result_db_row.test_type == "dbt_test":
            test_results = DbtTestResultSchema(
                display_name=test_result_db_row.test_name,
                results_sample=sample_data,
                error_message=test_result_db_row.test_results_description,
                failed_rows_count=TestsAPI._get_failed_rows_count(test_result_db_row),
            )
        else:
            test_sub_type_display_name = test_result_db_row.test_sub_type.replace(
                "_", " "
            ).title()
            if test_result_db_row.test_type == "anomaly_detection":
                if sample_data and test_result_db_row.test_sub_type != "dimension":
                    sample_data.sort(key=lambda metric: metric.get("end_time"))
                test_results = ElementaryTestResultSchema(
                    display_name=test_sub_type_display_name,
                    metrics=sample_data,
                    result_description=test_result_db_row.test_results_description,
                )
            elif test_result_db_row.test_type == "schema_change":
                test_results = ElementaryTestResultSchema(
                    display_name=test_sub_type_display_name.lower(),
                    result_description=test_result_db_row.test_results_description,
                )
        return test_results

    def _get_total_tests_results(
        self,
        test_metadatas: List[TestMetadataSchema],
    ) -> Dict[Optional[str], TotalsSchema]:
        totals = dict()
        for test in test_metadatas:
            self._update_test_results_totals(
                totals_dict=totals,
                model_unique_id=test.model_unique_id,
                status=test.latest_run_status,
            )
        return totals

    def _get_total_tests_runs(
        self, tests_runs: Dict[Optional[ModelUniqueIdType], List[TestRunSchema]]
    ) -> Dict[Optional[str], TotalsSchema]:
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
