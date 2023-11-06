import re
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional, Union, cast

from dateutil import tz

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.monitor.api.tests.schema import (
    DbtTestResultSchema,
    ElementaryTestResultSchema,
    InvocationSchema,
    InvocationsSchema,
    TestMetadataSchema,
    TestResultSchema,
    TestResultSummarySchema,
    TestRunSchema,
)
from elementary.monitor.api.totals_schema import TotalsSchema
from elementary.monitor.data_monitoring.schema import SelectorFilterSchema
from elementary.monitor.fetchers.tests.schema import TestResultDBRowSchema
from elementary.monitor.fetchers.tests.tests import TestsFetcher
from elementary.utils.log import get_logger
from elementary.utils.time import convert_utc_iso_format_to_datetime

logger = get_logger(__name__)


class TestsAPI(APIClient):
    def __init__(
        self,
        dbt_runner: BaseDbtRunner,
        days_back: int = 7,
        invocations_per_test: int = 720,
        disable_passed_test_metrics: bool = False,
    ):
        super().__init__(dbt_runner)
        self.tests_fetcher = TestsFetcher(dbt_runner=self.dbt_runner)
        self.test_results_db_rows = self._get_test_results_db_rows(
            days_back=days_back,
            invocations_per_test=invocations_per_test,
            disable_passed_test_metrics=disable_passed_test_metrics,
        )

    def _get_test_results_db_rows(
        self,
        days_back: Optional[int] = 7,
        invocations_per_test: int = 720,
        disable_passed_test_metrics: bool = False,
    ) -> List[TestResultDBRowSchema]:
        return self.tests_fetcher.get_all_test_results_db_rows(
            days_back=days_back,
            invocations_per_test=invocations_per_test,
            disable_passed_test_metrics=disable_passed_test_metrics,
        )

    def get_test_results_summary(
        self,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ) -> List[TestResultSummarySchema]:
        filtered_test_results_db_rows = self.test_results_db_rows
        if filter.tag:
            filtered_test_results_db_rows = [
                test_result
                for test_result in filtered_test_results_db_rows
                if (test_result.tags and filter.tag in test_result.tags)
            ]
        elif filter.owner:
            filtered_test_results_db_rows = [
                test_result
                for test_result in filtered_test_results_db_rows
                if (test_result.owners and filter.owner in test_result.owners)
            ]
        elif filter.model:
            filtered_test_results_db_rows = [
                test_result
                for test_result in filtered_test_results_db_rows
                if (
                    test_result.model_unique_id
                    and test_result.model_unique_id.endswith(filter.model)
                )
            ]

        filtered_test_results_db_rows = [
            test_result
            for test_result in filtered_test_results_db_rows
            if test_result.invocations_rank_index == 1
        ]
        return [
            TestResultSummarySchema(
                test_unique_id=test_result.test_unique_id,
                elementary_unique_id=test_result.elementary_unique_id,
                table_name=test_result.table_name,
                column_name=test_result.column_name,
                test_type=test_result.test_type,
                test_sub_type=test_result.test_sub_type,
                owners=test_result.model_owner,
                tags=test_result.tags,
                subscribers=self._get_test_subscribers(
                    test_meta=test_result.meta, model_meta=test_result.model_meta
                ),
                description=test_result.meta.get("description"),
                test_name=test_result.test_name,
                status=test_result.status,
                results_counter=test_result.failures,
            )
            for test_result in filtered_test_results_db_rows
        ]

    @staticmethod
    def _get_test_subscribers(test_meta: dict, model_meta: dict) -> List[str]:
        subscribers = []
        test_subscribers = test_meta.get("subscribers", [])
        model_subscribers = model_meta.get("subscribers", [])
        if isinstance(test_subscribers, list):
            subscribers.extend(test_subscribers)
        else:
            subscribers.append(test_subscribers)

        if isinstance(model_subscribers, list):
            subscribers.extend(model_subscribers)
        else:
            subscribers.append(model_subscribers)
        return subscribers

    def get_test_results(
        self,
        invocation_id: Optional[str],
        disable_samples: bool = False,
    ) -> Dict[Optional[str], List[TestResultSchema]]:
        filtered_test_results_db_rows = self.test_results_db_rows

        if invocation_id:
            filtered_test_results_db_rows = [
                test_result
                for test_result in filtered_test_results_db_rows
                if test_result.invocation_id == invocation_id
            ]

        filtered_test_results_db_rows = [
            test_result
            for test_result in filtered_test_results_db_rows
            if test_result.invocations_rank_index == 1
        ]

        tests_results: DefaultDict[Optional[str], List[TestResultSchema]] = defaultdict(
            list
        )
        for test_result_db_row in filtered_test_results_db_rows:
            metadata = self._get_test_metadata_from_test_result_db_row(
                test_result_db_row
            )
            inner_test_results = self._get_test_result_from_test_result_db_row(
                test_result_db_row, disable_samples=disable_samples
            )

            if inner_test_results is None:
                continue

            tests_results[test_result_db_row.model_unique_id].append(
                TestResultSchema(
                    metadata=metadata,
                    test_results=inner_test_results,
                )
            )

        return tests_results

    def get_test_runs(self) -> Dict[Optional[str], List[TestRunSchema]]:
        tests_invocations = self._get_invocations(self.test_results_db_rows)
        latest_test_results = [
            test_result
            for test_result in self.test_results_db_rows
            if test_result.invocations_rank_index == 1
        ]

        test_runs = defaultdict(list)
        for test_result_db_row in latest_test_results:
            test_invocations = tests_invocations.get(
                test_result_db_row.elementary_unique_id
            )
            test_run = TestRunSchema(
                metadata=self._get_test_metadata_from_test_result_db_row(
                    test_result_db_row
                ),
                test_runs=test_invocations,
            )
            test_runs[test_result_db_row.model_unique_id].append(test_run)
        return test_runs

    def _get_invocations(
        self, test_result_db_rows: List[TestResultDBRowSchema]
    ) -> Dict[str, InvocationsSchema]:
        grouped_invocations = defaultdict(list)
        grouped_invocation_ids: DefaultDict[str, List[str]] = defaultdict(list)
        for test_result_db_row in test_result_db_rows:
            try:
                elementary_unique_id = test_result_db_row.elementary_unique_id
                invocation_id = (
                    test_result_db_row.invocation_id
                    or test_result_db_row.test_execution_id
                )

                if invocation_id is None:
                    # Shouldn't happen, mainly a sanity
                    logger.warning("Test result without invocation ID found, skipping")
                    continue

                # Currently the way we flat test results causing that there is duplication in test invocation for
                # each test.
                # This if statement checks if the invocation is already counted or not.
                if invocation_id not in grouped_invocation_ids[elementary_unique_id]:
                    grouped_invocation_ids[elementary_unique_id].append(invocation_id)
                    grouped_invocations[elementary_unique_id].append(
                        InvocationSchema(
                            id=invocation_id,
                            time_utc=test_result_db_row.detected_at,
                            status=test_result_db_row.status,
                            affected_rows=self._parse_affected_row(
                                results_description=test_result_db_row.test_results_description
                                or ""
                            ),
                        )
                    )
            except Exception:
                logger.error(
                    f"Could not parse test ({test_result_db_row.test_unique_id}) invocation ({test_result_db_row.invocation_id or test_result_db_row.test_execution_id}) - continue to the next test"
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

        configuration: Dict[str, Any]
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
                anomaly_threshold=test_params.get("sensitivity")
                or test_params.get("anomaly_sensitivity"),
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
    ) -> Optional[Union[DbtTestResultSchema, ElementaryTestResultSchema]]:
        test_results: Optional[Union[DbtTestResultSchema, ElementaryTestResultSchema]]

        sample_data = test_result_db_row.sample_data if not disable_samples else None
        if test_result_db_row.test_type == "dbt_test":
            # Sample data is always a list for non-elementary tests
            sample_data = cast(Optional[list], sample_data)

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
                if (
                    isinstance(sample_data, list)
                    and test_result_db_row.test_sub_type != "dimension"
                ):
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
            else:
                # Unexpected test type - might have been introduced in a new package version.
                # So we have no choice but to log a warning and ignore it.
                logger.warning(
                    f"Unexpected elementary test type: {test_result_db_row.test_type}"
                )
                test_results = None

        return test_results

    @staticmethod
    def _get_failed_rows_count(test_result_db_row: TestResultDBRowSchema) -> int:
        failed_rows_count = -1
        if (
            test_result_db_row.status != "pass"
            and test_result_db_row.test_results_description
        ):
            found_rows_number_match = re.search(
                r"\d+", test_result_db_row.test_results_description
            )
            if found_rows_number_match:
                found_rows_number = found_rows_number_match.group()
                failed_rows_count = int(found_rows_number)
        return failed_rows_count
