from collections import defaultdict
from typing import DefaultDict, Dict, List, Optional

from dateutil import tz

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.monitor.api.source_freshnesses.schema import (
    DbtSourceFreshnessResultSchema,
    SourceFreshnessInvocationSchema,
    SourceFreshnessInvocationsSchema,
    SourceFreshnessMetadataSchema,
    SourceFreshnessResultSchema,
    SourceFreshnessRunSchema,
)
from elementary.monitor.api.totals_schema import TotalsSchema
from elementary.monitor.fetchers.source_freshnesses.schema import (
    SourceFreshnessResultDBRowSchema,
)
from elementary.monitor.fetchers.source_freshnesses.source_freshnesses import (
    SourceFreshnessesFetcher,
)
from elementary.utils.time import convert_utc_iso_format_to_datetime


class SourceFreshnessesAPI(APIClient):
    def __init__(
        self,
        dbt_runner: BaseDbtRunner,
        days_back: int = 7,
        invocations_per_test: int = 720,
    ):
        super().__init__(dbt_runner)
        self.tests_fetcher = SourceFreshnessesFetcher(dbt_runner=self.dbt_runner)
        self.source_freshness_results_db_rows = (
            self._get_source_freshness_results_db_rows(
                days_back=days_back,
                invocations_per_test=invocations_per_test,
            )
        )

    def _get_source_freshness_results_db_rows(
        self,
        days_back: Optional[int] = 7,
        invocations_per_test: int = 720,
    ) -> List[SourceFreshnessResultDBRowSchema]:
        return self.tests_fetcher.get_source_freshness_results_db_rows(
            days_back=days_back,
            invocations_per_test=invocations_per_test,
        )

    def get_source_freshness_results(
        self,
    ) -> Dict[str, List[SourceFreshnessResultSchema]]:
        filtered_source_freshness_results_db_rows = [
            test_result
            for test_result in self.source_freshness_results_db_rows
            if test_result.invocations_rank_index == 1
        ]
        tests_results: DefaultDict[
            str, List[SourceFreshnessResultSchema]
        ] = defaultdict(list)

        for (
            source_freshness_results_db_row
        ) in filtered_source_freshness_results_db_rows:
            metadata = self._get_test_metadata_from_source_freshness_results_db_row(
                source_freshness_results_db_row
            )

            tests_results[source_freshness_results_db_row.unique_id].append(
                SourceFreshnessResultSchema(
                    metadata=metadata,
                    test_results=DbtSourceFreshnessResultSchema(
                        status=source_freshness_results_db_row.status,
                        error_message=source_freshness_results_db_row.error,
                        max_loaded_at_time_ago_in_s=source_freshness_results_db_row.max_loaded_at_time_ago_in_s,
                        max_loaded_at=source_freshness_results_db_row.max_loaded_at,
                        detected_at=source_freshness_results_db_row.generated_at,
                    ),
                )
            )

        return tests_results

    def get_source_freshness_runs(self) -> Dict[str, List[SourceFreshnessRunSchema]]:
        source_freshness_invocations = self._get_source_freshness_invocations(
            self.source_freshness_results_db_rows
        )

        latest_source_freshness_results = [
            test_result
            for test_result in self.source_freshness_results_db_rows
            if test_result.invocations_rank_index == 1
        ]

        source_freshness_runs: Dict[str, List[SourceFreshnessRunSchema]] = defaultdict(
            list
        )
        for source_freshness_result in latest_source_freshness_results:
            source_freshness_runs[source_freshness_result.unique_id].append(
                SourceFreshnessRunSchema(
                    metadata=self._get_test_metadata_from_source_freshness_results_db_row(
                        source_freshness_result
                    ),
                    test_runs=source_freshness_invocations[
                        source_freshness_result.unique_id
                    ],
                    test_results=DbtSourceFreshnessResultSchema(
                        status=source_freshness_result.status,
                        error_message=source_freshness_result.error,
                        max_loaded_at_time_ago_in_s=source_freshness_result.max_loaded_at_time_ago_in_s,
                        max_loaded_at=source_freshness_result.max_loaded_at,
                        detected_at=source_freshness_result.generated_at,
                    ),
                )
            )

        return source_freshness_runs

    @staticmethod
    def _get_source_freshness_invocations(
        source_freshness_results: List[SourceFreshnessResultDBRowSchema],
    ) -> Dict[str, SourceFreshnessInvocationsSchema]:
        invocations_by_source_freshness_unique_id: Dict[
            str, Dict[str, SourceFreshnessInvocationSchema]
        ] = defaultdict(dict)

        for source_freshness_results_db_row in source_freshness_results:
            unique_id = source_freshness_results_db_row.unique_id
            invocation_id = source_freshness_results_db_row.invocation_id

            invocations_by_source_freshness_unique_id[unique_id][
                invocation_id
            ] = SourceFreshnessInvocationSchema(
                id=invocation_id,
                time_utc=source_freshness_results_db_row.generated_at,
                status=source_freshness_results_db_row.status,
            )

        source_freshness_invocations = dict()

        for unique_id, invocations in invocations_by_source_freshness_unique_id.items():
            totals = TotalsSchema()
            for run in invocations.values():
                totals.add_total(run.status)

            source_freshness_invocations[unique_id] = SourceFreshnessInvocationsSchema(
                fail_rate=(
                    round((totals.errors + totals.failures) / len(invocations), 2)
                    if invocations
                    else 0
                ),
                totals=totals,
                invocations=list(invocations.values()),
                description=f"There were {totals.failures or 'no'} failures, {totals.errors or 'no'} errors and {totals.warnings or 'no'} warnings on the last {len(invocations)} source freshness runs.",
            )

        return source_freshness_invocations

    @staticmethod
    def _get_test_metadata_from_source_freshness_results_db_row(
        source_freshness_results_db_row: SourceFreshnessResultDBRowSchema,
    ) -> SourceFreshnessMetadataSchema:
        # detected_at = generated_at
        detected_at_datetime = convert_utc_iso_format_to_datetime(
            source_freshness_results_db_row.generated_at
        )
        detected_at = detected_at_datetime.astimezone(tz.tzlocal())

        configuration = dict(
            error_after=source_freshness_results_db_row.error_after,
            warn_after=source_freshness_results_db_row.warn_after,
            filter=source_freshness_results_db_row.filter,
            loaded_at_field=source_freshness_results_db_row.loaded_at_field,
        )

        return SourceFreshnessMetadataSchema(
            test_unique_id=source_freshness_results_db_row.unique_id,
            elementary_unique_id=source_freshness_results_db_row.unique_id,
            database_name=source_freshness_results_db_row.database_name,
            schema_name=source_freshness_results_db_row.schema_name,
            table_name=source_freshness_results_db_row.table_name,
            test_name=source_freshness_results_db_row.test_type,
            latest_run_time=detected_at.isoformat(),
            latest_run_status=source_freshness_results_db_row.status,
            model_unique_id=source_freshness_results_db_row.unique_id,
            test_type=source_freshness_results_db_row.test_type,
            test_sub_type=source_freshness_results_db_row.test_sub_type,
            description=source_freshness_results_db_row.freshness_description,
            configuration=configuration,
        )
