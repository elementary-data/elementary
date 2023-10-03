from collections import defaultdict
from typing import DefaultDict, List, Optional

from dateutil import tz

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.monitor.api.source_freshnesses.schema import (
    DbtSourceFreshnessResultSchema,
    SourceFreshnessMetadataSchema,
    SourceFreshnessResultSchema,
)
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

    def get_source_freshness_results(self):
        filtered_source_freshness_results_db_rows = (
            self.source_freshness_results_db_rows
        )

        filtered_source_freshness_results_db_rows = [
            test_result
            for test_result in filtered_source_freshness_results_db_rows
            if test_result.invocations_rank_index == 1
        ]
        tests_results: DefaultDict[
            Optional[str], List[SourceFreshnessResultSchema]
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
                        status=source_freshness_results_db_row.normalized_status,
                        error_message=source_freshness_results_db_row.error,
                        max_loaded_at_time_ago_in_s=source_freshness_results_db_row.max_loaded_at_time_ago_in_s,
                        max_loaded_at=source_freshness_results_db_row.max_loaded_at,
                    ),
                )
            )

        return tests_results

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
            database_name=source_freshness_results_db_row.database_name,
            schema_name=source_freshness_results_db_row.schema_name,
            table_name=source_freshness_results_db_row.table_name,
            test_name=source_freshness_results_db_row.test_type,
            latest_run_time=detected_at.isoformat(),
            latest_run_status=source_freshness_results_db_row.normalized_status,
            model_unique_id=source_freshness_results_db_row.unique_id,
            test_type=source_freshness_results_db_row.test_type,
            test_sub_type=source_freshness_results_db_row.test_sub_type,
            description=source_freshness_results_db_row.freshness_description,
            configuration=configuration,
        )
