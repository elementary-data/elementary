import json
from typing import List, Optional

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.monitor.fetchers.source_freshnesses.schema import (
    SourceFreshnessResultDBRowSchema,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class SourceFreshnessesFetcher(FetcherClient):
    def __init__(self, dbt_runner: BaseDbtRunner):
        super().__init__(dbt_runner)

    def get_source_freshness_results_db_rows(
        self,
        days_back: Optional[int] = 7,
        invocations_per_test: int = 720,
    ) -> List[SourceFreshnessResultDBRowSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_source_freshness_results",
            macro_args=dict(
                days_back=days_back,
                invocations_per_test=invocations_per_test,
            ),
        )
        source_freshness_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        source_freshness_results = [
            SourceFreshnessResultDBRowSchema(**source_freshness_result)
            for source_freshness_result in source_freshness_results
        ]
        return source_freshness_results
