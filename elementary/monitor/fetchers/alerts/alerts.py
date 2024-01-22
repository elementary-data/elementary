import json
from typing import Dict, List, Optional, Union

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.config.config import Config
from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    AlertTypes,
    PendingAlertSchema,
    PendingModelAlertSchema,
    PendingSourceFreshnessAlertSchema,
    PendingTestAlertSchema,
)
from elementary.utils.log import get_logger
from elementary.utils.time import get_now_utc_str

logger = get_logger(__name__)


class AlertsFetcher(FetcherClient):
    def __init__(
        self,
        dbt_runner: BaseDbtRunner,
        config: Config,
        elementary_database_and_schema: str,
    ):
        super().__init__(dbt_runner)
        self.config = config
        self.elementary_database_and_schema = elementary_database_and_schema

    def skip_alerts(
        self,
        alerts_to_skip: Union[
            List[PendingTestAlertSchema],
            List[PendingModelAlertSchema],
            List[PendingSourceFreshnessAlertSchema],
        ],
    ):
        alert_ids = [alert.id for alert in alerts_to_skip]
        alert_ids_chunks = self._split_list_to_chunks(alert_ids)
        logger.info("Update skipped alerts")
        for alert_ids_chunk in alert_ids_chunks:
            self.dbt_runner.run(
                select="elementary_cli.update_alerts.update_skipped_alerts",
                vars={"alert_ids": alert_ids_chunk},
                quiet=True,
            )

    def _query_pending_alerts(
        self, days_back: int, type: Optional[AlertTypes] = None
    ) -> List[PendingAlertSchema]:
        pending_alerts_results = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_pending_alerts",
            macro_args={"days_back": days_back, "type": type.value if type else None},
        )
        return [
            PendingAlertSchema(**result)
            for result in json.loads(pending_alerts_results[0])
        ]

    def _query_last_alert_times(
        self, days_back: int, type: Optional[AlertTypes] = None
    ) -> Dict[str, str]:
        response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_last_alert_sent_times",
            macro_args={"days_back": days_back, "type": type.value if type else None},
        )
        return json.loads(response[0])

    def query_pending_test_alerts(self, days_back: int) -> List[PendingTestAlertSchema]:
        logger.info("Querying test alerts.")
        pending_test_alerts_results = self._query_pending_alerts(
            days_back=days_back, type=AlertTypes.TEST
        )
        return [
            PendingTestAlertSchema(**result.data)
            for result in pending_test_alerts_results
        ]

    def query_pending_model_alerts(
        self, days_back: int
    ) -> List[PendingModelAlertSchema]:
        logger.info("Querying model alerts.")
        pending_model_alerts_results = self._query_pending_alerts(
            days_back=days_back, type=AlertTypes.MODEL
        )
        return [
            PendingModelAlertSchema(**result.data)
            for result in pending_model_alerts_results
        ]

    def query_pending_source_freshness_alerts(
        self, days_back: int
    ) -> List[PendingSourceFreshnessAlertSchema]:
        logger.info("Querying source freshness alerts.")
        pending_source_freshness_alerts_results = self._query_pending_alerts(
            days_back=days_back, type=AlertTypes.SOURCE_FRESHNESS
        )
        return [
            PendingSourceFreshnessAlertSchema(**result.data)
            for result in pending_source_freshness_alerts_results
        ]

    def query_last_test_alert_times(self, days_back: int) -> Dict[str, str]:
        logger.info("Querying test alerts last sent times.")
        return self._query_last_alert_times(days_back=days_back, type=AlertTypes.TEST)

    def query_last_model_alert_times(self, days_back: int) -> Dict[str, str]:
        logger.info("Querying model alerts last sent times.")
        return self._query_last_alert_times(days_back=days_back, type=AlertTypes.MODEL)

    def query_last_source_freshness_alert_times(self, days_back: int) -> Dict[str, str]:
        logger.info("Querying source freshness alerts last sent times.")
        return self._query_last_alert_times(
            days_back=days_back, type=AlertTypes.SOURCE_FRESHNESS
        )

    def update_sent_alerts(self, alert_ids: List[str]) -> None:
        alert_ids_chunks = self._split_list_to_chunks(alert_ids)
        logger.info("Update sent alerts")
        for alert_ids_chunk in alert_ids_chunks:
            self.dbt_runner.run(
                select="elementary_cli.update_alerts.update_sent_alerts",
                vars={
                    "alert_ids": alert_ids_chunk,
                    "sent_at": get_now_utc_str(),
                },
                quiet=True,
            )

    @staticmethod
    def _split_list_to_chunks(items: list, chunk_size: int = 50) -> List[List]:
        chunk_list = []
        for i in range(0, len(items), chunk_size):
            chunk_list.append(items[i : i + chunk_size])
        return chunk_list
