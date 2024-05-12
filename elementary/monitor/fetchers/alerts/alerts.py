import json
from typing import Dict, List, Optional

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.config.config import Config
from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    AlertTypes,
    PendingAlertSchema,
)
from elementary.utils.log import get_logger
from elementary.utils.time import get_now_utc_str

logger = get_logger(__name__)


class AlertsFetcher(FetcherClient):
    def __init__(
        self,
        dbt_runner: BaseDbtRunner,
        config: Config,
    ):
        super().__init__(dbt_runner)
        self.config = config

    def skip_alerts(
        self,
        alerts_to_skip: List[PendingAlertSchema],
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

    def query_pending_alerts(
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

    def query_last_alert_times(
        self, days_back: int, type: Optional[AlertTypes] = None
    ) -> Dict[str, str]:
        response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_last_alert_sent_times",
            macro_args={"days_back": days_back, "type": type.value if type else None},
        )
        return json.loads(response[0])

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
