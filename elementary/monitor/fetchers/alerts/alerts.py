import json
from typing import Callable, Dict, List, Sequence

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.config.config import Config
from elementary.monitor.alerts.alert import Alert
from elementary.monitor.alerts.alerts import AlertsQueryResult
from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.monitor.fetchers.alerts.normalized_alert import NormalizedAlert
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

    def skip_alerts(self, alerts_to_skip: Sequence[Alert], table_name: str):
        alert_ids = [alert.id for alert in alerts_to_skip]
        alert_ids_chunks = self._split_list_to_chunks(alert_ids)
        logger.info(f'Update skipped alerts at "{table_name}"')
        for alert_ids_chunk in alert_ids_chunks:
            self.dbt_runner.run(
                select="elementary_cli.update_alerts.update_skipped_alerts",
                vars={
                    "alert_ids": alert_ids_chunk,
                    "table_name": table_name,
                },
                quiet=True,
            )

    def query_pending_test_alerts(
        self, days_back: int, disable_samples: bool = False
    ) -> AlertsQueryResult[TestAlert]:
        logger.info("Querying test alerts.")
        return self._query_alert_type(
            {
                "macro_name": "elementary_cli.get_pending_test_alerts",
                "macro_args": {
                    "days_back": days_back,
                    "disable_samples": disable_samples,
                },
            },
            TestAlert.create_test_alert_from_dict,
        )

    def query_pending_model_alerts(
        self, days_back: int
    ) -> AlertsQueryResult[ModelAlert]:
        logger.info("Querying model alerts.")
        res = self._query_alert_type(
            {
                "macro_name": "elementary_cli.get_pending_model_alerts",
                "macro_args": {"days_back": days_back},
            },
            ModelAlert,
        )
        return res

    def query_pending_source_freshness_alerts(
        self, days_back: int
    ) -> AlertsQueryResult[SourceFreshnessAlert]:
        logger.info("Querying source freshness alerts.")
        return self._query_alert_type(
            {
                "macro_name": "elementary_cli.get_pending_source_freshness_alerts",
                "macro_args": {"days_back": days_back},
            },
            SourceFreshnessAlert,
        )

    def query_last_test_alert_times(self, days_back: int) -> Dict[str, str]:
        logger.info("Querying test alerts last sent times.")
        response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_last_test_alert_sent_times",
            macro_args={"days_back": days_back},
        )
        return json.loads(response[0])

    def query_last_model_alert_times(self, days_back: int) -> Dict[str, str]:
        logger.info("Querying model alerts last sent times.")
        response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_last_model_alert_sent_times",
            macro_args={"days_back": days_back},
        )
        return json.loads(response[0])

    def query_last_source_freshness_alert_times(self, days_back: int) -> Dict[str, str]:
        logger.info("Querying source freshness alerts last sent times.")
        response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_last_source_freshness_alert_sent_times",
            macro_args={"days_back": days_back},
        )
        return json.loads(response[0])

    def _query_alert_type(
        self, run_operation_args: dict, alert_factory_func: Callable
    ) -> AlertsQueryResult:
        raw_alerts = self.dbt_runner.run_operation(**run_operation_args)
        alerts = []
        malformed_alerts = []
        if raw_alerts:
            alert_dicts = json.loads(raw_alerts[0])
            for alert_dict in alert_dicts:
                normalized_alert = self._normalize_alert(alert=alert_dict)
                try:
                    alerts.append(
                        alert_factory_func(
                            elementary_database_and_schema=self.elementary_database_and_schema,
                            timezone=self.config.timezone,
                            report_url=self.config.report_url,
                            **normalized_alert,
                        )
                    )
                except Exception:
                    malformed_alerts.append(
                        MalformedAlert(id=normalized_alert["id"], data=normalized_alert)
                    )
        if malformed_alerts:
            logger.error("Failed to parse some alerts.")
        return AlertsQueryResult(alerts, malformed_alerts)

    @classmethod
    def _normalize_alert(cls, alert: dict) -> dict:
        return NormalizedAlert(alert).get_normalized_alert()

    def update_sent_alerts(self, alert_ids: List[str], table_name: str) -> None:
        alert_ids_chunks = self._split_list_to_chunks(alert_ids)
        logger.info(f'Update sent alerts at "{table_name}"')
        for alert_ids_chunk in alert_ids_chunks:
            self.dbt_runner.run(
                select="elementary_cli.update_alerts.update_sent_alerts",
                vars={
                    "alert_ids": alert_ids_chunk,
                    "sent_at": get_now_utc_str(),
                    "table_name": table_name,
                },
                quiet=True,
            )

    @staticmethod
    def _split_list_to_chunks(items: list, chunk_size: int = 50) -> List[List]:
        chunk_list = []
        for i in range(0, len(items), chunk_size):
            chunk_list.append(items[i : i + chunk_size])
        return chunk_list
