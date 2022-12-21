import json
from collections import defaultdict
from datetime import datetime
from typing import Callable, Dict, List

from elementary.clients.api.api import APIClient
from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor.alerts.alert import Alert
from elementary.monitor.alerts.alerts import Alerts, AlertsQueryResult
from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.monitor.api.alerts.normalized_alert import NormalizedAlert
from elementary.utils.log import get_logger
from elementary.utils.time import get_now_utc_str

logger = get_logger(__name__)


class AlertsAPI(APIClient):
    def __init__(
        self, dbt_runner: DbtRunner, config: Config, elementary_database_and_schema: str
    ):
        super().__init__(dbt_runner)
        self.config = config
        self.elementary_database_and_schema = elementary_database_and_schema

    def get_new_alerts(self, days_back: int, disable_samples: bool = False) -> Alerts:
        test_alerts = self._query_test_alerts(days_back, disable_samples)
        model_alerts = self._query_model_alerts(days_back)
        source_freshness_alerts = self._query_source_freshness_alerts(days_back)

        new_test_alerts = self.skip_alerts(test_alerts, TestAlert.TABLE_NAME)
        new_model_alerts = self.skip_alerts(model_alerts, ModelAlert.TABLE_NAME)
        new_source_freshness_alerts = self.skip_alerts(
            source_freshness_alerts, SourceFreshnessAlert.TABLE_NAME
        )
        return Alerts(
            tests=new_test_alerts,
            models=new_model_alerts,
            source_freshnesses=new_source_freshness_alerts,
        )

    def skip_alerts(
        self, alerts: AlertsQueryResult[Alert], table_name: str
    ) -> AlertsQueryResult[Alert]:
        alerts_last_sent_time = self._get_alerts_last_sent_time(alerts)
        alerts_to_skip = self._get_alerts_to_skip(
            alerts=alerts, alerts_last_sent_time=alerts_last_sent_time
        )
        alert_ids_chunks = self._split_list_to_chunks(alerts_to_skip)
        for alert_ids_chunk in alert_ids_chunks:
            self.dbt_runner.run_operation(
                macro_name="update_skipped_alerts",
                macro_args={
                    "alert_ids": alert_ids_chunk,
                    "table_name": table_name,
                },
                json_logs=False,
            )
        alerts_to_send = []
        for alert in alerts.alerts:
            id = (
                alert.test_unique_id
                if isinstance(alert, TestAlert)
                else alert.unique_id
            )
            if id in alerts_to_skip:
                continue
            alerts_to_send.append(alert)

        malformed_alerts_to_send = []
        for alert in alerts.malformed_alerts:
            id = alert.data.get("unique_id") or alert.data.get("test_unique_id")
            if id in alerts_to_skip:
                continue
            malformed_alerts_to_send.append(alert)

        return AlertsQueryResult(
            alerts=alerts_to_send, malformed_alerts=malformed_alerts_to_send
        )

    def _get_alerts_last_sent_time(
        slef, alerts: AlertsQueryResult[Alert]
    ) -> Dict[str, str]:
        alerts_last_sent_time = defaultdict(lambda: None)
        for alert in alerts.alerts:
            if alert.alert_suppression.get("suppression_status") == "sent":
                if isinstance(alert, TestAlert):
                    current_last_sent_at = alerts_last_sent_time[alert.test_unique_id]
                    alerts_last_sent_time[alert.test_unique_id] = max(
                        alert.alert_suppression.get("sent_at"), current_last_sent_at
                    )
                else:
                    current_last_sent_at = alerts_last_sent_time[alert.unique_id]
                    alerts_last_sent_time[alert.unique_id] = max(
                        alert.alert_suppression.get("sent_at"), current_last_sent_at
                    )

        for alert in alerts.malformed_alerts:
            if (
                alert.data.get("alert_suppression", {}).get("suppression_status")
                == "sent"
            ):
                id = alert.data.get("unique_id") or alert.data.get("test_unique_id")
                current_last_sent_at = alerts_last_sent_time[id]
                alerts_last_sent_time[id] = max(
                    alert.data.get("sent_at"), current_last_sent_at
                )

        return alerts_last_sent_time

    def _get_alerts_to_skip(
        self, alerts: AlertsQueryResult[Alert], alerts_last_sent_time: Dict[str, str]
    ) -> List[str]:
        alerts_to_skip = []
        current_time_utc = datetime.utcnow()
        for alert in alerts.alerts:
            if alert.alert_suppression.get("suppression_status") == "pending":
                id = (
                    alert.test_unique_id
                    if isinstance(alert, TestAlert)
                    else alert.unique_id
                )
                suppression_interval = alert.alert_suppression_interval
                last_sent_time = (
                    datetime.fromisoformat(alerts_last_sent_time[id])
                    if alerts_last_sent_time[id]
                    else None
                )
                is_alert_in_suppression = (
                    (current_time_utc - last_sent_time).seconds / 3600
                    <= suppression_interval
                    if last_sent_time
                    else False
                )
                if is_alert_in_suppression:
                    alerts_to_skip.append(alert.id)

        for alert in alerts.malformed_alerts:
            if (
                alert.data.get("alert_suppression", {}).get("suppression_status")
                == "pending"
            ):
                id = alert.data.get("unique_id") or alert.data.get("test_unique_id")
                suppression_interval = alert.data.get("alert_suppression_interval")
                last_sent_time = (
                    datetime.fromisoformat(alerts_last_sent_time[id])
                    if alerts_last_sent_time[id]
                    else None
                )
                is_alert_in_suppression = (
                    (current_time_utc - last_sent_time).seconds / 3600
                    <= suppression_interval
                    if last_sent_time
                    else False
                )
                if is_alert_in_suppression:
                    alerts_to_skip.append(alert.id)

        return alerts_to_skip

    def _query_test_alerts(
        self, days_back: int, disable_samples: bool = False
    ) -> AlertsQueryResult[TestAlert]:
        logger.info("Querying test alerts.")
        return self._query_alert_type(
            {
                "macro_name": "get_new_test_alerts",
                "macro_args": {
                    "days_back": days_back,
                    "disable_samples": disable_samples,
                },
            },
            TestAlert.create_test_alert_from_dict,
        )

    def _query_model_alerts(self, days_back: int) -> AlertsQueryResult[ModelAlert]:
        logger.info("Querying model alerts.")
        return self._query_alert_type(
            {
                "macro_name": "get_new_model_alerts",
                "macro_args": {"days_back": days_back},
            },
            ModelAlert,
        )

    def _query_source_freshness_alerts(
        self, days_back: int
    ) -> AlertsQueryResult[SourceFreshnessAlert]:
        logger.info("Querying source freshness alerts.")
        return self._query_alert_type(
            {
                "macro_name": "get_new_source_freshness_alerts",
                "macro_args": {"days_back": days_back},
            },
            SourceFreshnessAlert,
        )

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
        for alert_ids_chunk in alert_ids_chunks:
            self.dbt_runner.run_operation(
                macro_name="update_sent_alerts",
                macro_args={
                    "alert_ids": alert_ids_chunk,
                    "sent_time": get_now_utc_str(),
                    "table_name": table_name,
                },
                json_logs=False,
            )

    @staticmethod
    def _split_list_to_chunks(items: list, chunk_size: int = 50) -> List[List]:
        chunk_list = []
        for i in range(0, len(items), chunk_size):
            chunk_list.append(items[i : i + chunk_size])
        return chunk_list
