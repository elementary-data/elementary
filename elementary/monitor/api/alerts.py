import copy
import json
from typing import Callable, List

from elementary.clients.api.api import APIClient
from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor.alerts.alerts import Alerts, AlertsQueryResult
from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class AlertsAPI(APIClient):
    def __init__(
        self, dbt_runner: DbtRunner, config: Config, elementary_database_and_schema: str
    ):
        super().__init__(dbt_runner)
        self.config = config
        self.elementary_database_and_schema = elementary_database_and_schema

    def query(self, days_back: int, disable_samples: bool = False) -> Alerts:
        return Alerts(
            tests=self._query_test_alerts(days_back, disable_samples),
            models=self._query_model_alerts(days_back),
            source_freshnesses=self._query_source_freshness_alerts(days_back),
        )

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
        try:
            normalized_alert = copy.deepcopy(alert)
            test_meta = try_load_json(normalized_alert.get("test_meta")) or {}
            model_meta = try_load_json(normalized_alert.get("model_meta")) or {}

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

            model_slack_channel = model_meta.get("channel")
            test_slack_channel = test_meta.get("channel")
            slack_channel = test_slack_channel or model_slack_channel

            normalized_alert["subscribers"] = subscribers
            normalized_alert["slack_channel"] = slack_channel
            return normalized_alert
        except Exception:
            logger.error(
                f"Failed to extract alert subscribers and alert custom slack channel {alert.get('id')}. Ignoring it for now and main slack channel will be used"
            )
            return alert

    def update_sent_alerts(self, alert_ids: List[str], table_name: str) -> None:
        alert_ids_chunks = self._split_list_to_chunks(alert_ids)
        for alert_ids_chunk in alert_ids_chunks:
            self.dbt_runner.run_operation(
                macro_name="update_sent_alerts",
                macro_args={"alert_ids": alert_ids_chunk, "table_name": table_name},
                json_logs=False,
            )

    @staticmethod
    def _split_list_to_chunks(items: list, chunk_size: int = 50) -> List[List]:
        chunk_list = []
        for i in range(0, len(items), chunk_size):
            chunk_list.append(items[i : i + chunk_size])
        return chunk_list
