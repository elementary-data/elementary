from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Union

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor.alerts.alerts import Alerts, AlertsQueryResult, AlertType
from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.monitor.api.alerts.alert_filters import filter_alerts
from elementary.monitor.data_monitoring.schema import SelectorFilterSchema
from elementary.monitor.fetchers.alerts.alerts import AlertsFetcher
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class AlertsAPI(APIClient):
    def __init__(
        self,
        dbt_runner: DbtRunner,
        config: Config,
        elementary_database_and_schema: str,
    ):
        super().__init__(dbt_runner)
        self.config = config
        self.elementary_database_and_schema = elementary_database_and_schema
        self.alerts_fetcher = AlertsFetcher(
            dbt_runner=self.dbt_runner,
            config=self.config,
            elementary_database_and_schema=self.elementary_database_and_schema,
        )

    def get_new_alerts(
        self,
        days_back: int,
        disable_samples: bool = False,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ) -> Alerts:
        new_test_alerts = self.get_test_alerts(days_back, disable_samples, filter)
        new_model_alerts = self.get_model_alerts(days_back, filter)
        new_source_freshness_alerts = self.get_source_freshness_alerts(
            days_back, filter
        )
        return Alerts(
            tests=new_test_alerts,
            models=new_model_alerts,
            source_freshnesses=new_source_freshness_alerts,
        )

    def get_test_alerts(
        self,
        days_back: int,
        disable_samples: bool = False,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ) -> AlertsQueryResult[TestAlert]:
        pending_test_alerts = self.alerts_fetcher.query_pending_test_alerts(
            days_back, disable_samples
        )
        last_alert_sent_times = self.alerts_fetcher.query_last_test_alert_times(
            days_back
        )
        test_alerts = self._sort_alerts(
            pending_test_alerts, last_alert_sent_times, filter
        )
        return test_alerts

    def get_model_alerts(
        self,
        days_back: int,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ) -> AlertsQueryResult[ModelAlert]:
        pending_model_alerts = self.alerts_fetcher.query_pending_model_alerts(days_back)
        last_alert_sent_times = self.alerts_fetcher.query_last_model_alert_times(
            days_back
        )
        model_alerts = self._sort_alerts(
            pending_model_alerts, last_alert_sent_times, filter
        )
        return model_alerts

    def get_source_freshness_alerts(
        self,
        days_back: int,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ) -> AlertsQueryResult[SourceFreshnessAlert]:
        pending_source_freshness_alerts = (
            self.alerts_fetcher.query_pending_source_freshness_alerts(days_back)
        )
        last_alert_sent_times = (
            self.alerts_fetcher.query_last_source_freshness_alert_times(days_back)
        )
        source_freshness_alerts = self._sort_alerts(
            pending_source_freshness_alerts, last_alert_sent_times, filter
        )
        return source_freshness_alerts

    def skip_alerts(
        self, alerts_to_skip: List[Union[AlertType, MalformedAlert]], table_name: str
    ) -> None:
        self.alerts_fetcher.skip_alerts(
            alerts_to_skip=alerts_to_skip, table_name=table_name
        )

    def update_sent_alerts(self, alert_ids: List[str], table_name: str) -> None:
        self.alerts_fetcher.update_sent_alerts(
            alert_ids=alert_ids, table_name=table_name
        )

    def _sort_alerts(
        self,
        pending_alerts: Union[
            AlertsQueryResult[TestAlert],
            AlertsQueryResult[ModelAlert],
            AlertsQueryResult[SourceFreshnessAlert],
        ],
        last_alert_sent_times: Dict[str, str],
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ) -> Union[
        AlertsQueryResult[TestAlert],
        AlertsQueryResult[ModelAlert],
        AlertsQueryResult[SourceFreshnessAlert],
    ]:
        suppressed_alerts = self._get_suppressed_alerts(
            pending_alerts, last_alert_sent_times
        )
        latest_alert_ids = self._get_latest_alerts(pending_alerts)
        alerts_to_skip = []
        alerts_to_send = []
        malformed_alerts_to_send = []

        for alert in pending_alerts.alerts:
            if alert.id in suppressed_alerts or alert.id not in latest_alert_ids:
                alerts_to_skip.append(alert)
            else:
                alerts_to_send.append(alert)

        for alert in pending_alerts.malformed_alerts:
            if alert.id in suppressed_alerts or alert.id not in latest_alert_ids:
                alerts_to_skip.append(alert)
            else:
                malformed_alerts_to_send.append(alert)

        return AlertsQueryResult(
            alerts=filter_alerts(alerts_to_send, filter),
            malformed_alerts=filter_alerts(malformed_alerts_to_send, filter),
            alerts_to_skip=filter_alerts(alerts_to_skip, filter),
        )

    def _get_suppressed_alerts(
        self,
        alerts: Union[
            AlertsQueryResult[TestAlert],
            AlertsQueryResult[ModelAlert],
            AlertsQueryResult[SourceFreshnessAlert],
        ],
        last_alert_sent_times: Dict[str, str],
    ) -> List[str]:
        suppressed_alerts = []
        current_time_utc = datetime.utcnow()
        for alert in [*alerts.alerts, *alerts.malformed_alerts]:
            alert_class_id = alert.alert_class_id
            suppression_interval = alert.alert_suppression_interval
            last_sent_time = (
                datetime.fromisoformat(last_alert_sent_times.get(alert_class_id))
                if last_alert_sent_times.get(alert_class_id)
                else None
            )
            is_alert_in_suppression = (
                (current_time_utc - last_sent_time).seconds / 3600
                <= suppression_interval
                if last_sent_time
                else False
            )
            if is_alert_in_suppression:
                suppressed_alerts.append(alert.id)

        return suppressed_alerts

    def _get_latest_alerts(
        self,
        alerts: Union[
            AlertsQueryResult[TestAlert],
            AlertsQueryResult[ModelAlert],
            AlertsQueryResult[SourceFreshnessAlert],
        ],
    ) -> List[str]:
        alert_last_times = defaultdict(lambda: None)
        latest_alert_ids = []
        for alert in [*alerts.alerts, *alerts.malformed_alerts]:
            alert_class_id = alert.alert_class_id
            current_last_alert = alert_last_times[alert_class_id]
            alert_detected_at = alert.detected_at
            if (
                not current_last_alert
                or current_last_alert["detected_at"] < alert_detected_at
            ):
                alert_last_times[alert_class_id] = dict(
                    alert_id=alert.id, detected_at=alert_detected_at
                )

        for alert_last_time in alert_last_times.values():
            latest_alert_ids.append(alert_last_time.get("alert_id"))
        return latest_alert_ids
