from datetime import datetime
from typing import Dict, List

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.config.config import Config
from elementary.monitor.fetchers.alerts.alerts import AlertsFetcher
from elementary.monitor.fetchers.alerts.schema.pending_alerts import PendingAlertSchema
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class AlertsAPI(APIClient):
    def __init__(
        self,
        dbt_runner: BaseDbtRunner,
        config: Config,
    ):
        super().__init__(dbt_runner)
        self.config = config
        self.alerts_fetcher = AlertsFetcher(
            dbt_runner=self.dbt_runner,
            config=self.config,
        )

    def get_new_alerts(self, days_back: int) -> List[PendingAlertSchema]:
        pending_alerts = self.alerts_fetcher.query_pending_alerts(days_back=days_back)
        return pending_alerts

    def get_alerts_last_sent_times(self, days_back: int) -> Dict[str, datetime]:
        alerts_last_sent_times = self.alerts_fetcher.query_last_alert_times(
            days_back=days_back
        )
        last_sent_times = dict()
        for alert_class_id, last_sent_time_as_string in alerts_last_sent_times.items():
            last_sent_times.update(
                {alert_class_id: datetime.fromisoformat(last_sent_time_as_string)}
            )
        return last_sent_times

    def skip_alerts(
        self,
        alerts_to_skip: List[PendingAlertSchema],
    ) -> None:
        self.alerts_fetcher.skip_alerts(alerts_to_skip=alerts_to_skip)

    def update_sent_alerts(self, alert_ids: List[str]) -> None:
        self.alerts_fetcher.update_sent_alerts(alert_ids=alert_ids)
