from collections import defaultdict
from datetime import datetime
from typing import DefaultDict, Dict, List, Optional, Union

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor.api.alerts.alert_filters import filter_alerts
from elementary.monitor.api.alerts.schema import (
    AlertsSchema,
    ModelAlertsSchema,
    SortedAlertsSchema,
    SourceFreshnessAlertsSchema,
    TestAlertsSchema,
)
from elementary.monitor.data_monitoring.schema import ResourceType, SelectorFilterSchema
from elementary.monitor.fetchers.alerts.alerts import AlertsFetcher
from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    PendingModelAlertSchema,
    PendingSourceFreshnessAlertSchema,
    PendingTestAlertSchema,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class AlertsAPI(APIClient):
    def __init__(
        self,
        dbt_runner: DbtRunner,
        config: Config,
        elementary_database_and_schema: str,
        global_suppression_interval: int,
        override_meta_suppression_interval: bool = False,
    ):
        super().__init__(dbt_runner)
        self.config = config
        self.elementary_database_and_schema = elementary_database_and_schema
        self.alerts_fetcher = AlertsFetcher(
            dbt_runner=self.dbt_runner,
            config=self.config,
            elementary_database_and_schema=self.elementary_database_and_schema,
        )
        self.global_suppression_interval = global_suppression_interval
        self.override_meta_suppression_interval = override_meta_suppression_interval

    def get_new_alerts(
        self,
        days_back: int,
        disable_samples: bool = False,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ) -> AlertsSchema:
        new_test_alerts = self.get_test_alerts(days_back, disable_samples, filter)
        new_model_alerts = self.get_model_alerts(days_back, filter)
        new_source_freshness_alerts = self.get_source_freshness_alerts(
            days_back, filter
        )
        return AlertsSchema(
            tests=new_test_alerts,
            models=new_model_alerts,
            source_freshnesses=new_source_freshness_alerts,
        )

    def get_test_alerts(
        self,
        days_back: int,
        disable_samples: bool = False,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ) -> TestAlertsSchema:
        pending_test_alerts = self.alerts_fetcher.query_pending_test_alerts(
            days_back, disable_samples
        )
        filtered_pending_test_alerts = filter_alerts(pending_test_alerts, filter)
        last_alert_sent_times = self.alerts_fetcher.query_last_test_alert_times(
            days_back
        )
        test_alerts = self._sort_alerts(
            filtered_pending_test_alerts, last_alert_sent_times
        )
        # Mypy issues with Union types. We need to ignore the are-type
        return TestAlertsSchema(send=test_alerts.send, skip=test_alerts.skip)  # type: ignore[arg-type]

    def get_model_alerts(
        self,
        days_back: int,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ) -> ModelAlertsSchema:
        pending_model_alerts = self.alerts_fetcher.query_pending_model_alerts(days_back)
        filtered_pending_model_alerts = filter_alerts(pending_model_alerts, filter)
        last_alert_sent_times = self.alerts_fetcher.query_last_model_alert_times(
            days_back
        )
        model_alerts = self._sort_alerts(
            filtered_pending_model_alerts, last_alert_sent_times
        )
        # Mypy issues with Union types. We need to ignore the are-type
        return ModelAlertsSchema(send=model_alerts.send, skip=model_alerts.skip)  # type: ignore[arg-type]

    def get_source_freshness_alerts(
        self,
        days_back: int,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ) -> SourceFreshnessAlertsSchema:
        pending_source_freshness_alerts = (
            self.alerts_fetcher.query_pending_source_freshness_alerts(days_back)
        )
        filtered_pending_source_freshness_alert = filter_alerts(
            pending_source_freshness_alerts, filter
        )
        last_alert_sent_times = (
            self.alerts_fetcher.query_last_source_freshness_alert_times(days_back)
        )
        source_freshness_alerts = self._sort_alerts(
            filtered_pending_source_freshness_alert, last_alert_sent_times
        )
        # Mypy issues with Union types. We need to ignore the are-type
        return SourceFreshnessAlertsSchema(
            send=source_freshness_alerts.send, skip=source_freshness_alerts.skip  # type: ignore[arg-type]
        )

    def skip_alerts(
        self,
        alerts_to_skip: Union[
            List[PendingTestAlertSchema],
            List[PendingModelAlertSchema],
            List[PendingSourceFreshnessAlertSchema],
        ],
        resource_type: ResourceType,
    ) -> None:
        self.alerts_fetcher.skip_alerts(
            alerts_to_skip=alerts_to_skip, resource_type=resource_type
        )

    def update_sent_alerts(
        self, alert_ids: List[str], resource_type: ResourceType
    ) -> None:
        self.alerts_fetcher.update_sent_alerts(
            alert_ids=alert_ids, resource_type=resource_type
        )

    def _sort_alerts(
        self,
        pending_alerts: Union[
            List[PendingTestAlertSchema],
            List[PendingModelAlertSchema],
            List[PendingSourceFreshnessAlertSchema],
        ],
        last_alert_sent_times: Dict[str, str],
    ) -> SortedAlertsSchema:
        suppressed_alerts = self._get_suppressed_alerts(
            pending_alerts, last_alert_sent_times
        )
        latest_alert_ids = self._get_latest_alerts(pending_alerts)
        alerts_to_skip = []
        alerts_to_send = []

        for valid_alert in pending_alerts:
            if (
                valid_alert.id in suppressed_alerts
                or valid_alert.id not in latest_alert_ids
            ):
                alerts_to_skip.append(valid_alert)
            else:
                alerts_to_send.append(valid_alert)

        # Mypy issues with Union types. We need to ignore the are-type
        return SortedAlertsSchema(send=alerts_to_send, skip=alerts_to_skip)  # type: ignore[arg-type]

    def _get_suppressed_alerts(
        self,
        alerts: Union[
            List[PendingTestAlertSchema],
            List[PendingModelAlertSchema],
            List[PendingSourceFreshnessAlertSchema],
        ],
        last_alert_sent_times: Dict[str, str],
    ) -> List[str]:
        suppressed_alerts = []
        current_time_utc = datetime.utcnow()
        for alert in alerts:
            alert_class_id = alert.alert_class_id
            if alert_class_id is None:
                # Shouldn't happen, but logging in any case
                logger.debug("Alert without an id detected!")
                continue

            suppression_interval = alert.get_suppression_interval(
                self.global_suppression_interval,
                self.override_meta_suppression_interval,
            )
            last_sent_time = (
                datetime.fromisoformat(last_alert_sent_times[alert_class_id])
                if last_alert_sent_times.get(alert_class_id)
                else None
            )
            is_alert_in_suppression = (
                (current_time_utc - last_sent_time).total_seconds() / 3600
                <= suppression_interval
                if last_sent_time
                else False
            )
            if is_alert_in_suppression:
                suppressed_alerts.append(alert.id)

        return suppressed_alerts

    @staticmethod
    def _get_latest_alerts(
        alerts: Union[
            List[PendingTestAlertSchema],
            List[PendingModelAlertSchema],
            List[PendingSourceFreshnessAlertSchema],
        ],
    ) -> List[str]:
        alert_last_times: DefaultDict[
            str,
            Optional[
                Union[
                    PendingModelAlertSchema,
                    PendingSourceFreshnessAlertSchema,
                    PendingTestAlertSchema,
                ]
            ],
        ] = defaultdict(lambda: None)
        latest_alert_ids = []
        for alert in alerts:
            alert_class_id = alert.alert_class_id
            if alert_class_id is None:
                # Shouldn't happen, but logging in any case
                logger.debug("Alert without an id detected!")
                continue

            current_last_alert = alert_last_times[alert_class_id]
            alert_detected_at = alert.detected_at
            if (
                not current_last_alert
                or current_last_alert.detected_at < alert_detected_at
            ):
                alert_last_times[alert_class_id] = alert

        for alert_last_time in alert_last_times.values():
            if alert_last_time:
                latest_alert_ids.append(alert_last_time.id)
        return latest_alert_ids
