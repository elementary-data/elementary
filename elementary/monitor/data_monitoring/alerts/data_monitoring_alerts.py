import json
from collections import defaultdict
from datetime import datetime
from typing import DefaultDict, Dict, List, Optional, Union

from alive_progress import alive_it

from elementary.config.config import Config
from elementary.monitor.alerts.group_of_alerts import GroupedByTableAlerts, GroupingType
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.api.alerts.alert_filters import filter_alerts
from elementary.monitor.api.alerts.alerts import AlertsAPI
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.integrations import (
    Integrations,
)
from elementary.monitor.data_monitoring.alerts.schema import SortedAlertsSchema
from elementary.monitor.data_monitoring.data_monitoring import DataMonitoring
from elementary.monitor.data_monitoring.schema import FiltersSchema
from elementary.monitor.fetchers.alerts.schema.pending_alerts import PendingAlertSchema
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger
from elementary.utils.time import convert_time_to_timezone

logger = get_logger(__name__)


class DataMonitoringAlerts(DataMonitoring):
    def __init__(
        self,
        config: Config,
        tracking: Optional[Tracking] = None,
        selector_filter: FiltersSchema = FiltersSchema(),
        force_update_dbt_package: bool = False,
        disable_samples: bool = False,
        send_test_message_on_success: bool = False,
        global_suppression_interval: int = 0,
        override_config: bool = False,
        populate_data: bool = True,
    ):
        super().__init__(
            config, tracking, force_update_dbt_package, disable_samples, selector_filter
        )

        self.global_suppression_interval = global_suppression_interval
        self.override_config = override_config
        self.should_populate_data = populate_data
        self.alerts_api = AlertsAPI(
            self.internal_dbt_runner,
            self.config,
        )
        self.sent_alert_count = 0
        self.send_test_message_on_success = send_test_message_on_success
        self.override_config_defaults = override_config
        self.alerts_integration = self._get_integration_client()

    def _get_integration_client(self) -> BaseIntegration:
        return Integrations.get_integration(
            config=self.config,
            tracking=self.tracking,
            override_config_defaults=self.override_config_defaults,
        )

    def _populate_data(
        self,
        days_back: Optional[int] = None,
        dbt_full_refresh: bool = False,
        dbt_vars: Optional[dict] = None,
    ) -> bool:
        logger.info("Running internal dbt run to populate alerts")
        vars = dbt_vars or dict()
        if days_back:
            vars.update(days_back=days_back)
        success = self.internal_dbt_runner.run(
            models="elementary_cli.alerts.alerts_v2",
            full_refresh=dbt_full_refresh,
            vars=vars,
        )
        self.execution_properties["alerts_populate_success"] = success
        if not success:
            logger.info("Could not populate alerts successfully")

        return success

    def _fetch_data(self, days_back: int) -> List[PendingAlertSchema]:
        return self.alerts_api.get_new_alerts(
            days_back=days_back,
        )

    def _filter_data(self, data: List[PendingAlertSchema]) -> List[PendingAlertSchema]:
        return filter_alerts(data, alerts_filter=self.selector_filter)

    def _fetch_last_sent_times(self, days_back: int) -> Dict[str, datetime]:
        return self.alerts_api.get_alerts_last_sent_times(
            days_back=days_back,
        )

    def _sort_alerts(
        self,
        alerts: List[PendingAlertSchema],
        alerts_last_sent_times: Dict[str, datetime],
    ) -> SortedAlertsSchema:
        suppressed_alerts = self._get_suppressed_alerts(alerts, alerts_last_sent_times)
        latest_alert_ids = self._get_latest_alerts(alerts)
        alerts_to_skip = []
        alerts_to_send = []

        for valid_alert in alerts:
            if (
                valid_alert.id in suppressed_alerts
                or valid_alert.id not in latest_alert_ids
            ):
                alerts_to_skip.append(valid_alert)
            else:
                alerts_to_send.append(valid_alert)
        return SortedAlertsSchema(send=alerts_to_send, skip=alerts_to_skip)

    def _get_suppressed_alerts(
        self,
        alerts: List[PendingAlertSchema],
        alerts_last_sent_times: Dict[str, datetime],
    ) -> List[str]:
        suppressed_alerts = []
        current_time_utc = convert_time_to_timezone(datetime.utcnow())
        for alert in alerts:
            alert_class_id = alert.alert_class_id
            suppression_interval = alert.data.get_suppression_interval(
                self.global_suppression_interval,
                self.override_config,
            )
            last_sent_time = alerts_last_sent_times.get(alert_class_id)
            is_alert_in_suppression = (
                (
                    current_time_utc - convert_time_to_timezone(last_sent_time)
                ).total_seconds()
                / 3600
                <= suppression_interval
                if last_sent_time
                else False
            )
            if is_alert_in_suppression:
                suppressed_alerts.append(alert.id)

        return suppressed_alerts

    @staticmethod
    def _get_latest_alerts(
        alerts: List[PendingAlertSchema],
    ) -> List[str]:
        alert_last_times: DefaultDict[
            str,
            Optional[PendingAlertSchema],
        ] = defaultdict(lambda: None)
        latest_alert_ids = []
        for alert in alerts:
            alert_class_id = alert.alert_class_id
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

    def _format_alerts(
        self,
        alerts: List[PendingAlertSchema],
    ) -> List[
        Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ]
    ]:
        formatted_alerts = []
        grouped_by_table_alerts = []
        model_ids_to_alerts_map = defaultdict(lambda: [])

        default_alerts_group_by_strategy = GroupingType(
            self.config.slack_group_alerts_by
        )
        for alert in alerts:
            group_alerts_by = (
                alert.data.group_alerts_by or default_alerts_group_by_strategy
            )
            formatted_alert = alert.data.format_alert(
                timezone=self.config.timezone,
                report_url=self.config.report_url,
                elementary_database_and_schema=self.elementary_database_and_schema,
                global_suppression_interval=self.global_suppression_interval,
                override_config=self.override_config,
                disable_samples=self.disable_samples,
            )
            try:
                grouping_type = GroupingType(group_alerts_by)
                if grouping_type == GroupingType.BY_TABLE:
                    model_ids_to_alerts_map[formatted_alert.model_unique_id].append(
                        formatted_alert
                    )
                else:
                    formatted_alerts.append(formatted_alert)
            except ValueError:
                formatted_alerts.append(formatted_alert)
                logger.error(
                    f"Failed to extract value as a group-by config: '{group_alerts_by}'. Allowed Values: {list(GroupingType.__members__.keys())} Ignoring it for now and default grouping strategy will be used"
                )

        for alerts_by_model in model_ids_to_alerts_map.values():
            grouped_by_table_alerts.append(GroupedByTableAlerts(alerts=alerts_by_model))

        self.execution_properties["had_group_by_table"] = (
            len(grouped_by_table_alerts) > 0
        )
        self.execution_properties["had_group_by_alert"] = len(formatted_alerts) > 0

        all_alerts = formatted_alerts + grouped_by_table_alerts
        return sorted(
            all_alerts,
            key=lambda alert: alert.detected_at or datetime.max,
        )

    def _send_test_message(self):
        self.alerts_integration.send_test_message(
            channel_name=self.config.slack_channel_name
        )

    def _send_alerts(
        self,
        alerts: List[
            Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
                GroupedByTableAlerts,
            ]
        ],
    ):
        if not alerts:
            self.execution_properties["sent_alert_count"] = self.sent_alert_count
            return

        alerts_with_progress_bar = alive_it(alerts, title="Sending alerts")
        sent_successfully_alerts = []
        for alert in alerts_with_progress_bar:
            sent_successfully = self.alerts_integration.send_alert(alert=alert)
            if sent_successfully:
                if isinstance(alert, GroupedByTableAlerts):
                    sent_successfully_alerts.extend(alert.alerts)
                else:
                    sent_successfully_alerts.append(alert)
            else:
                if isinstance(alert, GroupedByTableAlerts):
                    for grouped_alert in alert.alerts:
                        logger.error(
                            f"Could not send the alert - {grouped_alert.id}. Full alert: {json.dumps(grouped_alert.data)}"
                        )
                else:
                    logger.error(
                        f"Could not send the alert - {alert.id}. Full alert: {json.dumps(alert.data)}"
                    )
                self.success = False

        # Now update as sent:
        self.sent_alert_count = len(sent_successfully_alerts)
        self._update_sent_alerts([alert.id for alert in sent_successfully_alerts])

        # Now update sent alerts counter:
        self.execution_properties["sent_alert_count"] = self.sent_alert_count

    def _update_sent_alerts(self, alert_ids: List[str]):
        self.alerts_api.update_sent_alerts(alert_ids=alert_ids)

    def _skip_alerts(self, alerts: List[PendingAlertSchema]):
        self.alerts_api.skip_alerts(alerts)

    def run_alerts(
        self,
        days_back: int,
        dbt_full_refresh: bool = False,
        dbt_vars: Optional[dict] = None,
    ) -> bool:
        # Populate data
        if self.should_populate_data:
            popopulated_data_successfully = self._populate_data(
                days_back=days_back,
                dbt_full_refresh=dbt_full_refresh,
                dbt_vars=dbt_vars,
            )
            if not popopulated_data_successfully:
                self.success = False
                self.execution_properties["success"] = self.success
                return self.success

        # Fetch and filter data
        alerts = self._fetch_data(days_back)
        alerts = self._filter_data(alerts)
        alerts_last_sent_times = self._fetch_last_sent_times(days_back)
        sorted_alerts = self._sort_alerts(
            alerts=alerts, alerts_last_sent_times=alerts_last_sent_times
        )
        alerts_to_skip = sorted_alerts.skip
        alerts_to_send = sorted_alerts.send

        # Skip alerts
        self._skip_alerts(alerts_to_skip)

        # Format alerts
        formatted_alerts = self._format_alerts(alerts=alerts_to_send)

        # Send alerts
        self._send_alerts(formatted_alerts)

        if self.send_test_message_on_success and len(alerts_to_send) == 0:
            self._send_test_message()
        self.execution_properties["alert_count"] = len(alerts_to_send)
        self.execution_properties["elementary_test_count"] = len(
            [
                alert
                for alert in formatted_alerts
                if isinstance(alert, TestAlertModel) and alert.is_elementary_test
            ]
        )
        self.execution_properties["has_subscribers"] = any(
            alert.data.subscribers for alert in alerts_to_send
        )
        self.execution_properties["run_end"] = True
        self.execution_properties["success"] = self.success
        return self.success
