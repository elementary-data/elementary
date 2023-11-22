import json
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Union

from alive_progress import alive_it

from elementary.config.config import Config
from elementary.monitor.alerts.group_of_alerts import GroupedByTableAlerts, GroupingType
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.api.alerts.alerts import AlertsAPI
from elementary.monitor.api.alerts.schema import AlertsSchema
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.integrations import (
    Integrations,
)
from elementary.monitor.data_monitoring.data_monitoring import DataMonitoring
from elementary.monitor.data_monitoring.schema import ResourceType
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class DataMonitoringAlerts(DataMonitoring):
    def __init__(
        self,
        config: Config,
        tracking: Optional[Tracking] = None,
        filter: Optional[str] = None,
        force_update_dbt_package: bool = False,
        disable_samples: bool = False,
        send_test_message_on_success: bool = False,
        global_suppression_interval: int = 0,
        override_config: bool = False,
    ):
        super().__init__(
            config, tracking, force_update_dbt_package, disable_samples, filter
        )

        self.global_suppression_interval = global_suppression_interval
        self.override_config = override_config
        self.alerts_api = AlertsAPI(
            self.internal_dbt_runner,
            self.config,
            self.elementary_database_and_schema,
            self.global_suppression_interval,
            self.override_config,
        )
        self.sent_alert_count = 0
        self.send_test_message_on_success = send_test_message_on_success
        self.override_config_defaults = override_config
        self.alerts_integraion = self._get_integration_client()

    def _get_integration_client(self) -> BaseIntegration:
        return Integrations.get_integration(
            config=self.config,
            tracking=self.tracking,
            override_config_defaults=self.override_config_defaults,
        )

    def _fetch_data(self, days_back: int) -> AlertsSchema:
        return self.alerts_api.get_new_alerts(
            days_back=days_back,
            disable_samples=self.disable_samples,
            filter=self.filter.get_filter(),
        )

    def _format_alerts(
        self,
        alerts: AlertsSchema,
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
        for alert in alerts.all_alerts:
            group_alerts_by = alert.group_alerts_by or default_alerts_group_by_strategy
            formatted_alert = alert.format_alert(
                timezone=self.config.timezone,
                report_url=self.config.report_url,
                elementary_database_and_schema=self.elementary_database_and_schema,
                global_suppression_interval=self.global_suppression_interval,
                override_config=self.override_config,
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
        self.alerts_integraion.send_test_message(
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

        sent_alert_ids_by_type: Dict[ResourceType, List[str]] = {
            ResourceType.TEST: [],
            ResourceType.MODEL: [],
            ResourceType.SOURCE_FRESHNESS: [],
        }

        alerts_with_progress_bar = alive_it(alerts, title="Sending alerts")
        sent_successfully_alerts = []
        for alert in alerts_with_progress_bar:
            sent_successfully = self.alerts_integraion.send_alert(alert=alert)
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

        for sent_alert in sent_successfully_alerts:
            if isinstance(sent_alert, TestAlertModel):
                sent_alert_ids_by_type[ResourceType.TEST].append(sent_alert.id)
            elif isinstance(sent_alert, ModelAlertModel):
                sent_alert_ids_by_type[ResourceType.MODEL].append(sent_alert.id)
            elif isinstance(sent_alert, SourceFreshnessAlertModel):
                sent_alert_ids_by_type[ResourceType.SOURCE_FRESHNESS].append(
                    sent_alert.id
                )

        # Now update as sent:
        for resource_type, alert_ids in sent_alert_ids_by_type.items():
            self.sent_alert_count += len(alert_ids)
            self.alerts_api.update_sent_alerts(alert_ids, resource_type)

        # Now update sent alerts counter:
        self.execution_properties["sent_alert_count"] = self.sent_alert_count

    def _skip_alerts(self, alerts: AlertsSchema):
        self.alerts_api.skip_alerts(alerts.tests.skip, ResourceType.TEST)
        self.alerts_api.skip_alerts(alerts.models.skip, ResourceType.MODEL)
        self.alerts_api.skip_alerts(
            alerts.source_freshnesses.skip,
            ResourceType.SOURCE_FRESHNESS,
        )

    def run_alerts(
        self,
        days_back: int,
        dbt_full_refresh: bool = False,
        dbt_vars: Optional[dict] = None,
    ) -> bool:
        logger.info("Running internal dbt run to aggregate alerts")
        success = self.internal_dbt_runner.run(
            models="elementary_cli.alerts", full_refresh=dbt_full_refresh, vars=dbt_vars
        )
        self.execution_properties["alerts_run_success"] = success
        if not success:
            logger.info("Could not aggregate alerts successfully")
            self.success = False
            self.execution_properties["success"] = self.success
            return self.success

        alerts = self._fetch_data(days_back)
        self._skip_alerts(alerts)
        formatted_alerts = self._format_alerts(alerts=alerts)
        self._send_alerts(formatted_alerts)
        if self.send_test_message_on_success and alerts.count == 0:
            self._send_test_message()
        self.execution_properties["alert_count"] = alerts.count
        self.execution_properties["elementary_test_count"] = len(
            [
                alert
                for alert in formatted_alerts
                if isinstance(alert, TestAlertModel) and alert.is_elementary_test
            ]
        )
        self.execution_properties["has_subscribers"] = any(
            alert.subscribers for alert in alerts.all_alerts
        )
        self.execution_properties["run_end"] = True
        self.execution_properties["success"] = self.success
        return self.success
