import json
import re
from collections import defaultdict
from datetime import datetime
from typing import DefaultDict, List, Optional, Set, Tuple

from alive_progress import alive_it

from elementary.clients.slack.client import SlackClient
from elementary.clients.slack.schema import SlackMessageSchema
from elementary.config.config import Config
from elementary.monitor.alerts.alert import Alert
from elementary.monitor.alerts.alerts import Alerts
from elementary.monitor.alerts.group_of_alerts import (
    GroupingType,
    GroupOfAlerts,
    GroupOfAlertsBySingleAlert,
    GroupOfAlertsByTable,
)
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.monitor.api.alerts.alerts import AlertsAPI
from elementary.monitor.data_monitoring.data_monitoring import DataMonitoring
from elementary.monitor.data_monitoring.selector_filter import SelectorFilter
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


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

        SelectorFilter.validate_alert_selector(filter)
        super().__init__(
            config, tracking, force_update_dbt_package, disable_samples, filter
        )

        self.alerts_api = AlertsAPI(
            self.internal_dbt_runner,
            self.config,
            self.elementary_database_and_schema,
            global_suppression_interval,
            override_config,
        )
        self.sent_alert_count = 0
        self.send_test_message_on_success = send_test_message_on_success
        self.override_meta_slack_channel = override_config

        if self.slack_client is None:
            raise Exception("Could not initialize slack client!")

        # Type hint to mark that in this class the slack client cannot be None
        self.slack_client: SlackClient

    def _parse_emails_to_ids(self, emails: Optional[List[str]]) -> List[str]:
        def _regex_match_owner_email(potential_email: str) -> bool:
            email_regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"

            return bool(re.fullmatch(email_regex, potential_email))

        def _get_user_id(email: str) -> str:
            user_id = self.slack_client.get_user_id_from_email(email)
            return f"<@{user_id}>" if user_id else email

        if emails is None:
            return []

        return [
            _get_user_id(email) if _regex_match_owner_email(email) else email
            for email in emails
        ]

    def _fix_owners_and_subscribers(self, group_alert: GroupOfAlerts):
        """
        Only reason it's not in the __init__ of GroupOfAlerts, is that it goes to the slack API.
        This function is based on _parse_emails_to_ids which uses slack's API to get the handle for owners, subscribers.
        :param group_alert:
        :return:
        """
        for alert in group_alert.alerts:
            alert.owners = self._parse_emails_to_ids(alert.owners)
            alert.subscribers = self._parse_emails_to_ids(alert.subscribers)
        all_owners: Set[str] = set([])
        all_subscribers: Set[str] = set([])
        for alert in group_alert.alerts:
            all_owners.update(alert.owners or [])
            all_subscribers.update(alert.subscribers or [])
        group_alert.set_owners(list(all_owners))
        group_alert.set_subscribers(list(all_subscribers))

    def _group_alerts_per_config(self, alerts: List[Alert]) -> List[GroupOfAlerts]:
        """
        reads self.config and alerts' config, and groups alerts in a smart way
        1. split by grouping type
        2. split Table grouped-by, by the Table
        3. concat

        :param alerts:
        :return:
        """
        default_alerts_group_by_strategy = GroupingType(
            self.config.slack_group_alerts_by
        )
        alerts_by_grouping_mechanism = defaultdict(lambda: [])
        for alert in alerts:
            if not alert.slack_group_alerts_by:
                alerts_by_grouping_mechanism[default_alerts_group_by_strategy].append(
                    alert
                )
                continue
            try:
                grouping_type = GroupingType(alert.slack_group_alerts_by)
                alerts_by_grouping_mechanism[grouping_type].append(alert)
            except ValueError:
                alerts_by_grouping_mechanism[default_alerts_group_by_strategy].append(
                    alert
                )
                logger.error(
                    f"Failed to extract value as a group-by config: '{alert.slack_group_alerts_by}'. Allowed Values: {list(GroupingType.__members__.keys())} Ignoring it for now and default grouping strategy will be used"
                )
        table_to_alerts: DefaultDict[str, list] = defaultdict(list)
        for alert in alerts_by_grouping_mechanism[GroupingType.BY_TABLE]:
            if alert.model_unique_id is None:
                continue
            table_to_alerts[alert.model_unique_id].append(alert)

        by_table_group: List[GroupOfAlerts] = [
            GroupOfAlertsByTable(
                alerts=table_to_alerts[model_unique_id],
                default_channel_destination=self.config.slack_channel_name,
                override_slack_channel=self.override_meta_slack_channel,
                env=self.config.env,
                report_url=self.config.report_url,
            )
            for model_unique_id in table_to_alerts.keys()
        ]

        by_alert_group: List[GroupOfAlerts] = [
            GroupOfAlertsBySingleAlert(
                alerts=[al],
                default_channel_destination=self.config.slack_channel_name,
                override_slack_channel=self.override_meta_slack_channel,
                env=self.config.env,
            )
            for al in alerts_by_grouping_mechanism[GroupingType.BY_ALERT]
        ]

        self.execution_properties["had_group_by_table"] = len(by_table_group) > 0
        self.execution_properties["had_group_by_alert"] = len(by_alert_group) > 0

        grouped_alerts = by_table_group + by_alert_group
        return sorted(
            grouped_alerts,
            key=lambda group: min(
                alert.detected_at or datetime.max for alert in group.alerts
            ),
        )

    def _send_test_message(self):
        self.slack_client.send_message(
            channel_name=self.config.slack_channel_name,
            message=SlackMessageSchema(
                text=f"Elementary monitor ran successfully on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            ),
        )
        logger.info("Sent the test message.")

    def _send_alerts(self, alerts: Alerts):
        all_alerts_to_send = alerts.get_all()
        if not all_alerts_to_send:
            self.execution_properties["sent_alert_count"] = self.sent_alert_count
            return

        sent_alert_ids_and_tables: List[Tuple[str, str]] = []

        alerts_groups: List[GroupOfAlerts] = self._group_alerts_per_config(
            all_alerts_to_send
        )
        alerts_with_progress_bar = alive_it(alerts_groups, title="Sending alerts")
        for alert_group in alerts_with_progress_bar:
            self._fix_owners_and_subscribers(alert_group)
            alert_msg = alert_group.to_slack()
            sent_successfully = self.slack_client.send_message(
                channel_name=alert_group.channel_destination,
                message=alert_msg,
            )
            alerts_ids_and_tables = [
                (alert.id, alert.alerts_table) for alert in alert_group.alerts
            ]
            if sent_successfully:
                sent_alert_ids_and_tables.extend(alerts_ids_and_tables)
            else:
                logger.error(
                    f"Could not send the alert[s] - {[alert_id_and_table[0] for alert_id_and_table in alerts_ids_and_tables]}. Full alert: {json.dumps(dict(alert_msg))}"
                )
                self.success = False

        # Now update as sent:
        table_name_to_alert_ids = defaultdict(lambda: [])
        for alert_id, table_name in sent_alert_ids_and_tables:
            table_name_to_alert_ids[table_name].append(alert_id)

        for table_name, alert_ids in table_name_to_alert_ids.items():
            self.alerts_api.update_sent_alerts(alert_ids, table_name)

        # Now update sent alerts counter:
        self.sent_alert_count += len(sent_alert_ids_and_tables)
        self.execution_properties["sent_alert_count"] = self.sent_alert_count

    def _skip_alerts(self, alerts: Alerts):
        self.alerts_api.skip_alerts(
            alerts.tests.get_alerts_to_skip(), TestAlert.TABLE_NAME
        )
        self.alerts_api.skip_alerts(
            alerts.models.get_alerts_to_skip(), ModelAlert.TABLE_NAME
        )
        self.alerts_api.skip_alerts(
            alerts.source_freshnesses.get_alerts_to_skip(),
            SourceFreshnessAlert.TABLE_NAME,
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

        alerts = self.alerts_api.get_new_alerts(
            days_back,
            disable_samples=self.disable_samples,
            filter=self.filter.get_filter(),
        )
        self.execution_properties[
            "elementary_test_count"
        ] = alerts.get_elementary_test_count()
        self.execution_properties["alert_count"] = alerts.count
        malformed_alert_count = alerts.malformed_count
        if malformed_alert_count > 0:
            self.success = False
        self.execution_properties["malformed_alert_count"] = malformed_alert_count
        self.execution_properties["has_subscribers"] = any(
            alert.subscribers for alert in alerts.get_all()
        )
        self._skip_alerts(alerts)
        self._send_alerts(alerts)
        if self.send_test_message_on_success and alerts.count == 0:
            self._send_test_message()
        self.execution_properties["run_end"] = True
        self.execution_properties["success"] = self.success
        return self.success
