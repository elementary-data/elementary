from typing import Union

from elementary.monitor.alerts.alerts_groups import GroupedByTableAlerts
from elementary.monitor.alerts.alerts_groups.base_alerts_group import BaseAlertsGroup
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)


class BaseIntegrationMock(BaseIntegration):
    def _initial_client(self, *args, **kwargs):
        return None

    def _get_dbt_test_template(self, alert: TestAlertModel, *args, **kwargs):
        return "dbt_test"

    def _get_elementary_test_template(self, alert: TestAlertModel, *args, **kwargs):
        return "elementary_test"

    def _get_model_template(self, alert: ModelAlertModel, *args, **kwargs):
        return "model"

    def _get_snapshot_template(self, alert: ModelAlertModel, *args, **kwargs):
        return "snapshot"

    def _get_source_freshness_template(
        self, alert: SourceFreshnessAlertModel, *args, **kwargs
    ):
        return "source_freshness"

    def _get_group_by_table_template(
        self, alert: GroupedByTableAlerts, *args, **kwargs
    ):
        return "grouped_by_table"

    def _get_alerts_group_template(self, alert: BaseAlertsGroup, *args, **kwargs):
        return "grouped"

    def _get_fallback_template(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ],
        *args,
        **kwargs
    ):
        return "fall_back"

    def send_alert(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ],
        *args,
        **kwargs
    ) -> bool:
        return True

    def send_test_message(self, *args, **kwargs) -> bool:
        return True
