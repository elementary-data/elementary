from typing import Dict, List, Optional, Union

from elementary.monitor.alerts.alerts_groups.alerts_group import AlertsGroup
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData,
    get_test_runs_by_owner_link,
)


class GroupedByOwnerAlerts(AlertsGroup):
    owner: Optional[str]

    def __init__(
        self,
        owner: Optional[str],
        alerts: List[Union[TestAlertModel, ModelAlertModel, SourceFreshnessAlertModel]],
    ) -> None:
        super().__init__(alerts)
        self.owner = owner

    @property
    def report_url(self) -> Optional[str]:
        return self.alerts[0].report_url

    @property
    def summary(self) -> str:
        return f"{self.owner}: {len(self.alerts)} issues detected"

    def get_report_link(self) -> Optional[ReportLinkData]:
        if not self.model_errors:
            return get_test_runs_by_owner_link(self.report_url, self.owner)

        return None

    @property
    def unified_meta(self) -> Dict:
        model_unified_meta = {}
        test_unified_meta = {}
        for alert in self.alerts:
            alert_unified_meta = alert.unified_meta
            if alert_unified_meta:
                if isinstance(alert, ModelAlertModel):
                    model_unified_meta = alert_unified_meta
                    break

                test_unified_meta = alert_unified_meta
        return model_unified_meta or test_unified_meta
