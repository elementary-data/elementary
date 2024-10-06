from typing import Optional

from elementary.monitor.alerts.grouped_alerts.grouped_alert import GroupedAlert
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData,
    get_model_test_runs_link,
)
from elementary.utils.models import get_shortened_model_name


class GroupedByTableAlerts(GroupedAlert):
    @property
    def model_unique_id(self) -> Optional[str]:
        return self.alerts[0].model_unique_id

    @property
    def model(self) -> str:
        return get_shortened_model_name(self.model_unique_id)

    @property
    def report_url(self) -> Optional[str]:
        return self.alerts[0].report_url

    @property
    def summary(self) -> str:
        return f"{self.model}: {len(self.alerts)} issues detected"

    def get_report_link(self) -> Optional[ReportLinkData]:
        if not self.model_errors:
            return get_model_test_runs_link(self.report_url, self.model_unique_id)

        return None
