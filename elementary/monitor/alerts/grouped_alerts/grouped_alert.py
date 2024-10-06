from datetime import datetime
from typing import List, Union

from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel


class GroupedAlert:
    def __init__(
        self,
        alerts: List[Union[TestAlertModel, ModelAlertModel, SourceFreshnessAlertModel]],
    ) -> None:
        self.alerts = alerts
        self.test_errors: List[Union[TestAlertModel, SourceFreshnessAlertModel]] = []
        self.test_warnings: List[Union[TestAlertModel, SourceFreshnessAlertModel]] = []
        self.test_failures: List[Union[TestAlertModel, SourceFreshnessAlertModel]] = []
        self.model_errors: List[ModelAlertModel] = []
        self._sort_alerts()

    @property
    def detected_at(self) -> datetime:
        return min(alert.detected_at or datetime.max for alert in self.alerts)

    @property
    def status(self) -> str:
        if self.model_errors or self.test_errors:
            return "error"
        elif self.test_failures:
            return "failure"
        else:
            return "warn"

    def _sort_alerts(self):
        for alert in self.alerts:
            if isinstance(alert, ModelAlertModel):
                self.model_errors.append(alert)
            elif alert.status == "error":
                self.test_errors.append(alert)
            elif alert.status == "warn":
                self.test_warnings.append(alert)
            else:
                self.test_failures.append(alert)
