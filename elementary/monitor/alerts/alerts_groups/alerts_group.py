from typing import List, Optional, Union

from elementary.monitor.alerts.alerts_groups.base_alerts_group import BaseAlertsGroup
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel


class AlertsGroup(BaseAlertsGroup):
    test_errors: List[Union[TestAlertModel, SourceFreshnessAlertModel]]
    test_warnings: List[Union[TestAlertModel, SourceFreshnessAlertModel]]
    test_failures: List[Union[TestAlertModel, SourceFreshnessAlertModel]]
    model_errors: List[ModelAlertModel]

    def __init__(
        self,
        alerts: List[Union[TestAlertModel, ModelAlertModel, SourceFreshnessAlertModel]],
        env: Optional[str] = None,
    ) -> None:
        super().__init__(alerts, env=env)
        self.test_errors = []
        self.test_warnings = []
        self.test_failures = []
        self.model_errors = []
        self._sort_alerts()

    @property
    def status(self) -> str:
        if self.model_errors or self.test_errors:
            return "error"
        elif self.test_failures:
            return "failure"
        else:
            return "warn"

    def _sort_alerts(self) -> None:
        for alert in self.alerts:
            if isinstance(alert, ModelAlertModel):
                self.model_errors.append(alert)
            elif alert.status == "error":
                self.test_errors.append(alert)
            elif alert.status == "warn":
                self.test_warnings.append(alert)
            else:
                self.test_failures.append(alert)
