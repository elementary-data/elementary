from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union

from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.utils.models import get_shortened_model_name


class GroupingType(Enum):
    BY_ALERT = "alert"
    BY_TABLE = "table"


class GroupedByTableAlerts:
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
    def model_unique_id(self) -> Optional[str]:
        return self.alerts[0].model_unique_id

    @property
    def model(self) -> str:
        return get_shortened_model_name(self.model_unique_id)

    @property
    def detected_at(self) -> datetime:
        # We return the minimum alert detected at time as the group detected at time
        return min(alert.detected_at or datetime.max for alert in self.alerts)

    @property
    def report_url(self) -> Optional[str]:
        return self.alerts[0].report_url

    @property
    def integration_params(self) -> Dict:
        # If a model level integration params are defined, we use them.
        # Else we use one of the tests level integration params.
        model_integration_params = dict()
        test_integration_params = dict()
        for alert in self.alerts:
            alert_integration_params = alert.integration_params
            if alert_integration_params:
                if isinstance(alert, ModelAlertModel):
                    model_integration_params = alert_integration_params
                    break

                test_integration_params = alert_integration_params
        return model_integration_params or test_integration_params

    @property
    def data(self) -> List[Dict]:
        return [alert.data for alert in self.alerts]

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
