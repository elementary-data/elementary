from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union

from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData,
    get_model_test_runs_link,
)
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
    def unified_meta(self) -> Dict:
        # If a model level unified meta is defined, we use is.
        # Else we use one of the tests level unified metas.
        model_unified_meta = dict()
        test_unified_meta = dict()
        for alert in self.alerts:
            alert_unified_meta = alert.unified_meta
            if alert_unified_meta:
                if isinstance(alert, ModelAlertModel):
                    model_unified_meta = alert_unified_meta
                    break

                test_unified_meta = alert_unified_meta
        return model_unified_meta or test_unified_meta

    @property
    def data(self) -> List[Dict]:
        return [alert.data for alert in self.alerts]

    @property
    def summary(self) -> str:
        return f"{self.model}: {len(self.alerts)} issues detected"

    @property
    def status(self) -> str:
        if self.model_errors or self.test_errors:
            return "error"
        elif self.test_failures:
            return "failure"
        else:
            return "warn"

    def get_report_link(self) -> Optional[ReportLinkData]:
        if not self.model_errors:
            return get_model_test_runs_link(self.report_url, self.model_unique_id)

        return None

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
