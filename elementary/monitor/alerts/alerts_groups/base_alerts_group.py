from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Sequence, Union

from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel


class BaseAlertsGroup(ABC):
    def __init__(
        self,
        alerts: Sequence[
            Union[TestAlertModel, ModelAlertModel, SourceFreshnessAlertModel]
        ],
    ) -> None:
        self.alerts = alerts

    @property
    def summary(self) -> str:
        return f"{len(self.alerts)} issues detected"

    @property
    def detected_at(self) -> datetime:
        return min(alert.detected_at or datetime.max for alert in self.alerts)

    @property
    @abstractmethod
    def status(self) -> str:
        ...

    @property
    def data(self) -> List[Dict]:
        return [alert.data for alert in self.alerts]

    @property
    def unified_meta(self) -> Dict:
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
