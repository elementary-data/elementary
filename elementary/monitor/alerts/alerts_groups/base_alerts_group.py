from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Union

from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel


class BaseAlertsGroup(ABC):
    def __init__(
        self,
        alerts: Sequence[
            Union[TestAlertModel, ModelAlertModel, SourceFreshnessAlertModel]
        ],
        env: Optional[str] = None,
    ) -> None:
        self.alerts = alerts
        self.env = env

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
        return {}
