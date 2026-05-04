from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Sequence

from elementary.monitor.alerts.alert import AlertModel


class BaseAlertsGroup(ABC):
    def __init__(
        self,
        alerts: Sequence[AlertModel],
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
