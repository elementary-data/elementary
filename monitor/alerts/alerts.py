from dataclasses import dataclass
from typing import List, Generic
from typing import TypeVar

from monitor.alerts.alert import Alert
from monitor.alerts.malformed import MalformedAlert
from monitor.alerts.model import ModelAlert
from monitor.alerts.test import TestAlert

AlertType = TypeVar('AlertType')


@dataclass
class AlertsQueryResult(Generic[AlertType]):
    alerts: List[AlertType]
    malformed_alerts: List[MalformedAlert]

    @property
    def count(self) -> int:
        return len(self.alerts) + len(self.malformed_alerts)

    def get_all(self) -> List[Alert]:
        return self.alerts + self.malformed_alerts


@dataclass
class Alerts:
    tests: AlertsQueryResult[TestAlert]
    models: AlertsQueryResult[ModelAlert]

    @property
    def count(self) -> int:
        return self.models.count + self.tests.count
