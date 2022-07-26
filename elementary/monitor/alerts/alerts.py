from dataclasses import dataclass
from typing import List, Generic
from typing import TypeVar

from elementary.monitor.alerts.alert import Alert
from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.test import TestAlert

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

    @property
    def malformed_count(self):
        return len(self.models.malformed_alerts) + len(self.models.malformed_alerts)
