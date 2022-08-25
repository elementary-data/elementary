from collections import defaultdict
from dataclasses import dataclass
from typing import List, Generic
from typing import TypeVar

from elementary.monitor.alerts.alert import Alert
from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.test import TestAlert, ElementaryTestAlert

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
        return len(self.models.malformed_alerts) + len(self.tests.malformed_alerts)

    def get_all(self) -> List[Alert]:
        return self.models.get_all() + self.tests.get_all()

    def get_elementary_test_count(self):
        elementary_test_count = defaultdict(int)
        for test_result in self.tests.alerts:
            if isinstance(test_result, ElementaryTestAlert):
                elementary_test_count[test_result.test_name] += 1
        return elementary_test_count
