from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Generic, List, Optional, Union

from elementary.monitor.alerts.alert import Alert, AlertType
from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import ElementaryTestAlert, TestAlert


@dataclass
class AlertsQueryResult(Generic[AlertType]):
    alerts: List[AlertType]
    malformed_alerts: List[MalformedAlert]
    alerts_to_skip: Optional[List[Union[AlertType, MalformedAlert]]] = None

    @property
    def count(self) -> int:
        return len(self.alerts) + len(self.malformed_alerts)

    def get_all(self) -> List[Alert]:
        ret: List[Alert] = []
        ret.extend(self.alerts)
        ret.extend(self.malformed_alerts)
        return ret

    def get_alerts_to_skip(self) -> List[Union[AlertType, MalformedAlert]]:
        return self.alerts_to_skip or []


@dataclass
class Alerts:
    tests: AlertsQueryResult[TestAlert]
    models: AlertsQueryResult[ModelAlert]
    source_freshnesses: AlertsQueryResult[SourceFreshnessAlert]

    @property
    def count(self) -> int:
        return self.models.count + self.tests.count + self.source_freshnesses.count

    @property
    def malformed_count(self):
        return (
            len(self.models.malformed_alerts)
            + len(self.tests.malformed_alerts)
            + len(self.source_freshnesses.malformed_alerts)
        )

    def get_all(self) -> List[Alert]:
        return (
            self.models.get_all()
            + self.tests.get_all()
            + self.source_freshnesses.get_all()
        )

    def get_elementary_test_count(self):
        elementary_test_count: DefaultDict[str, int] = defaultdict(int)
        for test_result in self.tests.alerts:
            if isinstance(test_result, ElementaryTestAlert):
                elementary_test_count[test_result.test_name] += 1
        return elementary_test_count
