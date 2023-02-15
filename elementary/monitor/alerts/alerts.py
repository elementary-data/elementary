from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Generic, List, Optional, TypeVar, Union

from elementary.monitor.alerts.alert import Alert
from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import ElementaryTestAlert, TestAlert

AlertType = TypeVar("AlertType")


@dataclass
class AlertsQueryResult(Generic[AlertType]):
    alerts: List[AlertType]
    malformed_alerts: List[MalformedAlert]
    alerts_to_skip: Optional[List[Union[AlertType, MalformedAlert]]] = None

    @property
    def count(self) -> int:
        return len(self.alerts) + len(self.malformed_alerts)

    def get_all(self) -> List[Alert]:
        return self.alerts + self.malformed_alerts

    def get_alerts_to_skip(self) -> List[Optional[Union[AlertType, MalformedAlert]]]:
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
        elementary_test_count = defaultdict(int)
        for test_result in self.tests.alerts:
            if isinstance(test_result, ElementaryTestAlert):
                elementary_test_count[test_result.test_name] += 1
        return elementary_test_count


class GroupingType(Enum):
    BY_ALERT = "by_alert"
    BY_TABLE = "by_table"
    ALL = "all"


@dataclass
class GroupOfAlerts:
    alerts: List[Alert]
    grouping_type: GroupingType
    owners: List[List[str] | str]
    subscribers: List[List[str] | str]
    channel_destination: str

    def __post_init__(self):
        dest_channels = set([alert.slack_channel for alert in self.alerts])
        if len(dest_channels) > 1:
            raise ValueError("Failed initializing a Group of Alerts with alerts that has different slack channel dest")
        if len(dest_channels) == 0:
            self.channel_destination = list(dest_channels)[0]


