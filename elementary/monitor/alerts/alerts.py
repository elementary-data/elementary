from collections import defaultdict
from dataclasses import dataclass
from attrs import define, field
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



class GroupOfAlerts:
    # alerts: List[Alert]
    # grouping_type: GroupingType
    # channel_destination: str
    # owners: List[str]
    # subscribers: List[str]
    # errors: List[Alert]
    # warnings: List[Alert]
    # failures: List[Alert]

    def __init__(self,
                 alerts: List[Alert],
                 grouping_type: GroupingType,
                 default_channel_destination: str):

        self.alerts = alerts
        self.grouping_type = grouping_type

        # sort out model unique id - for groupby table:
        if self.grouping_type == GroupingType.BY_TABLE:
            models = set([al.model_unique_id for al in alerts])
            if len(models) != 1:
                raise ValueError(f"failed initializing a GroupOfAlerts grouped by table, for alerts with mutliple models: {list(models)}")

        # sort out dest_channels: we get the default value, but if we have one other channel configured we switch to it.
        dest_channels = set([alert.slack_channel for alert in self.alerts])
        dest_channels.remove(None)  # no point in counting them, and no point in sending to a None channel
        if len(dest_channels) > 1:
            raise ValueError(f"Failed initializing a Group of Alerts with alerts that has different slack channel dest: {list(dest_channels)}")
        if len(dest_channels) == 1:
            self.channel_destination = list(dest_channels)[0]
        else:
            self.channel_destination = default_channel_destination

        # sort out errors / warnings / failures
        self.errors = []
        self.warnings = []
        self.failures = []
        for alert in self.alerts:
            if isinstance(alert, ModelAlert) or alert.status == "error":
                self.errors.append(alert)
            elif alert.status == "warn":
                self.warnings.append(alert)
            else:
                self.failures.append(alert)

        # sort out owners and subscribers
        owners = set([])
        subscribers = set([])
        for al in self.alerts:
            if al.owners is not None:
                if isinstance(al.owners, list):
                    owners.update(al.owners)
                else:  # it's a string
                    owners.add(al.owners)
            if al.subscribers is not None:
                if isinstance(al.subscribers, list):
                    subscribers.update(al.subscribers)
                else:  # it's a string
                    subscribers.add(al.subscribers)
        self.owners = list(owners)
        self.subscribers = list(subscribers)

    def to_slack(self):
        if self.grouping_type == GroupingType.BY_ALERT:
            return self.alerts[0].to_slack()
        raise NotImplementedError