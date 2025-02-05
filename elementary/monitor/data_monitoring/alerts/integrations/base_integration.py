from abc import ABC, abstractmethod
from typing import Generator, List, Sequence, Tuple, Union

from elementary.monitor.alerts.alerts_groups import AlertsGroup, GroupedByTableAlerts
from elementary.monitor.alerts.alerts_groups.base_alerts_group import BaseAlertsGroup
from elementary.monitor.alerts.alerts_groups.grouped_by_owner import (
    GroupedByOwnerAlerts,
)
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class BaseIntegration(ABC):
    def __init__(self, *args, **kwargs) -> None:
        self.client = self._initial_client(*args, **kwargs)

    @abstractmethod
    def _initial_client(self, *args, **kwargs):
        raise NotImplementedError

    def _get_alert_template(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
            BaseAlertsGroup,
        ],
        *args,
        **kwargs,
    ):
        if isinstance(alert, TestAlertModel):
            if alert.is_elementary_test:
                return self._get_elementary_test_template(alert)
            else:
                return self._get_dbt_test_template(alert)
        elif isinstance(alert, ModelAlertModel):
            if alert.materialization == "snapshot":
                return self._get_snapshot_template(alert)
            else:
                return self._get_model_template(alert)
        elif isinstance(alert, SourceFreshnessAlertModel):
            return self._get_source_freshness_template(alert)
        elif isinstance(alert, GroupedByTableAlerts):
            return self._get_group_by_table_template(alert)
        elif isinstance(alert, GroupedByOwnerAlerts):
            return self._get_group_by_owner_template(alert)
        elif isinstance(alert, BaseAlertsGroup):
            return self._get_alerts_group_template(alert)

    @abstractmethod
    def _get_dbt_test_template(self, alert: TestAlertModel, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _get_elementary_test_template(self, alert: TestAlertModel, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _get_model_template(self, alert: ModelAlertModel, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _get_snapshot_template(self, alert: ModelAlertModel, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _get_source_freshness_template(
        self, alert: SourceFreshnessAlertModel, *args, **kwargs
    ):
        raise NotImplementedError

    @abstractmethod
    def _get_group_by_table_template(
        self, alert: GroupedByTableAlerts, *args, **kwargs
    ):
        raise NotImplementedError

    @abstractmethod
    def _get_group_by_owner_template(
        self, alert: GroupedByOwnerAlerts, *args, **kwargs
    ):
        raise NotImplementedError

    @abstractmethod
    def _get_alerts_group_template(self, alert: BaseAlertsGroup, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _get_fallback_template(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ],
        *args,
        **kwargs,
    ):
        raise NotImplementedError

    @abstractmethod
    def send_alert(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
            BaseAlertsGroup,
        ],
        *args,
        **kwargs,
    ) -> bool:
        raise NotImplementedError

    @staticmethod
    def _group_alerts(
        alerts: Sequence[
            Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
                GroupedByTableAlerts,
            ]
        ],
        threshold: int,
    ) -> Sequence[
        Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
            AlertsGroup,
        ]
    ]:
        # Deprecated: the grouping logic is now handled outside of the integration, and the integration only sends the alerts
        if not alerts:
            return []

        flattened_alerts: List[
            Union[TestAlertModel, ModelAlertModel, SourceFreshnessAlertModel]
        ] = []
        env = None
        for alert in alerts:
            if isinstance(alert, BaseAlertsGroup):
                flattened_alerts.extend(alert.alerts)
                if env is None and alert.env is not None:
                    env = alert.env
            else:
                flattened_alerts.append(alert)

        if len(flattened_alerts) >= threshold:
            logger.info(f"Grouping {len(flattened_alerts)} alerts into one")
            return [
                AlertsGroup(alerts=flattened_alerts, env=env),
            ]
        return alerts

    def send_alerts(
        self,
        alerts: Sequence[
            Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
                GroupedByTableAlerts,
            ]
        ],
        group_alerts_threshold: int,
        *args,
        **kwargs,
    ) -> Generator[
        Tuple[
            Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
            ],
            bool,
        ],
        None,
        None,
    ]:
        # Deprecated: the grouping logic is now handled outside of the integration, and the integration only sends the alerts
        grouped_alerts = self._group_alerts(alerts, group_alerts_threshold)
        for alert in grouped_alerts:
            if isinstance(alert, BaseAlertsGroup):
                sent_successfully = self.send_alert(alert, *args, **kwargs)
                for inner_alert in alert.alerts:
                    yield inner_alert, sent_successfully
            else:
                yield alert, self.send_alert(alert, *args, **kwargs)

    @abstractmethod
    def send_test_message(self, *args, **kwargs) -> bool:
        raise NotImplementedError
