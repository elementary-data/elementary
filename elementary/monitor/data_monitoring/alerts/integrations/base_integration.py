from abc import ABC, abstractmethod
from typing import Union

from elementary.monitor.alerts.group_of_alerts import GroupedByTableAlerts
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel


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
        ],
        *args,
        **kwargs
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
    def _get_fallback_template(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ],
        *args,
        **kwargs
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
        ],
        *args,
        **kwargs
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def send_test_message(self, *args, **kwargs) -> bool:
        raise NotImplementedError
