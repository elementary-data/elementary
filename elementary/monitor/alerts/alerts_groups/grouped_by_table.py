from typing import Dict, List, Optional, Set

from elementary.monitor.alerts.alerts_groups.alerts_group import AlertsGroup
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData,
    get_model_test_runs_link,
)
from elementary.utils.models import get_shortened_model_name


class GroupedByTableAlerts(AlertsGroup):
    @property
    def model_unique_id(self) -> Optional[str]:
        models_unique_ids = [
            alert.model_unique_id
            for alert in self.alerts
            if alert.model_unique_id is not None
        ]
        if not models_unique_ids:
            return None
        return models_unique_ids[0]

    @property
    def model(self) -> Optional[str]:
        if not self.model_unique_id:
            return None
        return get_shortened_model_name(self.model_unique_id)

    @property
    def report_url(self) -> Optional[str]:
        return self.alerts[0].report_url

    @property
    def summary(self) -> str:
        return (
            f"{self.model}: {len(self.alerts)} issues detected"
            if self.model
            else f"{len(self.alerts)} Issues detected"
        )

    def get_report_link(self) -> Optional[ReportLinkData]:
        if not self.model_errors:
            return get_model_test_runs_link(self.report_url, self.model_unique_id)

        return None

    @property
    def unified_meta(self) -> Dict:
        model_unified_meta = {}
        test_unified_meta = {}
        for alert in self.alerts:
            alert_unified_meta = alert.unified_meta
            if alert_unified_meta:
                if isinstance(alert, ModelAlertModel):
                    model_unified_meta = alert_unified_meta
                    break

                test_unified_meta = alert_unified_meta
        return model_unified_meta or test_unified_meta

    @property
    def tags(self) -> List[str]:
        tags: Set[str] = set()
        for alert in self.alerts:
            tags.update(alert.tags)
        return list(tags)

    @property
    def owners(self) -> List[str]:
        owners: Set[str] = set()
        for alert in self.alerts:
            owners.update(alert.owners)
        return list(owners)

    @property
    def subscribers(self) -> List[str]:
        subscribers: Set[str] = set()
        for alert in self.alerts:
            subscribers.update(alert.subscribers)
        return list(subscribers)
