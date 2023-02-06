from typing import List, Optional, Union

from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.monitor.data_monitoring.schema import DataMonitoringAlertsFilter
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)


def filter_alerts(
    alerts: Union[
        List[TestAlert],
        List[ModelAlert],
        List[SourceFreshnessAlert],
        List[MalformedAlert],
    ],
    filter: Optional[DataMonitoringAlertsFilter] = None,
) -> Union[
    List[TestAlert],
    List[ModelAlert],
    List[SourceFreshnessAlert],
    List[MalformedAlert],
]:
    if filter is None:
        return alerts
    filtered_alerts = []
    if filter.tag:
        filtered_alerts = _filter_alerts_by_tag(alerts, filter)
    elif filter.model:
        filtered_alerts = _filter_alerts_by_model(alerts, filter)
    elif filter.owner:
        filtered_alerts = _filter_alerts_by_owner(alerts, filter)
    elif filter.node_names is not None:
        filtered_alerts = _filter_alerts_by_node_names(alerts, filter)
    return filtered_alerts


def _filter_alerts_by_tag(
    alerts: Union[
        List[TestAlert],
        List[ModelAlert],
        List[SourceFreshnessAlert],
        List[MalformedAlert],
    ],
    filter: Optional[DataMonitoringAlertsFilter],
) -> Union[
    List[TestAlert],
    List[ModelAlert],
    List[SourceFreshnessAlert],
    List[MalformedAlert],
]:
    if filter.tag is None:
        return alerts

    filtered_alerts = []
    for alert in alerts:
        alert_tags = (
            try_load_json(alert.tags)
            if not isinstance(alert, MalformedAlert)
            else try_load_json(alert.data.get("tags"))
        )
        if alert_tags and filter.tag in alert_tags:
            filtered_alerts.append(alert)
    return filtered_alerts


def _filter_alerts_by_owner(
    alerts: Union[
        List[TestAlert],
        List[ModelAlert],
        List[SourceFreshnessAlert],
        List[MalformedAlert],
    ],
    filter: Optional[DataMonitoringAlertsFilter],
) -> Union[
    List[TestAlert],
    List[ModelAlert],
    List[SourceFreshnessAlert],
    List[MalformedAlert],
]:
    if filter.owner is None:
        return alerts

    filtered_alerts = []
    for alert in alerts:
        alert_owners = (
            try_load_json(alert.owners)
            if not isinstance(alert, MalformedAlert)
            else try_load_json(alert.data.get("owners"))
        )
        if alert_owners and filter.owner in alert_owners:
            filtered_alerts.append(alert)
    return filtered_alerts


def _filter_alerts_by_model(
    alerts: Union[
        List[TestAlert],
        List[ModelAlert],
        List[SourceFreshnessAlert],
        List[MalformedAlert],
    ],
    filter: Optional[DataMonitoringAlertsFilter],
) -> Union[
    List[TestAlert],
    List[ModelAlert],
    List[SourceFreshnessAlert],
    List[MalformedAlert],
]:
    if filter.model is None:
        return alerts

    filtered_alerts = []
    for alert in alerts:
        alert_model_unique_id = alert.model_unique_id
        if alert_model_unique_id and alert_model_unique_id.endswith(filter.model):
            filtered_alerts.append(alert)
    return filtered_alerts


def _filter_alerts_by_node_names(
    alerts: Union[
        List[TestAlert],
        List[ModelAlert],
        List[SourceFreshnessAlert],
        List[MalformedAlert],
    ],
    filter: Optional[DataMonitoringAlertsFilter],
) -> Union[
    List[TestAlert],
    List[ModelAlert],
    List[SourceFreshnessAlert],
    List[MalformedAlert],
]:
    if filter.node_names is None:
        return alerts

    filtered_alerts = []
    for alert in alerts:
        alert_node_name = None
        if isinstance(alert, TestAlert):
            alert_node_name = alert.test_name
        elif isinstance(alert, ModelAlert) or isinstance(alert, SourceFreshnessAlert):
            alert_node_name = alert.model_unique_id
        # Malformed alert
        else:
            alert_node_name = alert.test_name or alert.model_unique_id
        if alert_node_name:
            for node_name in filter.node_names:
                if alert_node_name.endswith(node_name) or node_name.endswith(
                    alert_node_name
                ):
                    filtered_alerts.append(alert)
                    break
    return filtered_alerts
