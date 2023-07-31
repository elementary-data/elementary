from typing import Callable, List

from elementary.monitor.alerts.alert import AlertType
from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.monitor.data_monitoring.schema import (
    ResourceType,
    SelectorFilterSchema,
    Status,
)
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)


def filter_alerts(
    alerts: List[AlertType],
    alerts_filter: SelectorFilterSchema = SelectorFilterSchema(),
) -> List[AlertType]:
    # If the filter is on invocation stuff, it's not relevant to alerts and we return an empty list
    if (
        alerts_filter.invocation_id is not None
        or alerts_filter.invocation_time is not None
        or alerts_filter.last_invocation
    ):
        logger.warning("Invalid filter for alerts: %s", alerts_filter.selector)
        return []

    # If the filter is empty, we want to return all of the alerts
    filtered_alerts = alerts
    if alerts_filter.tag is not None:
        filtered_alerts = _filter_alerts_by_tag(filtered_alerts, alerts_filter)
    if alerts_filter.model is not None:
        filtered_alerts = _filter_alerts_by_model(filtered_alerts, alerts_filter)
    if alerts_filter.owner is not None:
        filtered_alerts = _filter_alerts_by_owner(filtered_alerts, alerts_filter)
    if alerts_filter.statuses is not None:
        filtered_alerts = _filter_alerts_by_status(filtered_alerts, alerts_filter)
    if alerts_filter.resource_types is not None:
        filtered_alerts = _filter_alerts_by_resource_type(
            filtered_alerts, alerts_filter
        )
    if alerts_filter.node_names is not None:
        filtered_alerts = _filter_alerts_by_node_names(filtered_alerts, alerts_filter)

    return filtered_alerts


def _filter_alerts_by_tag(
    alerts: List[AlertType],
    tag_filter: SelectorFilterSchema,
) -> List[AlertType]:

    if tag_filter.tag is None:
        return alerts

    filtered_alerts = []
    for alert in alerts:
        raw_tags = (
            alert.tags
            if not isinstance(alert, MalformedAlert)
            else alert.data.get("tags")
        )
        alert_tags = try_load_json(raw_tags) if isinstance(raw_tags, str) else raw_tags

        if alert_tags and tag_filter.tag in alert_tags:
            filtered_alerts.append(alert)
    return filtered_alerts


def _filter_alerts_by_owner(
    alerts: List[AlertType],
    owner_filter: SelectorFilterSchema,
) -> List[AlertType]:

    if owner_filter.owner is None:
        return alerts

    filtered_alerts = []
    for alert in alerts:
        raw_owners = (
            alert.owners
            if not isinstance(alert, MalformedAlert)
            else alert.data.get("owners")
        )
        alert_owners = (
            try_load_json(raw_owners) if isinstance(raw_owners, str) else raw_owners
        )

        if alert_owners and owner_filter.owner in alert_owners:
            filtered_alerts.append(alert)
    return filtered_alerts


def _filter_alerts_by_model(
    alerts: List[AlertType],
    model_filter: SelectorFilterSchema,
) -> List[AlertType]:

    if model_filter.model is None:
        return alerts

    filtered_alerts: List[AlertType] = []
    for alert in alerts:
        alert_model_unique_id = alert.model_unique_id
        if alert_model_unique_id and alert_model_unique_id.endswith(model_filter.model):
            filtered_alerts.append(alert)
    return filtered_alerts


def _filter_alerts_by_node_names(
    alerts: List[AlertType],
    node_name_filter: SelectorFilterSchema,
) -> List[AlertType]:

    if node_name_filter.node_names is None:
        return alerts

    filtered_alerts = []
    for alert in alerts:
        alert_node_name = None
        if isinstance(alert, TestAlert):
            alert_node_name = alert.test_name
        elif isinstance(alert, ModelAlert) or isinstance(alert, SourceFreshnessAlert):
            alert_node_name = alert.model_unique_id
        elif isinstance(alert, MalformedAlert):
            alert_node_name = alert.test_name or alert.model_unique_id
        else:
            # Shouldn't happen
            raise Exception(f"Unexpected alert type: {type(alert)}")

        if alert_node_name:
            for node_name in node_name_filter.node_names:
                if alert_node_name.endswith(node_name) or node_name.endswith(
                    alert_node_name
                ):
                    filtered_alerts.append(alert)
                    break
    return filtered_alerts


def _filter_alerts_by_status(
    alerts: List[AlertType],
    status_filter: SelectorFilterSchema,
) -> List[AlertType]:

    if status_filter.statuses is None:
        return alerts

    statuses: List[Status] = status_filter.statuses
    filter_func: Callable[[AlertType], bool] = (
        lambda alert: Status(alert.status) in statuses
    )
    return list(filter(filter_func, alerts))


def _filter_alerts_by_resource_type(
    alerts: List[AlertType],
    resource_type_filter: SelectorFilterSchema,
) -> List[AlertType]:

    if resource_type_filter.resource_types is None:
        return alerts

    resource_types: List[ResourceType] = resource_type_filter.resource_types
    filter_func: Callable[[AlertType], bool] = (
        lambda alert: ResourceType.from_table_name(alert.alerts_table) in resource_types
    )
    return list(filter(filter_func, alerts))
