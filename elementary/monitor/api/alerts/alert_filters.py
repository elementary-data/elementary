from typing import List

from elementary.monitor.alerts.alert import AlertType
from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.monitor.data_monitoring.schema import SelectorFilterSchema
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)


def filter_alerts(
    alerts: List[AlertType],
    alerts_filter: SelectorFilterSchema = SelectorFilterSchema(),
) -> List[AlertType]:
    filtered_alerts = []
    # If the filter is empty, we want to return all of the alerts
    print("filtering alerts by:", alerts_filter)
    if not alerts_filter.selector:
        filtered_alerts = alerts
    elif alerts_filter.tag:
        filtered_alerts = _filter_alerts_by_tag(alerts, alerts_filter)
    elif alerts_filter.model:
        filtered_alerts = _filter_alerts_by_model(alerts, alerts_filter)
    elif alerts_filter.owner:
        filtered_alerts = _filter_alerts_by_owner(alerts, alerts_filter)
    elif alerts_filter.status:
        filtered_alerts = _filter_alerts_by_status(alerts, alerts_filter)
    elif alerts_filter.node_names is not None:
        filtered_alerts = _filter_alerts_by_node_names(alerts, alerts_filter)
    # If the filter contains a filter that we don't support, we want to return an empty list of alerts
    elif alerts_filter.selector:
        logger.error(
            f"An unsupported alerts selector has been provided - {alerts_filter.selector}!\nNo alert has been sent!"
        )
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
    if status_filter.status is None:
        return alerts

    if status_filter.status not in ["warn", "fail"]:
        raise ValueError("Status selector must be either 'warn' or 'fail'")

    return list(filter(lambda alert: alert.status == status_filter.status, alerts))
