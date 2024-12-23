from typing import List, Optional

from elementary.monitor.data_monitoring.schema import (
    FilterSchema,
    FiltersSchema,
    FilterType,
    ResourceType,
    Status,
)
from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    AlertTypes,
    PendingAlertSchema,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


def get_string_ends(input_string: Optional[str], splitter: str) -> List[str]:
    if input_string is None:
        return []
    parts = input_string.split(splitter)
    result = []

    for i in range(len(parts)):
        result.append(splitter.join(parts[i:]))

    return result


def _get_alert_node_name(alert: PendingAlertSchema) -> Optional[str]:
    alert_node_name = None
    alert_type = AlertTypes(alert.type)
    if alert_type is AlertTypes.TEST:
        alert_node_name = alert.data.test_name  # type: ignore[union-attr]
    elif alert_type is AlertTypes.MODEL or alert_type is AlertTypes.SOURCE_FRESHNESS:
        alert_node_name = alert.data.model_unique_id
    else:
        raise ValueError(f"Unexpected alert type: {alert_type}")
    return alert_node_name


def apply_filters_schema_on_alert(
    alert: PendingAlertSchema, filters_schema: FiltersSchema
) -> bool:
    tags = alert.data.tags or []
    models = [
        alert.data.model_unique_id,
        *get_string_ends(alert.data.model_unique_id, "."),
    ]
    owners = alert.data.unified_owners or []
    status = Status(alert.data.status)
    resource_type = ResourceType(alert.data.resource_type)

    alert_node_name = _get_alert_node_name(alert)
    node_names = (
        [alert_node_name, *get_string_ends(alert_node_name, ".")]
        if alert_node_name
        else []
    )

    return (
        all(
            filter_schema.apply_filter_on_values(tags)
            for filter_schema in filters_schema.tags
        )
        and all(
            filter_schema.apply_filter_on_values(models)
            for filter_schema in filters_schema.models
        )
        and all(
            filter_schema.apply_filter_on_values(owners)
            for filter_schema in filters_schema.owners
        )
        and all(
            filter_schema.apply_filter_on_value(status)
            for filter_schema in filters_schema.statuses
        )
        and all(
            filter_schema.apply_filter_on_value(resource_type)
            for filter_schema in filters_schema.resource_types
        )
        and (
            FilterSchema(
                values=filters_schema.node_names, type=FilterType.IS
            ).apply_filter_on_values(node_names)
            if filters_schema.node_names
            else True
        )
    )


def filter_alerts(
    alerts: List[PendingAlertSchema],
    alerts_filter: FiltersSchema = FiltersSchema(),
) -> List[PendingAlertSchema]:
    # If the filter is on invocation stuff, it's not relevant to alerts and we return an empty list
    if (
        alerts_filter.invocation_id is not None
        or alerts_filter.invocation_time is not None
        or alerts_filter.last_invocation
    ):
        logger.warning("Invalid filter for alerts: %s", alerts_filter.selector)
        return []  # type: ignore[return-value]

    return [
        alert for alert in alerts if apply_filters_schema_on_alert(alert, alerts_filter)
    ]
