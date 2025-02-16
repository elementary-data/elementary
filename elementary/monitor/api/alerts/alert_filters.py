from typing import List, Optional

from elementary.monitor.data_monitoring.schema import (
    FilterFields,
    FiltersSchema,
    ResourceType,
    Status,
)
from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    AlertTypes,
    PendingAlertSchema,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


def get_string_ends(input_string: str, splitter: str) -> List[str]:
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
    models = (
        [
            alert.data.model_unique_id,
            *get_string_ends(alert.data.model_unique_id, "."),
        ]
        if alert.data.model_unique_id
        else []
    )
    owners = alert.data.unified_owners or []
    status = Status(alert.data.status)
    resource_type = ResourceType(alert.data.resource_type)
    if hasattr(alert.data, "test_unique_id"):
        test_ids = [alert.data.test_unique_id] if alert.data.test_unique_id else []
    else:
        test_ids = []

    alert_node_name = _get_alert_node_name(alert)
    node_names = (
        [alert_node_name, *get_string_ends(alert_node_name, ".")]
        if alert_node_name
        else []
    )

    return filters_schema.apply(
        FilterFields(
            tags=tags,
            models=models,
            owners=owners,
            statuses=[status],
            resource_types=[resource_type],
            node_names=node_names,
            test_ids=test_ids,
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
