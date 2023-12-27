from typing import Callable, List, Union

from elementary.monitor.data_monitoring.schema import (
    FiltersSchema,
    ResourceType,
    ResourceTypeFilterSchema,
    SelectorFilterSchema,
    Status,
    StatusFilterSchema,
)
from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    PendingModelAlertSchema,
    PendingSourceFreshnessAlertSchema,
    PendingTestAlertSchema,
)
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)


def filter_alerts(
    alerts: Union[
        List[PendingTestAlertSchema],
        List[PendingModelAlertSchema],
        List[PendingSourceFreshnessAlertSchema],
    ],
    alerts_filter: FiltersSchema = FiltersSchema(),
) -> Union[
    List[PendingTestAlertSchema],
    List[PendingModelAlertSchema],
    List[PendingSourceFreshnessAlertSchema],
]:
    filter = alerts_filter.to_selector_filter_schema()
    # If the filter is on invocation stuff, it's not relevant to alerts and we return an empty list
    if (
        alerts_filter.invocation_id is not None
        or alerts_filter.invocation_time is not None
        or alerts_filter.last_invocation
    ):
        logger.warning("Invalid filter for alerts: %s", alerts_filter.selector)
        return []  # type: ignore[return-value]

    # If the filter is empty, we want to return all of the alerts
    filtered_alerts = alerts
    if filter.tag is not None:
        filtered_alerts = _filter_alerts_by_tag(filtered_alerts, filter)
    if filter.model is not None:
        filtered_alerts = _filter_alerts_by_model(filtered_alerts, filter)
    if filter.owner is not None:
        filtered_alerts = _filter_alerts_by_owner(filtered_alerts, filter)
    filtered_alerts = _filter_alerts_by_statuses(
        filtered_alerts, alerts_filter.statuses
    )
    filtered_alerts = _filter_alerts_by_resource_types(
        filtered_alerts, alerts_filter.resource_types
    )
    if alerts_filter.node_names is not None:
        filtered_alerts = _filter_alerts_by_node_names(filtered_alerts, alerts_filter)

    return filtered_alerts


def _filter_alerts_by_tag(
    alerts: Union[
        List[PendingTestAlertSchema],
        List[PendingModelAlertSchema],
        List[PendingSourceFreshnessAlertSchema],
    ],
    tag_filter: SelectorFilterSchema,
) -> Union[
    List[PendingTestAlertSchema],
    List[PendingModelAlertSchema],
    List[PendingSourceFreshnessAlertSchema],
]:
    if tag_filter.tag is None:
        return alerts

    filtered_alerts = []
    for alert in alerts:
        alert_tags = alert.tags

        if alert_tags and tag_filter.tag in alert_tags:
            filtered_alerts.append(alert)
    return filtered_alerts  # type: ignore[return-value]


def _filter_alerts_by_owner(
    alerts: Union[
        List[PendingTestAlertSchema],
        List[PendingModelAlertSchema],
        List[PendingSourceFreshnessAlertSchema],
    ],
    owner_filter: SelectorFilterSchema,
) -> Union[
    List[PendingTestAlertSchema],
    List[PendingModelAlertSchema],
    List[PendingSourceFreshnessAlertSchema],
]:
    if owner_filter.owner is None:
        return alerts

    filtered_alerts = []
    for alert in alerts:
        raw_owners = alert.unified_owners
        alert_owners = (
            try_load_json(raw_owners) if isinstance(raw_owners, str) else raw_owners
        )

        if alert_owners and owner_filter.owner in alert_owners:
            filtered_alerts.append(alert)
    return filtered_alerts  # type: ignore[return-value]


def _filter_alerts_by_model(
    alerts: Union[
        List[PendingTestAlertSchema],
        List[PendingModelAlertSchema],
        List[PendingSourceFreshnessAlertSchema],
    ],
    model_filter: SelectorFilterSchema,
) -> Union[
    List[PendingTestAlertSchema],
    List[PendingModelAlertSchema],
    List[PendingSourceFreshnessAlertSchema],
]:
    if model_filter.model is None:
        return alerts

    filtered_alerts: Union[
        List[PendingTestAlertSchema],
        List[PendingModelAlertSchema],
        List[PendingSourceFreshnessAlertSchema],
    ] = []  # type: ignore[assignment]
    for alert in alerts:
        alert_model_unique_id = alert.model_unique_id
        if alert_model_unique_id and alert_model_unique_id.endswith(model_filter.model):
            filtered_alerts.append(alert)  # type: ignore[arg-type]
    return filtered_alerts  # type: ignore[return-value]


def _filter_alerts_by_node_names(
    alerts: Union[
        List[PendingTestAlertSchema],
        List[PendingModelAlertSchema],
        List[PendingSourceFreshnessAlertSchema],
    ],
    node_name_filter: FiltersSchema,
) -> Union[
    List[PendingTestAlertSchema],
    List[PendingModelAlertSchema],
    List[PendingSourceFreshnessAlertSchema],
]:
    if node_name_filter.node_names is None:
        return alerts

    filtered_alerts = []
    for alert in alerts:
        alert_node_name = None
        if isinstance(alert, PendingTestAlertSchema):
            alert_node_name = alert.test_name
        elif isinstance(alert, PendingModelAlertSchema) or isinstance(
            alert, PendingSourceFreshnessAlertSchema
        ):
            alert_node_name = alert.model_unique_id
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
    return filtered_alerts  # type: ignore[return-value]


def _filter_alerts_by_statuses(
    alerts: Union[
        List[PendingTestAlertSchema],
        List[PendingModelAlertSchema],
        List[PendingSourceFreshnessAlertSchema],
    ],
    statuses_filter: list[StatusFilterSchema],
) -> Union[
    List[PendingTestAlertSchema],
    List[PendingModelAlertSchema],
    List[PendingSourceFreshnessAlertSchema],
]:
    if len(statuses_filter) == 0:
        return alerts

    filtered_alerts = alerts
    for filter_item in statuses_filter:
        statuses: List[Status] = filter_item.values
        filter_func: Callable[
            [
                Union[
                    PendingTestAlertSchema,
                    PendingModelAlertSchema,
                    PendingSourceFreshnessAlertSchema,
                ]
            ],
            bool,
        ] = (
            lambda alert: alert.status in statuses
        )
        filtered_alerts = list(filter(filter_func, filtered_alerts))  # type: ignore

    return filtered_alerts


def _filter_alerts_by_resource_types(
    alerts: Union[
        List[PendingTestAlertSchema],
        List[PendingModelAlertSchema],
        List[PendingSourceFreshnessAlertSchema],
    ],
    resource_types_filter: list[ResourceTypeFilterSchema],
) -> Union[
    List[PendingTestAlertSchema],
    List[PendingModelAlertSchema],
    List[PendingSourceFreshnessAlertSchema],
]:
    if len(resource_types_filter) == 0:
        return alerts

    filtered_alerts = alerts
    for filter_item in resource_types_filter:
        resource_types: List[ResourceType] = filter_item.values
        filter_func: Callable[
            [
                Union[
                    PendingTestAlertSchema,
                    PendingModelAlertSchema,
                    PendingSourceFreshnessAlertSchema,
                ]
            ],
            bool,
        ] = (
            lambda alert: alert.resource_type.value in resource_types
        )
        filtered_alerts = list(filter(filter_func, filtered_alerts))  # type: ignore

    return filtered_alerts
