from elementary.monitor.api.alerts.alert_filters import (
    _filter_alerts_by_models,
    _filter_alerts_by_node_names,
    _filter_alerts_by_owners,
    _filter_alerts_by_resource_types,
    _filter_alerts_by_statuses,
    _filter_alerts_by_tags,
    _find_common_alerts,
    filter_alerts,
)
from elementary.monitor.data_monitoring.schema import (
    FilterSchema,
    FiltersSchema,
    ResourceType,
    ResourceTypeFilterSchema,
    Status,
    StatusFilterSchema,
    SupportedFilterTypes,
)
from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    AlertTypes,
    PendingAlertSchema,
)


def initial_alerts():
    test_alerts = [
        PendingAlertSchema(
            id="test_alert_1",
            alert_class_id="test_id_1",
            type=AlertTypes.TEST,
            detected_at="2022-10-10 10:00:00",
            created_at="2022-10-10 10:00:00",
            updated_at="2022-10-10 10:00:00",
            status="pending",
            data=dict(
                id="1",
                alert_class_id="test_id_1",
                model_unique_id="elementary.model_id_1",
                test_unique_id="test_id_1",
                test_name="test_1",
                test_created_at="2022-10-10 10:10:10",
                tags='["one", "two"]',
                model_meta=dict(owner='["jeff", "john"]'),
                status="fail",
                elementary_unique_id="elementary.model_id_1.test_id_1.9cf2f5f6ad.None.generic",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                suppression_status="pending",
                test_type="dbt_test",
                test_sub_type="generic",
                test_results_description="a mock alert",
                test_results_query="select * from table",
                test_short_name="test_1",
                severity="ERROR",
            ),
        ),
        PendingAlertSchema(
            id="test_alert_2",
            alert_class_id="test_id_2",
            type=AlertTypes.TEST,
            detected_at="2022-10-10 10:00:00",
            created_at="2022-10-10 10:00:00",
            updated_at="2022-10-10 10:00:00",
            status="pending",
            data=dict(
                id="2",
                alert_class_id="test_id_2",
                model_unique_id="elementary.model_id_1",
                test_unique_id="test_id_2",
                test_name="test_2",
                test_created_at="2022-10-10 09:10:10",
                tags='["three"]',
                model_meta=dict(owner='["jeff", "john"]'),
                status="fail",
                elementary_unique_id="elementary.model_id_1.test_id_2.9cf2f5f6ad.None.generic",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                suppression_status="pending",
                test_type="dbt_test",
                test_sub_type="generic",
                test_results_description="a mock alert",
                test_results_query="select * from table",
                test_short_name="test_2",
                severity="ERROR",
            ),
        ),
        PendingAlertSchema(
            id="test_alert_3",
            alert_class_id="test_id_3",
            type=AlertTypes.TEST,
            detected_at="2022-10-10 10:00:00",
            created_at="2022-10-10 10:00:00",
            updated_at="2022-10-10 10:00:00",
            status="pending",
            data=dict(
                id="3",
                alert_class_id="test_id_3",
                model_unique_id="elementary.model_id_2",
                test_unique_id="test_id_3",
                test_name="test_3",
                test_created_at="2022-10-10 10:10:10",
                # invalid tag
                tags="one",
                model_meta=dict(owner='["john"]'),
                status="fail",
                elementary_unique_id="elementary.model_id_1.test_id_3.9cf2f5f6ad.None.generic",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                suppression_status="pending",
                test_type="dbt_test",
                test_sub_type="generic",
                test_results_description="a mock alert",
                test_results_query="select * from table",
                test_short_name="test_3",
                severity="ERROR",
            ),
        ),
        PendingAlertSchema(
            id="test_alert_4",
            alert_class_id="test_id_4",
            type=AlertTypes.TEST,
            detected_at="2022-10-10 10:00:00",
            created_at="2022-10-10 10:00:00",
            updated_at="2022-10-10 10:00:00",
            status="pending",
            data=dict(
                id="4",
                alert_class_id="test_id_4",
                model_unique_id="elementary.model_id_2",
                test_unique_id="test_id_4",
                test_name="test_4",
                test_created_at="2022-10-10 09:10:10",
                tags='["three", "four"]',
                model_meta=dict(owner='["jeff"]'),
                status="warn",
                elementary_unique_id="elementary.model_id_1.test_id_4.9cf2f5f6ad.None.generic",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                suppression_status="pending",
                test_type="dbt_test",
                test_sub_type="generic",
                test_results_description="a mock alert",
                test_results_query="select * from table",
                test_short_name="test_4",
                severity="ERROR",
            ),
        ),
    ]
    model_alerts = [
        PendingAlertSchema(
            id="model_alert_1",
            alert_class_id="elementary.model_id_1",
            type=AlertTypes.MODEL,
            detected_at="2022-10-10 10:00:00",
            created_at="2022-10-10 10:00:00",
            updated_at="2022-10-10 10:00:00",
            status="pending",
            data=dict(
                id="1",
                alert_class_id="elementary.model_id_1",
                model_unique_id="elementary.model_id_1",
                alias="modely",
                path="my/path",
                original_path="",
                materialization="table",
                message="",
                full_refresh=False,
                detected_at="2022-10-10 10:00:00",
                alert_suppression_interval=0,
                tags='["one", "two"]',
                model_meta=dict(owner='["jeff", "john"]'),
                status="error",
                database_name="test_db",
                schema_name="test_schema",
                suppression_status="pending",
            ),
        ),
        PendingAlertSchema(
            id="model_alert_2",
            alert_class_id="elementary.model_id_1",
            type=AlertTypes.MODEL,
            detected_at="2022-10-10 10:00:00",
            created_at="2022-10-10 10:00:00",
            updated_at="2022-10-10 10:00:00",
            status="pending",
            data=dict(
                id="2",
                alert_class_id="elementary.model_id_1",
                model_unique_id="elementary.model_id_1",
                alias="modely",
                path="my/path",
                original_path="",
                materialization="table",
                message="",
                full_refresh=False,
                detected_at="2022-10-10 09:00:00",
                alert_suppression_interval=3,
                tags='["three"]',
                model_meta=dict(owner='["john"]'),
                status="error",
                database_name="test_db",
                schema_name="test_schema",
                suppression_status="pending",
            ),
        ),
        PendingAlertSchema(
            id="model_alert_3",
            alert_class_id="elementary.model_id_2",
            type=AlertTypes.MODEL,
            detected_at="2022-10-10 08:00:00",
            created_at="2022-10-10 08:00:00",
            updated_at="2022-10-10 08:00:00",
            status="pending",
            data=dict(
                id="3",
                alert_class_id="elementary.model_id_2",
                model_unique_id="elementary.model_id_2",
                alias="model2",
                path="my/path2",
                original_path="",
                materialization="table",
                message="",
                full_refresh=False,
                detected_at="2022-10-10 08:00:00",
                alert_suppression_interval=1,
                tags='["three", "four"]',
                model_meta=dict(owner='["jeff"]'),
                status="skipped",
                database_name="test_db",
                schema_name="test_schema",
                suppression_status="pending",
            ),
        ),
    ]
    source_freshness_alerts = [
        PendingAlertSchema(
            id="freshness_alert_1",
            alert_class_id="elementary.model_id_1",
            type=AlertTypes.SOURCE_FRESHNESS,
            detected_at="2022-10-10 08:00:00",
            created_at="2022-10-10 08:00:00",
            updated_at="2022-10-10 08:00:00",
            status="pending",
            data=dict(
                id="1",
                source_freshness_execution_id="1",
                alert_class_id="elementary.model_id_1",
                model_unique_id="elementary.model_id_1",
                alias="modely",
                path="my/path",
                original_path="",
                materialization="table",
                message="",
                full_refresh=False,
                detected_at="2022-10-10 10:00:00",
                alert_suppression_interval=0,
                tags='["one", "two"]',
                model_meta=dict(owner='["jeff", "john"]'),
                original_status="error",
                status="fail",
                snapshotted_at="2023-08-15T12:26:06.884065+00:00",
                max_loaded_at="1969-12-31T00:00:00+00:00",
                max_loaded_at_time_ago_in_s=1692188766.884065,
                source_name="elementary_integration_tests",
                identifier="any_type_column_anomalies_validation",
                error_after='{"count": null, "period": null}',
                warn_after='{"count": 1, "period": "minute"}',
                filter="null",
                error="problemz",
                database_name="test_db",
                schema_name="test_schema",
                suppression_status="pending",
            ),
        ),
        PendingAlertSchema(
            id="freshness_alert_2",
            alert_class_id="elementary.model_id_2",
            type=AlertTypes.SOURCE_FRESHNESS,
            detected_at="2022-10-10 08:00:00",
            created_at="2022-10-10 08:00:00",
            updated_at="2022-10-10 08:00:00",
            status="pending",
            data=dict(
                id="2",
                source_freshness_execution_id="2",
                alert_class_id="elementary.model_id_2",
                model_unique_id="elementary.model_id_2",
                alias="modely",
                path="my/path",
                original_path="",
                materialization="table",
                message="",
                full_refresh=False,
                detected_at="2022-10-10 10:00:00",
                alert_suppression_interval=0,
                tags='["one", "two"]',
                model_meta=dict(owner='["jeff", "john"]'),
                status="warn",
                original_status="warn",
                snapshotted_at="2023-08-15T12:26:06.884065+00:00",
                max_loaded_at="1969-12-31T00:00:00+00:00",
                max_loaded_at_time_ago_in_s=1692188766.884065,
                source_name="elementary_integration_tests",
                identifier="any_type_column_anomalies_validation",
                error_after='{"count": null, "period": null}',
                warn_after='{"count": 1, "period": "minute"}',
                filter="null",
                error="problemz",
                database_name="test_db",
                schema_name="test_schema",
                suppression_status="pending",
            ),
        ),
        PendingAlertSchema(
            id="freshness_alert_3",
            alert_class_id="elementary.model_id_3",
            type=AlertTypes.SOURCE_FRESHNESS,
            detected_at="2022-10-10 08:00:00",
            created_at="2022-10-10 08:00:00",
            updated_at="2022-10-10 08:00:00",
            status="pending",
            data=dict(
                id="3",
                source_freshness_execution_id="3",
                alert_class_id="elementary.model_id_3",
                model_unique_id="elementary.model_id_3",
                alias="modely",
                path="my/path",
                original_path="",
                materialization="table",
                message="",
                full_refresh=False,
                detected_at="2022-10-10 10:00:00",
                alert_suppression_interval=0,
                tags='["one", "two"]',
                model_meta=dict(owner='["jeff", "john"]'),
                original_status="runtime error",
                status="error",
                snapshotted_at="2023-08-15T12:26:06.884065+00:00",
                max_loaded_at="1969-12-31T00:00:00+00:00",
                max_loaded_at_time_ago_in_s=1692188766.884065,
                source_name="elementary_integration_tests",
                identifier="any_type_column_anomalies_validation",
                error_after='{"count": null, "period": null}',
                warn_after='{"count": 1, "period": "minute"}',
                filter="null",
                error="problemz",
                database_name="test_db",
                schema_name="test_schema",
                suppression_status="pending",
            ),
        ),
    ]
    return test_alerts, model_alerts, source_freshness_alerts


def test_find_common_alerts():
    test_alerts, model_alerts, _ = initial_alerts()

    common_alerts = _find_common_alerts(test_alerts, model_alerts)
    assert len(common_alerts) == 0

    common_alerts = _find_common_alerts(
        [test_alerts[0], test_alerts[1], test_alerts[2]],
        [test_alerts[0], test_alerts[2], test_alerts[3]],
    )
    assert len(common_alerts) == 2
    assert sorted([alert.id for alert in common_alerts]) == [
        "test_alert_1",
        "test_alert_3",
    ]


def test_filter_alerts_by_tags():
    test_alerts, model_alerts, _ = initial_alerts()

    filter = FiltersSchema(
        tags=[FilterSchema(values=["one"], type=SupportedFilterTypes.IS)]
    )
    filter_test_alerts = _filter_alerts_by_tags(test_alerts, filter.tags)
    filter_model_alerts = _filter_alerts_by_tags(model_alerts, filter.tags)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_1"
    assert filter_test_alerts[1].id == "test_alert_3"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_1"

    filter = FiltersSchema(
        tags=[FilterSchema(values=["three"], type=SupportedFilterTypes.IS)]
    )
    filter_test_alerts = _filter_alerts_by_tags(test_alerts, filter.tags)
    filter_model_alerts = _filter_alerts_by_tags(model_alerts, filter.tags)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_2"
    assert filter_test_alerts[1].id == "test_alert_4"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_2"
    assert filter_model_alerts[1].id == "model_alert_3"

    filter = FiltersSchema(
        tags=[FilterSchema(values=["four"], type=SupportedFilterTypes.IS)]
    )
    filter_test_alerts = _filter_alerts_by_tags(test_alerts, filter.tags)
    filter_model_alerts = _filter_alerts_by_tags(model_alerts, filter.tags)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_4"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_3"

    filter = FiltersSchema(
        tags=[
            FilterSchema(values=["one"], type=SupportedFilterTypes.IS),
            FilterSchema(values=["two"], type=SupportedFilterTypes.IS),
        ]
    )
    filter_test_alerts = _filter_alerts_by_tags(test_alerts, filter.tags)
    filter_model_alerts = _filter_alerts_by_tags(model_alerts, filter.tags)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_1"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_1"

    filter = FiltersSchema(
        tags=[
            FilterSchema(values=["one"], type=SupportedFilterTypes.IS),
            FilterSchema(values=["four"], type=SupportedFilterTypes.IS),
        ]
    )
    filter_test_alerts = _filter_alerts_by_tags(test_alerts, filter.tags)
    filter_model_alerts = _filter_alerts_by_tags(model_alerts, filter.tags)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 0

    filter = FiltersSchema(
        tags=[
            FilterSchema(values=["one", "four"], type=SupportedFilterTypes.IS),
        ]
    )
    filter_test_alerts = _filter_alerts_by_tags(test_alerts, filter.tags)
    filter_model_alerts = _filter_alerts_by_tags(model_alerts, filter.tags)
    assert len(filter_test_alerts) == 3
    assert sorted([alert.id for alert in filter_test_alerts]) == [
        "test_alert_1",
        "test_alert_3",
        "test_alert_4",
    ]
    assert len(filter_model_alerts) == 2
    assert sorted([alert.id for alert in filter_model_alerts]) == [
        "model_alert_1",
        "model_alert_3",
    ]


def test_filter_alerts_by_owners():
    test_alerts, model_alerts, _ = initial_alerts()

    filter = FiltersSchema(
        owners=[FilterSchema(values=["jeff"], type=SupportedFilterTypes.IS)]
    )
    filter_test_alerts = _filter_alerts_by_owners(test_alerts, filter.owners)
    filter_model_alerts = _filter_alerts_by_owners(model_alerts, filter.owners)
    assert len(filter_test_alerts) == 3
    assert filter_test_alerts[0].id == "test_alert_1"
    assert filter_test_alerts[1].id == "test_alert_2"
    assert filter_test_alerts[2].id == "test_alert_4"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_1"
    assert filter_model_alerts[1].id == "model_alert_3"

    filter = FiltersSchema(
        owners=[FilterSchema(values=["john"], type=SupportedFilterTypes.IS)]
    )
    filter_test_alerts = _filter_alerts_by_owners(test_alerts, filter.owners)
    filter_model_alerts = _filter_alerts_by_owners(model_alerts, filter.owners)
    assert len(filter_test_alerts) == 3
    assert filter_test_alerts[0].id == "test_alert_1"
    assert filter_test_alerts[1].id == "test_alert_2"
    assert filter_test_alerts[2].id == "test_alert_3"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_1"
    assert filter_model_alerts[1].id == "model_alert_2"


def test_filter_alerts_by_model():
    test_alerts, model_alerts, _ = initial_alerts()

    filter = FiltersSchema(
        models=[FilterSchema(values=["model_id_1"], type=SupportedFilterTypes.IS)]
    )
    filter_test_alerts = _filter_alerts_by_models(test_alerts, filter.models)
    filter_model_alerts = _filter_alerts_by_models(model_alerts, filter.models)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_1"
    assert filter_test_alerts[1].id == "test_alert_2"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_1"
    assert filter_model_alerts[1].id == "model_alert_2"

    filter = FiltersSchema(
        models=[FilterSchema(values=["model_id_2"], type=SupportedFilterTypes.IS)]
    )
    filter_test_alerts = _filter_alerts_by_models(test_alerts, filter.models)
    filter_model_alerts = _filter_alerts_by_models(model_alerts, filter.models)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_3"
    assert filter_test_alerts[1].id == "test_alert_4"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_3"

    filter = FiltersSchema(
        models=[
            FilterSchema(
                values=["model_id_1", "model_id_2"], type=SupportedFilterTypes.IS
            )
        ]
    )
    filter_test_alerts = _filter_alerts_by_models(test_alerts, filter.models)
    filter_model_alerts = _filter_alerts_by_models(model_alerts, filter.models)
    assert len(filter_test_alerts) == 4
    assert filter_test_alerts[0].id == "test_alert_1"
    assert filter_test_alerts[1].id == "test_alert_2"
    assert filter_test_alerts[2].id == "test_alert_3"
    assert filter_test_alerts[3].id == "test_alert_4"
    assert len(filter_model_alerts) == 3
    assert filter_model_alerts[0].id == "model_alert_1"
    assert filter_model_alerts[1].id == "model_alert_2"
    assert filter_model_alerts[2].id == "model_alert_3"

    filter = FiltersSchema(
        models=[
            FilterSchema(values=["model_id_1"], type=SupportedFilterTypes.IS),
            FilterSchema(values=["model_id_2"], type=SupportedFilterTypes.IS),
        ]
    )
    filter_test_alerts = _filter_alerts_by_models(test_alerts, filter.models)
    filter_model_alerts = _filter_alerts_by_models(model_alerts, filter.models)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 0


def test_filter_alerts_by_node_names():
    test_alerts, model_alerts, _ = initial_alerts()

    filter = FiltersSchema(node_names=["test_3", "model_id_1"])
    filter_test_alerts = _filter_alerts_by_node_names(test_alerts, filter.node_names)
    filter_model_alerts = _filter_alerts_by_node_names(model_alerts, filter.node_names)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_3"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_1"
    assert filter_model_alerts[1].id == "model_alert_2"

    filter = FiltersSchema(node_names=["model_id_2"])
    filter_test_alerts = _filter_alerts_by_node_names(test_alerts, filter.node_names)
    filter_model_alerts = _filter_alerts_by_node_names(model_alerts, filter.node_names)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_3"

    filter = FiltersSchema(node_names=["model_id_3"])
    filter_test_alerts = _filter_alerts_by_node_names(test_alerts, filter.node_names)
    filter_model_alerts = _filter_alerts_by_node_names(model_alerts, filter.node_names)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 0


def test_filter_alerts_by_statuses():
    (
        test_alerts,
        model_alerts,
        source_freshness_alerts,
    ) = initial_alerts()

    filter = FiltersSchema(
        statuses=[
            StatusFilterSchema(values=[Status.WARN], type=SupportedFilterTypes.IS)
        ]
    )
    filter_test_alerts = _filter_alerts_by_statuses(test_alerts, filter.statuses)
    filter_model_alerts = _filter_alerts_by_statuses(model_alerts, filter.statuses)
    filter_source_freshness_alerts = _filter_alerts_by_statuses(
        source_freshness_alerts, filter.statuses
    )
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_4"
    assert len(filter_model_alerts) == 0
    assert len(filter_source_freshness_alerts) == 1

    filter = FiltersSchema(
        statuses=[
            StatusFilterSchema(
                values=[Status.ERROR, Status.SKIPPED], type=SupportedFilterTypes.IS
            )
        ]
    )
    filter_test_alerts = _filter_alerts_by_statuses(test_alerts, filter.statuses)
    filter_model_alerts = _filter_alerts_by_statuses(model_alerts, filter.statuses)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 3

    filter = FiltersSchema(
        statuses=[
            StatusFilterSchema(
                values=[Status.FAIL, Status.WARN, Status.RUNTIME_ERROR],
                type=SupportedFilterTypes.IS,
            )
        ]
    )
    filter_test_alerts = _filter_alerts_by_statuses(test_alerts, filter.statuses)
    filter_model_alerts = _filter_alerts_by_statuses(model_alerts, filter.statuses)
    filter_source_freshness_alerts = _filter_alerts_by_statuses(
        source_freshness_alerts, filter.statuses
    )
    assert len(filter_test_alerts) == 4
    assert len(filter_model_alerts) == 0
    assert len(filter_source_freshness_alerts) == 2


def test_filter_alerts_by_resource_types():
    test_alerts, model_alerts, _ = initial_alerts()
    all_alerts = test_alerts + model_alerts

    filter = FiltersSchema(
        resource_types=[
            ResourceTypeFilterSchema(
                values=[ResourceType.TEST], type=SupportedFilterTypes.IS
            )
        ]
    )
    filter_test_alerts = _filter_alerts_by_resource_types(
        all_alerts, filter.resource_types
    )
    assert filter_test_alerts == test_alerts

    filter = FiltersSchema(
        resource_types=[
            ResourceTypeFilterSchema(
                values=[ResourceType.MODEL], type=SupportedFilterTypes.IS
            )
        ]
    )
    filter_test_alerts = _filter_alerts_by_resource_types(
        all_alerts, filter.resource_types
    )
    assert filter_test_alerts == model_alerts


def test_filter_alerts():
    test_alerts, model_alerts, _ = initial_alerts()

    # Test that empty filter returns all the alerts except for skipped.
    filter = FiltersSchema()
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == len(test_alerts)
    assert len(filter_model_alerts) == len(model_alerts) - 1  # 1 skipped model alert

    # Test that passing no filter returns all the alerts.
    filter_test_alerts = filter_alerts(test_alerts)
    filter_model_alerts = filter_alerts(model_alerts)
    assert len(filter_test_alerts) == len(test_alerts)
    assert len(filter_model_alerts) == len(model_alerts) - 1  # 1 skipped model alert

    # Test that filter with unsupported selector returns no alert
    filter = FiltersSchema(last_invocation=True, selector="last_invocation")
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 0


def test_multi_filters():
    test_alerts, _, _ = initial_alerts()

    filter = FiltersSchema(
        tags=[FilterSchema(values=["one", "three"], type=SupportedFilterTypes.IS)],
        owners=[FilterSchema(values=["jeff"], type=SupportedFilterTypes.IS)],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    assert len(filter_test_alerts) == 3
    assert sorted([alert.id for alert in filter_test_alerts]) == [
        "test_alert_1",
        "test_alert_2",
        "test_alert_4",
    ]

    filter = FiltersSchema(
        tags=[FilterSchema(values=["one", "three"], type=SupportedFilterTypes.IS)],
        owners=[FilterSchema(values=["fake"], type=SupportedFilterTypes.IS)],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    assert len(filter_test_alerts) == 0

    filter = FiltersSchema(
        tags=[FilterSchema(values=["one", "three"], type=SupportedFilterTypes.IS)],
        owners=[FilterSchema(values=["jeff"], type=SupportedFilterTypes.IS)],
        statuses=[
            StatusFilterSchema(
                values=[Status.WARN],
                type=SupportedFilterTypes.IS,
            )
        ],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_4"

    filter = FiltersSchema(
        tags=[FilterSchema(values=["one", "three"], type=SupportedFilterTypes.IS)],
        owners=[FilterSchema(values=["jeff"], type=SupportedFilterTypes.IS)],
        statuses=[
            StatusFilterSchema(
                values=[Status.FAIL],
                type=SupportedFilterTypes.IS,
            )
        ],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert sorted([alert.id for alert in filter_test_alerts]) == [
        "test_alert_1",
        "test_alert_2",
    ]
