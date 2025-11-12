from datetime import datetime

from elementary.monitor.api.alerts.alert_filters import filter_alerts
from elementary.monitor.data_monitoring.schema import (
    FilterSchema,
    FiltersSchema,
    FilterType,
    ResourceType,
    ResourceTypeFilterSchema,
    Status,
    StatusFilterSchema,
)
from elementary.monitor.fetchers.alerts.schema.alert_data import (
    ModelAlertDataSchema,
    SourceFreshnessAlertDataSchema,
    TestAlertDataSchema,
)
from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    AlertStatus,
    AlertTypes,
    PendingAlertSchema,
)


def initial_alerts():
    test_alerts = [
        PendingAlertSchema(
            id="test_alert_1",
            alert_class_id="test_id_1",
            type=AlertTypes.TEST,
            detected_at=datetime(2022, 10, 10, 10, 0, 0),
            created_at=datetime(2022, 10, 10, 10, 0, 0),
            updated_at=datetime(2022, 10, 10, 10, 0, 0),
            status=AlertStatus.PENDING,
            data=TestAlertDataSchema(
                id="1",
                alert_class_id="test_id_1",
                model_unique_id="elementary.model_id_1",
                test_unique_id="test_id_1",
                test_name="test_1",
                tags=["one", "two"],
                model_meta=dict(owner='["jeff", "john"]'),
                status="fail",
                elementary_unique_id="elementary.model_id_1.test_id_1.9cf2f5f6ad.None.generic",
                detected_at=datetime(2022, 10, 10, 10, 0, 0),
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                test_type="dbt_test",
                test_sub_type="generic",
                test_results_description="a mock alert",
                test_results_query="select * from table",
                test_short_name="test_1",
                severity="ERROR",
                resource_type=ResourceType.TEST,
            ),
        ),
        PendingAlertSchema(
            id="test_alert_2",
            alert_class_id="test_id_2",
            type=AlertTypes.TEST,
            detected_at=datetime(2022, 10, 10, 10, 0, 0),
            created_at=datetime(2022, 10, 10, 10, 0, 0),
            updated_at=datetime(2022, 10, 10, 10, 0, 0),
            status=AlertStatus.PENDING,
            data=TestAlertDataSchema(
                id="2",
                alert_class_id="test_id_2",
                model_unique_id="elementary.model_id_1",
                test_unique_id="test_id_2",
                test_name="test_2",
                tags=["three"],
                model_meta=dict(owner='["jeff", "john"]'),
                status="fail",
                elementary_unique_id="elementary.model_id_1.test_id_2.9cf2f5f6ad.None.generic",
                detected_at=datetime(2022, 10, 10, 10, 0, 0),
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                test_type="dbt_test",
                test_sub_type="generic",
                test_results_description="a mock alert",
                test_results_query="select * from table",
                test_short_name="test_2",
                severity="ERROR",
                resource_type=ResourceType.TEST,
            ),
        ),
        PendingAlertSchema(
            id="test_alert_3",
            alert_class_id="test_id_3",
            type=AlertTypes.TEST,
            detected_at=datetime(2022, 10, 10, 10, 0, 0),
            created_at=datetime(2022, 10, 10, 10, 0, 0),
            updated_at=datetime(2022, 10, 10, 10, 0, 0),
            status=AlertStatus.PENDING,
            data=TestAlertDataSchema(
                id="3",
                alert_class_id="test_id_3",
                model_unique_id="elementary.model_id_2",
                test_unique_id="test_id_3",
                test_name="test_3",
                # invalid tag
                tags="one",  # type: ignore[arg-type]
                model_meta=dict(owner='["john"]'),
                status="fail",
                elementary_unique_id="elementary.model_id_1.test_id_3.9cf2f5f6ad.None.generic",
                detected_at=datetime(2022, 10, 10, 10, 0, 0),
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                test_type="dbt_test",
                test_sub_type="generic",
                test_results_description="a mock alert",
                test_results_query="select * from table",
                test_short_name="test_3",
                severity="ERROR",
                resource_type=ResourceType.TEST,
            ),
        ),
        PendingAlertSchema(
            id="test_alert_4",
            alert_class_id="test_id_4",
            type=AlertTypes.TEST,
            detected_at=datetime(2022, 10, 10, 10, 0, 0),
            created_at=datetime(2022, 10, 10, 10, 0, 0),
            updated_at=datetime(2022, 10, 10, 10, 0, 0),
            status=AlertStatus.PENDING,
            data=TestAlertDataSchema(
                id="4",
                alert_class_id="test_id_4",
                model_unique_id="elementary.model_id_2",
                test_unique_id="test_id_4",
                test_name="test_4",
                tags=["three", "four"],
                model_meta=dict(owner='["jeff"]'),
                status="warn",
                elementary_unique_id="elementary.model_id_1.test_id_4.9cf2f5f6ad.None.generic",
                detected_at=datetime(2022, 10, 10, 10, 0, 0),
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                test_type="dbt_test",
                test_sub_type="generic",
                test_results_description="a mock alert",
                test_results_query="select * from table",
                test_short_name="test_4",
                severity="ERROR",
                resource_type=ResourceType.TEST,
            ),
        ),
    ]
    model_alerts = [
        PendingAlertSchema(
            id="model_alert_1",
            alert_class_id="elementary.model_id_1",
            type=AlertTypes.MODEL,
            detected_at=datetime(2022, 10, 10, 10, 0, 0),
            created_at=datetime(2022, 10, 10, 10, 0, 0),
            updated_at=datetime(2022, 10, 10, 10, 0, 0),
            status=AlertStatus.PENDING,
            data=ModelAlertDataSchema(
                id="1",
                alert_class_id="elementary.model_id_1",
                model_unique_id="elementary.model_id_1",
                alias="modely",
                path="my/path",
                original_path="",
                materialization="table",
                message="",
                full_refresh=False,
                detected_at=datetime(2022, 10, 10, 10, 0, 0),
                tags=["one", "two"],
                model_meta=dict(owner='["jeff", "john"]'),
                status="error",
                database_name="test_db",
                schema_name="test_schema",
                resource_type=ResourceType.MODEL,
            ),
        ),
        PendingAlertSchema(
            id="model_alert_2",
            alert_class_id="elementary.model_id_1",
            type=AlertTypes.MODEL,
            detected_at=datetime(2022, 10, 10, 10, 0, 0),
            created_at=datetime(2022, 10, 10, 10, 0, 0),
            updated_at=datetime(2022, 10, 10, 10, 0, 0),
            status=AlertStatus.PENDING,
            data=ModelAlertDataSchema(
                id="2",
                alert_class_id="elementary.model_id_1",
                model_unique_id="elementary.model_id_1",
                alias="modely",
                path="my/path",
                original_path="",
                materialization="table",
                message="",
                full_refresh=False,
                detected_at=datetime(2022, 10, 10, 9, 0, 0),
                tags=["three"],
                model_meta=dict(owner='["john"]'),
                status="error",
                database_name="test_db",
                schema_name="test_schema",
                resource_type=ResourceType.MODEL,
            ),
        ),
        PendingAlertSchema(
            id="model_alert_3",
            alert_class_id="elementary.model_id_2",
            type=AlertTypes.MODEL,
            detected_at=datetime(2022, 10, 10, 8, 0, 0),
            created_at=datetime(2022, 10, 10, 8, 0, 0),
            updated_at=datetime(2022, 10, 10, 8, 0, 0),
            status=AlertStatus.PENDING,
            data=ModelAlertDataSchema(
                id="3",
                alert_class_id="elementary.model_id_2",
                model_unique_id="elementary.model_id_2",
                alias="model2",
                path="my/path2",
                original_path="",
                materialization="table",
                message="",
                full_refresh=False,
                detected_at=datetime(2022, 10, 10, 8, 0, 0),
                tags=["three", "four"],
                model_meta=dict(owner='["jeff"]'),
                status="skipped",
                database_name="test_db",
                schema_name="test_schema",
                resource_type=ResourceType.MODEL,
            ),
        ),
        PendingAlertSchema(
            id="model_alert_4",
            alert_class_id="elementary.model_id_3",
            type=AlertTypes.MODEL,
            detected_at=datetime(2022, 10, 10, 7, 0, 0),
            created_at=datetime(2022, 10, 10, 7, 0, 0),
            updated_at=datetime(2022, 10, 10, 7, 0, 0),
            status=AlertStatus.PENDING,
            data=ModelAlertDataSchema(
                id="4",
                alert_class_id="elementary.model_id_3",
                model_unique_id="elementary.model_id_3",
                alias="model3",
                path="my/path3",
                original_path="",
                materialization="incremental",
                message="",
                full_refresh=False,
                detected_at=datetime(2022, 10, 10, 7, 0, 0),
                tags=["microbatch"],
                model_meta=dict(owner='["alice"]'),
                status="partial success",
                database_name="test_db",
                schema_name="test_schema",
                resource_type=ResourceType.MODEL,
            ),
        ),
    ]
    source_freshness_alerts = [
        PendingAlertSchema(
            id="freshness_alert_1",
            alert_class_id="elementary.model_id_1",
            type=AlertTypes.SOURCE_FRESHNESS,
            detected_at=datetime(2022, 10, 10, 8, 0, 0),
            created_at=datetime(2022, 10, 10, 8, 0, 0),
            updated_at=datetime(2022, 10, 10, 8, 0, 0),
            status=AlertStatus.PENDING,
            data=SourceFreshnessAlertDataSchema(
                id="1",
                source_freshness_execution_id="1",
                alert_class_id="elementary.model_id_1",
                model_unique_id="elementary.model_id_1",
                path="my/path",
                detected_at=datetime(2022, 10, 10, 10, 0, 0),
                tags=["one", "two"],
                model_meta=dict(owner='["jeff", "john"]'),
                original_status="error",
                status="fail",
                snapshotted_at=datetime(2023, 8, 15, 12, 26, 6, 884065),
                max_loaded_at=datetime(1969, 12, 31, 0, 0, 0),
                max_loaded_at_time_ago_in_s=1692188766,
                source_name="elementary_integration_tests",
                identifier="any_type_column_anomalies_validation",
                error_after='{"count": null, "period": null}',
                warn_after='{"count": 1, "period": "minute"}',
                filter="null",
                error="problemz",
                database_name="test_db",
                schema_name="test_schema",
                resource_type=ResourceType.SOURCE_FRESHNESS,
            ),
        ),
        PendingAlertSchema(
            id="freshness_alert_2",
            alert_class_id="elementary.model_id_2",
            type=AlertTypes.SOURCE_FRESHNESS,
            detected_at=datetime(2022, 10, 10, 8, 0, 0),
            created_at=datetime(2022, 10, 10, 8, 0, 0),
            updated_at=datetime(2022, 10, 10, 8, 0, 0),
            status=AlertStatus.PENDING,
            data=SourceFreshnessAlertDataSchema(
                id="2",
                source_freshness_execution_id="2",
                alert_class_id="elementary.model_id_2",
                model_unique_id="elementary.model_id_2",
                path="my/path",
                detected_at=datetime(2022, 10, 10, 10, 0, 0),
                tags=["one", "two"],
                model_meta=dict(owner='["jeff", "john"]'),
                original_status="warn",
                status="warn",
                snapshotted_at=datetime(2023, 8, 15, 12, 26, 6, 884065),
                max_loaded_at=datetime(1969, 12, 31, 0, 0, 0),
                max_loaded_at_time_ago_in_s=1692188766,
                source_name="elementary_integration_tests",
                identifier="any_type_column_anomalies_validation",
                error_after='{"count": null, "period": null}',
                warn_after='{"count": 1, "period": "minute"}',
                filter="null",
                error="problemz",
                database_name="test_db",
                schema_name="test_schema",
                resource_type=ResourceType.SOURCE_FRESHNESS,
            ),
        ),
        PendingAlertSchema(
            id="freshness_alert_3",
            alert_class_id="elementary.model_id_3",
            type=AlertTypes.SOURCE_FRESHNESS,
            detected_at=datetime(2022, 10, 10, 8, 0, 0),
            created_at=datetime(2022, 10, 10, 8, 0, 0),
            updated_at=datetime(2022, 10, 10, 8, 0, 0),
            status=AlertStatus.PENDING,
            data=SourceFreshnessAlertDataSchema(
                id="3",
                source_freshness_execution_id="3",
                alert_class_id="elementary.model_id_3",
                model_unique_id="elementary.model_id_3",
                path="my/path",
                detected_at=datetime(2022, 10, 10, 10, 0, 0),
                tags=["one", "two"],
                model_meta=dict(owner='["jeff", "john"]'),
                original_status="runtime error",
                status="error",
                snapshotted_at=datetime(2023, 8, 15, 12, 26, 6, 884065),
                max_loaded_at=datetime(1969, 12, 31, 0, 0, 0),
                max_loaded_at_time_ago_in_s=1692188766,
                source_name="elementary_integration_tests",
                identifier="any_type_column_anomalies_validation",
                error_after='{"count": null, "period": null}',
                warn_after='{"count": 1, "period": "minute"}',
                filter="null",
                error="problemz",
                database_name="test_db",
                schema_name="test_schema",
                resource_type=ResourceType.SOURCE_FRESHNESS,
            ),
        ),
    ]
    return test_alerts, model_alerts, source_freshness_alerts


def test_filter_alerts_by_tags():
    test_alerts, model_alerts, _ = initial_alerts()

    filter = FiltersSchema(
        tags=[FilterSchema(values=["one"], type=FilterType.IS)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_1"
    assert filter_test_alerts[1].id == "test_alert_3"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_1"

    filter = FiltersSchema(
        tags=[FilterSchema(values=["one"], type=FilterType.IS_NOT)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_2"
    assert filter_test_alerts[1].id == "test_alert_4"
    assert len(filter_model_alerts) == 3
    assert filter_model_alerts[0].id == "model_alert_2"
    assert filter_model_alerts[1].id == "model_alert_3"
    assert filter_model_alerts[2].id == "model_alert_4"

    filter = FiltersSchema(
        tags=[FilterSchema(values=["three"], type=FilterType.IS)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_2"
    assert filter_test_alerts[1].id == "test_alert_4"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_2"
    assert filter_model_alerts[1].id == "model_alert_3"

    filter = FiltersSchema(
        tags=[FilterSchema(values=["three"], type=FilterType.IS_NOT)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_1"
    assert filter_test_alerts[1].id == "test_alert_3"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_1"
    assert filter_model_alerts[1].id == "model_alert_4"

    filter = FiltersSchema(
        tags=[FilterSchema(values=["four"], type=FilterType.IS)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_4"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_3"

    filter = FiltersSchema(
        tags=[FilterSchema(values=["four"], type=FilterType.IS_NOT)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 3
    assert sorted([alert.id for alert in filter_test_alerts]) == [
        "test_alert_1",
        "test_alert_2",
        "test_alert_3",
    ]
    assert len(filter_model_alerts) == 3
    assert sorted([alert.id for alert in filter_model_alerts]) == [
        "model_alert_1",
        "model_alert_2",
        "model_alert_4",
    ]

    filter = FiltersSchema(
        tags=[
            FilterSchema(values=["one"], type=FilterType.IS),
            FilterSchema(values=["two"], type=FilterType.IS),
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_1"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_1"

    filter = FiltersSchema(
        tags=[
            FilterSchema(values=["one"], type=FilterType.IS_NOT),
            FilterSchema(values=["two"], type=FilterType.IS_NOT),
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert sorted([alert.id for alert in filter_test_alerts]) == [
        "test_alert_2",
        "test_alert_4",
    ]
    assert len(filter_model_alerts) == 3
    assert sorted([alert.id for alert in filter_model_alerts]) == [
        "model_alert_2",
        "model_alert_3",
        "model_alert_4",
    ]

    filter = FiltersSchema(
        tags=[
            FilterSchema(values=["one"], type=FilterType.IS),
            FilterSchema(values=["four"], type=FilterType.IS),
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 0

    filter = FiltersSchema(
        tags=[
            FilterSchema(values=["one"], type=FilterType.IS_NOT),
            FilterSchema(values=["four"], type=FilterType.IS_NOT),
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_2"
    assert len(filter_model_alerts) == 2
    assert sorted([alert.id for alert in filter_model_alerts]) == [
        "model_alert_2",
        "model_alert_4",
    ]

    filter = FiltersSchema(
        tags=[
            FilterSchema(values=["one", "four"], type=FilterType.IS),
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
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

    filter = FiltersSchema(
        tags=[
            FilterSchema(values=["one", "four"], type=FilterType.IS_NOT),
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_2"
    assert len(filter_model_alerts) == 2
    assert sorted([alert.id for alert in filter_model_alerts]) == [
        "model_alert_2",
        "model_alert_4",
    ]

    filter = FiltersSchema(
        tags=[
            FilterSchema(values=["one"], type=FilterType.IS),
            FilterSchema(values=["three"], type=FilterType.IS_NOT),
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert sorted([alert.id for alert in filter_test_alerts]) == [
        "test_alert_1",
        "test_alert_3",
    ]
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_1"

    filter = FiltersSchema(
        tags=[
            FilterSchema(values=["one", "two"], type=FilterType.IS),
            FilterSchema(values=["three", "four"], type=FilterType.IS_NOT),
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert sorted([alert.id for alert in filter_test_alerts]) == [
        "test_alert_1",
        "test_alert_3",
    ]
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_1"


def test_filter_alerts_by_owners():
    test_alerts, model_alerts, _ = initial_alerts()

    filter = FiltersSchema(
        owners=[FilterSchema(values=["jeff"], type=FilterType.IS)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 3
    assert filter_test_alerts[0].id == "test_alert_1"
    assert filter_test_alerts[1].id == "test_alert_2"
    assert filter_test_alerts[2].id == "test_alert_4"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_1"
    assert filter_model_alerts[1].id == "model_alert_3"

    filter = FiltersSchema(
        owners=[FilterSchema(values=["jeff"], type=FilterType.IS_NOT)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_3"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_2"
    assert filter_model_alerts[1].id == "model_alert_4"

    filter = FiltersSchema(
        owners=[FilterSchema(values=["john"], type=FilterType.IS)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 3
    assert filter_test_alerts[0].id == "test_alert_1"
    assert filter_test_alerts[1].id == "test_alert_2"
    assert filter_test_alerts[2].id == "test_alert_3"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_1"
    assert filter_model_alerts[1].id == "model_alert_2"

    filter = FiltersSchema(
        owners=[FilterSchema(values=["john"], type=FilterType.IS_NOT)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_4"
    assert len(filter_model_alerts) == 2
    assert sorted([alert.id for alert in filter_model_alerts]) == [
        "model_alert_3",
        "model_alert_4",
    ]

    filter = FiltersSchema(
        owners=[
            FilterSchema(values=["jeff"], type=FilterType.IS),
            FilterSchema(values=["john"], type=FilterType.IS_NOT),
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_4"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_3"

    filter = FiltersSchema(
        owners=[
            FilterSchema(values=["jeff", "john"], type=FilterType.IS),
            FilterSchema(values=["fake"], type=FilterType.IS_NOT),
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 4
    assert sorted([alert.id for alert in filter_test_alerts]) == [
        "test_alert_1",
        "test_alert_2",
        "test_alert_3",
        "test_alert_4",
    ]
    assert len(filter_model_alerts) == 3
    assert sorted([alert.id for alert in filter_model_alerts]) == [
        "model_alert_1",
        "model_alert_2",
        "model_alert_3",
    ]


def test_filter_alerts_by_model():
    test_alerts, model_alerts, _ = initial_alerts()

    filter = FiltersSchema(
        models=[FilterSchema(values=["model_id_1"], type=FilterType.IS)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_1"
    assert filter_test_alerts[1].id == "test_alert_2"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_1"
    assert filter_model_alerts[1].id == "model_alert_2"

    filter = FiltersSchema(
        models=[FilterSchema(values=["model_id_1"], type=FilterType.IS_NOT)],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_3"
    assert filter_test_alerts[1].id == "test_alert_4"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_3"
    assert filter_model_alerts[1].id == "model_alert_4"

    filter = FiltersSchema(
        models=[FilterSchema(values=["model_id_2"], type=FilterType.IS)], statuses=[]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_3"
    assert filter_test_alerts[1].id == "test_alert_4"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_3"

    filter = FiltersSchema(
        models=[FilterSchema(values=["model_id_2"], type=FilterType.IS_NOT)],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "test_alert_1"
    assert filter_test_alerts[1].id == "test_alert_2"
    assert len(filter_model_alerts) == 3
    assert filter_model_alerts[0].id == "model_alert_1"
    assert filter_model_alerts[1].id == "model_alert_2"
    assert filter_model_alerts[2].id == "model_alert_4"

    filter = FiltersSchema(
        models=[FilterSchema(values=["model_id_1", "model_id_2"], type=FilterType.IS)],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
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
            FilterSchema(values=["model_id_1"], type=FilterType.IS),
            FilterSchema(values=["model_id_2"], type=FilterType.IS),
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 0


def test_filter_alerts_by_node_names():
    test_alerts, model_alerts, _ = initial_alerts()

    filter = FiltersSchema(node_names=["test_3", "model_id_1"], statuses=[])
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_3"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "model_alert_1"
    assert filter_model_alerts[1].id == "model_alert_2"

    filter = FiltersSchema(node_names=["model_id_2"], statuses=[])
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_3"

    filter = FiltersSchema(node_names=["model_id_3"], statuses=[])
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "model_alert_4"


def test_filter_alerts_by_statuses():
    (
        test_alerts,
        model_alerts,
        source_freshness_alerts,
    ) = initial_alerts()

    filter = FiltersSchema(
        statuses=[StatusFilterSchema(values=[Status.WARN], type=FilterType.IS)],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    filter_source_freshness_alerts = filter_alerts(source_freshness_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_4"
    assert len(filter_model_alerts) == 0
    assert len(filter_source_freshness_alerts) == 1

    filter = FiltersSchema(
        statuses=[
            StatusFilterSchema(
                values=[Status.ERROR, Status.SKIPPED], type=FilterType.IS
            )
        ]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 3

    filter = FiltersSchema(
        statuses=[
            StatusFilterSchema(
                values=[Status.FAIL, Status.WARN, Status.RUNTIME_ERROR],
                type=FilterType.IS,
            )
        ]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    filter_source_freshness_alerts = filter_alerts(source_freshness_alerts, filter)
    assert len(filter_test_alerts) == 4
    assert len(filter_model_alerts) == 0
    assert len(filter_source_freshness_alerts) == 2

    filter = FiltersSchema(
        statuses=[
            StatusFilterSchema(
                values=[Status.FAIL, Status.WARN, Status.ERROR],
                type=FilterType.IS_NOT,
            )
        ]
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 2
    assert sorted([alert.id for alert in filter_model_alerts]) == [
        "model_alert_3",
        "model_alert_4",
    ]


def test_filter_alerts_by_resource_types():
    test_alerts, model_alerts, _ = initial_alerts()
    all_alerts = test_alerts + model_alerts

    filter = FiltersSchema(
        resource_types=[
            ResourceTypeFilterSchema(values=[ResourceType.TEST], type=FilterType.IS)
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(all_alerts, filter)
    assert filter_test_alerts == test_alerts

    filter = FiltersSchema(
        resource_types=[
            ResourceTypeFilterSchema(values=[ResourceType.MODEL], type=FilterType.IS)
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(all_alerts, filter)
    assert filter_test_alerts == model_alerts

    filter = FiltersSchema(
        resource_types=[
            ResourceTypeFilterSchema(values=[ResourceType.TEST], type=FilterType.IS_NOT)
        ],
        statuses=[],
    )
    filter_test_alerts = filter_alerts(all_alerts, filter)
    assert filter_test_alerts == model_alerts


def test_filter_alerts():
    test_alerts, model_alerts, _ = initial_alerts()

    # Test that empty filter returns all the alerts except for skipped and partial success.
    filter = FiltersSchema()
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == len(test_alerts)
    assert (
        len(filter_model_alerts) == len(model_alerts) - 2
    )  # 1 skipped + 1 partial success model alert

    # Test that passing no filter returns all the alerts.
    filter_test_alerts = filter_alerts(test_alerts)
    filter_model_alerts = filter_alerts(model_alerts)
    assert len(filter_test_alerts) == len(test_alerts)
    assert (
        len(filter_model_alerts) == len(model_alerts) - 2
    )  # 1 skipped + 1 partial success model alert

    # Test that filter with unsupported selector returns no alert
    filter = FiltersSchema(last_invocation=True, selector="last_invocation")
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 0


def test_multi_filters():
    test_alerts, _, _ = initial_alerts()

    filter = FiltersSchema(
        tags=[FilterSchema(values=["one", "three"], type=FilterType.IS)],
        owners=[FilterSchema(values=["jeff"], type=FilterType.IS)],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    assert len(filter_test_alerts) == 3
    assert sorted([alert.id for alert in filter_test_alerts]) == [
        "test_alert_1",
        "test_alert_2",
        "test_alert_4",
    ]

    filter = FiltersSchema(
        tags=[FilterSchema(values=["one", "three"], type=FilterType.IS)],
        owners=[FilterSchema(values=["fake"], type=FilterType.IS)],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    assert len(filter_test_alerts) == 0

    filter = FiltersSchema(
        tags=[FilterSchema(values=["one", "three"], type=FilterType.IS)],
        owners=[FilterSchema(values=["jeff"], type=FilterType.IS)],
        statuses=[
            StatusFilterSchema(
                values=[Status.WARN],
                type=FilterType.IS,
            )
        ],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "test_alert_4"

    filter = FiltersSchema(
        tags=[FilterSchema(values=["one", "three"], type=FilterType.IS)],
        owners=[FilterSchema(values=["jeff"], type=FilterType.IS)],
        statuses=[
            StatusFilterSchema(
                values=[Status.FAIL],
                type=FilterType.IS,
            )
        ],
    )
    filter_test_alerts = filter_alerts(test_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert sorted([alert.id for alert in filter_test_alerts]) == [
        "test_alert_1",
        "test_alert_2",
    ]
