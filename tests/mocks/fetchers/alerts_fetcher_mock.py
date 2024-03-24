from datetime import datetime, timedelta
from typing import List

from elementary.config.config import Config
from elementary.monitor.fetchers.alerts.alerts import AlertsFetcher
from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    AlertTypes,
    PendingAlertSchema,
)
from elementary.utils.time import DATETIME_FORMAT
from tests.mocks.dbt_runner_mock import MockDbtRunner

CURRENT_DATETIME_UTC = datetime.utcnow()
CURRENT_TIMESTAMP_UTC = CURRENT_DATETIME_UTC.strftime(DATETIME_FORMAT)

PENDDING_TEST_ALERTS_MOCK_DATA = [
    # Alert within suppression interval
    dict(
        id="alert_id_1",
        alert_class_id="test_id_1.column.generic",
        test_unique_id="test_id_1",
        model_unique_id="model_id_1",
        detected_at=CURRENT_TIMESTAMP_UTC,
        database_name="test_db",
        schema_name="test_schema",
        table_name="table",
        column_name="column",
        test_type="dbt_test",
        test_sub_type="generic",
        test_results_description="a mock alert",
        owners='["jeff"]',
        tags='["best_test"]',
        test_results_query="select * from table",
        test_rows_sample="[]",
        other=None,
        test_name="test_1",
        test_short_name="short",
        test_params="{}",
        severity="ERROR",
        test_meta='{ "alerts_config": {"alert_suppression_interval": 2} }',
        model_meta="{}",
        status="fail",
        suppression_status="pending",
        sent_at=None,
        test_created_at="2022-10-09 10:10:10",
        elementary_unique_id="elementary.model_id_1.test_id_1.9cf2f5f6ad.None.generic",
    ),
    # Alert after suppression interval
    dict(
        id="alert_id_2",
        alert_class_id="test_id_4.column.generic",
        test_unique_id="test_id_4",
        model_unique_id="model_id_1",
        detected_at=CURRENT_TIMESTAMP_UTC,
        database_name="test_db",
        schema_name="test_schema",
        table_name="table",
        column_name="column",
        test_type="dbt_test",
        test_sub_type="generic",
        test_results_description="a mock alert",
        owners='["jeff"]',
        tags='["best_test"]',
        test_results_query="select * from table",
        test_rows_sample="[]",
        other=None,
        test_name="test_4",
        test_short_name="short",
        test_params="{}",
        severity="ERROR",
        test_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
        model_meta="{}",
        status="fail",
        suppression_status="pending",
        sent_at=None,
        test_created_at="2022-10-09 10:10:10",
        elementary_unique_id="elementary.model_id_1.test_id_4.9cf2f5f6ad.None.generic",
    ),
    # Alert without suppression interval
    dict(
        id="alert_id_3",
        alert_class_id="test_id_2.column.row_count",
        test_unique_id="test_id_2",
        model_unique_id="model_id_2",
        detected_at=CURRENT_TIMESTAMP_UTC,
        database_name="test_db",
        schema_name="test_schema",
        table_name="table",
        column_name="column",
        test_type="anomaly_detection",
        test_sub_type="row_count",
        test_results_description="a mock alert",
        owners='["jeff"]',
        tags='["best_test"]',
        test_results_query="select * from table",
        test_rows_sample="[]",
        other=None,
        test_name="test_2",
        test_short_name="shorter",
        test_params="{}",
        severity="ERROR",
        test_meta='{ "alerts_config": {"alert_suppression_interval": 0} }',
        model_meta="{}",
        status="fail",
        suppression_status="pending",
        sent_at=None,
        test_created_at="2022-10-09 10:10:10",
        elementary_unique_id="elementary.model_id_1.test_id_2.9cf2f5f6ad.None.generic",
    ),
    # First occurrence alert with suppression interval
    dict(
        id="alert_id_4",
        alert_class_id="test_id_3.column.row_count",
        test_unique_id="test_id_3",
        model_unique_id="model_id_2",
        detected_at=CURRENT_TIMESTAMP_UTC,
        database_name="test_db",
        schema_name="test_schema",
        table_name="table",
        column_name="column",
        test_type="anomaly_detection",
        test_sub_type="row_count",
        test_results_description="a mock alert",
        owners='["jeff"]',
        tags='["best_test"]',
        test_results_query="select * from table",
        test_rows_sample="[]",
        other=None,
        test_name="test_2",
        test_short_name="shorter",
        test_params="{}",
        severity="ERROR",
        test_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
        model_meta="{}",
        status="fail",
        suppression_status="pending",
        sent_at=None,
        test_created_at="2022-10-09 10:10:10",
        elementary_unique_id="elementary.model_id_1.test_id_3.9cf2f5f6ad.None.generic",
    ),
    # Duplicated alert that should be deduped
    dict(
        id="alert_id_5",
        alert_class_id="test_id_3.column.row_count",
        test_unique_id="test_id_3",
        model_unique_id="model_id_2",
        detected_at=(CURRENT_DATETIME_UTC - timedelta(hours=1)).strftime(
            DATETIME_FORMAT
        ),
        database_name="test_db",
        schema_name="test_schema",
        table_name="table",
        column_name="column",
        test_type="anomaly_detection",
        test_sub_type="row_count",
        test_results_description="a mock alert",
        owners='["jeff"]',
        tags='["best_test"]',
        test_results_query="select * from table",
        test_rows_sample="[]",
        other=None,
        test_name="test_2",
        test_short_name="shorter",
        test_params="{}",
        severity="ERROR",
        test_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
        model_meta="{}",
        status="fail",
        suppression_status="pending",
        sent_at=None,
        test_created_at="2022-10-09 10:10:10",
        elementary_unique_id="elementary.model_id_1.test_id_5.9cf2f5f6ad.None.generic",
    ),
]

PENDDING_MODEL_ALERTS_MOCK_DATA = [
    # Alert within suppression interval
    dict(
        id="alert_id_1",
        alert_class_id="model_id_1",
        model_unique_id="model_id_1",
        alias="model",
        path="",
        original_path="",
        materialization="table",
        detected_at=CURRENT_TIMESTAMP_UTC,
        database_name="test_db",
        schema_name="test_schema",
        full_refresh=False,
        message="",
        owners="[]",
        tags="[]",
        model_meta='{ "alerts_config": {"alert_suppression_interval": 2} }',
        suppression_status="pending",
        sent_at=None,
        status="error",
    ),
    # Alert after suppression interval
    dict(
        id="alert_id_2",
        alert_class_id="model_id_4",
        model_unique_id="model_id_4",
        alias="model",
        path="",
        original_path="",
        materialization="table",
        detected_at=CURRENT_TIMESTAMP_UTC,
        database_name="test_db",
        schema_name="test_schema",
        full_refresh=False,
        message="",
        owners="[]",
        tags="[]",
        model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
        suppression_status="pending",
        sent_at=None,
        status="error",
    ),
    # Alert without suppression interval
    dict(
        id="alert_id_3",
        alert_class_id="model_id_2",
        model_unique_id="model_id_2",
        alias="model",
        path="",
        original_path="",
        materialization="table",
        detected_at=CURRENT_TIMESTAMP_UTC,
        database_name="test_db",
        schema_name="test_schema",
        full_refresh=False,
        message="",
        owners="[]",
        tags="[]",
        model_meta='{ "alerts_config": {"alert_suppression_interval": 0} }',
        suppression_status="pending",
        sent_at=None,
        status="error",
    ),
    # First occurrence alert with suppression interval
    dict(
        id="alert_id_4",
        alert_class_id="model_id_3",
        model_unique_id="model_id_3",
        alias="model",
        path="",
        original_path="",
        materialization="table",
        detected_at=CURRENT_TIMESTAMP_UTC,
        database_name="test_db",
        schema_name="test_schema",
        full_refresh=False,
        message="",
        owners="[]",
        tags="[]",
        model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
        suppression_status="pending",
        sent_at=None,
        status="error",
    ),
    # Duplicated alert that should be deduped
    dict(
        id="alert_id_5",
        alert_class_id="model_id_3",
        model_unique_id="model_id_3",
        alias="model",
        path="",
        original_path="",
        materialization="table",
        detected_at=(CURRENT_DATETIME_UTC - timedelta(hours=1)).strftime(
            DATETIME_FORMAT
        ),
        database_name="test_db",
        schema_name="test_schema",
        full_refresh=False,
        message="",
        owners="[]",
        tags="[]",
        model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
        suppression_status="pending",
        sent_at=None,
        status="error",
    ),
]

PENDDING_SOURCE_FRESHNESS_ALERTS_MOCK_DATA = [
    # Alert within suppression interval
    dict(
        id="alert_id_1",
        source_freshness_execution_id="alert_id_1",
        alert_class_id="source_id_1",
        model_unique_id="source_id_1",
        detected_at=CURRENT_TIMESTAMP_UTC,
        snapshotted_at="2022-10-11 10:00:00",
        max_loaded_at="2022-10-11 10:00:00",
        max_loaded_at_time_ago_in_s=123123,
        database_name="test_db",
        schema_name="test_schema",
        source_name="source_1",
        identifier="identifier",
        error_after="10",
        warn_after="10",
        filter="",
        status="fail",
        original_status="fail",
        owners="[]",
        path="",
        error="",
        tags="[]",
        model_meta='{ "alerts_config": {"alert_suppression_interval": 2} }',
        suppression_status="pending",
        sent_at=None,
    ),
    # Alert after suppression interval
    dict(
        id="alert_id_2",
        source_freshness_execution_id="alert_id_2",
        alert_class_id="source_id_4",
        model_unique_id="source_id_4",
        detected_at=CURRENT_TIMESTAMP_UTC,
        snapshotted_at="2022-10-11 10:00:00",
        max_loaded_at="2022-10-11 10:00:00",
        max_loaded_at_time_ago_in_s=123123,
        database_name="test_db",
        schema_name="test_schema",
        source_name="source_1",
        identifier="identifier",
        error_after="10",
        warn_after="10",
        filter="",
        status="fail",
        original_status="fail",
        owners="[]",
        path="",
        error="",
        tags="[]",
        model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
        suppression_status="pending",
        sent_at=None,
    ),
    # Alert without suppression interval
    dict(
        id="alert_id_3",
        source_freshness_execution_id="alert_id_3",
        alert_class_id="source_id_2",
        model_unique_id="source_id_2",
        detected_at=CURRENT_TIMESTAMP_UTC,
        snapshotted_at="2022-10-11 10:00:00",
        max_loaded_at="2022-10-11 10:00:00",
        max_loaded_at_time_ago_in_s=123123,
        database_name="test_db",
        schema_name="test_schema",
        source_name="source_2",
        identifier="identifier",
        error_after="10",
        warn_after="10",
        filter="",
        status="fail",
        original_status="fail",
        owners="[]",
        path="",
        error="",
        tags="[]",
        model_meta='{ "alerts_config": {"alert_suppression_interval": 0} }',
        suppression_status="pending",
        sent_at=None,
    ),
    # First occurrence alert with suppression interval
    dict(
        id="alert_id_4",
        source_freshness_execution_id="alert_id_4",
        alert_class_id="source_id_3",
        model_unique_id="source_id_3",
        detected_at=CURRENT_TIMESTAMP_UTC,
        snapshotted_at="2022-10-11 10:00:00",
        max_loaded_at="2022-10-11 10:00:00",
        max_loaded_at_time_ago_in_s=123123,
        database_name="test_db",
        schema_name="test_schema",
        source_name="source_3",
        identifier="identifier",
        error_after="10",
        warn_after="10",
        filter="",
        status="fail",
        original_status="fail",
        owners="[]",
        path="",
        error="",
        tags="[]",
        model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
        suppression_status="pending",
        sent_at=None,
    ),
    # Duplicated alert that should be deduped
    dict(
        id="alert_id_5",
        source_freshness_execution_id="alert_id_5",
        alert_class_id="source_id_3",
        model_unique_id="source_id_3",
        detected_at=(CURRENT_DATETIME_UTC - timedelta(hours=1)).strftime(
            DATETIME_FORMAT
        ),
        snapshotted_at="2022-10-11 10:00:00",
        max_loaded_at="2022-10-11 10:00:00",
        max_loaded_at_time_ago_in_s=123123,
        database_name="test_db",
        schema_name="test_schema",
        source_name="source_3",
        identifier="identifier",
        error_after="10",
        warn_after="10",
        filter="",
        status="fail",
        original_status="fail",
        owners="[]",
        path="",
        error="",
        tags="[]",
        model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
        suppression_status="pending",
        sent_at=None,
    ),
]


class MockAlertsFetcher(AlertsFetcher):
    def __init__(self):
        mock_dbt_runner = MockDbtRunner()
        config = Config()
        super().__init__(mock_dbt_runner, config)

    def query_pending_alerts(self, *args, **kwargs) -> List[PendingAlertSchema]:
        pending_test_alerts = [
            PendingAlertSchema(
                id=pending_alert["id"],
                alert_class_id=pending_alert["alert_class_id"],
                type=AlertTypes.TEST,
                detected_at=pending_alert["detected_at"],
                created_at=pending_alert["detected_at"],
                updated_at=pending_alert["detected_at"],
                status="pending",
                data=pending_alert,
            )
            for pending_alert in PENDDING_TEST_ALERTS_MOCK_DATA
        ]

        pending_model_alerts = [
            PendingAlertSchema(
                id=pending_alert["id"],
                alert_class_id=pending_alert["alert_class_id"],
                type=AlertTypes.MODEL,
                detected_at=pending_alert["detected_at"],
                created_at=pending_alert["detected_at"],
                updated_at=pending_alert["detected_at"],
                status="pending",
                data=pending_alert,
            )
            for pending_alert in PENDDING_MODEL_ALERTS_MOCK_DATA
        ]

        pending_source_freshness_alerts = [
            PendingAlertSchema(
                id=pending_alert["id"],
                alert_class_id=pending_alert["alert_class_id"],
                type=AlertTypes.SOURCE_FRESHNESS,
                detected_at=pending_alert["detected_at"],
                created_at=pending_alert["detected_at"],
                updated_at=pending_alert["detected_at"],
                status="pending",
                data=pending_alert,
            )
            for pending_alert in PENDDING_SOURCE_FRESHNESS_ALERTS_MOCK_DATA
        ]
        return [
            *pending_test_alerts,
            *pending_model_alerts,
            *pending_source_freshness_alerts,
        ]

    def query_last_alert_times(self, *args, **kwargs):
        return {
            "test_id_1.column.generic": (
                CURRENT_DATETIME_UTC - timedelta(hours=1.5)
            ).strftime(DATETIME_FORMAT),
            "test_id_2.column.row_count": (
                CURRENT_DATETIME_UTC - timedelta(minutes=1)
            ).strftime(DATETIME_FORMAT),
            "test_id_4.column.generic": (
                CURRENT_DATETIME_UTC - timedelta(hours=1.5)
            ).strftime(DATETIME_FORMAT),
            "model_id_1": (CURRENT_DATETIME_UTC - timedelta(hours=1.5)).strftime(
                DATETIME_FORMAT
            ),
            "model_id_2": (CURRENT_DATETIME_UTC - timedelta(minutes=1)).strftime(
                DATETIME_FORMAT
            ),
            "model_id_4": (CURRENT_DATETIME_UTC - timedelta(hours=1.5)).strftime(
                DATETIME_FORMAT
            ),
            "source_id_1": (CURRENT_DATETIME_UTC - timedelta(hours=1.5)).strftime(
                DATETIME_FORMAT
            ),
            "source_id_2": (CURRENT_DATETIME_UTC - timedelta(minutes=1)).strftime(
                DATETIME_FORMAT
            ),
            "source_id_4": (CURRENT_DATETIME_UTC - timedelta(hours=1.5)).strftime(
                DATETIME_FORMAT
            ),
        }
