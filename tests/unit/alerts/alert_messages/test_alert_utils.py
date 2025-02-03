from datetime import datetime
from typing import List, Optional

from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData as ReportLink,
)


def build_base_test_alert_model(
    status: str,
    table_name: Optional[str],
    tags: Optional[List[str]],
    owners: Optional[List[str]],
    test_description: Optional[str],
    error_message: Optional[str],
    test_rows_sample: Optional[dict],
    test_results_query: Optional[str],
    test_params: Optional[dict],
    test_type: str = "dbt_test",
    test_sub_type: str = "generic",
    other: Optional[dict] = None,
) -> TestAlertModel:
    return TestAlertModel(
        id="test_id",
        test_unique_id="test_unique_id",
        elementary_unique_id="elementary_unique_id",
        test_name="test_name",
        severity="error",
        test_type=test_type,
        test_sub_type=test_sub_type,
        test_short_name="test_short_name",
        alert_class_id="test_alert_class_id",
        test_results_description=error_message,
        test_results_query=test_results_query,
        table_name=table_name,
        model_unique_id=None,
        test_description=test_description,
        other=other,
        test_params=test_params,
        test_meta=None,
        test_rows_sample=test_rows_sample,
        column_name=None,
        detected_at=None,
        database_name=None,
        schema_name=None,
        owners=owners,
        tags=tags,
        subscribers=None,
        status=status,
        model_meta={},
        timezone="UTC",
        report_url=None,
        alert_fields=None,
        elementary_database_and_schema="test_db.test_schema",
    )


def build_base_model_alert_model(
    status: str,
    tags: Optional[List[str]],
    owners: Optional[List[str]],
    path: str,
    materialization: str,
    full_refresh: bool,
    detected_at: Optional[datetime],
    alias: str,
    message: Optional[str] = None,
    suppression_interval: Optional[int] = None,
) -> ModelAlertModel:
    return ModelAlertModel(
        id="test_id",
        alias=alias,
        path=path,
        original_path=path,
        materialization=materialization,
        full_refresh=full_refresh,
        alert_class_id="test_alert_class_id",
        message=message,
        model_unique_id="test_model_unique_id",
        detected_at=detected_at,
        owners=owners,
        tags=tags,
        subscribers=None,
        status=status,
        model_meta={},
        suppression_interval=suppression_interval,
        timezone="UTC",
        report_url=None,
        alert_fields=None,
    )


def get_mock_report_link(has_link: bool) -> Optional[ReportLink]:
    if has_link:
        return ReportLink(url="http://test.com", text="View Report")
    return None


# Common test parameters
STATUS_VALUES = ["fail", "warn", "error", None]
BOOLEAN_VALUES = [True, False]
MATERIALIZATION_VALUES = ["table", "view", "incremental"]


def build_base_source_freshness_alert_model(
    status: str,
    tags: Optional[List[str]],
    owners: Optional[List[str]],
    path: str,
    has_error: bool,
    has_message: bool,
    detected_at: Optional[datetime],
    snapshotted_at: Optional[datetime] = None,
    max_loaded_at: Optional[datetime] = None,
    max_loaded_at_time_ago_in_s: Optional[float] = None,
    suppression_interval: Optional[int] = None,
) -> SourceFreshnessAlertModel:
    return SourceFreshnessAlertModel(
        id="test_id",
        source_name="test_source",
        identifier="test_identifier",
        original_status=status,
        path=path,
        error="Test error message" if has_error else None,
        alert_class_id="test_alert_class_id",
        source_freshness_execution_id="test_execution_id",
        error_after="2 hours" if has_message else None,
        warn_after="1 hour" if has_message else None,
        snapshotted_at=snapshotted_at,
        max_loaded_at=max_loaded_at,
        max_loaded_at_time_ago_in_s=max_loaded_at_time_ago_in_s,
        filter="test_filter" if has_message else None,
        freshness_description="Test freshness description" if has_message else None,
        detected_at=detected_at,
        owners=owners,
        tags=tags,
        subscribers=None,
        status=status,
        model_meta={},
        suppression_interval=suppression_interval,
        timezone="UTC",
        report_url=None,
        alert_fields=None,
        elementary_database_and_schema="test_db.test_schema",
    )
