from datetime import datetime
from typing import List, Optional

from elementary.monitor.alerts.model_alert import ModelAlertModel
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
    data = {
        "id": "test_id",
        "test_unique_id": "test_unique_id",
        "elementary_unique_id": "elementary_unique_id",
        "test_name": "test_name",
        "severity": "error",
        "test_type": test_type,
        "test_sub_type": test_sub_type,
        "test_short_name": "test_short_name",
        "alert_class_id": "test_alert_class_id",
        "test_results_description": error_message,
        "test_results_query": test_results_query,
        "table_name": table_name,
        "model_unique_id": None,
        "test_description": test_description,
        "other": other,
        "test_params": test_params,
        "test_meta": None,
        "test_rows_sample": test_rows_sample,
        "column_name": None,
        "detected_at": None,
        "database_name": None,
        "schema_name": None,
        "owners": owners,
        "tags": tags,
        "subscribers": None,
        "status": status,
        "model_meta": {},
        "timezone": None,
        "report_url": None,
        "alert_fields": None,
        "elementary_database_and_schema": "test_db.test_schema",
    }
    return TestAlertModel(**data)


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
