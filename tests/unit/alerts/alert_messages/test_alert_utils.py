import itertools
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from elementary.messages.message_body import MessageBody
from elementary.monitor.alerts.alert_messages.builder import AlertMessageBuilder
from elementary.monitor.alerts.alerts_groups.alerts_group import AlertsGroup
from elementary.monitor.alerts.alerts_groups.grouped_by_table import (
    GroupedByTableAlerts,
)
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
    test_rows_sample: Optional[List[Dict[str, Any]]],
    test_results_query: Optional[str],
    test_params: Optional[dict],
    test_type: str = "dbt_test",
    test_sub_type: str = "generic",
    other: Optional[dict] = None,
    model_unique_id: Optional[str] = "test_model_unique_id",
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
        model_unique_id=model_unique_id,
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
    detected_at: datetime,
    alias: str,
    message: str,
    suppression_interval: Optional[int] = None,
) -> ModelAlertModel:
    return ModelAlertModel(
        id="test_id",
        model_unique_id="test_model_unique_id",
        elementary_unique_id="elementary_unique_id",
        status=status,
        severity="error",
        alert_class_id="test_alert_class_id",
        model_name="test_model",
        alias=alias,
        message=message,
        path=path,
        original_path=path,
        materialization=materialization,
        full_refresh=full_refresh,
        detected_at=detected_at,
        database_name=None,
        schema_name=None,
        owners=owners,
        tags=tags,
        test_meta=None,
        test_results_query=None,
        test_rows_sample=None,
        test_params=None,
        test_name=None,
        test_type=None,
        test_sub_type=None,
        test_results_description=None,
        test_description=None,
        column_name=None,
        table_name=None,
        other=None,
        timezone="UTC",
        report_url=None,
        alert_fields=None,
        elementary_database_and_schema="test_db.test_schema",
        suppression_interval=suppression_interval,
    )


def get_mock_report_link(has_link: bool) -> Optional[ReportLink]:
    if has_link:
        return ReportLink(url="http://test.com", text="View Report")
    return None


def get_alerts_group_test_params() -> List[Tuple[bool, bool, bool, bool, bool]]:
    return [
        (
            has_model_errors,
            has_test_failures,
            has_test_warnings,
            has_test_errors,
            has_link,
        )
        for has_model_errors, has_test_failures, has_test_warnings, has_test_errors, has_link in itertools.product(
            [True, False], repeat=5
        )
        if any(
            [has_model_errors, has_test_failures, has_test_warnings, has_test_errors]
        )
    ]


def _get_owners_by_mod(i: int) -> List[str]:
    mod_value = i % 3
    if mod_value == 0:
        return []
    elif mod_value == 1:
        return ["owner1"]
    else:  # mod_value == 2
        return ["owner1", "owner2"]


def create_test_alerts(
    has_model_errors: bool,
    has_test_failures: bool,
    has_test_warnings: bool,
    has_test_errors: bool,
    detected_at: datetime,
    count: int,
) -> List[Union[TestAlertModel, ModelAlertModel, SourceFreshnessAlertModel]]:
    alerts: List[Union[TestAlertModel, ModelAlertModel, SourceFreshnessAlertModel]] = []

    if has_model_errors:
        for i in range(count):
            owners = _get_owners_by_mod(i)
            model_alert = build_base_model_alert_model(
                status="error",
                tags=["tag1"],
                owners=owners,
                path="models/test_model.sql",
                materialization="table",
                full_refresh=True,
                detected_at=detected_at,
                alias="test_model",
                message="Test model error",
                suppression_interval=None,
            )
            alerts.append(model_alert)

    if has_test_failures:
        for i in range(count):
            owners = _get_owners_by_mod(i)
            test_alert = build_base_test_alert_model(
                status="fail",
                table_name=f"test_table_{i + 1}",
                tags=["tag1"],
                owners=owners,
                test_description="Test failure description",
                error_message="Test failure message",
                test_rows_sample=None,
                test_results_query=None,
                test_params=None,
            )
            alerts.append(test_alert)

    if has_test_warnings:
        for i in range(count):
            owners = _get_owners_by_mod(i)
            test_alert = build_base_test_alert_model(
                status="warn",
                table_name=f"test_table_{i + 1}",
                tags=["tag1"],
                owners=owners,
                test_description=f"Test warning description {i + 1}",
                error_message=f"Test warning message {i + 1}",
                test_rows_sample=None,
                test_results_query=None,
                test_params=None,
            )
            alerts.append(test_alert)

    if has_test_errors:
        for i in range(count):
            owners = _get_owners_by_mod(i)
            test_alert = build_base_test_alert_model(
                status="error",
                table_name=f"test_table_{i + 1}",
                tags=["tag1"],
                owners=owners,
                test_description=f"Test error description {i + 1}",
                error_message="Test error message",
                test_rows_sample=None,
                test_results_query=None,
                test_params=None,
            )
            alerts.append(test_alert)

    return alerts


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


def get_alert_message_body(
    alert: Union[
        TestAlertModel,
        ModelAlertModel,
        SourceFreshnessAlertModel,
        GroupedByTableAlerts,
        AlertsGroup,
    ],
) -> MessageBody:
    alert_message_builder = AlertMessageBuilder()
    return alert_message_builder.build(alert)
