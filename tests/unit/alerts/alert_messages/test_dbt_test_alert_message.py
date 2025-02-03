import itertools
import uuid
from pathlib import Path
from typing import List, Optional

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from elementary.monitor.alerts.alert_messages.test_alert_message import (
    get_dbt_test_alert_message_body,
)
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData as ReportLink,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(autouse=True)
def mock_uuid(monkeypatch):
    class MockUUID:
        def __init__(self):
            self.counter = 0

        def __call__(self):
            self.counter += 1
            return uuid.UUID(
                f"00000000-0000-0000-0000-{self.counter:012d}"  # noqa: E231
            )

    mock = MockUUID()
    monkeypatch.setattr(uuid, "uuid4", mock)
    return mock


def build_dbt_test_alert_model(
    status: str,
    table_name: Optional[str],
    tags: Optional[List[str]],
    owners: Optional[List[str]],
    test_description: Optional[str],
    error_message: Optional[str],
    test_rows_sample: Optional[dict],
    test_results_query: Optional[str],
    test_params: Optional[dict],
) -> TestAlertModel:
    data = {
        "id": "test_id",
        "test_unique_id": "test_unique_id",
        "elementary_unique_id": "elementary_unique_id",
        "test_name": "test_name",
        "severity": "error",
        "test_type": "dbt_test",
        "test_sub_type": "generic",
        "test_short_name": "test_short_name",
        "alert_class_id": "test_alert_class_id",
        "test_results_description": error_message,
        "test_results_query": test_results_query,
        "table_name": table_name,
        "model_unique_id": None,
        "test_description": test_description,
        "other": None,
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


def get_expected_adaptive_filename(
    status: str,
    has_link: bool,
    has_description: bool,
    has_tags: bool,
    has_owners: bool,
    has_table: bool,
    has_error: bool,
    has_sample: bool,
) -> str:
    return f"adaptive_card_dbt_test_alert_status-{status}_link-{has_link}_description-{has_description}_tags-{has_tags}_owners-{has_owners}_table-{has_table}_error-{has_error}_sample-{has_sample}.json"


STATUS_VALUES = ["fail", "warn", "error", None]
BOOLEAN_VALUES = [True, False]

combinations = itertools.product(
    STATUS_VALUES,
    BOOLEAN_VALUES,
    BOOLEAN_VALUES,
    BOOLEAN_VALUES,
    BOOLEAN_VALUES,
    BOOLEAN_VALUES,
    BOOLEAN_VALUES,
    BOOLEAN_VALUES,
)


@pytest.mark.parametrize(
    "status,has_link,has_description,has_tags,has_owners,has_table,has_error,has_sample",
    combinations,
)
def test_get_dbt_test_alert_message_body(
    monkeypatch,
    status: str,
    has_link: bool,
    has_description: bool,
    has_tags: bool,
    has_owners: bool,
    has_table: bool,
    has_error: bool,
    has_sample: bool,
):
    test_alert_model = build_dbt_test_alert_model(
        status=status,
        table_name=None if not has_table else "test_table",
        tags=["tag1", "tag2"] if has_tags else None,
        owners=["owner1", "owner2"] if has_owners else None,
        test_description="Test description" if has_description else None,
        error_message="Test error message" if has_error else None,
        test_rows_sample=(
            {"column1": "value1", "column2": "value2"} if has_sample else None
        ),
        test_results_query=None,
        test_params=None,
    )

    def mock_report_link() -> Optional[ReportLink]:
        if has_link:
            return ReportLink(url="http://test.com", text="View Report")
        return None

    monkeypatch.setattr(test_alert_model, "get_report_link", mock_report_link)

    message_body = get_dbt_test_alert_message_body(test_alert_model)
    adaptive_card_filename = get_expected_adaptive_filename(
        status=status,
        has_link=has_link,
        has_description=has_description,
        has_tags=has_tags,
        has_owners=has_owners,
        has_table=has_table,
        has_error=has_error,
        has_sample=has_sample,
    )
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)
