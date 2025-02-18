from pathlib import Path

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from tests.unit.alerts.alert_messages.test_alert_utils import (
    build_base_test_alert_model,
    get_alert_message_body,
    get_mock_report_link,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def build_dbt_test_alert_model(*args, **kwargs):
    kwargs.pop("test_type", None)  # Remove test_type if present
    return build_base_test_alert_model(*args, test_type="dbt_test", **kwargs)


def get_expected_adaptive_filename(
    status: str,
    has_link: bool,
    has_description: bool,
    has_tags: bool,
    has_owners: bool,
    has_table: bool,
    has_error: bool,
    has_sample: bool,
    has_env: bool,
) -> str:
    return (
        f"adaptive_card_dbt_test_alert"
        f"_status-{status}"
        f"_link-{has_link}"
        f"_description-{has_description}"
        f"_tags-{has_tags}"
        f"_owners-{has_owners}"
        f"_table-{has_table}"
        f"_error-{has_error}"
        f"_sample-{has_sample}"
        f"_env-{has_env}"
        ".json"
    )


@pytest.mark.parametrize(
    "status,has_link,has_description,has_tags,has_owners,has_table,has_error,has_sample,has_env",
    [
        (None, False, False, False, False, False, False, False, False),
        ("fail", False, False, False, False, False, False, False, False),
        ("fail", True, True, True, True, True, True, True, False),
        ("warn", True, False, True, False, True, False, True, False),
        ("error", False, True, False, True, False, True, False, True),
        ("error", True, True, True, True, True, True, True, True),
        ("warn", True, True, True, True, True, True, True, True),
        (None, True, True, False, False, True, True, False, True),
    ],
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
    has_env: bool,
):
    env = "Test Env" if has_env else None
    test_alert_model = build_dbt_test_alert_model(
        status=status,
        table_name=None if not has_table else "test_table",
        tags=["tag1", "tag2"] if has_tags else None,
        owners=["owner1", "owner2"] if has_owners else None,
        test_description="Test description" if has_description else None,
        error_message="Test error message" if has_error else None,
        test_rows_sample=(
            [{"column1": "value1", "column2": "value2"}] if has_sample else None
        ),
        test_results_query=None,
        test_params=None,
        env=env,
    )

    monkeypatch.setattr(
        test_alert_model, "get_report_link", lambda: get_mock_report_link(has_link)
    )

    message_body = get_alert_message_body(test_alert_model)
    adaptive_card_filename = get_expected_adaptive_filename(
        status=status,
        has_link=has_link,
        has_description=has_description,
        has_tags=has_tags,
        has_owners=has_owners,
        has_table=has_table,
        has_error=has_error,
        has_sample=has_sample,
        has_env=has_env,
    )
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)
