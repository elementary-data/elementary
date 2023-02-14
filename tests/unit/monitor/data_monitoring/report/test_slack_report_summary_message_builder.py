import json
from typing import List

import pytest

from elementary.monitor.data_monitoring.report.slack_report_summary_message_builder import (
    SlackReportSummaryMessageBuilder,
)
from elementary.monitor.fetchers.tests.schema import TestResultSummarySchema


def test_get_test_results_totals(test_results_summary):
    message_builder = SlackReportSummaryMessageBuilder()
    totals = message_builder._get_test_results_totals(test_results_summary)
    assert totals.get("schema_changes") == 2
    assert totals.get("passed") == 4
    assert totals.get("error") == 1
    assert totals.get("failed") == 2
    assert totals.get("warning") == 1


def test_add_details_to_slack_alert_attachments_limit(test_results_summary):
    # Within attachments limitation
    message_builder = SlackReportSummaryMessageBuilder()
    message_builder._add_details_to_slack_alert(test_results_summary)
    attachments_as_string = json.dumps(
        message_builder.slack_message.get("attachments")[0].get("blocks")
    )
    assert ":small_red_triangle: *Failed tests*" in attachments_as_string
    assert ":warning: *Warning*" in attachments_as_string
    assert ":wrench: *Schema changes*" in attachments_as_string
    assert ":exclamation: *Error*" in attachments_as_string

    message_builder = SlackReportSummaryMessageBuilder()
    message_builder._add_details_to_slack_alert((test_results_summary * 5)[0:37])
    attachments_as_string = json.dumps(
        message_builder.slack_message.get("attachments")[0].get("blocks")
    )
    assert ":small_red_triangle: *Failed tests*" in attachments_as_string
    assert ":warning: *Warning*" in attachments_as_string
    assert ":wrench: *Schema changes*" in attachments_as_string
    assert ":exclamation: *Error*" in attachments_as_string

    # Over attachments limitation
    message_builder = SlackReportSummaryMessageBuilder()
    message_builder._add_details_to_slack_alert((test_results_summary * 5)[0:38])
    attachments_as_string = json.dumps(
        message_builder.slack_message.get("attachments")[0].get("blocks")
    )
    assert "The amount of results exceeded Slack" in attachments_as_string


def test_owners_tags_and_subscribers_of_passed_tests_are_filtered_out(
    test_results_summary,
):
    # Within attachments limitation
    message_builder = SlackReportSummaryMessageBuilder()
    message_builder._add_preview_to_slack_alert(test_results_summary)
    attachments_as_string = json.dumps(
        message_builder.slack_message.get("attachments")[0].get("blocks")
    )
    assert "Jeff" in attachments_as_string
    assert "Joe" in attachments_as_string
    assert "Ron" in attachments_as_string
    assert "subscriber1" in attachments_as_string
    assert "subscriber2" in attachments_as_string
    assert "production" in attachments_as_string
    assert "dev" in attachments_as_string

    assert "Jack" not in attachments_as_string
    assert "subscriber22" not in attachments_as_string
    assert "staging" not in attachments_as_string


@pytest.fixture
def test_results_summary() -> List[TestResultSummarySchema]:
    schema_changes_test_results = [
        TestResultSummarySchema(
            test_unique_id="test_id_1",
            elementary_unique_id="elementary_unique_id_1",
            table_name="table_1",
            column_name="column_1",
            test_type="schema_change",
            test_sub_type="column_added",
            owners=["Jeff"],
            tags=["production"],
            subscribers=["subscriber1"],
            description="A test",
            test_name="first_test",
            status="fail",
            results_counter=None,
        ),
        TestResultSummarySchema(
            test_unique_id="test_id_2",
            elementary_unique_id="elementary_unique_id_2",
            table_name="table_2",
            column_name="column_1",
            test_type="schema_change",
            test_sub_type="column_added",
            owners=["Joe"],
            tags=["production"],
            subscribers=[],
            description="Another test",
            test_name="second_test",
            status="warning",
            results_counter=None,
        ),
        TestResultSummarySchema(
            test_unique_id="test_id_3",
            elementary_unique_id="elementary_unique_id_3",
            table_name="table_3",
            test_type="schema_change",
            test_sub_type="generic",
            owners=["Jack"],
            tags=[],
            subscribers=[],
            description="A test",
            test_name="first_test",
            status="pass",
            results_counter=None,
        ),
    ]
    passed_test_results = [
        TestResultSummarySchema(
            test_unique_id="test_id_4",
            elementary_unique_id="elementary_unique_id_4",
            table_name="table_3",
            test_type="dbt_test",
            test_sub_type="generic",
            owners=["Jack"],
            tags=["staging"],
            subscribers=[],
            description="A test",
            test_name="test",
            status="pass",
            results_counter=None,
        ),
        TestResultSummarySchema(
            test_unique_id="test_id_5",
            elementary_unique_id="elementary_unique_id_5",
            table_name="table_3",
            test_type="dbt_test",
            test_sub_type="generic",
            owners=["Jack"],
            tags=["staging"],
            subscribers=["subscriber23"],
            description="A test",
            test_name="test",
            status="pass",
            results_counter=None,
        ),
        TestResultSummarySchema(
            test_unique_id="test_id_6",
            elementary_unique_id="elementary_unique_id_6",
            table_name="table_3",
            test_type="dbt_test",
            test_sub_type="generic",
            owners=["Jack"],
            tags=["staging"],
            subscribers=[],
            description="A test",
            test_name="test",
            status="pass",
            results_counter=None,
        ),
    ]
    error_test_results = [
        TestResultSummarySchema(
            test_unique_id="test_id_7",
            elementary_unique_id="elementary_unique_id_7",
            table_name="table_4",
            test_type="dbt_test",
            test_sub_type="generic",
            owners=["Ron"],
            tags=["dev"],
            subscribers=["subscriber2"],
            description="Oh no!",
            test_name="test with error",
            status="error",
            results_counter=None,
        )
    ]
    warrning_test_results = [
        TestResultSummarySchema(
            test_unique_id="test_id_8",
            elementary_unique_id="elementary_unique_id_8",
            table_name="table_8",
            test_type="dbt_test",
            test_sub_type="generic",
            owners=["Ron"],
            tags=[],
            subscribers=[],
            description="Not importent",
            test_name="warn me",
            status="warning",
            results_counter=7,
        )
    ]
    failed_test_results = [
        TestResultSummarySchema(
            test_unique_id="test_id_9",
            elementary_unique_id="elementary_unique_id_9",
            table_name="table_9",
            test_type="dbt_test",
            test_sub_type="generic",
            owners=["Ron"],
            tags=[],
            subscribers=[],
            description=None,
            test_name="Test me please",
            status="fail",
            results_counter=42,
        ),
        TestResultSummarySchema(
            test_unique_id="test_id_10",
            elementary_unique_id="elementary_unique_id_10",
            table_name="table_10",
            test_type="dbt_test",
            test_sub_type="generic",
            owners=["Ron"],
            tags=["important"],
            subscribers=["subscriber3"],
            description=None,
            test_name="Test me please",
            status="fail",
            results_counter=57,
        ),
    ]
    return [
        *schema_changes_test_results,
        *passed_test_results,
        *warrning_test_results,
        *error_test_results,
        *failed_test_results,
    ]
