import json
from unittest import mock

import pytest

from tests.mocks.fetchers.alerts_fetcher_mock import MockAlertsFetcher


def test_split_list_to_chunks(alerts_fetcher_mock: MockAlertsFetcher):
    mock_list = [None] * 150

    split_list = alerts_fetcher_mock._split_list_to_chunks(mock_list, chunk_size=10)
    assert len(split_list) == 15
    for chunk in split_list:
        assert len(chunk) == 10

    split_list = alerts_fetcher_mock._split_list_to_chunks(mock_list)
    assert len(split_list) == 3
    for chunk in split_list:
        assert len(chunk) == 50


@mock.patch("subprocess.run")
def test_update_sent_alerts(
    mock_subprocess_run, alerts_fetcher_mock: MockAlertsFetcher
):
    mock_alerts_ids_to_update = ["mock_alert_id"] * 60
    alerts_fetcher_mock.update_sent_alerts(
        alert_ids=mock_alerts_ids_to_update, table_name="mock_table"
    )

    # Test that alert ids were split into chunks
    assert mock_subprocess_run.call_count == 2

    calls_args = mock_subprocess_run.call_args_list
    for call_args in calls_args:
        # Test that update_sent_alerts has been called with alert_ids as arguments.
        assert call_args[0][0][1] == "run"
        assert call_args[0][0][2] == "-s"
        assert call_args[0][0][3] == "update_alerts.update_sent_alerts"
        dbt_run_params = json.loads(call_args[0][0][9])
        assert "alert_ids" in dbt_run_params
        assert "table_name" in dbt_run_params
        assert "sent_at" in dbt_run_params


@mock.patch("subprocess.run")
def test_skip_alerts(mock_subprocess_run, alerts_fetcher_mock: MockAlertsFetcher):
    # Create 100 alerts
    test_alerts = alerts_fetcher_mock.query_pending_test_alerts()
    mock_alerts_ids_to_skip = test_alerts.alerts * 20

    alerts_fetcher_mock.skip_alerts(
        alerts_to_skip=mock_alerts_ids_to_skip, table_name="mock_table"
    )

    # Test that alert ids were split into chunks
    assert mock_subprocess_run.call_count == 2

    calls_args = mock_subprocess_run.call_args_list
    for call_args in calls_args:
        # Test that update_skipped_alerts has been called with alert_ids as arguments.
        assert call_args[0][0][1] == "run"
        assert call_args[0][0][2] == "-s"
        assert call_args[0][0][3] == "update_alerts.update_skipped_alerts"
        dbt_run_params = json.loads(call_args[0][0][9])
        assert "alert_ids" in dbt_run_params
        assert "table_name" in dbt_run_params


@pytest.fixture
def alerts_fetcher_mock() -> MockAlertsFetcher:
    return MockAlertsFetcher()
