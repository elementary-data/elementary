from typing import List
from unittest import mock

import pytest
from click.testing import CliRunner

from elementary.config.config import Config
from elementary.monitor import cli as monitor_cli
from elementary.monitor.debug import Debug
from elementary.operations import cli as operations_cli
from elementary.operations.upload_source_freshness import UploadSourceFreshnessOperation

DBT_VARS = "{query_max_size: 1000, nested: {key: value}}"
EXPECTED_DBT_VARS = {"query_max_size": 1000, "nested": {"key": "value"}}


@pytest.fixture
def base_args(tmp_path) -> List[str]:
    return ["--config-dir", str(tmp_path), "--target-path", str(tmp_path)]


@pytest.fixture(autouse=True)
def mock_tracking():
    with mock.patch.object(
        monitor_cli, "AnonymousCommandLineTracking"
    ), mock.patch.object(operations_cli, "AnonymousCommandLineTracking"):
        yield


def _invoked_config(mock_cls: mock.MagicMock) -> Config:
    mock_cls.assert_called_once()
    return mock_cls.call_args.kwargs["config"]


@pytest.mark.parametrize("dbt_vars_args", [[], ["--dbt-vars", DBT_VARS]])
@mock.patch.object(monitor_cli, "DataMonitoringAlerts")
def test_monitor_dbt_vars(mock_alerts, base_args, dbt_vars_args):
    mock_alerts.return_value.run_alerts.return_value = True
    result = CliRunner().invoke(
        monitor_cli.monitor,
        [*base_args, "--slack-webhook", "https://hook", "-d", "3", *dbt_vars_args],
    )
    assert result.exit_code == 0, result.output
    config = _invoked_config(mock_alerts)
    assert config.dbt_vars == (EXPECTED_DBT_VARS if dbt_vars_args else {})
    mock_alerts.return_value.run_alerts.assert_called_once_with(3, False)


@mock.patch.object(monitor_cli, "SelectorFilter")
@mock.patch.object(monitor_cli, "DataMonitoringReport")
def test_report_dbt_vars(mock_report, _mock_selector_filter, base_args):
    mock_report.return_value.generate_report.return_value = (True, None)
    result = CliRunner().invoke(
        monitor_cli.report,
        [*base_args, "--open-browser", "false", "--dbt-vars", DBT_VARS],
    )
    assert result.exit_code == 0, result.output
    assert _invoked_config(mock_report).dbt_vars == EXPECTED_DBT_VARS


@mock.patch.object(monitor_cli, "SelectorFilter")
@mock.patch.object(monitor_cli, "DataMonitoringReport")
def test_send_report_dbt_vars(mock_report, _mock_selector_filter, base_args):
    mock_report.return_value.send_report.return_value = True
    result = CliRunner().invoke(
        monitor_cli.send_report,
        [
            *base_args,
            "--slack-token",
            "token",
            "--slack-channel-name",
            "channel",
            "--dbt-vars",
            DBT_VARS,
        ],
    )
    assert result.exit_code == 0, result.output
    assert _invoked_config(mock_report).dbt_vars == EXPECTED_DBT_VARS


@mock.patch.object(monitor_cli, "Debug")
def test_debug_dbt_vars(mock_debug):
    mock_debug.return_value.run.return_value = True
    result = CliRunner().invoke(monitor_cli.debug, ["--dbt-vars", DBT_VARS])
    assert result.exit_code == 0, result.output
    mock_debug.assert_called_once()
    assert mock_debug.call_args.args[0].dbt_vars == EXPECTED_DBT_VARS


@mock.patch.object(operations_cli, "UploadSourceFreshnessOperation")
def test_upload_source_freshness_dbt_vars(mock_operation, tmp_path):
    result = CliRunner().invoke(
        operations_cli.upload_source_freshness,
        ["--target-path", str(tmp_path), "--dbt-vars", DBT_VARS],
    )
    assert result.exit_code == 0, result.output
    mock_operation.assert_called_once()
    assert mock_operation.call_args.args[0].dbt_vars == EXPECTED_DBT_VARS


@pytest.mark.parametrize(
    "invalid_dbt_vars", ["[1, 2]", "a: [b", "just-a-string", "{2026-10-01: value}"]
)
@mock.patch.object(monitor_cli, "Debug")
def test_invalid_dbt_vars(mock_debug, invalid_dbt_vars):
    result = CliRunner().invoke(monitor_cli.debug, ["--dbt-vars", invalid_dbt_vars])
    assert result.exit_code == 2
    assert "--dbt-vars" in result.output
    mock_debug.assert_not_called()


@mock.patch.object(monitor_cli, "Debug")
def test_dbt_vars_dates_become_strings(mock_debug):
    mock_debug.return_value.run.return_value = True
    result = CliRunner().invoke(
        monitor_cli.debug,
        ["--dbt-vars", "{start: 2026-01-01, nested: {at: 2026-01-02 10:00:00}}"],
    )
    assert result.exit_code == 0, result.output
    assert mock_debug.call_args.args[0].dbt_vars == {
        "start": "2026-01-01",
        "nested": {"at": "2026-01-02 10:00:00"},
    }


@mock.patch("elementary.monitor.debug.create_internal_dbt_runner")
def test_debug_uses_internal_dbt_runner(mock_create_runner, tmp_path):
    config = Config(config_dir=str(tmp_path), target_path=str(tmp_path))
    assert Debug(config).run()
    mock_create_runner.assert_called_once_with(config)


@mock.patch("elementary.operations.upload_source_freshness.create_internal_dbt_runner")
def test_upload_source_freshness_uses_internal_dbt_runner(mock_create_runner, tmp_path):
    config = Config(config_dir=str(tmp_path), target_path=str(tmp_path))
    UploadSourceFreshnessOperation(config).upload_results(
        results={}, metadata={"invocation_id": "inv"}, rows_per_insert=10
    )
    mock_create_runner.assert_called_once_with(config)
