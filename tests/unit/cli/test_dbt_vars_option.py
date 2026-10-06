from typing import List
from unittest import mock

import pytest
from click.testing import CliRunner

from elementary.config.config import Config
from elementary.monitor.cli import monitor
from elementary.operations.cli import run_operation

DBT_VARS = "{query_max_size: 1000, nested: {key: value}}"
EXPECTED_DBT_VARS = {"query_max_size": 1000, "nested": {"key": "value"}}


@pytest.fixture(autouse=True)
def isolated_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path))
    monkeypatch.setattr(Config, "DEFAULT_CONFIG_DIR", str(tmp_path))
    monkeypatch.setattr(Config, "DEFAULT_TARGET_PATH", str(tmp_path))
    with mock.patch("elementary.monitor.cli.AnonymousCommandLineTracking"), mock.patch(
        "elementary.operations.cli.AnonymousCommandLineTracking"
    ):
        yield


def _common_args(tmp_path) -> List[str]:
    return ["--config-dir", str(tmp_path), "--target-path", str(tmp_path)]


def _invoke(cli, args: List[str]):
    result = CliRunner().invoke(cli, args, catch_exceptions=False)
    assert result.exit_code == 0, result.output
    return result


@pytest.mark.parametrize(
    "extra_args,expected_dbt_vars",
    [
        ([], {"days_back": 1}),
        (["--dbt-vars", DBT_VARS], {**EXPECTED_DBT_VARS, "days_back": 1}),
        (["--days-back", "3"], {"days_back": 3}),
        (["--dbt-vars", "{days_back: 10}", "--days-back", "3"], {"days_back": 3}),
    ],
)
def test_monitor_dbt_vars(tmp_path, extra_args, expected_dbt_vars):
    with mock.patch(
        "elementary.monitor.cli.DataMonitoringAlerts"
    ) as data_monitoring_alerts:
        _invoke(
            monitor,
            [*_common_args(tmp_path), "--slack-webhook", "mock", *extra_args],
        )
    config = data_monitoring_alerts.call_args.kwargs["config"]
    assert config.dbt_vars == expected_dbt_vars
    data_monitoring_alerts.return_value.run_alerts.assert_called_once_with(
        expected_dbt_vars["days_back"], False
    )


@pytest.mark.parametrize("dbt_vars_args", [[], ["--dbt-vars", DBT_VARS]])
def test_report_dbt_vars(tmp_path, dbt_vars_args):
    with mock.patch("elementary.monitor.cli.SelectorFilter"), mock.patch(
        "elementary.monitor.cli.DataMonitoringReport"
    ) as data_monitoring_report:
        data_monitoring_report.return_value.generate_report.return_value = (
            True,
            None,
        )
        _invoke(monitor, ["report", *_common_args(tmp_path), *dbt_vars_args])
    config = data_monitoring_report.call_args.kwargs["config"]
    assert config.dbt_vars == (EXPECTED_DBT_VARS if dbt_vars_args else None)


@pytest.mark.parametrize("dbt_vars_args", [[], ["--dbt-vars", DBT_VARS]])
def test_send_report_dbt_vars(tmp_path, dbt_vars_args):
    with mock.patch("elementary.monitor.cli.SelectorFilter"), mock.patch(
        "elementary.monitor.cli.DataMonitoringReport"
    ) as data_monitoring_report:
        _invoke(
            monitor,
            [
                "send-report",
                *_common_args(tmp_path),
                "--slack-token",
                "mock",
                "--slack-channel-name",
                "mock",
                *dbt_vars_args,
            ],
        )
    config = data_monitoring_report.call_args.kwargs["config"]
    assert config.dbt_vars == (EXPECTED_DBT_VARS if dbt_vars_args else None)


@pytest.mark.parametrize("dbt_vars_args", [[], ["--dbt-vars", DBT_VARS]])
def test_debug_dbt_vars(dbt_vars_args):
    with mock.patch("elementary.monitor.cli.Debug") as debug:
        _invoke(monitor, ["debug", *dbt_vars_args])
    config = debug.call_args.args[0]
    assert config.dbt_vars == (EXPECTED_DBT_VARS if dbt_vars_args else None)


@pytest.mark.parametrize("dbt_vars_args", [[], ["--dbt-vars", DBT_VARS]])
def test_upload_source_freshness_dbt_vars(tmp_path, dbt_vars_args):
    with mock.patch(
        "elementary.operations.cli.UploadSourceFreshnessOperation"
    ) as operation:
        _invoke(
            run_operation,
            [
                "upload-source-freshness",
                "--target-path",
                str(tmp_path),
                *dbt_vars_args,
            ],
        )
    config = operation.call_args.args[0]
    assert config.dbt_vars == (EXPECTED_DBT_VARS if dbt_vars_args else None)


@pytest.mark.parametrize(
    "args",
    [
        ["report"],
        ["send-report"],
        ["debug"],
    ],
)
@pytest.mark.parametrize("dbt_vars", ["{invalid", "not_a_mapping"])
def test_invalid_dbt_vars(args, dbt_vars):
    result = CliRunner().invoke(monitor, [*args, "--dbt-vars", dbt_vars])
    assert result.exit_code == 2
    assert "--dbt-vars" in result.output


@pytest.mark.parametrize("dbt_vars", ["{invalid", "not_a_mapping"])
def test_invalid_dbt_vars_upload_source_freshness(dbt_vars):
    result = CliRunner().invoke(
        run_operation, ["upload-source-freshness", "--dbt-vars", dbt_vars]
    )
    assert result.exit_code == 2
    assert "--dbt-vars" in result.output
