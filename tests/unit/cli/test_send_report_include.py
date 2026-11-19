from typing import Optional
from unittest import mock

import pytest
from click.testing import CliRunner

from elementary.config.config import Config
from elementary.monitor.cli import monitor


@pytest.fixture(autouse=True)
def isolated_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path))
    monkeypatch.setattr(Config, "DEFAULT_CONFIG_DIR", str(tmp_path))
    monkeypatch.setattr(Config, "DEFAULT_TARGET_PATH", str(tmp_path))
    with mock.patch("elementary.monitor.cli.AnonymousCommandLineTracking"):
        yield


@pytest.mark.parametrize(
    "include,expected_include_description",
    [
        (None, False),
        ("descriptions", True),
        ("description", True),
        ("other", False),
    ],
)
def test_send_report_include_description(
    tmp_path, include: Optional[str], expected_include_description: bool
):
    include_args = ["--include", include] if include else []
    with mock.patch("elementary.monitor.cli.SelectorFilter"), mock.patch(
        "elementary.monitor.cli.DataMonitoringReport"
    ) as data_monitoring_report:
        result = CliRunner().invoke(
            monitor,
            [
                "send-report",
                "--config-dir",
                str(tmp_path),
                "--target-path",
                str(tmp_path),
                "--slack-token",
                "mock",
                "--slack-channel-name",
                "mock",
                *include_args,
            ],
            catch_exceptions=False,
        )
    assert result.exit_code == 0, result.output
    send_report = data_monitoring_report.return_value.send_report
    assert (
        send_report.call_args.kwargs["include_description"]
        == expected_include_description
    )
