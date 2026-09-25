from typing import Optional
from unittest.mock import MagicMock

import pytest

from elementary.clients.slack.client import SlackWebClient, SlackWebhookClient
from elementary.config.config import Config
from elementary.monitor.data_monitoring.report.data_monitoring_report import (
    DataMonitoringReport,
)
from tests.mocks.dbt_runner_mock import MockDbtRunner

LOCAL_REPORT_PATH = "/tmp/elementary_report.html"


class DataMonitoringReportMock(DataMonitoringReport):
    def __init__(self, config: Config, bucket_website_url: Optional[str] = None):
        super().__init__(config=config, tracking=None)
        # Stand-ins for the parts of send_report that need a warehouse or a bucket.
        self.generate_report = MagicMock(return_value=(True, LOCAL_REPORT_PATH))
        self.send_test_results_summary = MagicMock(return_value=True)
        self.s3_client = MagicMock()
        self.s3_client.send_report.return_value = (True, bucket_website_url)

    def _init_internal_dbt_runner(self):
        return MockDbtRunner()

    def get_latest_invocation(self):
        return dict()

    def _get_warehouse_info(self, *args, **kwargs):
        return None

    def get_elementary_database_and_schema(self):
        return "<elementary_database>.<elementary_schema>"


@pytest.fixture
def webhook_and_s3_config(tmp_path) -> Config:
    return Config(
        config_dir=str(tmp_path / "config"),
        target_path=str(tmp_path / "target"),
        slack_webhook="https://hooks.slack.com/services/T000/B000/XXXX",
        s3_bucket_name="elementary-reports",
    )


def test_send_report_with_slack_webhook_skips_html_attachment(
    webhook_and_s3_config: Config,
):
    report = DataMonitoringReportMock(webhook_and_s3_config)
    assert isinstance(report.slack_client, SlackWebhookClient)

    assert report.send_report() is True

    report.s3_client.send_report.assert_called_once()
    report.send_test_results_summary.assert_called_once()
    assert report.success is True


def test_send_report_with_slack_token_still_sends_html_attachment(tmp_path):
    config = Config(
        config_dir=str(tmp_path / "config"),
        target_path=str(tmp_path / "target"),
        slack_token="xoxb-mock",
        slack_channel_name="data-reports",
        s3_bucket_name="elementary-reports",
    )
    report = DataMonitoringReportMock(config)
    assert isinstance(report.slack_client, SlackWebClient)
    report.slack_client.send_report = MagicMock(return_value=True)

    assert report.send_report() is True

    report.slack_client.send_report.assert_called_once_with(
        "data-reports", LOCAL_REPORT_PATH
    )
