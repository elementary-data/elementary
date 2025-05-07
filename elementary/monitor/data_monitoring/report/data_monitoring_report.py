import base64
import json
import os
import os.path
import webbrowser
from typing import Optional, Tuple

from elementary.clients.azure.client import AzureClient
from elementary.clients.gcs.client import GCSClient
from elementary.clients.s3.client import S3Client
from elementary.clients.slack.client import SlackClient
from elementary.config.config import Config
from elementary.monitor.api.invocations.invocations import InvocationsAPI
from elementary.monitor.api.report.report import ReportAPI
from elementary.monitor.api.report.schema import ReportDataSchema
from elementary.monitor.api.tests.tests import TestsAPI
from elementary.monitor.data_monitoring.data_monitoring import DataMonitoring
from elementary.monitor.data_monitoring.report.slack_report_summary_message_builder import (
    SlackReportSummaryMessageBuilder,
)
from elementary.monitor.data_monitoring.schema import FiltersSchema
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class DataMonitoringReport(DataMonitoring):
    def __init__(
        self,
        config: Config,
        tracking: Tracking,
        selector_filter: FiltersSchema = FiltersSchema(),
        force_update_dbt_package: bool = False,
        disable_samples: bool = False,
    ):
        super().__init__(
            config, tracking, force_update_dbt_package, disable_samples, selector_filter
        )
        self.report_api = ReportAPI(self.internal_dbt_runner)
        self.s3_client = S3Client.create_client(self.config, tracking=self.tracking)
        self.gcs_client = GCSClient.create_client(self.config, tracking=self.tracking)
        self.azure_client = AzureClient.create_client(
            self.config, tracking=self.tracking
        )
        self.slack_client = SlackClient.create_client(
            self.config, tracking=self.tracking
        )

    def generate_report(
        self,
        days_back: int = 7,
        test_runs_amount: int = 720,
        file_path: Optional[str] = None,
        disable_passed_test_metrics: bool = False,
        should_open_browser: bool = True,
        exclude_elementary_models: bool = False,
        project_name: Optional[str] = None,
    ) -> Tuple[bool, str]:
        html_path = self._get_report_file_path(file_path)
        output_data = self.get_report_data(
            days_back=days_back,
            test_runs_amount=test_runs_amount,
            disable_passed_test_metrics=disable_passed_test_metrics,
            exclude_elementary_models=exclude_elementary_models,
            project_name=project_name,
        )
        template_html_path = os.path.join(os.path.dirname(__file__), "index.html")

        with open(template_html_path, "r", encoding="utf-8") as template_html_file:
            template_html_code = template_html_file.read()

        dumped_output_data = json.dumps(output_data)
        encoded_output_data = base64.b64encode(dumped_output_data.encode("utf-8"))
        compiled_output_html = (
            f"<script>"
            f"window.elementaryData = JSON.parse(atob('{encoded_output_data.decode('utf-8')}'));"
            f"</script>"
            f"{template_html_code}"
        )
        with open(html_path, "w", encoding="utf-8") as html_file:
            html_file.write(compiled_output_html)

        with open(
            os.path.join(self.config.target_dir, "elementary_output.json"),
            "w",
            encoding="utf-8",
        ) as elementary_output_json_file:
            elementary_output_json_file.write(dumped_output_data)

        if should_open_browser:
            try:
                webbrowser.open_new_tab("file://" + html_path)
            except webbrowser.Error:
                logger.error("Unable to open the web browser.")

        self.execution_properties["report_end"] = True
        self.execution_properties["success"] = self.success
        return self.success, html_path

    def get_report_data(
        self,
        days_back: int = 7,
        test_runs_amount: int = 720,
        disable_passed_test_metrics: bool = False,
        exclude_elementary_models: bool = False,
        project_name: Optional[str] = None,
    ):
        report_api = ReportAPI(self.internal_dbt_runner)
        report_data, error = report_api.get_report_data(
            days_back=days_back,
            test_runs_amount=test_runs_amount,
            disable_passed_test_metrics=disable_passed_test_metrics,
            exclude_elementary_models=exclude_elementary_models,
            disable_samples=self.disable_samples,
            project_name=project_name or self.project_name,
            filter=self.selector_filter.to_selector_filter_schema(),
            env=self.config.env,
            warehouse_type=self.warehouse_info.type if self.warehouse_info else None,
        )
        self._add_report_tracking(report_data, error)
        if error:
            logger.exception(
                f"Could not generate the report - Error: {error}\nPlease reach out to our community for help with this issue.",
                exc_info=error,
            )
            self.success = False

        report_data_dict = report_data.dict()
        return report_data_dict

    def validate_report_selector(self):
        self.selector_filter.validate_report_selector()

    def _add_report_tracking(
        self, report_data: ReportDataSchema, error: Optional[Exception] = None
    ):
        if error:
            if self.tracking:
                self.tracking.record_internal_exception(error)
            return

        test_metadatas = []
        for tests in report_data.test_results.values():
            for test in tests:
                test_metadatas.append(test.get("metadata"))

        self.execution_properties["elementary_test_count"] = len(
            [
                test_metadata
                for test_metadata in test_metadatas
                if test_metadata.get("test_type") != "dbt_test"
            ]
        )
        self.execution_properties["test_result_count"] = len(test_metadatas)

        if self.config.anonymous_tracking_enabled and isinstance(
            self.tracking, AnonymousTracking
        ):
            report_data.tracking = dict(
                posthog_api_key=self.tracking.POSTHOG_PROJECT_API_KEY,
                report_generator_anonymous_user_id=self.tracking.anonymous_user_id,
                anonymous_warehouse_id=self.warehouse_info.id
                if self.warehouse_info
                else None,
            )

    def send_report(
        self,
        days_back: int = 7,
        test_runs_amount: int = 720,
        file_path: Optional[str] = None,
        disable_passed_test_metrics: bool = False,
        should_open_browser: bool = False,
        exclude_elementary_models: bool = False,
        project_name: Optional[str] = None,
        remote_file_path: Optional[str] = None,
        disable_html_attachment: bool = False,
        include_description: bool = False,
    ):
        # Generate the report
        generated_report_successfully, local_html_path = self.generate_report(
            days_back=days_back,
            test_runs_amount=test_runs_amount,
            disable_passed_test_metrics=disable_passed_test_metrics,
            file_path=file_path,
            should_open_browser=should_open_browser,
            exclude_elementary_models=exclude_elementary_models,
            project_name=project_name,
        )

        if not generated_report_successfully:
            self.success = False
            self.execution_properties["success"] = self.success
            return self.success

        bucket_website_url = None
        upload_succeeded = False
        # If a s3 client or a gcs client is provided, we want to upload the report to the bucket.
        if self.s3_client or self.gcs_client or self.azure_client:
            self.validate_report_selector()
            upload_succeeded, bucket_website_url = self.upload_report(
                local_html_path=local_html_path, remote_file_path=remote_file_path
            )

        should_send_report_over_slack = True
        # If we upload the report to a bucket, we don't want to share it via Slack.
        if (
            upload_succeeded and bucket_website_url is not None
        ) or disable_html_attachment:
            should_send_report_over_slack = False

        # If a Slack client is provided, we want to send a results summary and attachment of the report if needed.
        if self.slack_client:
            # Send test results summary
            self.send_test_results_summary(
                days_back=days_back,
                test_runs_amount=test_runs_amount,
                disable_passed_test_metrics=disable_passed_test_metrics,
                bucket_website_url=bucket_website_url,
                include_description=include_description,
            )
            if should_send_report_over_slack:
                self.validate_report_selector()
                self.send_report_attachment(local_html_path=local_html_path)

        return self.success

    def send_report_attachment(self, local_html_path: str) -> bool:
        if self.slack_client:
            send_succeeded = self.slack_client.send_report(
                self.config.slack_channel_name, local_html_path
            )
            self.execution_properties["sent_to_slack_successfully"] = send_succeeded
            if not send_succeeded:
                self.success = False

        self.execution_properties["success"] = self.success
        return self.success

    def upload_report(
        self, local_html_path: str, remote_file_path: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        if self.gcs_client:
            send_succeeded, bucket_website_url = self.gcs_client.send_report(
                local_html_path, remote_bucket_file_path=remote_file_path
            )
            self.execution_properties["sent_to_gcs_successfully"] = send_succeeded
            if not send_succeeded:
                self.success = False

        if self.s3_client:
            send_succeeded, bucket_website_url = self.s3_client.send_report(
                local_html_path, remote_bucket_file_path=remote_file_path
            )
            self.execution_properties["sent_to_s3_successfully"] = send_succeeded
            if not send_succeeded:
                self.success = False

        if self.azure_client:
            send_succeeded, bucket_website_url = self.azure_client.send_report(
                local_html_path, remote_bucket_file_path=remote_file_path
            )
            self.execution_properties["sent_to_azure_successfully"] = send_succeeded
            if not send_succeeded:
                self.success = False

        self.execution_properties["success"] = self.success
        return self.success, bucket_website_url

    def send_test_results_summary(
        self,
        days_back: int,
        test_runs_amount: int,
        disable_passed_test_metrics: bool = False,
        bucket_website_url: Optional[str] = None,
        include_description: bool = False,
    ) -> bool:
        tests_api = TestsAPI(
            dbt_runner=self.internal_dbt_runner,
            days_back=days_back,
            invocations_per_test=test_runs_amount,
            disable_passed_test_metrics=disable_passed_test_metrics,
        )
        invocations_api = InvocationsAPI(
            dbt_runner=self.internal_dbt_runner,
        )
        invocation = invocations_api.get_test_invocation_from_filter(
            self.selector_filter.to_selector_filter_schema()
        )
        summary_test_results = tests_api.get_test_results_summary(
            filter=self.selector_filter.to_selector_filter_schema(),
            dbt_invocation=invocation,
        )
        if self.slack_client:
            send_succeeded = self.slack_client.send_message(
                channel_name=self.config.slack_channel_name,
                message=SlackReportSummaryMessageBuilder().get_slack_message(
                    test_results=summary_test_results,
                    bucket_website_url=bucket_website_url,
                    include_description=include_description,
                    filter=self.selector_filter.to_selector_filter_schema(),
                    days_back=days_back,
                    env=self.config.env,
                    project_name=self.config.project_name,
                ),
            )
        else:
            send_succeeded = False

        self.execution_properties[
            "sent_test_results_summary_successfully"
        ] = send_succeeded
        self.success = send_succeeded

        if send_succeeded:
            logger.info("Sent test results summary to Slack")

        self.execution_properties["success"] = self.success
        return self.success

    def _get_report_file_path(self, file_path: Optional[str] = None) -> str:
        if file_path:
            if file_path.endswith(".htm") or file_path.endswith(".html"):
                return os.path.abspath(file_path)
            raise ValueError("Report file path must end with .html")
        return os.path.abspath(
            os.path.join(
                self.config.target_dir,
                "elementary_report.html",
            )
        )
