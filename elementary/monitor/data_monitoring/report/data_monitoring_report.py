import json
import os
import os.path
import webbrowser
from typing import Optional, Tuple

import pkg_resources

from elementary.clients.gcs.client import GCSClient
from elementary.clients.s3.client import S3Client
from elementary.config.config import Config
from elementary.monitor.api.report.report import ReportAPI
from elementary.monitor.api.report.schema import ReportDataSchema
from elementary.monitor.api.tests.tests import TestsAPI
from elementary.monitor.data_monitoring.data_monitoring import DataMonitoring
from elementary.monitor.data_monitoring.report.slack_report_summary_message_builder import (
    SlackReportSummaryMessageBuilder,
)
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class DataMonitoringReport(DataMonitoring):
    def __init__(
        self,
        config: Config,
        tracking: AnonymousTracking,
        filter: Optional[str] = None,
        force_update_dbt_package: bool = False,
        disable_samples: bool = False,
    ):
        super().__init__(
            config, tracking, force_update_dbt_package, disable_samples, filter
        )
        self.report_api = ReportAPI(self.internal_dbt_runner)
        self.s3_client = S3Client.create_client(self.config, tracking=self.tracking)
        self.gcs_client = GCSClient.create_client(self.config, tracking=self.tracking)

    def generate_report(
        self,
        days_back: Optional[int] = None,
        test_runs_amount: Optional[int] = None,
        file_path: Optional[str] = None,
        disable_passed_test_metrics: bool = False,
        should_open_browser: bool = True,
        exclude_elementary_models: bool = False,
        project_name: Optional[str] = None,
    ) -> Tuple[bool, str]:
        html_path = self._get_report_file_path(file_path)
        with open(html_path, "w", encoding="utf-8") as html_file:
            output_data = self.get_report_data(
                days_back=days_back,
                test_runs_amount=test_runs_amount,
                disable_passed_test_metrics=disable_passed_test_metrics,
                exclude_elementary_models=exclude_elementary_models,
                project_name=project_name,
            )
            template_html_path = pkg_resources.resource_filename(__name__, "index.html")
            with open(template_html_path, "r", encoding="utf-8") as template_html_file:
                template_html_code = template_html_file.read()
                dumped_output_data = json.dumps(output_data)
                compiled_output_html = f"""
                        {template_html_code}
                        <script>
                            var elementaryData = {dumped_output_data}
                        </script>
                    """
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
        days_back: Optional[int] = None,
        test_runs_amount: Optional[int] = None,
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
            filter=self.filter.get_filter(),
            env=self.config.env,
        )
        self._add_report_tracking(report_data, error)
        if error:
            logger.exception(
                f"Could not generate the report - Error: {error}\nPlease reach out to our community for help with this issue."
            )
            self.success = False

        report_data_dict = report_data.dict()
        return report_data_dict

    def _add_report_tracking(
        self, report_data: ReportDataSchema, error: Exception = None
    ):
        if error:
            self.tracking.record_cli_internal_exception(error)
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

        if self.config.anonymous_tracking_enabled:
            report_data.tracking = dict(
                posthog_api_key=self.tracking.POSTHOG_PROJECT_API_KEY,
                report_generator_anonymous_user_id=self.tracking.anonymous_user_id,
                anonymous_warehouse_id=self.tracking.anonymous_warehouse.id
                if self.tracking.anonymous_warehouse
                else None,
            )

    def send_report(
        self,
        days_back: Optional[int] = None,
        test_runs_amount: Optional[int] = None,
        file_path: Optional[str] = None,
        disable_passed_test_metrics: bool = False,
        should_open_browser: bool = False,
        exclude_elementary_models: bool = False,
        project_name: Optional[str] = None,
        remote_file_path: Optional[str] = None,
        disable_html_attachment: Optional[bool] = False,
        include_description: Optional[bool] = False,
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
        # If we upload the report to a bucket, we don't want to share it via Slack.
        should_send_report_over_slack = not (
            disable_html_attachment or self.s3_client or self.gcs_client
        )

        # If a s3 client or a gcs client is provided, we want to upload the report to the bucket.
        if self.s3_client or self.gcs_client:
            upload_succeeded, bucket_website_url = self.upload_report(
                local_html_path=local_html_path, remote_file_path=remote_file_path
            )

        # If a Slack client is provided, we want send a results summary and attachment of the report if needed.
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
                self.send_report_attachment(local_html_path=local_html_path)

        return self.success

    def send_report_attachment(self, local_html_path: str) -> bool:
        if self.slack_client:
            send_succeded = self.slack_client.send_report(
                self.config.slack_channel_name, local_html_path
            )
            self.execution_properties["sent_to_slack_successfully"] = send_succeded
            if not send_succeded:
                self.success = False

        self.execution_properties["success"] = self.success
        return self.success

    def upload_report(
        self, local_html_path: str, remote_file_path: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        if self.gcs_client:
            send_succeded, bucket_website_url = self.gcs_client.send_report(
                local_html_path, remote_bucket_file_path=remote_file_path
            )
            self.execution_properties["sent_to_gcs_successfully"] = send_succeded
            if not send_succeded:
                self.success = False

        if self.s3_client:
            send_succeded, bucket_website_url = self.s3_client.send_report(
                local_html_path, remote_bucket_file_path=remote_file_path
            )
            self.execution_properties["sent_to_s3_successfully"] = send_succeded
            if not send_succeded:
                self.success = False

        self.execution_properties["success"] = self.success
        return self.success, bucket_website_url

    def send_test_results_summary(
        self,
        days_back: Optional[int] = None,
        test_runs_amount: Optional[int] = None,
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
        summary_test_results = tests_api.get_test_results_summary(
            filter=self.filter.get_filter(),
        )
        send_succeeded = self.slack_client.send_message(
            channel_name=self.config.slack_channel_name,
            message=SlackReportSummaryMessageBuilder().get_slack_message(
                test_results=summary_test_results,
                bucket_website_url=bucket_website_url,
                include_description=include_description,
                filter=self.filter.get_filter(),
                days_back=days_back,
                env=self.config.env,
            ),
        )

        self.execution_properties[
            "sent_test_results_summary_succesfully"
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
