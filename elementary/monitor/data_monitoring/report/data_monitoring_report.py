import json
import os
import os.path
import webbrowser
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import pkg_resources

from elementary.clients.gcs.client import GCSClient
from elementary.clients.s3.client import S3Client
from elementary.config.config import Config
from elementary.monitor.data_monitoring.data_monitoring import DataMonitoring
from elementary.monitor.data_monitoring.report.slack_report_summary_message_builder import (
    SlackReportSummaryMessageBuilder,
)
from elementary.monitor.data_monitoring.schema import (
    DataMonitoringReportTestResultsSchema,
    DataMonitoringReportTestRunsSchema,
)
from elementary.monitor.fetchers.filters.filters import FiltersFetcher
from elementary.monitor.fetchers.lineage.lineage import LineageFetcher
from elementary.monitor.fetchers.lineage.schema import LineageSchema
from elementary.monitor.fetchers.models.models import ModelsFetcher
from elementary.monitor.fetchers.models.schema import (
    ModelRunsSchema,
    NormalizedExposureSchema,
    NormalizedModelSchema,
    NormalizedSourceSchema,
)
from elementary.monitor.fetchers.sidebar.schema import SidebarsSchema
from elementary.monitor.fetchers.sidebar.sidebar import SidebarFetcher
from elementary.monitor.fetchers.tests.schema import TestResultDBRowSchema, TotalsSchema
from elementary.monitor.fetchers.tests.tests import TestsFetcher
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.utils.log import get_logger
from elementary.utils.time import get_now_utc_iso_format

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
        self.tests_fetcher = TestsFetcher(dbt_runner=self.internal_dbt_runner)
        self.models_fetcher = ModelsFetcher(dbt_runner=self.internal_dbt_runner)
        self.sidebar_fetcher = SidebarFetcher(dbt_runner=self.internal_dbt_runner)
        self.lineage_fetcher = LineageFetcher(dbt_runner=self.internal_dbt_runner)
        self.filter_fetcher = FiltersFetcher(dbt_runner=self.internal_dbt_runner)
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
        now_utc = get_now_utc_iso_format()
        html_path = self._get_report_file_path(file_path)
        with open(html_path, "w", encoding="utf-8") as html_file:
            output_data = {"creation_time": now_utc, "days_back": days_back}

            models = self.models_fetcher.get_models(exclude_elementary_models)
            sources = self.models_fetcher.get_sources()
            exposures = self.models_fetcher.get_exposures()

            models_runs = self.models_fetcher.get_models_runs(
                days_back=days_back, exclude_elementary_models=exclude_elementary_models
            )

            test_results_db_rows = self.tests_fetcher.get_all_test_results_db_rows(
                days_back=days_back,
                invocations_per_test=test_runs_amount,
                disable_passed_test_metrics=disable_passed_test_metrics,
            )
            test_results = self._get_test_results_and_totals(test_results_db_rows)
            test_runs = self._get_test_runs_and_totals(test_results_db_rows)
            serializable_models, sidebars = self._get_dbt_models_and_sidebars(
                models, sources, exposures
            )
            models_coverages = self._get_dbt_models_test_coverages()
            models_runs_dicts, model_runs_totals = self._get_models_runs_and_totals(
                models_runs
            )
            lineage = self._get_lineage(exclude_elementary_models)
            filters = self.filter_fetcher.get_filters(
                test_results.totals, test_runs.totals, models, sources, models_runs
            )

            test_metadatas = []
            for tests in test_results.results.values():
                for test in tests:
                    test_metadatas.append(test.metadata)
            self.execution_properties["elementary_test_count"] = len(
                [
                    test_metadata
                    for test_metadata in test_metadatas
                    if test_metadata.test_type != "dbt_test"
                ]
            )
            self.execution_properties["test_result_count"] = len(test_metadatas)

            serializable_test_results = defaultdict(list)
            for model_unique_id, test_result in test_results.results.items():
                serializable_test_results[model_unique_id].extend(
                    [result.dict() for result in test_result]
                )

            serializable_test_runs = defaultdict(list)
            for model_unique_id, test_run in test_runs.runs.items():
                serializable_test_runs[model_unique_id].extend(
                    [run.dict() for run in test_run]
                )

            output_data["models"] = serializable_models
            output_data["sidebars"] = sidebars.dict()
            output_data["invocation"] = dict(test_results.invocation)
            output_data["test_results"] = serializable_test_results
            output_data["test_results_totals"] = self._serialize_totals(
                test_results.totals
            )
            output_data["test_runs"] = serializable_test_runs
            output_data["test_runs_totals"] = self._serialize_totals(test_runs.totals)
            output_data["coverages"] = models_coverages
            output_data["model_runs"] = models_runs_dicts
            output_data["model_runs_totals"] = model_runs_totals
            output_data["filters"] = filters.dict()
            output_data["lineage"] = lineage.dict()
            if self.config.anonymous_tracking_enabled:
                output_data["tracking"] = {
                    "posthog_api_key": self.tracking.POSTHOG_PROJECT_API_KEY,
                    "report_generator_anonymous_user_id": self.tracking.anonymous_user_id,
                    "anonymous_warehouse_id": self.tracking.anonymous_warehouse.id
                    if self.tracking.anonymous_warehouse
                    else None,
                }
            output_data["env"] = {
                "project_name": project_name or self.project_name,
                "env": self.config.env,
            }
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
        test_results_db_rows = self.tests_fetcher.get_all_test_results_db_rows(
            days_back=days_back,
            invocations_per_test=test_runs_amount,
            disable_passed_test_metrics=disable_passed_test_metrics,
        )
        summary_test_results = self.tests_fetcher.get_test_results_summary(
            test_results_db_rows=test_results_db_rows,
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

    def _get_lineage(self, exclude_elementary_models: bool = False) -> LineageSchema:
        return self.lineage_fetcher.get_lineage(exclude_elementary_models)

    def _get_test_results_and_totals(
        self, test_results_db_rows: List[TestResultDBRowSchema]
    ) -> DataMonitoringReportTestResultsSchema:
        try:
            tests_results, invocation = self.tests_fetcher.get_test_results(
                filter=self.filter.get_filter(),
                test_results_db_rows=test_results_db_rows,
                disable_samples=self.disable_samples,
            )
            test_metadatas = []
            for test_results in tests_results.values():
                test_metadatas.extend([result.metadata for result in test_results])
            test_results_totals = self.tests_fetcher.get_total_tests_results(
                test_metadatas
            )
            return DataMonitoringReportTestResultsSchema(
                results=tests_results,
                totals=test_results_totals,
                invocation=invocation,
            )
        except Exception as e:
            logger.exception(f"Could not get test results and totals - Error: {e}")
            self.tracking.record_cli_internal_exception(e)
            self.success = False
            return DataMonitoringReportTestResultsSchema()

    def _get_test_runs_and_totals(
        self, test_results_db_rows: List[TestResultDBRowSchema]
    ) -> DataMonitoringReportTestRunsSchema:
        try:
            tests_runs = self.tests_fetcher.get_test_runs(test_results_db_rows)
            test_runs_totals = self.tests_fetcher.get_total_tests_runs(
                tests_runs=tests_runs
            )
            return DataMonitoringReportTestRunsSchema(
                runs=tests_runs, totals=test_runs_totals
            )
        except Exception as e:
            logger.exception(f"Could not get test runs and totals - Error: {e}")
            self.tracking.record_cli_internal_exception(e)
            self.success = False
            return DataMonitoringReportTestRunsSchema()

    @staticmethod
    def _serialize_totals(totals: Dict[str, TotalsSchema]) -> Dict[str, dict]:
        serialized_totals = dict()
        for model_unique_id, total in totals.items():
            serialized_totals[model_unique_id] = total.dict()
        return serialized_totals

    def _get_models_runs_and_totals(self, models_runs: List[ModelRunsSchema]):
        models_runs_dicts = []
        model_runs_totals = {}
        for model_runs in models_runs:
            models_runs_dicts.append(model_runs.dict(by_alias=True))
            model_runs_totals[model_runs.unique_id] = {
                "errors": model_runs.totals.errors,
                "warnings": 0,
                "failures": 0,
                "passed": model_runs.totals.success,
            }
        return models_runs_dicts, model_runs_totals

    def _get_dbt_models_and_sidebars(
        self,
        models: Dict[str, NormalizedModelSchema],
        sources: Dict[str, NormalizedSourceSchema],
        exposures: Dict[str, NormalizedExposureSchema],
    ) -> Tuple[Dict, SidebarsSchema]:
        self.execution_properties["model_count"] = len(models)
        self.execution_properties["source_count"] = len(sources)
        self.execution_properties["exposure_count"] = len(exposures)

        nodes = dict(**models, **sources, **exposures)
        serializable_nodes = dict()
        for key in nodes.keys():
            serializable_nodes[key] = dict(nodes[key])

        # Currently we don't show exposures as part of the sidebar
        sidebars = self.sidebar_fetcher.get_sidebars(
            artifacts=[*models.values(), *sources.values()]
        )

        return serializable_nodes, sidebars

    def _get_dbt_models_test_coverages(self) -> Dict[str, Dict[str, int]]:
        coverages = self.models_fetcher.get_test_coverages()
        return {model_id: dict(coverage) for model_id, coverage in coverages.items()}

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
