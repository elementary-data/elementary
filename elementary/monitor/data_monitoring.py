import json
import os
import os.path
import webbrowser
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pkg_resources
from alive_progress import alive_it

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.clients.gcs.client import GCSClient
from elementary.clients.s3.client import S3Client
from elementary.clients.slack.client import SlackClient
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils
from elementary.monitor.alerts.alert import Alert
from elementary.monitor.alerts.alerts import Alerts
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import TestAlert, ElementaryTestAlert
from elementary.monitor.api.alerts import AlertsAPI
from elementary.monitor.api.lineage.lineage import LineageAPI
from elementary.monitor.api.lineage.schema import LineageSchema
from elementary.monitor.api.models.models import ModelsAPI
from elementary.monitor.api.sidebar.sidebar import SidebarAPI
from elementary.monitor.api.tests.schema import (
    InvocationSchema,
    ModelUniqueIdType,
    TestMetadataSchema,
    TestUniqueIdType,
)
from elementary.monitor.api.tests.tests import TestsAPI
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.utils import package
from elementary.utils.log import get_logger
from elementary.utils.time import get_now_utc_iso_format

logger = get_logger(__name__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class DataMonitoring:
    def __init__(
        self,
        config: Config,
        tracking: AnonymousTracking,
        force_update_dbt_package: bool = False,
        send_test_message_on_success: bool = False,
    ):
        self.config = config
        self.tracking = tracking
        self.dbt_runner = DbtRunner(
            dbt_project_utils.PATH, self.config.profiles_dir, self.config.profile_target
        )
        self.execution_properties = {}
        dbt_pkg_version = self.get_elementary_dbt_pkg_version()
        tracking.set_env("dbt_pkg_version", dbt_pkg_version)
        if dbt_pkg_version:
            self._check_dbt_package_compatibility(dbt_pkg_version)
        # slack client is optional
        self.slack_client = SlackClient.create_client(self.config)
        self.s3_client = S3Client.create_client(self.config)
        self.gcs_client = GCSClient.create_client(self.config)
        self._download_dbt_package_if_needed(force_update_dbt_package)
        self.elementary_database_and_schema = self.get_elementary_database_and_schema()
        self.alerts_api = AlertsAPI(
            self.dbt_runner, self.config, self.elementary_database_and_schema
        )
        self.sent_alert_count = 0
        self.success = True
        self.send_test_message_on_success = send_test_message_on_success

    def _send_alerts_to_slack(self, alerts: List[Alert], alerts_table_name: str):
        if not alerts:
            return

        sent_alert_ids = []
        alerts_with_progress_bar = alive_it(alerts, title="Sending alerts")
        for alert in alerts_with_progress_bar:
            alert_msg = alert.to_slack()
            sent_successfully = self.slack_client.send_message(
                channel_name=alert.slack_channel
                if alert.slack_channel
                else self.config.slack_channel_name,
                message=alert_msg,
            )
            if sent_successfully:
                sent_alert_ids.append(alert.id)
            else:
                logger.error(
                    f"Could not send the alert - {alert.id}. Full alert: {json.dumps(dict(alert_msg))}"
                )
                self.success = False
        self.alerts_api.update_sent_alerts(sent_alert_ids, alerts_table_name)
        self.sent_alert_count += len(sent_alert_ids)

    def _download_dbt_package_if_needed(self, force_update_dbt_packages: bool):
        internal_dbt_package_exists = dbt_project_utils.dbt_package_exists()
        self.execution_properties["dbt_package_exists"] = internal_dbt_package_exists
        self.execution_properties[
            "force_update_dbt_packages"
        ] = force_update_dbt_packages
        if not internal_dbt_package_exists or force_update_dbt_packages:
            logger.info("Downloading edr internal dbt package")
            package_downloaded = self.dbt_runner.deps()
            self.execution_properties["package_downloaded"] = package_downloaded
            if not package_downloaded:
                logger.error("Could not download internal dbt package")
                self.success = False
                return

    def _send_test_message(self):
        self.slack_client.send_message(
            channel_name=self.config.slack_channel_name,
            message=f"Elementary monitor ran successfully on {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        )

    def _send_alerts(self, alerts: Alerts):
        self._send_alerts_to_slack(alerts.tests.get_all(), TestAlert.TABLE_NAME)
        self._send_alerts_to_slack(alerts.models.get_all(), ModelAlert.TABLE_NAME)
        self._send_alerts_to_slack(
            alerts.source_freshnesses.get_all(), SourceFreshnessAlert.TABLE_NAME
        )
        self.execution_properties["sent_alert_count"] = self.sent_alert_count

    def run(
        self,
        days_back: int,
        dbt_full_refresh: bool = False,
        dbt_vars: Optional[dict] = None,
    ) -> bool:
        logger.info("Running internal dbt run to aggregate alerts")
        success = self.dbt_runner.run(
            models="alerts", full_refresh=dbt_full_refresh, vars=dbt_vars
        )
        self.execution_properties["alerts_run_success"] = success
        if not success:
            logger.info("Could not aggregate alerts successfully")
            self.success = False
            self.execution_properties["success"] = self.success
            return self.success

        alerts = self.alerts_api.query(days_back)
        self.execution_properties[
            "elementary_test_count"
        ] = alerts.get_elementary_test_count()
        self.execution_properties["alert_count"] = alerts.count
        malformed_alert_count = alerts.malformed_count
        if malformed_alert_count > 0:
            self.success = False
        self.execution_properties["malformed_alert_count"] = malformed_alert_count
        self.execution_properties["has_subscribers"] = any(
            alert.subscribers for alert in alerts.get_all()
        )
        self._send_alerts(alerts)
        if self.send_test_message_on_success and alerts.count == 0:
            self._send_test_message()
        self.execution_properties["run_end"] = True
        self.execution_properties["success"] = self.success
        return self.success

    def generate_report(
        self,
        days_back: Optional[int] = None,
        test_runs_amount: Optional[int] = None,
        file_path: Optional[str] = None,
        disable_passed_test_metrics: bool = False,
        should_open_browser: bool = True,
        exclude_elementary_models: bool = False,
    ) -> Tuple[bool, str]:
        now_utc = get_now_utc_iso_format()
        html_path = self._get_report_file_path(now_utc, file_path)
        with open(html_path, "w") as html_file:
            output_data = {"creation_time": now_utc, "days_back": days_back}
            (
                test_results,
                test_results_totals,
                test_runs_totals,
            ) = self._get_test_results_and_totals(
                days_back=days_back,
                test_runs_amount=test_runs_amount,
                disable_passed_test_metrics=disable_passed_test_metrics,
            )
            models, dbt_sidebar = self._get_dbt_models_and_sidebar(
                exclude_elementary_models
            )
            models_coverages = self._get_dbt_models_test_coverages()
            models_runs, model_runs_totals = self._get_models_runs_and_totals(
                days_back=days_back, exclude_elementary_models=exclude_elementary_models
            )
            lineage = self._get_lineage(exclude_elementary_models)
            output_data["models"] = models
            output_data["dbt_sidebar"] = dbt_sidebar
            output_data["test_results"] = test_results
            output_data["test_results_totals"] = test_results_totals
            output_data["test_runs_totals"] = test_runs_totals
            output_data["coverages"] = models_coverages
            output_data["model_runs"] = models_runs
            output_data["model_runs_totals"] = model_runs_totals
            output_data["lineage"] = lineage.dict()
            output_data["tracking"] = {
                "posthog_api_key": self.tracking.POSTHOG_PROJECT_API_KEY,
                "report_generator_anonymous_user_id": self.tracking.anonymous_user_id,
                "anonymous_warehouse_id": self.tracking.anonymous_warehouse.id
                if self.tracking.anonymous_warehouse
                else None,
            }
            template_html_path = pkg_resources.resource_filename(__name__, "index.html")
            with open(template_html_path, "r") as template_html_file:
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
            os.path.join(self.config.target_dir, "elementary_output.json"), "w"
        ) as elementary_output_json_file:
            elementary_output_json_file.write(dumped_output_data)

        if should_open_browser:
            webbrowser.open_new_tab("file://" + html_path)
        self.execution_properties["report_end"] = True
        self.execution_properties["success"] = self.success
        return self.success, html_path

    def send_report(
        self, local_html_path: str, remote_file_path: Optional[str] = None
    ) -> bool:
        if self.slack_client:
            send_succeded = self.slack_client.send_report(
                self.config.slack_channel_name, local_html_path
            )
            self.execution_properties["sent_to_slack_successfully"] = send_succeded
            if not send_succeded:
                self.success = False

        if self.s3_client:
            send_succeded = self.s3_client.send_report(
                local_html_path, remote_bucket_file_path=remote_file_path
            )
            self.execution_properties["sent_to_s3_successfully"] = send_succeded
            if not send_succeded:
                self.success = False

        if self.gcs_client:
            send_succeded = self.gcs_client.send_report(
                local_html_path, remote_bucket_file_path=remote_file_path
            )
            self.execution_properties["sent_to_gcs_successfully"] = send_succeded
            if not send_succeded:
                self.success = False

        self.execution_properties["success"] = self.success
        return self.success

    def _get_lineage(self, exclude_elementary_models: bool = False) -> LineageSchema:
        lineage_api = LineageAPI(dbt_runner=self.dbt_runner)
        return lineage_api.get_lineage(exclude_elementary_models)

    def _get_test_results_and_totals(
        self,
        days_back: Optional[int] = None,
        test_runs_amount: Optional[int] = None,
        disable_passed_test_metrics: bool = False,
    ):
        tests_api = TestsAPI(dbt_runner=self.dbt_runner)
        try:
            tests_metadata = tests_api.get_tests_metadata(days_back=days_back)
            tests_sample_data = tests_api.get_tests_sample_data(
                days_back=days_back,
                disable_passed_test_metrics=disable_passed_test_metrics,
            )
            invocations = tests_api.get_invocations(
                invocations_per_test=test_runs_amount, days_back=days_back
            )
            tests_results = self._create_tests_results(
                tests_metadata=tests_metadata,
                tests_sample_data=tests_sample_data,
                invocations=invocations,
            )
            test_results_totals = tests_api.get_total_tests_results(tests_metadata)
            test_runs_totals = tests_api.get_total_tests_runs(
                tests_metadata=tests_metadata, tests_invocations=invocations
            )
            self.execution_properties["test_result_count"] = len(tests_metadata)
            return tests_results, test_results_totals, test_runs_totals
        except Exception as e:
            logger.error(f"Could not get test results and totals - Error: {e}")
            self.success = False
            return dict(), dict(), dict()

    def _create_tests_results(
        self,
        tests_metadata: List[TestMetadataSchema],
        tests_sample_data: Dict[TestUniqueIdType, Dict[str, Any]],
        invocations: Dict[TestUniqueIdType, List[InvocationSchema]],
    ) -> Dict[ModelUniqueIdType, Dict[str, Any]]:
        elementary_test_count = defaultdict(int)
        tests_results = defaultdict(list)
        for test in tests_metadata:
            test_sub_type_unique_id = TestsAPI.get_test_sub_type_unique_id(**dict(test))
            metadata = dict(test)
            test_sample_data = tests_sample_data.get(test_sub_type_unique_id)
            test_invocations = invocations.get(test_sub_type_unique_id)
            test_result = TestAlert.create_test_alert_from_dict(
                **metadata,
                elementary_database_and_schema=self.elementary_database_and_schema,
                test_rows_sample=test_sample_data,
                test_runs=json.loads(test_invocations.json())
                if test_invocations
                else {},
            )
            if isinstance(test_result, ElementaryTestAlert):
                elementary_test_count[test_result.test_name] += 1
            tests_results[test.model_unique_id].append(
                test_result.to_test_alert_api_dict()
            )
        self.execution_properties["elementary_test_count"] = elementary_test_count
        return tests_results

    def _get_models_runs_and_totals(
        self, days_back: Optional[int] = None, exclude_elementary_models: bool = False
    ):
        models_api = ModelsAPI(dbt_runner=self.dbt_runner)
        models_runs = models_api.get_models_runs(
            days_back=days_back, exclude_elementary_models=exclude_elementary_models
        )
        models_runs_dicts = []
        model_runs_totals = {}
        for model_runs in models_runs:
            models_runs_dicts.append(model_runs.dict(by_alias=True))
            model_runs_totals[model_runs.unique_id] = {
                "errors": model_runs.totals.errors,
                "warnings": 0,
                "resolved": 0,
                "passed": model_runs.totals.success,
            }
        return models_runs_dicts, model_runs_totals

    def _get_dbt_models_and_sidebar(
        self, exclude_elementary_models: bool = False
    ) -> Tuple[Dict, Dict]:
        models_api = ModelsAPI(dbt_runner=self.dbt_runner)
        sidebar_api = SidebarAPI(dbt_runner=self.dbt_runner)

        models = models_api.get_models(exclude_elementary_models)
        sources = models_api.get_sources()
        exposures = models_api.get_exposures()

        self.execution_properties["model_count"] = len(models)
        self.execution_properties["source_count"] = len(sources)
        self.execution_properties["exposure_count"] = len(exposures)

        nodes = dict(**models, **sources, **exposures)
        serializable_nodes = dict()
        for key in nodes.keys():
            serializable_nodes[key] = dict(nodes[key])

        # Currently we don't show exposures as part of the sidebar
        dbt_sidebar = sidebar_api.get_sidebar(models=models, sources=sources)

        return serializable_nodes, dbt_sidebar

    def _get_dbt_models_test_coverages(self) -> Dict[str, Dict[str, int]]:
        models_api = ModelsAPI(dbt_runner=self.dbt_runner)
        coverages = models_api.get_test_coverages()
        return {model_id: dict(coverage) for model_id, coverage in coverages.items()}

    def properties(self):
        data_monitoring_properties = {
            "data_monitoring_properties": self.execution_properties
        }
        return data_monitoring_properties

    def _get_report_file_path(
        self, generation_time: str, file_path: Optional[str] = None
    ) -> str:
        if file_path:
            if file_path.endswith(".htm") or file_path.endswith(".html"):
                return os.path.abspath(file_path)
            raise ValueError("Report file path must end with .html")
        return os.path.abspath(
            os.path.join(
                self.config.target_dir,
                f"elementary - {generation_time} utc.html".replace(" ", "_").replace(
                    ":", "-"
                ),
            )
        )

    def get_elementary_database_and_schema(self):
        try:
            return self.dbt_runner.run_operation(
                "get_elementary_database_and_schema", quiet=True
            )[0]
        except Exception:
            logger.error("Failed to parse Elementary's database and schema.")
            return "<elementary_database>.<elementary_schema>"

    def get_elementary_dbt_pkg_version(self) -> Optional[str]:
        try:
            dbt_pkg_version = self.dbt_runner.run_operation(
                "get_elementary_dbt_pkg_version", quiet=True
            )[0]
            return dbt_pkg_version or None
        except Exception as err:
            logger.debug(f"Unable to get Elementary's dbt package version: {err}")
            return None

    @staticmethod
    def _check_dbt_package_compatibility(dbt_pkg_version: str):
        logger.info("Checking compatibility between edr and Elementary's dbt package.")
        try:
            package.check_dbt_pkg_compatible(dbt_pkg_version)
        except Exception as err:
            logger.error(f"Failed to check compatibility with dbt package: {err}")
