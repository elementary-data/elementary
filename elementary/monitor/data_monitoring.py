import json
import os
import os.path
import re
import webbrowser
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import click
import pkg_resources
from alive_progress import alive_it
from packaging import version

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.clients.gcs.client import GCSClient
from elementary.clients.s3.client import S3Client
from elementary.clients.slack.client import SlackClient
from elementary.clients.slack.schema import SlackMessageSchema
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils
from elementary.monitor.alerts.alert import Alert
from elementary.monitor.alerts.alerts import Alerts
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import ElementaryTestAlert, TestAlert
from elementary.monitor.api.alerts import AlertsAPI
from elementary.monitor.api.filters.filters import FiltersAPI
from elementary.monitor.api.lineage.lineage import LineageAPI
from elementary.monitor.api.lineage.schema import LineageSchema
from elementary.monitor.api.models.models import ModelsAPI
from elementary.monitor.api.models.schema import (
    ModelRunsSchema,
    NormalizedExposureSchema,
    NormalizedModelSchema,
    NormalizedSourceSchema,
)
from elementary.monitor.api.sidebar.schema import SidebarsSchema
from elementary.monitor.api.sidebar.sidebar import SidebarAPI
from elementary.monitor.api.tests.schema import (
    InvocationSchema,
    ModelUniqueIdType,
    TestMetadataSchema,
    TestUniqueIdType,
    TotalsSchema,
)
from elementary.monitor.api.tests.tests import TestsAPI
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.utils import package
from elementary.utils.json_utils import parse_str_to_list, prettify_json_str_set
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
        disable_samples: bool = False,
    ):
        self.config = config
        self.tracking = tracking
        self.dbt_runner = DbtRunner(
            dbt_project_utils.PATH,
            self.config.profiles_dir,
            self.config.profile_target,
            dbt_env_vars=self.config.dbt_env_vars,
        )
        self.tests_api = TestsAPI(dbt_runner=self.dbt_runner)
        self.models_api = ModelsAPI(dbt_runner=self.dbt_runner)
        self.sidebar_api = SidebarAPI(dbt_runner=self.dbt_runner)
        self.lineage_api = LineageAPI(dbt_runner=self.dbt_runner)
        self.filter_api = FiltersAPI(dbt_runner=self.dbt_runner)
        self.execution_properties = {}
        latest_invocation = self.get_latest_invocation()
        self.project_name = latest_invocation.get("project_name")
        tracking.set_env("target_name", latest_invocation.get("target_name"))
        tracking.set_env("dbt_orchestrator", latest_invocation.get("orchestrator"))
        tracking.set_env("dbt_version", latest_invocation.get("dbt_version"))
        dbt_pkg_version = latest_invocation.get("elementary_version")
        tracking.set_env("dbt_pkg_version", dbt_pkg_version)
        if dbt_pkg_version:
            self._check_dbt_package_compatibility(dbt_pkg_version)
        # slack client is optional
        self.slack_client = SlackClient.create_client(
            self.config, tracking=self.tracking
        )
        self.s3_client = S3Client.create_client(self.config, tracking=self.tracking)
        self.gcs_client = GCSClient.create_client(self.config, tracking=self.tracking)
        self._download_dbt_package_if_needed(force_update_dbt_package)
        self.elementary_database_and_schema = self.get_elementary_database_and_schema()
        self.alerts_api = AlertsAPI(
            self.dbt_runner, self.config, self.elementary_database_and_schema
        )
        self.sent_alert_count = 0
        self.success = True
        self.send_test_message_on_success = send_test_message_on_success
        self.disable_samples = disable_samples

    def _parse_emails_to_ids(self, slack_members: Union[str, List[str]]) -> str:
        if not slack_members:
            return slack_members

        def _regex_match_owner_email(potential_email: str) -> bool:
            email_regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
            return bool(re.fullmatch(email_regex, potential_email))

        def _get_user_id(email: str) -> str:
            user_id = self.slack_client.get_user_id_from_email(email)
            return f"<@{user_id}>" if user_id else email

        if isinstance(slack_members, str):
            slack_members = parse_str_to_list(slack_members)
        ids = [
            _get_user_id(slack_member)
            if _regex_match_owner_email(slack_member)
            else slack_member
            for slack_member in slack_members
        ]
        return prettify_json_str_set(ids)

    def _send_alerts_to_slack(self, alerts: List[Alert], alerts_table_name: str):
        if not alerts:
            return

        sent_alert_ids = []
        alerts_with_progress_bar = alive_it(alerts, title="Sending alerts")
        for alert in alerts_with_progress_bar:
            alert.owners = self._parse_emails_to_ids(alert.owners)
            alert.subscribers = self._parse_emails_to_ids(alert.subscribers)
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
            message=SlackMessageSchema(
                text=f"Elementary monitor ran successfully on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            ),
        )
        logger.info("Sent the test message.")

    def _send_alerts(self, alerts: Alerts):
        self._send_alerts_to_slack(alerts.tests.get_all(), TestAlert.TABLE_NAME)
        self._send_alerts_to_slack(alerts.models.get_all(), ModelAlert.TABLE_NAME)
        self._send_alerts_to_slack(
            alerts.source_freshnesses.get_all(), SourceFreshnessAlert.TABLE_NAME
        )
        self.execution_properties["sent_alert_count"] = self.sent_alert_count

    def run_alerts(
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

        alerts = self.alerts_api.query(days_back, disable_samples=self.disable_samples)
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
        project_name: Optional[str] = None,
    ) -> Tuple[bool, str]:
        now_utc = get_now_utc_iso_format()
        html_path = self._get_report_file_path(now_utc, file_path)
        with open(html_path, "w") as html_file:
            output_data = {"creation_time": now_utc, "days_back": days_back}

            models = self.models_api.get_models(exclude_elementary_models)
            sources = self.models_api.get_sources()
            exposures = self.models_api.get_exposures()
            tests_metadata = self.tests_api.get_tests_metadata(days_back=days_back)
            models_runs = self.models_api.get_models_runs(
                days_back=days_back, exclude_elementary_models=exclude_elementary_models
            )

            (
                test_results,
                test_results_totals,
                test_runs_totals,
            ) = self._get_test_results_and_totals(
                days_back=days_back,
                test_runs_amount=test_runs_amount,
                disable_passed_test_metrics=disable_passed_test_metrics,
                tests_metadata=tests_metadata,
            )
            serializable_models, sidebars = self._get_dbt_models_and_sidebars(
                models, sources, exposures
            )
            models_coverages = self._get_dbt_models_test_coverages()
            models_runs_dicts, model_runs_totals = self._get_models_runs_and_totals(
                models_runs
            )
            lineage = self._get_lineage(exclude_elementary_models)
            filters = self.filter_api.get_filters(
                test_results_totals, test_runs_totals, models, sources, models_runs
            )

            output_data["models"] = serializable_models
            output_data["sidebars"] = sidebars.dict()
            output_data["test_results"] = test_results
            output_data["test_results_totals"] = self._serialize_totals(
                test_results_totals
            )
            output_data["test_runs_totals"] = self._serialize_totals(test_runs_totals)
            output_data["coverages"] = models_coverages
            output_data["model_runs"] = models_runs_dicts
            output_data["model_runs_totals"] = model_runs_totals
            output_data["filters"] = filters.dict()
            output_data["lineage"] = lineage.dict()
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
            try:
                webbrowser.open_new_tab("file://" + html_path)
            except webbrowser.Error as e:
                logger.error("Unable to open the web browser.")

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
        return self.lineage_api.get_lineage(exclude_elementary_models)

    def _get_test_results_and_totals(
        self,
        tests_metadata: Optional[List[TestMetadataSchema]],
        days_back: Optional[int] = None,
        test_runs_amount: Optional[int] = None,
        disable_passed_test_metrics: bool = False,
    ):
        try:
            if self.disable_samples:
                tests_sample_data = {}
            else:
                tests_sample_data = self.tests_api.get_tests_sample_data(
                    days_back=days_back,
                    disable_passed_test_metrics=disable_passed_test_metrics,
                )
            invocations = self.tests_api.get_invocations(
                invocations_per_test=test_runs_amount, days_back=days_back
            )
            tests_results = self._create_tests_results(
                tests_metadata=tests_metadata,
                tests_sample_data=tests_sample_data,
                invocations=invocations,
            )
            test_results_totals = self.tests_api.get_total_tests_results(tests_metadata)
            test_runs_totals = self.tests_api.get_total_tests_runs(
                tests_metadata=tests_metadata, tests_invocations=invocations
            )
            self.execution_properties["test_result_count"] = len(tests_metadata)
            return tests_results, test_results_totals, test_runs_totals
        except Exception as e:
            logger.exception(f"Could not get test results and totals - Error: {e}")
            self.tracking.record_cli_internal_exception(e)
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
            test_sub_type_unique_id = self.tests_api.get_test_sub_type_unique_id(
                **dict(test)
            )
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
        sidebars = self.sidebar_api.get_sidebars(
            artifacts=[*models.values(), *sources.values()]
        )

        return serializable_nodes, sidebars

    def _get_dbt_models_test_coverages(self) -> Dict[str, Dict[str, int]]:
        coverages = self.models_api.get_test_coverages()
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
        except Exception as ex:
            logger.error("Failed to parse Elementary's database and schema.")
            self.tracking.record_cli_internal_exception(ex)
            return "<elementary_database>.<elementary_schema>"

    def get_latest_invocation(self) -> Dict[str, Any]:
        try:
            latest_invocation = self.dbt_runner.run_operation(
                "get_latest_invocation", quiet=True
            )[0]
            return json.loads(latest_invocation)[0] if latest_invocation else {}
        except Exception as err:
            logger.error(f"Unable to get the latest invocation: {err}")
            self.tracking.record_cli_internal_exception(err)
            return {}

    @staticmethod
    def _check_dbt_package_compatibility(dbt_pkg_ver: str):
        dbt_pkg_ver = version.parse(dbt_pkg_ver)
        py_pkg_ver = version.parse(package.get_package_version())
        logger.info(
            f"Checking compatibility between edr ({py_pkg_ver}) and Elementary's dbt package ({dbt_pkg_ver})."
        )
        if (
            dbt_pkg_ver.major != py_pkg_ver.major
            or dbt_pkg_ver.minor != py_pkg_ver.minor
        ):
            click.secho(
                f"You are using incompatible versions between edr ({py_pkg_ver}) and Elementary's dbt package ({dbt_pkg_ver}).\n "
                "Please upgrade the major and minor versions to align.\n",
                fg="yellow",
            )
