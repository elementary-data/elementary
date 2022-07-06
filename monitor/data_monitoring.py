import json
import os
import webbrowser
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pkg_resources
from alive_progress import alive_it

from clients.dbt.dbt_runner import DbtRunner
from clients.slack.schema import SlackMessageSchema
from clients.slack.slack_client import SlackClient
from config.config import Config
from monitor.api.models.models import ModelsAPI
from monitor.api.sidebar.sidebar import SidebarAPI
from monitor.api.tests.schema import InvocationSchema, ModelUniqueIdType, TestMetadataSchema, TestUniqueIdType
from monitor.api.tests.tests import TestsAPI
from monitor.test_result import TestResult
from utils.log import get_logger
from utils.time import get_now_utc_str

logger = get_logger(__name__)
FILE_DIR = os.path.dirname(os.path.realpath(__file__))

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


@dataclass
class AlertsQueryResult:
    test_results: List[TestResult]
    failed_to_parse_alert_dicts: List[dict]


@dataclass
class SlackAlert:
    id: str
    message: SlackMessageSchema


class DataMonitoring(object):
    DBT_PACKAGE_NAME = 'elementary'
    DBT_PROJECT_PATH = os.path.join(FILE_DIR, 'dbt_project')
    DBT_PROJECT_MODELS_PATH = os.path.join(FILE_DIR, 'dbt_project', 'models')
    # Compatibility for previous dbt versions
    DBT_PROJECT_MODULES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_modules', DBT_PACKAGE_NAME)
    DBT_PROJECT_PACKAGES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_packages', DBT_PACKAGE_NAME)

    def __init__(
            self,
            config: Config,
            force_update_dbt_package: bool = False,
            slack_webhook: Optional[str] = None,
            slack_token: Optional[str] = None,
            slack_channel_name: Optional[str] = None
    ) -> None:
        self.config = config
        self.dbt_runner = DbtRunner(self.DBT_PROJECT_PATH, self.config.profiles_dir, self.config.profile_target)
        self.execution_properties = {}
        self.slack_webhook = slack_webhook or self.config.slack_notification_webhook
        self.slack_token = slack_token or self.config.slack_token
        self.slack_channel_name = slack_channel_name or self.config.slack_notification_channel_name
        # slack client is optional
        self.slack_client = SlackClient.create_slack_client(self.slack_token, self.slack_webhook)
        self._download_dbt_package_if_needed(force_update_dbt_package)
        self.success = True

    def _dbt_package_exists(self) -> bool:
        return os.path.exists(self.DBT_PROJECT_PACKAGES_PATH) or os.path.exists(self.DBT_PROJECT_MODULES_PATH)

    @staticmethod
    def _split_list_to_chunks(items: list, chunk_size: int = 50) -> List[List]:
        chunk_list = []
        for i in range(0, len(items), chunk_size):
            chunk_list.append(items[i: i + chunk_size])
        return chunk_list

    def _update_sent_alerts(self, alert_ids) -> None:
        alert_ids_chunks = self._split_list_to_chunks(alert_ids)
        for alert_ids_chunk in alert_ids_chunks:
            self.dbt_runner.run_operation(macro_name='update_sent_alerts',
                                          macro_args={'alert_ids': alert_ids_chunk},
                                          json_logs=False)

    def _query_alerts(self, days_back: int) -> AlertsQueryResult:
        results = self.dbt_runner.run_operation(macro_name='get_new_alerts', macro_args={'days_back': days_back})
        test_result_alerts = []
        failed_to_parse_alert_dicts = []
        if results:
            test_result_alert_dicts = json.loads(results[0])
            self.execution_properties['alert_rows'] = len(test_result_alert_dicts)
            for test_result_alert_dict in test_result_alert_dicts:
                test_result = TestResult.create_test_result_from_dict(test_result_alert_dict)
                if test_result:
                    test_result_alerts.append(test_result)
                else:
                    failed_to_parse_alert_dicts.append(test_result_alert_dict)
                    self.success = False

        return AlertsQueryResult(test_result_alerts, failed_to_parse_alert_dicts)

    def _send_alerts_to_slack(self, alerts: List[SlackAlert]) -> None:
        sent_alerts = []
        alerts_with_progress_bar = alive_it(alerts, title="Sending alerts")
        for alert in alerts_with_progress_bar:
            sent_successfully = self.slack_client.send_message(
                channel_name=self.slack_channel_name,
                message=alert.message
            )
            if sent_successfully:
                sent_alerts.append(alert.id)
            else:
                logger.error(
                    f"Could not send the alert - {alert.id}. Full alert: {json.dumps(dict(alert.message))}")
                self.success = False

        sent_alert_count = len(sent_alerts)
        self.execution_properties['sent_alert_count'] = sent_alert_count
        if sent_alert_count > 0:
            self._update_sent_alerts(sent_alerts)

    def _get_slack_alert_from_test_result(self, test_results: List[TestResult]):
        return [
            SlackAlert(
                test_result.id,
                test_result.generate_slack_message(is_slack_workflow=self.config.is_slack_workflow)
            )
            for test_result in test_results
        ]

    @staticmethod
    def _get_slack_alert_from_dict(alert_dicts: List[Dict[str, Any]]):
        return [
            SlackAlert(
                alert_dict['id'],
                SlackMessageSchema(
                    text=TestResult.format_section_msg(
                        f":small_red_triangle: Oops, we failed to format the alert :confused:\n"
                        f"Please share this with the Elementary team via <https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg|Slack> or a <https://github.com/elementary-data/elementary/issues/new|GitHub> issue.\n"
                        f"```{json.dumps(alert_dict, indent=2)}```"
                    )
                )
            )
            for alert_dict in alert_dicts
        ]

    def _download_dbt_package_if_needed(self, force_update_dbt_packages: bool):
        internal_dbt_package_exists = self._dbt_package_exists()
        self.execution_properties['dbt_package_exists'] = internal_dbt_package_exists
        self.execution_properties['force_update_dbt_packages'] = force_update_dbt_packages
        if not internal_dbt_package_exists or force_update_dbt_packages:
            logger.info("Downloading edr internal dbt package")
            package_downloaded = self.dbt_runner.deps()
            self.execution_properties['package_downloaded'] = package_downloaded
            if not package_downloaded:
                logger.info('Could not download internal dbt package')
                self.success = False
                return

    def _send_alerts(self, days_back: int):
        query_result = self._query_alerts(days_back)
        alert_count = len(query_result.test_results) + len(query_result.failed_to_parse_alert_dicts)
        self.execution_properties['alert_count'] = alert_count
        self._send_alerts_to_slack(self._get_slack_alert_from_test_result(query_result.test_results))
        self._send_alerts_to_slack(self._get_slack_alert_from_dict(query_result.failed_to_parse_alert_dicts))

    def run(self, days_back: int, dbt_full_refresh: bool = False) -> bool:
        logger.info("Running internal dbt run to aggregate alerts")
        success = self.dbt_runner.run(models='alerts', full_refresh=dbt_full_refresh)
        self.execution_properties['alerts_run_success'] = success
        if not success:
            logger.info('Could not aggregate alerts successfully')
            self.success = False
            self.execution_properties['success'] = self.success
            return self.success

        self._send_alerts(days_back)
        self.execution_properties['run_end'] = True
        self.execution_properties['success'] = self.success
        return self.success

    def generate_report(self, days_back: Optional[int] = None, test_runs_amount: Optional[int] = None) -> Tuple[
        bool, str]:
        now_utc = get_now_utc_str()
        elementary_output = {}
        elementary_output['creation_time'] = now_utc
        test_results, test_results_totals, test_runs_totals = self._get_test_results_and_totals(days_back=days_back,
                                                                                                test_runs_amount=test_runs_amount)
        models, dbt_sidebar = self._get_dbt_models_and_sidebar()
        models_coverages = self._get_dbt_models_test_coverages()
        elementary_output['models'] = models
        elementary_output['dbt_sidebar'] = dbt_sidebar
        elementary_output['test_results'] = test_results
        elementary_output['test_results_totals'] = test_results_totals
        elementary_output['test_runs_totals'] = test_runs_totals
        elementary_output['coverages'] = models_coverages

        html_index_path = pkg_resources.resource_filename(__name__, "index.html")
        with open(html_index_path, 'r') as index_html_file:
            html_code = index_html_file.read()
            elementary_output_str = json.dumps(elementary_output)
            elementary_output_html = f"""
                    {html_code}
                    <script>
                        var elementaryData = {elementary_output_str}
                    </script>
                """
            elementary_html_file_name = f"elementary - {now_utc} utc.html".replace(" ", "_").replace(":", "-")
            elementary_html_path = os.path.join(self.config.target_dir, elementary_html_file_name)
            with open(elementary_html_path, 'w') as elementary_output_html_file:
                elementary_output_html_file.write(elementary_output_html)
            with open(os.path.join(self.config.target_dir, 'elementary_output.json'), 'w') as \
                    elementary_output_json_file:
                elementary_output_json_file.write(elementary_output_str)

            elementary_html_file_path = 'file://' + elementary_html_path
            webbrowser.open_new_tab(elementary_html_file_path)
            self.execution_properties['report_end'] = True
            self.execution_properties['success'] = self.success
            return self.success, elementary_html_path

    def send_report(self, elementary_html_path: str) -> bool:
        if os.path.exists(elementary_html_path):
            file_uploaded_successfully = self.slack_client.upload_file(
                channel_name=self.slack_channel_name,
                file_path=elementary_html_path,
                message=SlackMessageSchema(text="Elementary monitoring report")
            )
            if not file_uploaded_successfully:
                self.success = False
        else:
            logger.error('Could not send Elementary monitoring report because it does not exist')
            self.success = False
        return self.success

    def _get_test_results_and_totals(self, days_back: Optional[int] = None, test_runs_amount: Optional[int] = None):
        tests_api = TestsAPI(dbt_runner=self.dbt_runner)
        try:
            tests_metadata = tests_api.get_tests_metadata(days_back=days_back)
            tests_sample_data = tests_api.get_tests_sample_data(days_back=days_back)
            invocations = tests_api.get_invocations(invocations_per_test=test_runs_amount, days_back=days_back)
            tests_results = self._create_tests_results(
                tests_metadata=tests_metadata,
                tests_sample_data=tests_sample_data,
                invocations=invocations
            )
            test_results_totals = tests_api.get_total_tests_results(tests_metadata)
            test_runs_totals = tests_api.get_total_tests_runs(tests_metadata=tests_metadata,
                                                              tests_invocations=invocations)
            self.execution_properties['test_results'] = len(tests_metadata)
            return tests_results, test_results_totals, test_runs_totals
        except Exception as e:
            logger.error(f"Could not get test results and totals - Error: {e}")
            self.success = False
            return dict(), dict(), dict()

    def _create_tests_results(
            self,
            tests_metadata: List[TestMetadataSchema],
            tests_sample_data: Dict[TestUniqueIdType, Dict[str, Any]],
            invocations: Dict[TestUniqueIdType, List[InvocationSchema]]
    ) -> Dict[ModelUniqueIdType, Dict[str, Any]]:
        tests_results = defaultdict(list)
        for test in tests_metadata:
            test_sub_type_unique_id = TestsAPI.get_test_sub_type_unique_id(**dict(test))
            metadata = dict(test)
            test_sample_data = tests_sample_data.get(test_sub_type_unique_id)
            test_invocations = invocations.get(test_sub_type_unique_id)
            test_result = TestResult.create_test_result_from_dict({
                **metadata,
                "test_rows_sample": test_sample_data,
                "test_runs": json.loads(test_invocations.json()) if test_invocations else {}
            })
            tests_results[test.model_unique_id].append(test_result.to_test_result_api_dict())
        return tests_results

    def _get_dbt_models_and_sidebar(self) -> Tuple[Dict, Dict]:
        models_api = ModelsAPI(dbt_runner=self.dbt_runner)
        sidebar_api = SidebarAPI(dbt_runner=self.dbt_runner)

        models = models_api.get_models()
        sources = models_api.get_sources()

        models_and_sources = dict(**models, **sources)
        serializeable_models = dict()
        for key in models_and_sources.keys():
            serializeable_models[key] = dict(models_and_sources[key])

        dbt_sidebar = sidebar_api.get_sidebar(models=models, sources=sources)

        return serializeable_models, dbt_sidebar

    def _get_dbt_models_test_coverages(self) -> Dict[str, Dict[str, int]]:
        models_api = ModelsAPI(dbt_runner=self.dbt_runner)
        coverages = models_api.get_test_coverages()
        for model_id, coverage in coverages.items():
            coverages[model_id] = dict(coverage)
        return coverages

    def properties(self):
        data_monitoring_properties = {'data_monitoring_properties': self.execution_properties}
        return data_monitoring_properties
