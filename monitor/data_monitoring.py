import json
import os
import webbrowser
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import pkg_resources
from alive_progress import alive_it

import utils.dbt
from clients.dbt.dbt_runner import DbtRunner
from clients.slack.schema import SlackMessageSchema
from clients.slack.slack_client import SlackClient
from config.config import Config
from monitor.alerts.alert import Alert
from monitor.alerts.alerts import Alerts
from monitor.alerts.model import ModelAlert
from monitor.alerts.test import TestAlert
from monitor.api.alerts import AlertsAPI
from monitor.api.lineage.lineage import LineageAPI
from monitor.api.lineage.schema import LineageSchema
from monitor.api.models.models import ModelsAPI
from monitor.api.sidebar.sidebar import SidebarAPI
from monitor.api.tests.schema import InvocationSchema, ModelUniqueIdType, TestMetadataSchema, TestUniqueIdType
from monitor.api.tests.tests import TestsAPI
from utils.log import get_logger
from utils.time import get_now_utc_str

logger = get_logger(__name__)
FILE_DIR = os.path.dirname(os.path.realpath(__file__))

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class DataMonitoring:
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
        self.alerts_api = AlertsAPI(self.dbt_runner)
        self.success = True

    def _dbt_package_exists(self) -> bool:
        return os.path.exists(self.DBT_PROJECT_PACKAGES_PATH) or os.path.exists(self.DBT_PROJECT_MODULES_PATH)

    def _send_alerts_to_slack(self, alerts: List[Alert], alerts_table_name: str) -> List[str]:
        if not alerts:
            return []

        sent_alert_ids = []
        alerts_with_progress_bar = alive_it(alerts, title="Sending alerts")
        for alert in alerts_with_progress_bar:
            alert_msg = alert.to_slack()
            sent_successfully = self.slack_client.send_message(
                channel_name=alert.slack_channel if alert.slack_channel else self.slack_channel_name,
                message=alert_msg
            )
            if sent_successfully:
                sent_alert_ids.append(alert.id)
            else:
                logger.error(f"Could not send the alert - {alert.id}. Full alert: {json.dumps(dict(alert_msg))}")
                self.success = False
        self.alerts_api.update_sent_alerts(sent_alert_ids, alerts_table_name)
        return sent_alert_ids

    def _download_dbt_package_if_needed(self, force_update_dbt_packages: bool):
        internal_dbt_package_exists = self._dbt_package_exists()
        self.execution_properties['dbt_package_exists'] = internal_dbt_package_exists
        self.execution_properties['force_update_dbt_packages'] = force_update_dbt_packages
        if not internal_dbt_package_exists or force_update_dbt_packages:
            logger.info("Downloading edr internal dbt package")
            package_downloaded = self.dbt_runner.deps()
            self.execution_properties['package_downloaded'] = package_downloaded
            if not package_downloaded:
                logger.error('Could not download internal dbt package')
                self.success = False
                return

    def _send_alerts(self, alerts: Alerts):
        sent_test_alert_ids = self._send_alerts_to_slack(alerts.tests.get_all(), TestAlert.TABLE_NAME)
        sent_model_alert_ids = self._send_alerts_to_slack(alerts.models.get_all(), ModelAlert.TABLE_NAME)
        sent_alert_count = len(sent_test_alert_ids) + len(sent_model_alert_ids)
        self.execution_properties['sent_alert_count'] = sent_alert_count

    def run(self, days_back: int, dbt_full_refresh: bool = False, dbt_vars: Optional[dict] = None) -> bool:
        logger.info("Running internal dbt run to aggregate alerts")
        success = self.dbt_runner.run(models='alerts', full_refresh=dbt_full_refresh, vars=dbt_vars)
        self.execution_properties['alerts_run_success'] = success
        if not success:
            logger.info('Could not aggregate alerts successfully')
            self.success = False
            self.execution_properties['success'] = self.success
            return self.success

        alerts = self.alerts_api.query(days_back)
        self.execution_properties['alert_count'] = alerts.count
        self._send_alerts(alerts)
        self.execution_properties['run_end'] = True
        self.execution_properties['success'] = self.success
        return self.success

    def generate_report(self, days_back: Optional[int] = None, test_runs_amount: Optional[int] = None,
                        file_path: Optional[str] = None, disable_passed_test_metrics: bool = False) -> Tuple[
        bool, str]:
        now_utc = get_now_utc_str()
        html_path = self._get_report_file_path(now_utc, file_path)
        with open(html_path, 'w') as html_file:
            output_data = {'creation_time': now_utc}
            test_results, test_results_totals, test_runs_totals = self._get_test_results_and_totals(
                days_back=days_back, test_runs_amount=test_runs_amount,
                disable_passed_test_metrics=disable_passed_test_metrics)
            models, dbt_sidebar = self._get_dbt_models_and_sidebar()
            models_coverages = self._get_dbt_models_test_coverages()
            lineage = self._get_lineage()
            output_data['models'] = models
            output_data['dbt_sidebar'] = dbt_sidebar
            output_data['test_results'] = test_results
            output_data['test_results_totals'] = test_results_totals
            output_data['test_runs_totals'] = test_runs_totals
            output_data['coverages'] = models_coverages
            output_data['lineage'] = lineage.dict()
            template_html_path = pkg_resources.resource_filename(__name__, "index.html")
            with open(template_html_path, 'r') as template_html_file:
                template_html_code = template_html_file.read()
                dumped_output_data = json.dumps(output_data)
                compiled_output_html = f"""
                        {template_html_code}
                        <script>
                            var elementaryData = {dumped_output_data}
                        </script>
                    """
                html_file.write(compiled_output_html)
        with open(os.path.join(self.config.target_dir, 'elementary_output.json'), 'w') as \
                elementary_output_json_file:
            elementary_output_json_file.write(dumped_output_data)

        webbrowser.open_new_tab('file://' + html_path)
        self.execution_properties['report_end'] = True
        self.execution_properties['success'] = self.success
        return self.success, html_path

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

    def _get_lineage(self) -> LineageSchema:
        lineage_api = LineageAPI(dbt_runner=self.dbt_runner)
        return lineage_api.get_lineage()

    def _get_test_results_and_totals(self, days_back: Optional[int] = None, test_runs_amount: Optional[int] = None, disable_passed_test_metrics: bool = False):
        tests_api = TestsAPI(dbt_runner=self.dbt_runner)
        try:
            tests_metadata = tests_api.get_tests_metadata(days_back=days_back)
            tests_sample_data = tests_api.get_tests_sample_data(days_back=days_back, disable_passed_test_metrics=disable_passed_test_metrics)
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
            test_result = TestAlert.create_test_alert_from_dict(
                **metadata,
                elementary_database_and_schema=utils.dbt.get_elementary_database_and_schema(self.dbt_runner),
                test_rows_sample=test_sample_data,
                test_runs=json.loads(test_invocations.json()) if test_invocations else {}
            )
            tests_results[test.model_unique_id].append(test_result.to_test_alert_api_dict())
        return tests_results

    def _get_dbt_models_and_sidebar(self) -> Tuple[Dict, Dict]:
        models_api = ModelsAPI(dbt_runner=self.dbt_runner)
        sidebar_api = SidebarAPI(dbt_runner=self.dbt_runner)

        models = models_api.get_models()
        sources = models_api.get_sources()
        exposures = models_api.get_exposures()

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
        data_monitoring_properties = {'data_monitoring_properties': self.execution_properties}
        return data_monitoring_properties

    def _get_report_file_path(self, generation_time: str, file_path: Optional[str] = None) -> str:
        if file_path:
            if file_path.endswith('.htm') or file_path.endswith('.html'):
                return os.path.abspath(file_path)
            raise ValueError('Report file path must end with .html')
        return os.path.abspath(os.path.join(
            self.config.target_dir,
            f"elementary - {generation_time} utc.html".replace(" ", "_").replace(":", "-")
        ))
