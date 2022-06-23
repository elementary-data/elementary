import os
from monitor.test_result import TestResult
from monitor.dbt_runner import DbtRunner
from config.config import Config
from clients.slack.slack_client import SlackClient
from clients.slack.schema import SlackMessageSchema
from utils.log import get_logger
from utils.time import get_now_utc_str
from utils.json_utils import try_load_json
import json
from alive_progress import alive_it
from typing import List, Optional
import pkg_resources
import webbrowser

logger = get_logger(__name__)
FILE_DIR = os.path.dirname(__file__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


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
        self.slack_client = SlackClient.init(token=slack_token, webhook=slack_webhook)
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

    def _query_alerts(self, days_back: int) -> list:
        results = self.dbt_runner.run_operation(macro_name='get_new_alerts', macro_args={'days_back': days_back})
        test_result_alerts = []
        if results:
            test_result_alert_dicts = json.loads(results[0])
            self.execution_properties['alert_rows'] = len(test_result_alert_dicts)
            for test_result_alert_dict in test_result_alert_dicts:
                test_result_object = TestResult.create_test_result_from_dict(
                    test_result_dict=test_result_alert_dict,
                )
                if test_result_object:
                    test_result_alerts.append(test_result_object)
                else:
                    self.success = False

        return test_result_alerts

    def _send_to_slack(self, test_result_alerts: List[TestResult]) -> None:
        sent_alerts = []
        alerts_with_progress_bar = alive_it(test_result_alerts, title="Sending alerts")
        for alert in alerts_with_progress_bar:
            alert_slack_message: SlackMessageSchema = alert.generate_slack_message(is_slack_workflow=self.config.is_slack_workflow)
            sent_successfully = self.slack_client.send_message(
                channel_name=self.slack_channel_name,
                message=alert_slack_message
            )
            if sent_successfully:
                sent_alerts.append(alert.id)
            else:
                logger.info(f"Could not sent the alert - {alert.id}")
                self.success = False
        
        sent_alert_count = len(sent_alerts)
        self.execution_properties['sent_alert_count'] = sent_alert_count
        if sent_alert_count > 0:
            self._update_sent_alerts(sent_alerts)

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
        alerts = self._query_alerts(days_back)
        alert_count = len(alerts)
        self.execution_properties['alert_count'] = alert_count
        if alert_count > 0:
            self._send_to_slack(alerts)

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

    def generate_report(self) -> bool:
        elementary_output = {}
        elementary_output['creation_time'] = get_now_utc_str()
        test_results, test_result_totals = self._get_test_results_and_totals()
        models, dbt_sidebar = self._get_dbt_models_and_sidebar()
        elementary_output['models'] = models
        elementary_output['dbt_sidebar'] = dbt_sidebar
        elementary_output['test_results'] = test_results
        elementary_output['totals'] = test_result_totals

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
            elementary_html_file_path = os.path.join(self.config.target_dir, 'elementary.html')
            with open(elementary_html_file_path, 'w') as elementary_output_html_file:
                elementary_output_html_file.write(elementary_output_html)
            with open(os.path.join(self.config.target_dir, 'elementary_output.json'), 'w') as \
                    elementary_output_json_file:
                elementary_output_json_file.write(elementary_output_str)

            elementary_html_file_path = 'file://' + elementary_html_file_path
            webbrowser.open_new_tab(elementary_html_file_path)
            self.execution_properties['report_end'] = True
            self.execution_properties['success'] = self.success
            return self.success
    
    def send_report(self) -> bool:
        elementary_html_file_path = os.path.join(self.config.target_dir, 'elementary.html')
        if os.path.exists(elementary_html_file_path):
            file_uploaded_succesfully = self.slack_client.upload_file(
                channel_name=self.slack_channel_name,
                file_path=elementary_html_file_path,
                message=SlackMessageSchema(text="Elementary monitoring report")
            )
            if not file_uploaded_succesfully:
                self.success = False
        else:
            logger.error('Could not send "Elementary monitor report" because it is not exists.')
            self.success = False
        return self.success

    def _get_test_results_and_totals(self):
        results = self.dbt_runner.run_operation(macro_name='get_test_results')
        test_results_api_dict = {}
        test_result_totals_api_dict = {}
        if results:
            test_result_dicts = json.loads(results[0])
            for test_result_dict in test_result_dicts:
                days_diff = test_result_dict.pop('days_diff')
                test_result_object = TestResult.create_test_result_from_dict(test_result_dict)
                if test_result_object:
                    model_unique_id = test_result_object.model_unique_id
                    if model_unique_id in test_results_api_dict:
                        test_results_api_dict[model_unique_id].append(test_result_object.to_test_result_api_dict())
                    else:
                        test_results_api_dict[model_unique_id] = [test_result_object.to_test_result_api_dict()]

                    self._update_test_results_totals(test_result_totals_api_dict, model_unique_id, days_diff,
                                                     test_result_object.status)
                else:
                    self.success = False

            self.execution_properties['test_results'] = len(test_result_dicts)
        return test_results_api_dict, test_result_totals_api_dict

    def _update_test_results_totals(self, totals_dict, model_unique_id, days_diff, status):
        if model_unique_id not in totals_dict:
            totals_dict[model_unique_id] = {'1d': {'errors': 0, 'warnings': 0, 'resolved': 0, 'passed': 0},
                                            '7d': {'errors': 0, 'warnings': 0, 'resolved': 0, 'passed': 0},
                                            '30d': {'errors': 0, 'warnings': 0, 'resolved': 0, 'passed': 0}}
        total_keys = []
        if days_diff < 1:
            total_keys.append('1d')
        if days_diff < 7:
            total_keys.append('7d')
        if days_diff < 30:
            total_keys.append('30d')

        if status == 'warn':
            totals_status = 'warnings'
        elif status == 'error' or status == 'fail':
            totals_status = 'errors'
        elif status == 'pass':
            totals_status = 'passed'
        else:
            totals_status = None

        if totals_status is not None:
            for key in total_keys:
                totals_dict[model_unique_id][key][totals_status] += 1

    def _get_dbt_models_and_sidebar(self):
        models = {}
        dbt_sidebar = {}
        results = self.dbt_runner.run_operation(macro_name='get_models')
        if results:
            model_dicts = json.loads(results[0])
            for model_dict in model_dicts:
                model_unique_id = model_dict.get('unique_id')
                self._normalize_dbt_model_dict(model_dict)
                models[model_unique_id] = model_dict
                self._update_dbt_sidebar(
                    dbt_sidebar=dbt_sidebar,
                    model_unique_id=model_unique_id,
                    model_full_path=model_dict.get('normalized_full_path')
                )

        results = self.dbt_runner.run_operation(macro_name='get_sources')
        if results:
            source_dicts = json.loads(results[0])
            for source_dict in source_dicts:
                source_unique_id = source_dict.get('unique_id')
                self._normalize_dbt_model_dict(source_dict, is_source=True)
                models[source_unique_id] = source_dict
                self._update_dbt_sidebar(
                    dbt_sidebar=dbt_sidebar,
                    model_unique_id=source_unique_id,
                    model_full_path=source_dict.get('normalized_full_path')
                )
        return models, dbt_sidebar

    @staticmethod
    def _update_dbt_sidebar(dbt_sidebar: dict, model_unique_id: str, model_full_path: str) -> None:
        if model_unique_id is None or model_full_path is None:
            return
        model_full_path_split = model_full_path.split(os.path.sep)
        for part in model_full_path_split:
            if part.endswith('.sql'):
                if 'files' in dbt_sidebar:
                    if model_unique_id not in dbt_sidebar['files']:
                        dbt_sidebar['files'].append(model_unique_id)
                else:
                    dbt_sidebar['files'] = [model_unique_id]
            else:
                if part not in dbt_sidebar:
                    dbt_sidebar[part] = {}
                dbt_sidebar = dbt_sidebar[part]

    @staticmethod
    def _normalize_dbt_model_dict(model: dict, is_source: bool = False) -> None:
        owners = model.get('owners')
        if owners:
            loaded_owners = try_load_json(owners)
            if loaded_owners:
                owners = loaded_owners
            else:
                owners = [owners]
        tags = model.get('tags')
        if tags:
            loaded_tags = try_load_json(tags)
            if loaded_tags:
                tags = loaded_tags
            else:
                tags = [tags]
        model['owners'] = owners
        model['tags'] = tags
        model_name = model.get('name')
        model['model_name'] = model_name
        model['normalized_full_path'] = DataMonitoring._normalize_model_path(
            model_path=model.get('full_path'),
            model_package_name=model.get('package_name'),
            is_source=is_source
        )
    
    @staticmethod
    def _normalize_model_path(model_path: str, model_package_name: Optional[str] = None, is_source: bool = False) -> str:
        splited_model_path = model_path.split(os.path.sep)
        model_file_name = splited_model_path[-1]

        # If source, change models directory into sources and file extension from .yml to .sql
        if is_source:
            if splited_model_path[0] == "models":
                splited_model_path[0] = "sources"
            if model_file_name.endswith(YAML_FILE_EXTENSION):
                head, _sep, tail = model_file_name.rpartition(YAML_FILE_EXTENSION)
                splited_model_path[-1] = head + SQL_FILE_EXTENSION + tail
        
        # Add package name to model path
        if model_package_name:
            splited_model_path.insert(0, model_package_name)
        
        return os.path.sep.join(splited_model_path)

    def properties(self):
        data_monitoring_properties = {'data_monitoring_properties': self.execution_properties}
        return data_monitoring_properties
