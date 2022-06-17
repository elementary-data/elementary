import os
from monitor.test_result import TestResult
from monitor.dbt_runner import DbtRunner
from config.config import Config
from utils.log import get_logger
from utils.time import get_now_utc_str
from utils.json_utils import try_load_json
import json
from alive_progress import alive_it
from typing import Optional
import pkg_resources
import webbrowser

logger = get_logger(__name__)
FILE_DIR = os.path.dirname(__file__)


class DataMonitoring(object):
    DBT_PACKAGE_NAME = 'elementary'
    DBT_PROJECT_PATH = os.path.join(FILE_DIR, 'dbt_project')
    DBT_PROJECT_MODELS_PATH = os.path.join(FILE_DIR, 'dbt_project', 'models')
    # Compatibility for previous dbt versions
    DBT_PROJECT_MODULES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_modules', DBT_PACKAGE_NAME)
    DBT_PROJECT_PACKAGES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_packages', DBT_PACKAGE_NAME)

    def __init__(self, config: Config, force_update_dbt_package: bool = False,
                 slack_webhook: Optional[str] = None) -> None:
        self.config = config
        self.dbt_runner = DbtRunner(self.DBT_PROJECT_PATH, self.config.profiles_dir, self.config.profile_target)
        self.execution_properties = {}
        self.slack_webhook = slack_webhook or self.config.slack_notification_webhook
        self._download_dbt_package_if_needed(force_update_dbt_package)

    def _dbt_package_exists(self) -> bool:
        return os.path.exists(self.DBT_PROJECT_PACKAGES_PATH) or os.path.exists(self.DBT_PROJECT_MODULES_PATH)

    @staticmethod
    def _split_list_to_chunks(items: list, chunk_size: int = 50) -> [list]:
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
                test_result_alerts.append(TestResult.create_test_result_from_dict(test_result_alert_dict))
        return test_result_alerts

    def _send_to_slack(self, test_result_alerts: [TestResult]) -> None:
        if self.slack_webhook is not None:
            sent_alerts = []
            alerts_with_progress_bar = alive_it(test_result_alerts, title="Sending alerts")
            for alert in alerts_with_progress_bar:
                alert.send_to_slack(self.slack_webhook, self.config.is_slack_workflow)
                sent_alerts.append(alert.id)

            sent_alert_count = len(sent_alerts)
            self.execution_properties['sent_alert_count'] = sent_alert_count
            if sent_alert_count > 0:
                self._update_sent_alerts(sent_alerts)
        else:
            logger.info("Alerts found but slack webhook is not configured (see documentation on how to configure "
                        "a slack webhook)")

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
                return

    def _send_alerts(self, days_back: int):
        alerts = self._query_alerts(days_back)
        alert_count = len(alerts)
        self.execution_properties['alert_count'] = alert_count
        if alert_count > 0:
            self._send_to_slack(alerts)

    def run(self, days_back: int, dbt_full_refresh: bool = False) -> None:

        logger.info("Running internal dbt run to aggregate alerts")
        success = self.dbt_runner.run(models='alerts', full_refresh=dbt_full_refresh)
        self.execution_properties['alerts_run_success'] = success
        if not success:
            logger.info('Could not aggregate alerts successfully')
            return

        self._send_alerts(days_back)

    def generate_report(self):
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
            self.execution_properties['report_success'] = True

    def _get_test_results_and_totals(self):
        results = self.dbt_runner.run_operation(macro_name='get_test_results')
        test_results_api_dict = {}
        test_result_totals_api_dict = {}
        if results:
            test_result_dicts = json.loads(results[0])
            for test_result_dict in test_result_dicts:
                days_diff = test_result_dict.pop('days_diff')
                test_result_object = TestResult.create_test_result_from_dict(test_result_dict)
                model_unique_id = test_result_object.model_unique_id
                if model_unique_id in test_results_api_dict:
                    test_results_api_dict[model_unique_id].append(test_result_object.to_test_result_api_dict())
                else:
                    test_results_api_dict[model_unique_id] = [test_result_object.to_test_result_api_dict()]

                self._update_test_results_totals(test_result_totals_api_dict, model_unique_id, days_diff,
                                                 test_result_object.status)

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
                self._update_dbt_sidebar(dbt_sidebar, model_unique_id, model_dict.get('normalized_full_path'),
                                         model_dict.get('package_name'))

        results = self.dbt_runner.run_operation(macro_name='get_sources')
        if results:
            source_dicts = json.loads(results[0])
            for source_dict in source_dicts:
                source_unique_id = source_dict.get('unique_id')
                self._normalize_dbt_model_dict(source_dict, is_source=True)
                models[source_unique_id] = source_dict
                self._update_dbt_sidebar(dbt_sidebar, source_unique_id, source_dict.get('normalized_full_path'),
                                         source_dict.get('package_name'))
        return models, dbt_sidebar

    @staticmethod
    def _update_dbt_sidebar(dbt_sidebar: dict, model_unique_id: str, model_full_path: str,
                            model_package_name: Optional[str]) -> None:
        if model_unique_id is None or model_full_path is None:
            return
        model_full_path_split = model_full_path.split(os.path.sep)
        if model_package_name and model_full_path_split:
            model_full_path_split.insert(0, model_package_name)
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
        model_full_path = model.get('full_path')
        model_full_path_split = model_full_path.split(os.path.sep)
        file_name = None
        if model_full_path and model_full_path_split:
            file_name = model_full_path_split[-1]
        model['file_name'] = file_name
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
        model['normalized_full_path'] = model_full_path
        if is_source:
            if model_full_path_split[0] == 'models':
                model_full_path_split[0] = 'sources'
            if model_full_path_split[-1].endswith('.yml'):
                model_full_path_split[-1] = model_name + '.sql'
            model['normalized_full_path'] = os.path.sep.join(model_full_path_split)

    def properties(self):
        data_monitoring_properties = {'data_monitoring_properties': self.execution_properties}
        return data_monitoring_properties



