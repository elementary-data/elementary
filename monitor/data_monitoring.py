import os
from monitor.alerts import Alert
from monitor.dbt_runner import DbtRunner
from config.config import Config
from utils.log import get_logger
import json
from alive_progress import alive_it
from typing import Union

logger = get_logger(__name__)
FILE_DIR = os.path.dirname(__file__)


class DataMonitoring(object):
    DBT_PACKAGE_NAME = 'elementary'
    DBT_PROJECT_PATH = os.path.join(FILE_DIR, 'dbt_project')
    DBT_PROJECT_MODELS_PATH = os.path.join(FILE_DIR, 'dbt_project', 'models')
    # Compatibility for previous dbt versions
    DBT_PROJECT_MODULES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_modules', DBT_PACKAGE_NAME)
    DBT_PROJECT_PACKAGES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_packages', DBT_PACKAGE_NAME)

    def __init__(self, config: Config, days_back: int, slack_webhook: Union[str, None]) -> None:
        self.config = config
        self.dbt_runner = DbtRunner(self.DBT_PROJECT_PATH, self.config.profiles_dir)
        self.execution_properties = {}
        self.days_back = days_back
        self.slack_webhook = slack_webhook or self.config.slack_notification_webhook

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

    def _query_alerts(self) -> list:
        json_alert_rows = self.dbt_runner.run_operation(macro_name='get_new_alerts',
                                                        macro_args={'days_back': self.days_back})
        self.execution_properties['alert_rows'] = len(json_alert_rows)
        alerts = []
        for json_alert_row in json_alert_rows:
            alert_row = json.loads(json_alert_row)
            alerts.append(Alert.create_alert_from_row(alert_row))
        return alerts

    def _send_to_slack(self, alerts: [Alert]) -> None:
        if self.slack_webhook is not None:
            sent_alerts = []
            alerts_with_progress_bar = alive_it(alerts, title="Sending alerts")
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

    def _send_alerts(self):
        alerts = self._query_alerts()
        alert_count = len(alerts)
        self.execution_properties['alert_count'] = alert_count
        if alert_count > 0:
            self._send_to_slack(alerts)

    def _read_configuration_to_sources_file(self) -> bool:
        logger.info("Reading configuration and writing to sources.yml")
        sources_yml = self.dbt_runner.run_operation(macro_name='read_configuration_to_sources_yml')
        if sources_yml is not None:
            if not os.path.exists(self.DBT_PROJECT_MODELS_PATH):
                os.makedirs(self.DBT_PROJECT_MODELS_PATH)
            sources_file_path = os.path.join(self.DBT_PROJECT_MODELS_PATH, 'sources.yml')
            with open(sources_file_path, 'w') as sources_file:
                sources_file.write(sources_yml)
            return True
        return False

    def run(self, force_update_dbt_package: bool = False, dbt_full_refresh: bool = False,
            alerts_only: bool = True) -> None:

        self._download_dbt_package_if_needed(force_update_dbt_package)

        if not alerts_only:
            success = self._read_configuration_to_sources_file()
            if not success:
                logger.info('Could not create configuration successfully')
                return

            logger.info("Running internal dbt run to create metadata and process configuration")
            success = self.dbt_runner.run(full_refresh=dbt_full_refresh)
            self.execution_properties['run_success'] = success
            if not success:
                logger.info('Could not run dbt run successfully')
                return

            logger.info("Running internal dbt data tests to collect metrics and calculate anomalies")
            success = self.dbt_runner.test(select="tag:elementary")
            self.execution_properties['test_success'] = success

        logger.info("Running internal dbt run to aggregate alerts")
        success = self.dbt_runner.run(models='alerts', full_refresh=dbt_full_refresh)
        self.execution_properties['alerts_run_success'] = success
        if not success:
            logger.info('Could not aggregate alerts successfully')
            return

        self._send_alerts()

    def properties(self):
        data_monitoring_properties = {'data_monitoring_properties': self.execution_properties}
        return data_monitoring_properties



