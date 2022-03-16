import os
from typing import Any
import snowflake.connector.errors
from exceptions.exceptions import ConfigError
from monitor.alerts import Alert
from monitor.dbt_runner import DbtRunner
from config.config import Config
from utils.dbt import get_snowflake_client
from utils.log import get_logger

logger = get_logger(__name__)
FILE_DIR = os.path.dirname(__file__)


class DataMonitoring(object):
    DBT_PACKAGE_NAME = 'elementary'
    DBT_PROJECT_PATH = os.path.join(FILE_DIR, 'dbt_project')
    DBT_PROJECT_MODELS_PATH = os.path.join(FILE_DIR, 'dbt_project', 'models')
    # Compatibility for previous dbt versions
    DBT_PROJECT_MODULES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_modules', DBT_PACKAGE_NAME)
    DBT_PROJECT_PACKAGES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_packages', DBT_PACKAGE_NAME)

    SELECT_NEW_ALERTS_QUERY = """
        SELECT alert_id, detected_at, database_name, schema_name, table_name, column_name, alert_type, sub_type, 
               alert_description
            FROM ALERTS
            WHERE alert_sent = FALSE;
    """

    UPDATE_SENT_ALERTS = """
        UPDATE ALERTS set alert_sent = TRUE
            WHERE alert_id IN (%s); 
    """

    COUNT_ROWS_QUERY = None

    def __init__(self, config: Config, db_connection: Any) -> None:
        self.config = config
        self.dbt_runner = DbtRunner(self.DBT_PROJECT_PATH, self.config.profiles_dir)
        self.db_connection = db_connection
        self.execution_properties = {}

    @staticmethod
    def create_data_monitoring(config: Config) -> 'DataMonitoring':
        if config.platform == 'snowflake':
            snowflake_conn = get_snowflake_client(config.credentials, server_side_binding=False,
                                                  custom_schema='elementary')
            return SnowflakeDataMonitoring(config, snowflake_conn)
        else:
            raise ConfigError("Unsupported platform")

    def _dbt_package_exists(self) -> bool:
        return os.path.exists(self.DBT_PROJECT_PACKAGES_PATH) or os.path.exists(self.DBT_PROJECT_MODULES_PATH)

    def _run_query(self, query: str, params: tuple = None) -> list:
        pass

    def _update_sent_alerts(self, alert_ids) -> None:
        results = self._run_query(self.UPDATE_SENT_ALERTS, (alert_ids,))
        logger.debug(f"Updated sent alerts -\n{str(results)}")

    def _query_alerts(self) -> list:
        alert_rows = self._run_query(self.SELECT_NEW_ALERTS_QUERY)
        self.execution_properties['alert_rows'] = len(alert_rows)
        alerts = []
        for alert_row in alert_rows:
            alerts.append(Alert.create_alert_from_row(alert_row))
        return alerts

    def _send_to_slack(self, alerts: [Alert]) -> None:
        slack_webhook = self.config.slack_notification_webhook
        if slack_webhook is not None:
            sent_alerts = []
            for alert in alerts:
                alert.send_to_slack(slack_webhook, self.config.is_slack_workflow)
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


class SnowflakeDataMonitoring(DataMonitoring):
    def __init__(self, config: 'Config', db_connection: Any):
        super().__init__(config, db_connection)

    def _run_query(self, query: str, params: tuple = None) -> list:
        with self.db_connection.cursor() as cursor:
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            results = cursor.fetchall()
            return results



