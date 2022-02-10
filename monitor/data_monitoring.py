import os
from typing import Any
import snowflake.connector.errors
from exceptions.exceptions import ConfigError
from monitor.alerts import Alert
from monitor.dbt_runner import DbtRunner
from config.config import Config
from utils.dbt import extract_credentials_and_data_from_profiles, get_profile_name_from_dbt_project, \
    get_snowflake_client
from utils.log import get_logger

logger = get_logger(__name__)
FILE_DIR = os.path.dirname(__file__)


class DataMonitoring(object):
    DBT_PACKAGE_NAME = 'elementary_data_reliability'
    DBT_PROJECT_PATH = os.path.join(FILE_DIR, 'dbt_project')
    # Compatibility for previous dbt versions
    DBT_PROJECT_MODULES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_modules', DBT_PACKAGE_NAME)
    DBT_PROJECT_PACKAGES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_packages', DBT_PACKAGE_NAME)
    DBT_PROJECT_SEEDS_PATH = os.path.join(DBT_PROJECT_PATH, 'data')

    MONITORING_CONFIGURATION = 'monitoring_configuration'

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
            snowflake_conn = get_snowflake_client(config.credentials, server_side_binding=False)
            return SnowflakeDataMonitoring(config, snowflake_conn)
        else:
            raise ConfigError("Unsupported platform")

    def _dbt_package_exists(self) -> bool:
        return os.path.exists(self.DBT_PROJECT_PACKAGES_PATH) or os.path.exists(self.DBT_PROJECT_MODULES_PATH)

    def _upload_configuration(self) -> bool:
        target_csv_dir = self.DBT_PROJECT_SEEDS_PATH
        if not os.path.exists(target_csv_dir):
            os.makedirs(target_csv_dir)

        monitoring_config_csv_path = os.path.join(target_csv_dir, f'{self.MONITORING_CONFIGURATION}.csv')
        csv_row_count = self.config.monitoring_configuration_in_dbt_sources_to_csv(monitoring_config_csv_path)
        self.execution_properties['configuration_csv_row_count'] = csv_row_count

        return self.dbt_runner.seed()

    def _run_query(self, query: str, params: tuple = None) -> list:
        pass

    def monitoring_configuration_exists(self) -> bool:
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
                alert.send_to_slack(slack_webhook)
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

    def _upload_monitoring_configuration_if_needed(self, reload_monitoring_configuration: bool):
        monitoring_configuration_exists = self.monitoring_configuration_exists()
        self.execution_properties['monitoring_configuration_exists'] = monitoring_configuration_exists
        self.execution_properties['reload_monitoring_configuration'] = reload_monitoring_configuration
        if not monitoring_configuration_exists or reload_monitoring_configuration:
            logger.info("Uploading monitoring configuration")
            config_updated = self._upload_configuration()
            self.execution_properties['config_uploaded'] = config_updated
            if not config_updated:
                logger.info('Could not upload monitoring configuration')
                return

    def _send_alerts(self):
        alerts = self._query_alerts()
        alert_count = len(alerts)
        self.execution_properties['alert_count'] = alert_count
        if alert_count > 0:
            self._send_to_slack(alerts)

    def run(self, reload_monitoring_configuration: bool = False, force_update_dbt_package: bool = False,
            dbt_full_refresh: bool = False) -> None:

        self._download_dbt_package_if_needed(force_update_dbt_package)
        self._upload_monitoring_configuration_if_needed(reload_monitoring_configuration)

        logger.info("Running dbt snapshots to detect changes in schemas")
        success = self.dbt_runner.snapshot()
        self.execution_properties['snapshot_success'] = success
        if not success:
            logger.info('Could not run dbt snapshot successfully')
            return

        logger.info("Running edr internal dbt package")
        success = self.dbt_runner.run(model=self.DBT_PACKAGE_NAME, full_refresh=dbt_full_refresh)
        self.execution_properties['run_success'] = success
        if not success:
            logger.info('Could not run dbt run successfully')
            return

        self._send_alerts()

    def properties(self):
        data_monitoring_properties = {'data_monitoring_properties': self.execution_properties}
        return data_monitoring_properties


class SnowflakeDataMonitoring(DataMonitoring):

    COUNT_ROWS_QUERY = """
    SELECT count(*) FROM IDENTIFIER(%s)
    """

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

    def _count_rows_in_table(self, table_name) -> int:
        try:
            results = self._run_query(self.COUNT_ROWS_QUERY,
                                      (table_name,))
            if len(results) == 1:
                return results[0][0]
        except snowflake.connector.errors.ProgrammingError:
            pass

        return 0

    def monitoring_configuration_exists(self) -> bool:
        if self._count_rows_in_table(self.MONITORING_CONFIGURATION) > 0:
            return True

        return False


