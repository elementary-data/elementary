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

    def __init__(self, config: 'Config', db_connection: Any) -> None:
        self.config = config
        self.dbt_runner = DbtRunner(self.DBT_PROJECT_PATH, self.config.profiles_dir)
        self.db_connection = db_connection

    @staticmethod
    def create_data_monitoring(config_dir: str, profiles_dir: str) -> 'DataMonitoring':
        profile_name = get_profile_name_from_dbt_project(DataMonitoring.DBT_PROJECT_PATH)
        config = Config(config_dir, profiles_dir, profile_name)
        if config.platform == 'snowflake':
            snowflake_conn = get_snowflake_client(config.credentials, server_side_binding=False)
            return SnowflakeDataMonitoring(config, snowflake_conn)
        else:
            raise ConfigError("Unsupported platform")

    def _dbt_package_exists(self) -> bool:
        return os.path.exists(self.DBT_PROJECT_PACKAGES_PATH) or os.path.exists(self.DBT_PROJECT_MODULES_PATH)

    def _update_configuration(self) -> bool:
        target_csv_dir = self.DBT_PROJECT_SEEDS_PATH
        if not os.path.exists(target_csv_dir):
            os.makedirs(target_csv_dir)

        monitoring_config_csv_path = os.path.join(target_csv_dir, f'{self.MONITORING_CONFIGURATION}.csv')
        self.config.monitoring_configuration_in_dbt_sources_to_csv(monitoring_config_csv_path)

        return self.dbt_runner.seed()

    def _run_query(self, query: str, params: tuple = None) -> list:
        pass

    def monitoring_configuration_exists(self) -> bool:
        pass

    def _update_sent_alerts(self, alert_ids) -> None:
        results = self._run_query(self.UPDATE_SENT_ALERTS, (alert_ids,))
        logger.debug(f"Updated sent alerts -\n{str(results)}")

    def _send_alerts_to_slack(self) -> None:
        slack_webhook = self.config.get_slack_notification_webhook()
        if slack_webhook is not None:
            alert_rows = self._run_query(self.SELECT_NEW_ALERTS_QUERY)
            sent_alerts = []
            for alert_row in alert_rows:
                alert = Alert.create_alert_from_row(alert_row)
                alert.send_to_slack(slack_webhook)
                sent_alerts.append(alert.id)

            if len(sent_alerts) > 0:
                self._update_sent_alerts(sent_alerts)

    def run(self, force_update_dbt_packages: bool = False, reload_monitoring_configuration: bool = False,
            dbt_full_refresh: bool = False) -> None:
        if not self._dbt_package_exists() or force_update_dbt_packages:
            logger.info("Downloading edr internal dbt package")
            if not self.dbt_runner.deps():
                return

        if not self.monitoring_configuration_exists() or reload_monitoring_configuration:
            logger.info("Uploading monitoring configuration")
            if not self._update_configuration():
                return

        # Run elementary dbt package
        logger.info("Taking schema snapshots")
        if not self.dbt_runner.snapshot():
            return
        logger.info("Running edr internal dbt package")
        if not self.dbt_runner.run(model=self.DBT_PACKAGE_NAME, full_refresh=dbt_full_refresh):
            return

        self._send_alerts_to_slack()


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


