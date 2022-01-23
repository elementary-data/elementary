import pytest
import csv
import os
import shutil
import json
from unittest import mock
from datetime import datetime

from observability.alerts import Alert
from snowflake.connector.connection import SnowflakeConnection, SnowflakeCursor
from observability.dbt_runner import DbtRunner
from observability.config import Config
from observability.data_monitoring import SnowflakeDataMonitoring, DataMonitoring

SOURCES = [{'sources':
               [{'name': 'unit_tests',
                 'database': 'elementary_tests',
                 'meta': {'observability': {'alert_on_schema_changes': 'true'}},
                 'tables':
                     [{'name': 'groups',
                       'meta': {'observability': {'alert_on_schema_changes': 'true'}},
                       'columns':
                           [{'name': 'group_a',
                             'meta': {'observability': {'alert_on_schema_changes': 'true'}}}]}]},
                {'schema': 'unit_tests_2',
                 'database': 'elementary_tests_2',
                 'meta': {'observability': {'alert_on_schema_changes': 'true'}},
                 'tables':
                     [{'identifier': 'groups_2',
                       'meta': {'observability': {'alert_on_schema_changes': 'true'}}}]}]}]


WEBHOOK_URL = 'https://my_webhook'
ALERT_ROW = ['123', datetime.now(), 'db', 'sc', 't1', 'c1', 'schema_change', 'column_added', 'Column was added']


def read_csv(csv_path):
    csv_lines = []
    with open(csv_path, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            csv_lines.append(row)
    return csv_lines


@pytest.fixture
def snowflake_con_mock():
    snowflake_con = mock.create_autospec(SnowflakeConnection)
    snowflake_cur = mock.create_autospec(SnowflakeCursor)
    # Cursor is a context manager so we need to mock the function __enter__
    snowflake_con.cursor.return_value.__enter__.return_value = snowflake_cur
    return snowflake_con


@pytest.fixture
def config_mock():
    config_mock = mock.create_autospec(Config)
    config_mock.profiles_dir_path = 'profiles_dir_path'
    config_mock.get_dbt_project_sources.return_value = SOURCES
    config_mock.get_slack_notification_webhook.return_value = WEBHOOK_URL
    return config_mock


@pytest.fixture
def dbt_runner_mock():
    return mock.create_autospec(DbtRunner)


@pytest.fixture
def snowflake_data_monitoring_with_empty_config_in_db(config_mock, snowflake_con_mock, dbt_runner_mock):
    # This mock cursor returns empty list to simulate empty configuration
    snowflake_cursor_context_manager_return_value = snowflake_con_mock.cursor.return_value.__enter__.return_value
    snowflake_cursor_context_manager_return_value.fetchall.return_value = []

    snowflake_data_mon = SnowflakeDataMonitoring(config_mock, snowflake_con_mock)
    snowflake_data_mon.dbt_runner = dbt_runner_mock
    snowflake_data_mon._dbt_package_exists = lambda: True
    return snowflake_data_mon


@pytest.fixture
def snowflake_data_monitoring(config_mock, snowflake_con_mock, dbt_runner_mock):
    # This cursor mock will use the side effect to return non empty configuration
    snowflake_cursor_context_manager_return_value = snowflake_con_mock.cursor.return_value.__enter__.return_value

    def execute_query_side_effect(*args, **kwargs):
        if 'count(*)' in args[0].lower():
            snowflake_cursor_context_manager_return_value.fetchall.return_value = [[1]]
        else:
            snowflake_cursor_context_manager_return_value.fetchall.return_value = []

    snowflake_cursor_context_manager_return_value.execute.side_effect = execute_query_side_effect

    snowflake_data_mon = SnowflakeDataMonitoring(config_mock, snowflake_con_mock)
    snowflake_data_mon.dbt_runner = dbt_runner_mock
    return snowflake_data_mon


@pytest.fixture
def snowflake_data_monitoring_with_alerts_in_db(config_mock, snowflake_con_mock, dbt_runner_mock):
    snowflake_cursor_context_manager_return_value = snowflake_con_mock.cursor.return_value.__enter__.return_value

    def execute_query_side_effect(*args, **kwargs):
        if args[0] == DataMonitoring.SELECT_NEW_ALERTS_QUERY:
            snowflake_cursor_context_manager_return_value.fetchall.return_value = [ALERT_ROW]
        else:
            snowflake_cursor_context_manager_return_value.fetchall.return_value = []

    snowflake_cursor_context_manager_return_value.execute.side_effect = execute_query_side_effect

    snowflake_data_mon = SnowflakeDataMonitoring(config_mock, snowflake_con_mock)
    snowflake_data_mon.dbt_runner = dbt_runner_mock
    return snowflake_data_mon


def assert_configuration_exists(data_monitoring):
    assert os.path.exists(data_monitoring.DBT_PROJECT_SEEDS_PATH)
    schema_config_csv_path = os.path.join(data_monitoring.DBT_PROJECT_SEEDS_PATH,
                                          f'{data_monitoring.MONITORING_SCHEMAS_CONFIGURATION}.csv')
    assert os.path.exists(schema_config_csv_path)
    assert os.path.exists(os.path.join(data_monitoring.DBT_PROJECT_SEEDS_PATH,
                                       f'{data_monitoring.MONITORING_TABLES_CONFIGURATION}.csv'))
    assert os.path.exists(os.path.join(data_monitoring.DBT_PROJECT_SEEDS_PATH,
                                       f'{data_monitoring.MONITORING_COLUMNS_CONFIGURATION}.csv'))
    schema_configuration_csv_lines = read_csv(schema_config_csv_path)
    # Sanity check on the created configuration CSVs
    assert len(schema_configuration_csv_lines) == len(SOURCES[0]['sources'])
    assert schema_configuration_csv_lines[0]['database_name'] == SOURCES[0]['sources'][0]['database']
    assert schema_configuration_csv_lines[0]['schema_name'] == SOURCES[0]['sources'][0]['name']


def delete_configuration(data_monitoring):
    if os.path.exists(data_monitoring.DBT_PROJECT_SEEDS_PATH):
        shutil.rmtree(data_monitoring.DBT_PROJECT_SEEDS_PATH)


def delete_dbt_package(data_monitoring):
    if os.path.exists(data_monitoring.DBT_PROJECT_MODULES_PATH):
        shutil.rmtree(data_monitoring.DBT_PROJECT_MODULES_PATH)

    if os.path.exists(data_monitoring.DBT_PROJECT_PACKAGES_PATH):
        shutil.rmtree(data_monitoring.DBT_PROJECT_PACKAGES_PATH)


@pytest.mark.parametrize("full_refresh, update_dbt_package, reload_config, dbt_package_exists", [
    (True, True, False, False),
    (True, False, False, False),
    (True, True, True, False),
    (True, False, True, False),
    (True, True, False, True),
    (True, False, False, True),
    (True, True, True, True),
    (True, False, True, True),
    (False, True, False, False),
    (False, False, False, False),
    (False, True, True, False),
    (False, False, True, False),
    (False, True, False, True),
    (False, False, False, True),
    (False, True, True, True),
    (False, False, True, True),
])
def test_data_monitoring_run_config_does_not_exist(full_refresh, update_dbt_package, reload_config, dbt_package_exists,
                                                   snowflake_data_monitoring_with_empty_config_in_db):
    snowflake_data_monitoring = snowflake_data_monitoring_with_empty_config_in_db
    delete_configuration(snowflake_data_monitoring)
    delete_dbt_package(snowflake_data_monitoring)
    snowflake_data_monitoring._dbt_package_exists = lambda: dbt_package_exists
    dbt_runner_mock = snowflake_data_monitoring.dbt_runner

    # The test function
    snowflake_data_monitoring.run(dbt_full_refresh=full_refresh, force_update_dbt_packages=update_dbt_package,
                                  reload_monitoring_configuration=reload_config)

    if update_dbt_package or not dbt_package_exists:
        dbt_runner_mock.deps.assert_called()
    else:
        dbt_runner_mock.deps.assert_not_called()

    # Validate configuration exists in the dbt_project seed path
    assert_configuration_exists(snowflake_data_monitoring)
    dbt_runner_mock.seed.assert_called()

    # Validate that snapshot and run were called as well
    dbt_runner_mock.snapshot.assert_called()
    dbt_runner_mock.run.assert_called()


@pytest.mark.parametrize("full_refresh, update_dbt_package, reload_config, dbt_package_exists", [
    (True, True, False, False),
    (True, False, False, False),
    (True, True, True, False),
    (True, False, True, False),
    (True, True, False, True),
    (True, False, False, True),
    (True, True, True, True),
    (True, False, True, True),
    (False, True, False, False),
    (False, False, False, False),
    (False, True, True, False),
    (False, False, True, False),
    (False, True, False, True),
    (False, False, False, True),
    (False, True, True, True),
    (False, False, True, True),
])
def test_data_monitoring_run(full_refresh, update_dbt_package, reload_config, dbt_package_exists,
                             snowflake_data_monitoring):
    delete_dbt_package(snowflake_data_monitoring)
    delete_configuration(snowflake_data_monitoring)
    snowflake_data_monitoring._dbt_package_exists = lambda: dbt_package_exists
    dbt_runner_mock = snowflake_data_monitoring.dbt_runner

    # The test function
    snowflake_data_monitoring.run(dbt_full_refresh=full_refresh, force_update_dbt_packages=update_dbt_package,
                                  reload_monitoring_configuration=reload_config)

    if update_dbt_package or not dbt_package_exists:
        dbt_runner_mock.deps.assert_called()
    else:
        dbt_runner_mock.deps.assert_not_called()

    if reload_config:
        assert_configuration_exists(snowflake_data_monitoring)
        dbt_runner_mock.seed.assert_called()
    else:
        dbt_runner_mock.seed.assert_not_called()

    # Validate that dbt snapshot and dbt run were called as well
    dbt_runner_mock.snapshot.assert_called()
    dbt_runner_mock.run.assert_called_with(model=snowflake_data_monitoring.DBT_PACKAGE_NAME, full_refresh=full_refresh)


def test_data_monitoring_update_configuration_in_db(snowflake_data_monitoring):
    delete_configuration(snowflake_data_monitoring)

    # The test function
    snowflake_data_monitoring._update_configuration()

    schema_config_csv_path = os.path.join(snowflake_data_monitoring.DBT_PROJECT_SEEDS_PATH,
                                          f'{snowflake_data_monitoring.MONITORING_SCHEMAS_CONFIGURATION}.csv')
    tables_config_csv_path = os.path.join(snowflake_data_monitoring.DBT_PROJECT_SEEDS_PATH,
                                          f'{snowflake_data_monitoring.MONITORING_TABLES_CONFIGURATION}.csv')
    columns_config_csv_path = os.path.join(snowflake_data_monitoring.DBT_PROJECT_SEEDS_PATH,
                                           f'{snowflake_data_monitoring.MONITORING_COLUMNS_CONFIGURATION}.csv')

    schema_configuration_csv_lines = read_csv(schema_config_csv_path)
    assert schema_configuration_csv_lines[0]['database_name'] == SOURCES[0]['sources'][0]['database']
    assert schema_configuration_csv_lines[0]['schema_name'] == SOURCES[0]['sources'][0]['name']
    assert schema_configuration_csv_lines[0]['alert_on_schema_changes'].lower() == \
           SOURCES[0]['sources'][0]['meta']['observability']['alert_on_schema_changes'].lower()

    assert schema_configuration_csv_lines[1]['database_name'] == SOURCES[0]['sources'][1]['database']
    assert schema_configuration_csv_lines[1]['schema_name'] == SOURCES[0]['sources'][1]['schema']
    assert schema_configuration_csv_lines[1]['alert_on_schema_changes'].lower() == \
           SOURCES[0]['sources'][1]['meta']['observability']['alert_on_schema_changes'].lower()

    table_configuration_csv_lines = read_csv(tables_config_csv_path)
    assert table_configuration_csv_lines[0]['database_name'] == SOURCES[0]['sources'][0]['database']
    assert table_configuration_csv_lines[0]['schema_name'] == SOURCES[0]['sources'][0]['name']
    assert table_configuration_csv_lines[0]['table_name'] == SOURCES[0]['sources'][0]['tables'][0]['name']

    assert table_configuration_csv_lines[1]['database_name'] == SOURCES[0]['sources'][1]['database']
    assert table_configuration_csv_lines[1]['schema_name'] == SOURCES[0]['sources'][1]['schema']
    assert table_configuration_csv_lines[1]['table_name'] == SOURCES[0]['sources'][1]['tables'][0]['identifier']

    column_configuration_csv_lines = read_csv(columns_config_csv_path)
    assert column_configuration_csv_lines[0]['database_name'] == SOURCES[0]['sources'][0]['database']
    assert column_configuration_csv_lines[0]['schema_name'] == SOURCES[0]['sources'][0]['name']
    assert column_configuration_csv_lines[0]['table_name'] == SOURCES[0]['sources'][0]['tables'][0]['name']
    assert column_configuration_csv_lines[0]['column_name'] == \
           SOURCES[0]['sources'][0]['tables'][0]['columns'][0]['name']
    assert column_configuration_csv_lines[0]['alert_on_schema_changes'].lower() == \
        SOURCES[0]['sources'][0]['tables'][0]['columns'][0]['meta']['observability']['alert_on_schema_changes'].lower()


@mock.patch('requests.post')
def test_data_monitoring_send_alerts(requests_post_mock, snowflake_data_monitoring_with_alerts_in_db):
    snowflake_data_monitoring = snowflake_data_monitoring_with_alerts_in_db
    alert = Alert.create_alert_from_row(ALERT_ROW)
    # The test function
    snowflake_data_monitoring._send_alerts_to_slack()
    requests_post_mock.assert_called_once_with(url=WEBHOOK_URL, headers={'Content-type': 'application/json'},
                                               data=json.dumps(alert.to_slack_message()))
