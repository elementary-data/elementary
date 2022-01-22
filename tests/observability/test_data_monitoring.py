import pytest
import csv
import os
import shutil
from unittest import mock
from snowflake.connector.connection import SnowflakeConnection, SnowflakeCursor
from observability.dbt_runner import DbtRunner
from observability.config import Config
from observability.data_monitoring import SnowflakeDataMonitoring

SOURCES = {'sources': [{'name': 'unit_tests',
                        'database': 'elementary_tests',
                        'meta': {'observability': {'alert_on_schema_changes': 'true'}}}]}


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
    config_mock.get_sources.return_value = SOURCES['sources']
    return config_mock


@pytest.fixture
def dbt_runner_mock():
    return mock.create_autospec(DbtRunner)


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
    assert len(schema_configuration_csv_lines) == len(SOURCES['sources'])
    assert schema_configuration_csv_lines[0]['database_name'] == SOURCES['sources'][0]['database']
    assert schema_configuration_csv_lines[0]['schema_name'] == SOURCES['sources'][0]['name']


def delete_configuration(data_monitoring):
    if os.path.exists(data_monitoring.DBT_PROJECT_SEEDS_PATH):
        shutil.rmtree(data_monitoring.DBT_PROJECT_SEEDS_PATH)


def test_data_monitoring_run_monitoring_config_does_not_exist(config_mock, snowflake_con_mock, dbt_runner_mock):
    # Scenario 1 - configuration doesn't exist yet
    snowflake_cursor_context_manager_return_value = snowflake_con_mock.cursor.return_value.__enter__.return_value
    snowflake_cursor_context_manager_return_value.fetchall.return_value = []

    reference = SnowflakeDataMonitoring(config_mock, snowflake_con_mock)
    reference.dbt_runner = dbt_runner_mock

    delete_configuration(reference)
    reference.run()
    assert_configuration_exists(reference)
    dbt_runner_mock.seed.assert_called()


def test_data_monitoring_run_with_reload_config_flag_true(config_mock, snowflake_con_mock, dbt_runner_mock):
    # Scenario 2 - configuration exists but reload flag given
    snowflake_cursor_context_manager_return_value = snowflake_con_mock.cursor.return_value.__enter__.return_value

    def execute_query_side_effect(*args, **kwargs):
        if 'count(*)' in args[0].lower():
            snowflake_cursor_context_manager_return_value.fetchall.return_value = [[1]]
        else:
            snowflake_cursor_context_manager_return_value.fetchall.return_value = []

    snowflake_cursor_context_manager_return_value.execute.side_effect = execute_query_side_effect

    reference = SnowflakeDataMonitoring(config_mock, snowflake_con_mock)
    reference.dbt_runner = dbt_runner_mock
    delete_configuration(reference)
    reference.run(reload_monitoring_configuration=True)
    assert_configuration_exists(reference)
    dbt_runner_mock.seed.assert_called()


