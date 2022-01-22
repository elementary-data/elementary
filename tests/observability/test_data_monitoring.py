import pytest
import csv
import os
import shutil
from unittest import mock
from snowflake.connector.connection import SnowflakeConnection, SnowflakeCursor
from observability.dbt_runner import DbtRunner
from observability.config import Config
from observability.data_monitoring import SnowflakeDataMonitoring, DataMonitoring

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


@pytest.fixture
def snowflake_data_monitoring_empty_config_in_db(config_mock, snowflake_con_mock, dbt_runner_mock):
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
    snowflake_data_mon._dbt_package_exists = lambda: True
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
    assert len(schema_configuration_csv_lines) == len(SOURCES['sources'])
    assert schema_configuration_csv_lines[0]['database_name'] == SOURCES['sources'][0]['database']
    assert schema_configuration_csv_lines[0]['schema_name'] == SOURCES['sources'][0]['name']


def delete_configuration(data_monitoring):
    if os.path.exists(data_monitoring.DBT_PROJECT_SEEDS_PATH):
        shutil.rmtree(data_monitoring.DBT_PROJECT_SEEDS_PATH)


def delete_dbt_package(data_monitoring):
    if os.path.exists(data_monitoring.DBT_PROJECT_MODULES_PATH):
        shutil.rmtree(data_monitoring.DBT_PROJECT_MODULES_PATH)

    if os.path.exists(data_monitoring.DBT_PROJECT_PACKAGES_PATH):
        shutil.rmtree(data_monitoring.DBT_PROJECT_PACKAGES_PATH)


@pytest.mark.parametrize("full_refresh, update_dbt_package, reload_config", [
    (True, True, False),
    (True, False, False),
    (True, True, True),
    (True, False, True),
    (False, True, False),
    (False, False, False),
    (False, True, True),
    (False, False, True),
])
def test_data_monitoring_run_config_does_not_exist(full_refresh, update_dbt_package, reload_config,
                                                   snowflake_data_monitoring_empty_config_in_db):
    snowflake_data_monitoring = snowflake_data_monitoring_empty_config_in_db
    delete_configuration(snowflake_data_monitoring)
    delete_dbt_package(snowflake_data_monitoring)
    dbt_runner_mock = snowflake_data_monitoring.dbt_runner

    # The function we test
    snowflake_data_monitoring.run(dbt_full_refresh=full_refresh, force_update_dbt_packages=update_dbt_package,
                                  reload_monitoring_configuration=reload_config)

    if update_dbt_package:
        dbt_runner_mock.deps.assert_called()
    else:
        dbt_runner_mock.deps.assert_not_called()

    # Validate configuration exists in the dbt_project seed path
    assert_configuration_exists(snowflake_data_monitoring)
    dbt_runner_mock.seed.assert_called()

    # Validate that snapshot and run were called as well
    dbt_runner_mock.snapshot.assert_called()
    dbt_runner_mock.run.assert_called()


@pytest.mark.parametrize("full_refresh, update_dbt_package, reload_config", [
    (True, True, False),
    (True, False, False),
    (True, True, True),
    (True, False, True),
    (False, True, False),
    (False, False, False),
    (False, True, True),
    (False, False, True),
])
def test_data_monitoring_run(full_refresh, update_dbt_package, reload_config, snowflake_data_monitoring):
    delete_dbt_package(snowflake_data_monitoring)
    delete_configuration(snowflake_data_monitoring)
    dbt_runner_mock = snowflake_data_monitoring.dbt_runner

    # The function we test
    snowflake_data_monitoring.run(dbt_full_refresh=full_refresh, force_update_dbt_packages=update_dbt_package,
                                  reload_monitoring_configuration=reload_config)

    if update_dbt_package:
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
