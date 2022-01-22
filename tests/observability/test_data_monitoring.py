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
def snowflake_con():
    snowflake_con = mock.create_autospec(SnowflakeConnection)
    snowflake_cur = mock.create_autospec(SnowflakeCursor)
    snowflake_con.cursor.return_value = snowflake_cur
    return snowflake_con


@pytest.fixture
def config():
    config_mock = mock.create_autospec(Config)
    config_mock.profiles_dir_path = 'bla'
    return config_mock


def test_data_monitoring_run_monitoring_config_does_not_exist(config, snowflake_con):
    config.get_sources.return_value = SOURCES['sources']

    # Scenario 1 - configuration doesn't exist yet
    snowflake_con.cursor.return_value.fetchall.return_value = []

    reference = SnowflakeDataMonitoring(config, snowflake_con)
    reference.dbt_runner = mock.create_autospec(DbtRunner)

    shutil.rmtree(reference.DBT_PROJECT_SEEDS_PATH)
    reference.run()
    assert os.path.exists(reference.DBT_PROJECT_SEEDS_PATH)
    schema_config_csv_path = os.path.join(reference.DBT_PROJECT_SEEDS_PATH,
                                          f'{reference.MONITORING_SCHEMAS_CONFIGURATION}.csv')
    assert os.path.exists(schema_config_csv_path)
    assert os.path.exists(os.path.join(reference.DBT_PROJECT_SEEDS_PATH,
                                       f'{reference.MONITORING_TABLES_CONFIGURATION}.csv'))
    assert os.path.exists(os.path.join(reference.DBT_PROJECT_SEEDS_PATH,
                                       f'{reference.MONITORING_COLUMNS_CONFIGURATION}.csv'))
    schema_configuration_csv_lines = read_csv(schema_config_csv_path)
    # Sanity check on the created configuration CSVs
    assert len(schema_configuration_csv_lines) == len(SOURCES['sources'])
    assert schema_configuration_csv_lines[0]['database_name'] == SOURCES['sources'][0]['database']
    assert schema_configuration_csv_lines[0]['schema_name'] == SOURCES['sources'][0]['name']
    reference.dbt_runner.seed.assert_called()


