import pytest
import os
import csv
from utils.ordered_yaml import OrderedYaml
from monitor.config import Config

FILE_DIR = os.path.dirname(__file__)

CONFIG = {'slack_notification_webhook': 'test_slack_webhook', 'dbt_projects': [FILE_DIR]}

SOURCES = {'sources':
                [{'name': 'unit_tests',
                  'database': 'elementary_tests',
                  'meta': {'edr': {'schema_changes': 'true'}},
                  'tables':
                      [{'name': 'groups',
                        'meta': {'edr': {'schema_changes': 'true'}},
                        'columns':
                            [{'name': 'group_a',
                              'meta': {'edr': {'schema_changes': 'true'}}}]}]},
                 {'schema': 'unit_tests_2',
                  'database': 'elementary_tests_2',
                  'meta': {'edr': {'schema_changes': 'true'}},
                  'tables':
                      [{'identifier': 'groups_2',
                        'meta': {'edr': {'schema_changes': 'true'}}}]}]}

DBT_PROJECT = {'name': 'elementary',
               'version': '1.0.0',
               'config-version': 2,
               'profile': 'elementary',
               'source-paths': ["models"],
               'analysis-paths': ["analyses"],
               'test-paths': ["tests"],
               'data-paths': ["data"],
               'macro-paths': ["macros"],
               'snapshot-paths': ["snapshots"],
               'target-path': "target",
               'clean-targets': ["target", "dbt_packages", "dbt_modules"],
               'models': []}

PROFILES = {'elementary': {'target': 'prod',
                           'outputs': {'prod':
                                           {'type': 'snowflake',
                                            'account': 'fakeaccount.fake_region.gcp',
                                            'user': 'fake_user',
                                            'password': 'fake_password',
                                            'role': 'role',
                                            'database': 'awesome_db',
                                            'schema': 'awesome_schema',
                                            'threads': 1,
                                            'client_session_keep_alive': False,
                                            'query_tag': 'best_tag'}}}}


def create_config_files():
    yml = OrderedYaml()
    yml.dump(CONFIG, os.path.join(FILE_DIR, 'config.yml'))
    schema_path = os.path.join(FILE_DIR, 'models')
    if not os.path.exists(schema_path):
        os.makedirs(schema_path)
    yml.dump(SOURCES, os.path.join(schema_path, 'schema.yml'))
    yml.dump(DBT_PROJECT, os.path.join(FILE_DIR, 'dbt_project.yml'))
    yml.dump(PROFILES, os.path.join(FILE_DIR, 'profiles.yml'))


def read_csv(csv_path):
    csv_lines = []
    with open(csv_path, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            csv_lines.append(row)
    return csv_lines


@pytest.fixture
def config():
    create_config_files()
    return Config(config_dir_path=FILE_DIR, profiles_dir_path=FILE_DIR)


def test_config_get_slack_notification_webhook(config):
    assert config.get_slack_notification_webhook() == CONFIG['slack_notification_webhook']


def test_config__get_sources_from_all_dbt_projects(config):
    extracted_sources = config._get_sources_from_all_dbt_projects()
    assert len(extracted_sources[0]['sources']) == len(SOURCES['sources'])
    source_dict = extracted_sources[0]
    assert source_dict['sources'][0]['name'] == SOURCES['sources'][0]['name']


@pytest.mark.parametrize("alert_on_schema_changes, expected_result", [
    (True, True),
    ('True', True),
    ('true', True),
    ('TrUe', True),
    ('TRUE', True),
    (False, False),
    ('false', False),
    ('False', False),
    ('FALSE', False),
    ('FaLSE', False),
    (None, None),
    ('bla', None),
    ('None', None)
])
def test_config__alert_on_schema_changes(alert_on_schema_changes, expected_result, config):
    source_dict = {'meta': {'edr': {'schema_changes': alert_on_schema_changes}}}
    assert config._alert_on_schema_changes(source_dict) is expected_result
    source_dict = {'meta': {'edr': {'alert': True}}}
    assert config._alert_on_schema_changes(source_dict) is None
    source_dict = {'meta': {'observ': {'alert_on_schema_changes': True}}}
    assert config._alert_on_schema_changes(source_dict) is None
    source_dict = {'metadata': {'observability': {'alert_on_schema_changes': True}}}
    assert config._alert_on_schema_changes(source_dict) is None


def test_config_monitoring_configuration_in_dbt_sources_to_csv(config):
    target_csv_path = os.path.join(FILE_DIR, 'monitoring_configuration.csv')

    # The test function
    config.monitoring_configuration_in_dbt_sources_to_csv(target_csv_path)

    # Validate content of the generated csv file
    monitoring_config_csv_lines = read_csv(target_csv_path)
    assert monitoring_config_csv_lines[0]['database_name'] == SOURCES['sources'][0]['database']
    assert monitoring_config_csv_lines[0]['schema_name'] == SOURCES['sources'][0]['name']
    assert monitoring_config_csv_lines[0]['alert_on_schema_changes'].lower() == \
           SOURCES['sources'][0]['meta']['edr']['schema_changes'].lower()

    assert monitoring_config_csv_lines[1]['database_name'] == SOURCES['sources'][0]['database']
    assert monitoring_config_csv_lines[1]['schema_name'] == SOURCES['sources'][0]['name']
    assert monitoring_config_csv_lines[1]['table_name'] == SOURCES['sources'][0]['tables'][0]['name']
    assert monitoring_config_csv_lines[1]['alert_on_schema_changes'].lower() == \
           SOURCES['sources'][0]['tables'][0]['meta']['edr']['schema_changes'].lower()

    assert monitoring_config_csv_lines[2]['database_name'] == SOURCES['sources'][0]['database']
    assert monitoring_config_csv_lines[2]['schema_name'] == SOURCES['sources'][0]['name']
    assert monitoring_config_csv_lines[2]['table_name'] == SOURCES['sources'][0]['tables'][0]['name']
    assert monitoring_config_csv_lines[2]['column_name'] == \
           SOURCES['sources'][0]['tables'][0]['columns'][0]['name']
    assert monitoring_config_csv_lines[2]['alert_on_schema_changes'].lower() == \
           SOURCES['sources'][0]['tables'][0]['columns'][0]['meta']['edr']['schema_changes'].lower()

    assert monitoring_config_csv_lines[3]['database_name'] == SOURCES['sources'][1]['database']
    assert monitoring_config_csv_lines[3]['schema_name'] == SOURCES['sources'][1]['schema']
    assert monitoring_config_csv_lines[3]['alert_on_schema_changes'].lower() == \
           SOURCES['sources'][1]['meta']['edr']['schema_changes'].lower()

    assert monitoring_config_csv_lines[4]['database_name'] == SOURCES['sources'][1]['database']
    assert monitoring_config_csv_lines[4]['schema_name'] == SOURCES['sources'][1]['schema']
    assert monitoring_config_csv_lines[4]['table_name'] == SOURCES['sources'][1]['tables'][0]['identifier']
    assert monitoring_config_csv_lines[4]['alert_on_schema_changes'].lower() == \
           SOURCES['sources'][1]['tables'][0]['meta']['edr']['schema_changes'].lower()
