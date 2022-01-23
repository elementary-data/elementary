import pytest
import os
from utils.ordered_yaml import OrderedYaml
from observability.config import Config

FILE_DIR = os.path.dirname(__file__)

CONFIG = {'monitoring_configuration': {'slack_notification_webhook': 'test_slack_webhook',
                                       'dbt_projects': [FILE_DIR]}}

SOURCES = {'sources': [{'name': 'unit_tests',
                        'database': 'elementary_tests',
                        'meta': {'observability': {'alert_on_schema_changes': 'true'}}}]}

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


@pytest.fixture
def config():
    create_config_files()
    return Config(config_dir_path=FILE_DIR, profiles_dir_path=FILE_DIR)


def test_config_get_slack_notification_webhook(config):
    assert config.get_slack_notification_webhook() == CONFIG['monitoring_configuration']['slack_notification_webhook']


def test_config_get_sources(config):
    extracted_sources = config.get_dbt_project_sources()
    assert len(extracted_sources[0]['sources']) == len(SOURCES['sources'])
    source_dict = extracted_sources[0]
    assert source_dict['sources'][0]['name'] == SOURCES['sources'][0]['name']

