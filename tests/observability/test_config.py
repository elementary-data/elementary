import pytest
import os
from utils.ordered_yaml import OrderedYaml
from observability.config import Config

FILE_DIR = os.path.dirname(__file__)

CONFIG = {'monitoring_configuration': {'slack_notification_webhook': 'test_slack_webhook',
                                       'config_files': [os.path.join(FILE_DIR, 'schema.yml')]}}

SOURCES = {'sources': [{'name': 'unit_tests',
                        'database': 'elementary_tests',
                        'meta': {'observability': {'alert_on_schema_changes': 'true'}}}]}


def create_config():
    yml = OrderedYaml()
    yml.dump(CONFIG, os.path.join(FILE_DIR, 'config.yml'))
    yml.dump(SOURCES, os.path.join(FILE_DIR, 'schema.yml'))


def test_config_get_slack_notification_webhook():
    create_config()
    config_dir_path = FILE_DIR
    profiles_dir_path = "/path/.dbt/"
    config = Config(config_dir_path=config_dir_path, profiles_dir_path=profiles_dir_path)
    assert config.get_slack_notification_webhook() == CONFIG['monitoring_configuration']['slack_notification_webhook']


def test_config_get_sources():
    create_config()
    config_dir_path = FILE_DIR
    profiles_dir_path = "/path/.dbt/"
    config = Config(config_dir_path=config_dir_path, profiles_dir_path=profiles_dir_path)
    extracted_sources = config.get_sources()
    assert len(extracted_sources) == len(SOURCES['sources'])
    source = extracted_sources[0]
    assert source['name'] == SOURCES['sources'][0]['name']

