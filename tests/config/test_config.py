import os

import pytest

from config.config import Config
from utils.ordered_yaml import OrderedYaml

FILE_DIR = os.path.dirname(os.path.realpath(__file__))

CONFIG = {
    'slack': {
        'notification_webhook': 'test_slack_webhook',
        'token': 'test_slack_token',
        'channel_name': 'test_channel_name',
        'workflows': False
    }
}
WORKFLOWS_CONFIG = {
    'slack': {
        'notification_webhook': 'test_slack_webhook',
        'token': 'test_slack_token',
        'channel_name': 'test_channel_name',
        'workflows': True
    }
}


def create_config_files(config: dict):
    yml = OrderedYaml()
    yml.dump(config, os.path.join(FILE_DIR, 'config.yml'))


@pytest.fixture
def config():
    create_config_files(CONFIG)
    return Config(config_dir=FILE_DIR, profiles_dir=FILE_DIR)


@pytest.fixture
def slack_workflows_config():
    create_config_files(WORKFLOWS_CONFIG)
    return Config(config_dir=FILE_DIR, profiles_dir=FILE_DIR)


def test_config_get_slack_notification_webhook(config):
    assert config.slack_notification_webhook == CONFIG['slack']['notification_webhook']


def test_config_get_slack_token(config: Config):
    assert config.slack_token == CONFIG['slack']['token']


def test_config_get_slack_notification_channel_name(config: Config):
    assert config.slack_channel_name == CONFIG['slack']['channel_name']


def test_slack_workflows_config_get_workflows(slack_workflows_config):
    assert slack_workflows_config.is_slack_workflow == WORKFLOWS_CONFIG['slack']['workflows']
