import os
import tempfile

import pytest

from elementary.config.config import Config
from elementary.utils.ordered_yaml import OrderedYaml

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


def create_config_files(directory, config: dict):
    yml = OrderedYaml()
    yml.dump(config, os.path.join(directory, 'config.yml'))


@pytest.fixture
def config(request):
    with tempfile.TemporaryDirectory() as temp_dir:
        create_config_files(temp_dir, request.param)
        yield Config(config_dir=temp_dir, profiles_dir=temp_dir)


@pytest.mark.parametrize('config', [CONFIG], indirect=["config"])
def test_config_get_slack_notification_webhook(config: Config):
    assert config.slack_webhook == CONFIG['slack']['notification_webhook']


@pytest.mark.parametrize('config', [CONFIG], indirect=["config"])
def test_config_get_slack_token(config: Config):
    assert config.slack_token == CONFIG['slack']['token']


@pytest.mark.parametrize('config', [CONFIG], indirect=["config"])
def test_config_get_slack_notification_channel_name(config: Config):
    assert config.slack_channel_name == CONFIG['slack']['channel_name']


@pytest.mark.parametrize('config', [WORKFLOWS_CONFIG], indirect=["config"])
def test_slack_workflows_config_get_workflows(config: Config):
    assert config.is_slack_workflow == WORKFLOWS_CONFIG['slack']['workflows']
