import os
from typing import Union
from utils.ordered_yaml import OrderedYaml

ordered_yaml = OrderedYaml()


class Config(object):
    SLACK = 'slack'
    TOKEN = 'token'
    NOTIFICATION_CHANNEL_NAME = 'channel_name'
    NOTIFICATION_WEBHOOK = 'notification_webhook'
    WORKFLOWS = 'workflows'
    CONFIG_FILE_NAME = 'config.yml'

    def __init__(self, config_dir: str, profiles_dir: str, profile_target: str = None) -> None:
        self.config_dir = config_dir
        self.profiles_dir = profiles_dir
        self.profile_target = profile_target
        self.config_dict = self._load_configuration()

    def _load_configuration(self) -> dict:
        if not os.path.exists(self.config_dir):
            os.makedirs(self.config_dir)

        config_file_path = os.path.join(self.config_dir, self.CONFIG_FILE_NAME)
        if not os.path.exists(config_file_path):
            return {}

        return ordered_yaml.load(config_file_path)

    @property
    def anonymous_tracking_enabled(self) -> bool:
        return self.config_dict.get('anonymous_usage_tracking', True)
    
    @property
    def slack_token(self) -> Union[str, None]:
        slack_config = self.config_dict.get(self.SLACK)
        if slack_config is not None:
            return slack_config.get(self.TOKEN)
        return None

    @property
    def slack_notification_channel_name(self) -> Union[str, None]:
        slack_config = self.config_dict.get(self.SLACK)
        if slack_config is not None:
            return slack_config.get(self.NOTIFICATION_CHANNEL_NAME)
        return None
    
    @property
    def slack_notification_webhook(self) -> Union[str, None]:
        slack_config = self.config_dict.get(self.SLACK)
        if slack_config is not None:
            return slack_config.get(self.NOTIFICATION_WEBHOOK)
        return None

    @property
    def is_slack_workflow(self) -> bool:
        slack_config = self.config_dict.get(self.SLACK)
        if slack_config is not None:
            workflows = slack_config.get(self.WORKFLOWS)
            if workflows is True:
                return True
        return False

    @property
    def target_dir(self) -> str:
        target_path = self.config_dict.get('target-path')
        if not target_path:
            return os.getcwd()
        return target_path

