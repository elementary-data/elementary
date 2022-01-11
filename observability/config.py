from typing import Union

from utils.yaml import get_ordered_yaml
import os


class Config(object):
    SLACK_NOTIFICATION_WEBHOOK = 'slack_notification_webhook'
    CONFIG_FILE_NAME = 'config.yml'

    def __init__(self, config_dir_path: str, profiles_dir_path: str) -> None:
        self.config_dir_path = config_dir_path
        self.profiles_dir_path = profiles_dir_path
        self.config_file_path = os.path.join(self.config_dir_path, self.CONFIG_FILE_NAME)
        self.yaml = get_ordered_yaml()

    def _get_monitoring_configuration(self) -> dict:
        if not os.path.exists(self.config_file_path):
            return {}

        config_dict = self.yaml.load(self.config_file_path)
        return config_dict.get('monitoring_configuration', {})

    def get_slack_notification_webhook(self) -> Union[str, None]:
        monitoring_config = self._get_monitoring_configuration()
        return monitoring_config.get(self.SLACK_NOTIFICATION_WEBHOOK)

    def get_sources(self) -> list:
        monitoring_config = self._get_monitoring_configuration()
        #TODO: maybe proivde here a list of dbt projects
        config_files = monitoring_config.get('config_files', [])
        sources = []
        for config_file in config_files:
            config_dict = self.yaml.load(config_file)
            sources.extend(config_dict.get('sources', []))

        return sources
