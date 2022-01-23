from typing import Union
import glob

from utils.dbt import get_model_paths_from_dbt_project, get_target_database_name
from utils.ordered_yaml import OrderedYaml
import os


class Config(object):
    SLACK_NOTIFICATION_WEBHOOK = 'slack_notification_webhook'
    CONFIG_FILE_NAME = 'config.yml'

    def __init__(self, config_dir_path: str, profiles_dir_path: str) -> None:
        self.config_dir_path = config_dir_path
        self.profiles_dir_path = profiles_dir_path
        self.config_file_path = os.path.join(self.config_dir_path, self.CONFIG_FILE_NAME)
        self.ordered_yaml = OrderedYaml()

    def _get_monitoring_configuration(self) -> dict:
        if not os.path.exists(self.config_file_path):
            return {}

        config_dict = self.ordered_yaml.load(self.config_file_path)
        return config_dict.get('monitoring_configuration', {})

    def get_slack_notification_webhook(self) -> Union[str, None]:
        monitoring_config = self._get_monitoring_configuration()
        return monitoring_config.get(self.SLACK_NOTIFICATION_WEBHOOK)

    @staticmethod
    def _find_schema_yml_files_in_dbt_project(dbt_project_models_path: str) -> list:
        return glob.glob(os.path.join(dbt_project_models_path, '*.yml'), recursive=True)

    def get_dbt_project_sources(self) -> list:
        monitoring_config = self._get_monitoring_configuration()
        dbt_projects = monitoring_config.get('dbt_projects', [])
        sources = []
        for dbt_project_path in dbt_projects:
            dbt_project_target_database = get_target_database_name(self.profiles_dir_path, dbt_project_path)
            model_paths = get_model_paths_from_dbt_project(dbt_project_path)
            for model_path in model_paths:
                dbt_project_models_path = os.path.join(dbt_project_path, model_path)
                schema_yml_files = self._find_schema_yml_files_in_dbt_project(dbt_project_models_path)
                for schema_yml_file in schema_yml_files:
                    schema_dict = self.ordered_yaml.load(schema_yml_file)
                    schema_sources = schema_dict.get('sources')
                    if schema_sources is not None:
                        dbt_project_sources = {'sources': schema_sources,
                                               'dbt_project_target_database': dbt_project_target_database}
                        sources.append(dbt_project_sources)
        return sources
