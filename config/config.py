import os
import csv
from typing import Union
import glob

from exceptions.exceptions import ConfigError
from utils.dbt import get_model_paths_from_dbt_project, get_target_database_name, \
    extract_credentials_and_data_from_profiles
from utils.ordered_yaml import OrderedYaml

ordered_yaml = OrderedYaml()


class Config(object):
    SLACK = 'slack'
    NOTIFICATION_WEBHOOK = 'notification_webhook'
    WORKFLOWS = 'workflows'
    CONFIG_FILE_NAME = 'config.yml'

    def __init__(self, config_dir: str, profiles_dir: str, profile_name: str) -> None:
        self.config_dir = config_dir
        self.profiles_dir = profiles_dir
        self.profile_name = profile_name
        self.credentials, self.profiles_data = extract_credentials_and_data_from_profiles(profiles_dir,
                                                                                          profile_name)
        self.config_dict = self._load_configuration()

    def _load_configuration(self) -> dict:
        if not os.path.exists(self.config_dir):
            os.makedirs(self.config_dir)

        config_file_path = os.path.join(self.config_dir, self.CONFIG_FILE_NAME)
        if not os.path.exists(config_file_path):
            return {}

        return ordered_yaml.load(config_file_path)

    @property
    def query_history_source(self):
        return self.profiles_data.get('query_history_source')

    @property
    def platform(self):
        return self.profiles_data.get('type', 'unknown')

    @property
    def anonymous_tracking_enabled(self) -> bool:
        return self.config_dict.get('anonymous_usage_tracking', True)

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

    @staticmethod
    def _find_schema_yml_files_in_dbt_project(dbt_project_models_path: str) -> list:
        return glob.glob(os.path.join(dbt_project_models_path, '*.yml'), recursive=True)

    def _get_sources_from_all_dbt_projects(self) -> list:
        config_dict = self._load_configuration()
        dbt_projects = config_dict.get('dbt_projects', [])
        sources = []
        for dbt_project_path in dbt_projects:
            try:
                dbt_project_target_database = get_target_database_name(self.profiles_dir, dbt_project_path)
                model_paths = get_model_paths_from_dbt_project(dbt_project_path)
                for model_path in model_paths:
                    dbt_project_models_path = os.path.join(dbt_project_path, model_path)
                    schema_yml_files = self._find_schema_yml_files_in_dbt_project(dbt_project_models_path)
                    for schema_yml_file in schema_yml_files:
                        schema_dict = ordered_yaml.load(schema_yml_file)
                        schema_sources = schema_dict.get('sources')
                        if schema_sources is not None:
                            dbt_project_sources = {'sources': schema_sources,
                                                   'dbt_project_target_database': dbt_project_target_database}
                            sources.append(dbt_project_sources)
            except FileNotFoundError as exc:
                raise ConfigError(f'No such file - {exc.filename}, please configure a valid dbt project path')
        return sources

    @staticmethod
    def _alert_on_schema_changes(source_dict: dict) -> Union[bool, None]:
        metadata = source_dict.get('meta', {})
        edr_config = metadata.get('edr', {})
        alert_on_schema_changes = edr_config.get('schema_changes')

        # Normalize alert_on_schema_changes to handle both booleans and strings
        alert_on_schema_changes_str = str(alert_on_schema_changes).lower()
        if alert_on_schema_changes_str == 'false':
            return False
        elif alert_on_schema_changes_str == 'true':
            return True
        else:
            return None

    def monitoring_configuration_in_dbt_sources_to_csv(self, target_csv_path: str) -> int:
        row_count = 0
        with open(target_csv_path, 'w') as target_csv:
            target_csv_writer = csv.DictWriter(target_csv, fieldnames=['database_name',
                                                                       'schema_name',
                                                                       'table_name',
                                                                       'column_name',
                                                                       'alert_on_schema_changes'])
            target_csv_writer.writeheader()

            all_configured_sources = self._get_sources_from_all_dbt_projects()
            for sources_dict in all_configured_sources:
                sources = sources_dict.get('sources', [])
                target_database = sources_dict.get('dbt_project_target_database')
                for source in sources:
                    source_db = source.get('database', target_database)
                    if source_db is None:
                        continue

                    schema_name = source.get('schema', source.get('name'))
                    if schema_name is None:
                        continue

                    alert_on_schema_changes = self._alert_on_schema_changes(source)
                    if alert_on_schema_changes is not None:
                        target_csv_writer.writerow({'database_name': source_db,
                                                    'schema_name': schema_name,
                                                    'table_name': None,
                                                    'column_name': None,
                                                    'alert_on_schema_changes': alert_on_schema_changes})
                        row_count += 1

                    source_tables = source.get('tables', [])
                    for source_table in source_tables:
                        table_name = source_table.get('identifier', source_table.get('name'))
                        if table_name is None:
                            continue

                        alert_on_schema_changes = self._alert_on_schema_changes(source_table)
                        if alert_on_schema_changes is not None:
                            target_csv_writer.writerow({'database_name': source_db,
                                                        'schema_name': schema_name,
                                                        'table_name': table_name,
                                                        'column_name': None,
                                                        'alert_on_schema_changes': alert_on_schema_changes})
                            row_count += 1

                        source_columns = source_table.get('columns', [])
                        for source_column in source_columns:
                            column_name = source_column.get('name')
                            if column_name is None:
                                continue

                            alert_on_schema_changes = self._alert_on_schema_changes(source_column)
                            if alert_on_schema_changes is not None:
                                target_csv_writer.writerow({'database_name': source_db,
                                                            'schema_name': schema_name,
                                                            'table_name': table_name,
                                                            'column_name': column_name,
                                                            'alert_on_schema_changes': alert_on_schema_changes})
                                row_count += 1
        return row_count
