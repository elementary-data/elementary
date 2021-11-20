from typing import Dict, Any
import dbt.config
from dbt.context.base import generate_base_context
from dbt.exceptions import DbtConfigError
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
import google.cloud.bigquery
import google.cloud.exceptions
from google.api_core import client_info
from lineage.exceptions import ConfigError
from lineage.utils import get_logger
import json
import os
import oyaml as yaml
import sqlfluff
import hashlib


class folded_unicode(str): pass


class literal_unicode(str): pass


def folded_unicode_representer(dumper, data):
    return dumper.represent_scalar(u'tag:yaml.org,2002:str', data, style='>')


def literal_unicode_representer(dumper, data):
    return dumper.represent_scalar(u'tag:yaml.org,2002:str', data, style='|')


yaml.add_representer(folded_unicode, folded_unicode_representer)
yaml.add_representer(literal_unicode, literal_unicode_representer)

logger = get_logger(__name__)

DBT_MANIFEST_FILENAME = 'manifest.json'
DBT_CATALOG_FILENAME = 'catalog.json'
LINEGAE_DIR_NAME = 'lineage'


def extract_profile_data(profiles_raw: Dict[str, Any], profile_name: str, target_name: str) -> Dict[str, Any]:
    profile_data = dict()
    try:
        selected_profile = profiles_raw[profile_name]
        profile_data = selected_profile['outputs'][target_name]
    except KeyError as exc:
        logger.debug(f"Failed extracting profile data: {profiles_raw}, {profile_name}, {target_name}, {exc}")

    return profile_data


def extract_credentials_and_data_from_profiles(profiles_dir: str, profile_name: str):
    try:
        profiles_raw = dbt.config.profile.read_profile(profiles_dir)
        empty_profile_renderer = dbt.config.renderer.ProfileRenderer(generate_base_context({}))
        dbt_profile = dbt.config.Profile.from_raw_profiles(profiles_raw, profile_name, empty_profile_renderer)
        profile_data = extract_profile_data(profiles_raw, profile_name, dbt_profile.target_name)
        return dbt_profile.credentials, profile_data
    except DbtConfigError as exc:
        logger.debug(f"Failed parsing selected profile - {profiles_dir}, {profile_name}, {exc}")
        raise ConfigError(f"Failed parsing selected profile - {profiles_dir}, {profile_name}")


def get_bigquery_client(profile_credentials):
    if profile_credentials.impersonate_service_account:
        creds = \
            BigQueryConnectionManager.get_impersonated_bigquery_credentials(profile_credentials)
    else:
        creds = BigQueryConnectionManager.get_bigquery_credentials(profile_credentials)

    database = profile_credentials.database
    location = getattr(profile_credentials, 'location', None)

    info = client_info.ClientInfo(user_agent=f'elementary')
    return google.cloud.bigquery.Client(
        database,
        creds,
        location=location,
        client_info=info,
    )


def load_dbt_manifest(dbt_target_dir: str) -> dict:
    return json.load(open(os.path.join(dbt_target_dir, DBT_MANIFEST_FILENAME), 'r'))


def load_dbt_catalog(dbt_target_dir: str) -> dict:
    return json.load(open(os.path.join(dbt_target_dir, DBT_CATALOG_FILENAME), 'r'))


def get_dbt_target_dir(dbt_dir: str) -> str:
    return os.path.join(dbt_dir, 'target')


class LineageYamlGenerator(object):
    def __init__(self, dbt_dir: str):
        self.dbt_dir = dbt_dir

    @staticmethod
    def create_lineage_dir(dbt_dir: str) -> str:
        lineage_dir_path = os.path.join(dbt_dir, LINEGAE_DIR_NAME)
        if not os.path.exists(lineage_dir_path):
            os.makedirs(lineage_dir_path)
        return lineage_dir_path

    @staticmethod
    def get_tables_and_columns(sql: str, dialect: str = 'snowflake') -> []:
        parsed = sqlfluff.parse(sql, dialect=dialect)
        return [ref.raw for ref in parsed.tree.recursive_crawl('table_reference', 'column_reference')]

    @staticmethod
    def get_tables(sql: str, dialect: str = 'snowflake') -> []:
        parsed = sqlfluff.parse(sql, dialect=dialect)
        return [ref.raw for ref in parsed.tree.recursive_crawl('table_reference')]

    @staticmethod
    def hash_on_tables_and_columns(table_and_column: []) -> str:
        return hashlib.md5(json.dumps(table_and_column).strip().encode("utf-8")).hexdigest()

    @staticmethod
    def get_model_columns(dbt_catalog: dict, model_name: str) -> []:
        return [column_metadata['name'] for column, column_metadata in dbt_catalog['nodes'][model_name]['columns'].items()]

    @staticmethod
    def resolve_column(target_column_name: str, source_models: [], dbt_catalog: dict) -> str:
        source_columns = []
        for source_model in source_models:
            source_columns_dict = dbt_catalog['nodes'].get(source_model['name'], {}).get('columns', {})
            if target_column_name in source_columns_dict:
                source_columns.append(f'{source_model["alias"]}.{source_columns_dict[target_column_name]["name"]}')
        if len(source_columns) == 1:
            return source_columns[0]
        else:
            if len(source_models) == 1:
                return f'{source_models[0]["alias"]}.?'
            return f'?.?'

    @staticmethod
    def strip_relation_name(relation_name: str) -> str:
        return relation_name.replace('`', '').replace('`', '')

    @classmethod
    def get_alias_from_relation_name(cls, relation_name: str) -> str:
        return cls.strip_relation_name(relation_name).rsplit('.', 1)[-1]

    def create_yml_files_for_dbt_models(self) -> None:
        lineage_dir_path = self.create_lineage_dir(self.dbt_dir)
        dbt_target_dir = get_dbt_target_dir(self.dbt_dir)
        dbt_manifest = load_dbt_manifest(dbt_target_dir)
        dbt_catalog = load_dbt_catalog(dbt_target_dir)
        nodes_dict = dbt_manifest['nodes']
        for node_name, node_metadata in nodes_dict.items():
            if node_metadata['resource_type'] == 'model':
                dbt_model = node_metadata
                compiled_sql = dbt_model['compiled_sql']
                table_and_column_list = self.get_tables_and_columns(compiled_sql, dialect='bigquery')
                sql_hash = self.hash_on_tables_and_columns(table_and_column_list)
                model_name = dbt_model["name"]
                source_models = []
                depends_on_models = dbt_model.get('depends_on', {}).get('nodes', [])
                for source in depends_on_models:
                    source_relation_name = dbt_manifest['nodes'][source]['relation_name']
                    source_models.append({'relation_name': source_relation_name,
                                          'name': source,
                                          'alias': self.get_alias_from_relation_name(source_relation_name)})
                if len(source_models) == 0:
                    sources = self.get_tables(compiled_sql, dialect='bigquery')
                    for source in sources:
                        source_models.append({'relation_name': source,
                                              'name': f'model.{self.get_alias_from_relation_name(source)}',
                                              'alias': self.get_alias_from_relation_name(source)})
                target_columns = self.get_model_columns(dbt_catalog, node_name)
                columns = {}
                for target_column in target_columns:
                    columns[f'{model_name}.{target_column}'] = self.resolve_column(target_column_name=target_column,
                                                                                   source_models=source_models,
                                                                                   dbt_catalog=dbt_catalog)
                edl_lineage = {'sql': literal_unicode(compiled_sql),
                               'sql_hash': sql_hash,
                               'target': {self.strip_relation_name(dbt_model['relation_name']): {'alias': model_name}},
                               'sources': {self.strip_relation_name(source_model['relation_name']):
                                               {'alias': source_model['alias']} for source_model in source_models},
                               'columns': columns,
                               'validated': False,
                               'version': '1.0.0'
                               }
                yaml_name = f'{model_name}.yml'
                with open(os.path.join(lineage_dir_path, yaml_name), 'w') as yaml_file:
                    yaml_file.write(yaml.dump(edl_lineage, default_flow_style=False))
                    print(f'Generated yaml - {os.path.join(lineage_dir_path, yaml_name)} successfully')

