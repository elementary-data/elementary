import os
import os.path
from pathlib import Path
from typing import Dict, Any, Union
import dbt.config
from dbt.context.base import generate_base_context
from dbt.exceptions import DbtConfigError
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
import google.cloud.bigquery
import google.cloud.exceptions
from google.api_core import client_info
from exceptions.exceptions import ConfigError
from utils.log import get_logger
import snowflake.connector

from utils.ordered_yaml import OrderedYaml

logger = get_logger(__name__)

DBT_DEFAULT_DIR = ".dbt"


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


def get_snowflake_client(profile_credentials, server_side_binding=True, custom_schema=None):
    if server_side_binding:
        snowflake.connector.paramstyle = 'numeric'
    schema = profile_credentials.schema
    if custom_schema is not None:
        schema = schema + '_' + custom_schema
    return snowflake.connector.connect(
        account=profile_credentials.account,
        user=profile_credentials.user,
        database=profile_credentials.database,
        schema=schema,
        warehouse=profile_credentials.warehouse,
        role=profile_credentials.role,
        autocommit=True,
        client_session_keep_alive=profile_credentials.client_session_keep_alive,
        application='elementary',
        **profile_credentials.auth_args()
    )


def get_profile_name_from_dbt_project(dbt_project_path: str) -> str:
    ordered_yaml = OrderedYaml()
    dbt_project_dict = ordered_yaml.load(os.path.join(dbt_project_path, 'dbt_project.yml'))
    return dbt_project_dict['profile']


def get_model_paths_from_dbt_project(dbt_project_path: str) -> list:
    ordered_yaml = OrderedYaml()
    dbt_project_dict = ordered_yaml.load(os.path.join(dbt_project_path, 'dbt_project.yml'))
    return dbt_project_dict.get('model-paths', dbt_project_dict.get('source-paths', ['models']))


def get_target_database_name(profiles_dir: str, dbt_project_path: str) -> Union[str, None]:
    try:
        profile_name = get_profile_name_from_dbt_project(dbt_project_path)
        credentials, profile_data = extract_credentials_and_data_from_profiles(profiles_dir, profile_name)
        return credentials.database
    except Exception:
        pass
    return None


def is_dbt_installed() -> bool:
    if os.path.exists(os.path.join(Path.home(), DBT_DEFAULT_DIR)):
        return True
    return False
