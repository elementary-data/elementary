import dbt.config
from dbt.context.base import generate_base_context
import snowflake.connector

snowflake.connector.paramstyle = 'qmark'


def connect_using_dbt_profiles(profiles_dir, profile_name):
    profiles_raw = dbt.config.profile.read_profile(profiles_dir)
    empty_profile_renderer = dbt.config.renderer.ProfileRenderer(generate_base_context({}))
    dbt_profile = dbt.config.Profile.from_raw_profiles(profiles_raw, profile_name, empty_profile_renderer)
    profile_type = dbt_profile.credentials.type
    if profile_type == 'snowflake':
        return snowflake.connector.connect(
            account=dbt_profile.credentials.account,
            user=dbt_profile.credentials.user,
            database=dbt_profile.credentials.database,
            schema=dbt_profile.credentials.schema,
            warehouse=dbt_profile.credentials.warehouse,
            role=dbt_profile.credentials.role,
            autocommit=True,
            client_session_keep_alive=dbt_profile.credentials.client_session_keep_alive,
            application='elementary',
            **dbt_profile.credentials.auth_args()
        )
    else:
        raise Exception("Unsupported profile type")