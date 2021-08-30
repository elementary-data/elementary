import dbt.config
from dbt.context.base import generate_base_context
import snowflake.connector


def connect_using_dbt_profiles(profiles_dir, profile_name):
    profiles_raw = dbt.config.profile.read_profile(profiles_dir)
    empty_profile_renderer = dbt.config.renderer.ProfileRenderer(generate_base_context({}))
    dbt_profile = dbt.config.Profile.from_raw_profiles(profiles_raw, profile_name, empty_profile_renderer)
    profile_type = dbt_profile.credentials.type
    if profile_type == 'snowflake':
        return snowflake.connector.connect(
            user=dbt_profile.credentials.user,
            password=dbt_profile.credentials.password,
            account=dbt_profile.credentials.account
        )
    else:
        raise Exception("Unsupported profile type")