import dbt.config
from dbt.context.base import generate_base_context


def extract_credentials_from_profiles(profiles_dir: str, profile_name: str):
    profiles_raw = dbt.config.profile.read_profile(profiles_dir)
    empty_profile_renderer = dbt.config.renderer.ProfileRenderer(generate_base_context({}))
    dbt_profile = dbt.config.Profile.from_raw_profiles(profiles_raw, profile_name, empty_profile_renderer)
    return dbt_profile.credentials
