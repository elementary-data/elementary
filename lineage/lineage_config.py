from utils.dbt import extract_credentials_and_data_from_profiles
from config.config import Config


class LineageConfig(Config):
    def __init__(self, config_dir: str, profiles_dir: str, profile_name: str) -> None:
        super().__init__(config_dir, profiles_dir)
        self.profile_name = profile_name
        self.credentials, self.profiles_data = extract_credentials_and_data_from_profiles(profiles_dir,
                                                                                          profile_name)

    @property
    def query_history_source(self):
        return self.profiles_data.get('query_history_source')

    @property
    def platform(self):
        return self.profiles_data.get('type', 'unknown')
