from typing import Optional

from elementary.config.config import Config

MOCK_PROFILES_DIR = "profiles_dir"


class MockConfig(Config):
    def __init__(self, project_dir: Optional[str] = None) -> None:
        super().__init__(project_dir=project_dir, profiles_dir=MOCK_PROFILES_DIR)
