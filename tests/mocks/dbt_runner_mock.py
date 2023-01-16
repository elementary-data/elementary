from elementary.clients.dbt.dbt_runner import DbtRunner

MOCK_PROJECT_DIR = "project_dir"
MOCK_PROFILES_DIR = "profiles_dir"


class MockDbtRunner(DbtRunner):
    def __init__(self) -> None:
        super().__init__(project_dir=MOCK_PROJECT_DIR, profiles_dir=MOCK_PROFILES_DIR)
