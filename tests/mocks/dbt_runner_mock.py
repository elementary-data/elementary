from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner

MOCK_PROJECT_DIR = "project_dir"
MOCK_PROFILES_DIR = "profiles_dir"


class MockDbtRunner(SubprocessDbtRunner):
    def __init__(self) -> None:
        super().__init__(
            project_dir=MOCK_PROJECT_DIR,
            profiles_dir=MOCK_PROFILES_DIR,
            run_deps_if_needed=False,
        )
