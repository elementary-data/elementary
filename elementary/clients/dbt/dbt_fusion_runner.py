import os

from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner

DBT_FUSION_PATH = os.getenv("DBT_FUSION_PATH", "~/.local/bin/dbt")


class DbtFusionRunner(SubprocessDbtRunner):
    def _get_dbt_command_name(self) -> str:
        return os.path.expanduser(DBT_FUSION_PATH)

    def _run_deps_if_needed(self):
        # Currently we don't support auto-updating deps for dbt fusion
        return
