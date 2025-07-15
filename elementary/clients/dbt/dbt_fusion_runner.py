import os

from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner


class DbtFusionRunner(SubprocessDbtRunner):
    def _get_dbt_command_name(self) -> str:
        return os.path.expanduser("~/.local/bin/dbt")
