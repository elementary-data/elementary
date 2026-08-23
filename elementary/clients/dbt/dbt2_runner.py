from elementary.clients.dbt.dbt_installation import get_dbt2_binary_path
from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner


class Dbt2Runner(SubprocessDbtRunner):
    """Runner for dbt 2.0 (the Fusion engine), which is distributed as a
    standalone binary (via the `dbt` PyPI package, the `dbt-core` 2.x package
    or the standalone installer) and has no importable Python API."""

    def _get_dbt_command_name(self) -> str:
        return get_dbt2_binary_path()
