import os
import subprocess
from typing import List, Optional

from elementary.clients.dbt.command_line_dbt_runner import (
    CommandLineDbtRunner,
    DbtCommandResult,
)
from elementary.clients.dbt.dbt_log import parse_dbt_output
from elementary.exceptions.exceptions import DbtCommandError
from elementary.utils.env_vars import is_debug
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class SubprocessDbtRunner(CommandLineDbtRunner):
    def _inner_run_command(
        self,
        dbt_command_args: List[str],
        capture_output: bool,
        quiet: bool,
        log_output: bool,
        log_format: str,
    ) -> DbtCommandResult:
        try:
            result = subprocess.run(
                ["dbt"] + dbt_command_args,
                check=self.raise_on_failure,
                capture_output=capture_output or quiet,
                env=self._get_command_env(),
                cwd=self.project_dir,
            )
            success = result.returncode == 0
            output = result.stdout.decode() if result.stdout else None

            return DbtCommandResult(success=success, output=output)
        except subprocess.CalledProcessError as err:
            logs = (
                list(parse_dbt_output(err.output.decode(), log_format))
                if err.output
                else []
            )
            if capture_output and (log_output or is_debug()):
                for log in logs:
                    logger.info(log.msg)
            raise DbtCommandError(
                base_command_args=dbt_command_args, logs=logs, err=err
            )

    def _parse_ls_command_result(
        self, select: Optional[str], result: DbtCommandResult
    ) -> List[str]:
        command_outputs = result.output.splitlines() if result.output else []
        # ls command didn't match nodes.
        # When no node is matched, ls command returns 2 dicts with warning message that there are no matches.
        if (
            len(command_outputs) == 2
            and try_load_json(command_outputs[0])
            and try_load_json(command_outputs[1])
        ):
            logger.warning(
                f"The selection criterion '{select}' does not match any nodes"
            )
            return []
        # When nodes are matched, ls command returns strings of the node names.
        else:
            return command_outputs

    def _get_command_env(self):
        env = os.environ.copy()
        if self.env_vars is not None:
            env.update(self.env_vars)
        return env
