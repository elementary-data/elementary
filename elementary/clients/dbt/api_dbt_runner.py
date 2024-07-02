import json
import os
from dataclasses import dataclass
from typing import Optional, cast

from dbt.cli.main import dbtRunner, dbtRunnerResult
from google.protobuf.json_format import MessageToDict

from elementary.clients.dbt.command_line_dbt_runner import (
    CommandLineDbtRunner,
    DbtCommandResult,
)
from elementary.exceptions.exceptions import DbtCommandError
from elementary.utils.log import get_logger

logger = get_logger(__name__)


@dataclass
class APIDbtCommandResult(DbtCommandResult):
    result_obj: dbtRunnerResult


class APIDbtRunner(CommandLineDbtRunner):
    def _inner_run_command(
        self,
        dbt_command_args: list[str],
        capture_output: bool,
        quiet: bool,
        log_output: bool,
        log_format: str,
    ) -> DbtCommandResult:
        # The dbt python API always prints the output and we collect the logs using a programmatic callback so no
        # need to capture the output anymore here.
        dbt_command_args = list(dbt_command_args)
        if "-q" not in dbt_command_args and "--quiet" not in dbt_command_args:
            dbt_command_args.extend(["--quiet"])

        dbt_logs = []

        def collect_dbt_command_logs(event):
            event_dump = json.dumps(MessageToDict(event))  # type: ignore[arg-type]
            logger.debug(f"dbt event msg: {event_dump}")
            if event.info.name == "JinjaLogInfo":
                dbt_logs.append(event_dump)

        dbt = dbtRunner(callbacks=[collect_dbt_command_logs])
        res: dbtRunnerResult = dbt.invoke(dbt_command_args)
        output = "\n".join(dbt_logs) or None
        if self.raise_on_failure and not res.success:
            raise DbtCommandError(base_command_args=dbt_command_args, err_msg=output)

        return APIDbtCommandResult(success=res.success, output=output, result_obj=res)

    def _parse_ls_command_result(
        self, select: Optional[str], result: DbtCommandResult
    ) -> list[str]:
        ls_result = cast(APIDbtCommandResult, result).result_obj.result
        return cast(list[str], ls_result)

    def _get_command_env(self):
        env = os.environ.copy()
        if self.env_vars is not None:
            env.update(self.env_vars)
        return env
