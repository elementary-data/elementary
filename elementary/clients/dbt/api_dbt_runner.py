import json
from dataclasses import dataclass
from typing import List, Optional, cast

from dbt.cli.main import dbtRunner, dbtRunnerResult
from google.protobuf.json_format import MessageToDict

from elementary.clients.dbt.command_line_dbt_runner import (
    CommandLineDbtRunner,
    DbtCommandResult,
)
from elementary.clients.dbt.dbt_log import DbtLog
from elementary.exceptions.exceptions import DbtCommandError
from elementary.utils.cwd import with_chdir
from elementary.utils.env_vars_context import env_vars_context
from elementary.utils.log import get_logger

logger = get_logger(__name__)


@dataclass
class APIDbtCommandResult(DbtCommandResult):
    result_obj: dbtRunnerResult


class APIDbtRunner(CommandLineDbtRunner):
    def _inner_run_command(
        self,
        dbt_command_args: List[str],
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
            if event.info.name in ["JinjaLogInfo", "RunningOperationCaughtError"]:
                event_dump = json.dumps(MessageToDict(event))  # type: ignore[arg-type]
                dbt_logs.append(event_dump)

        with env_vars_context(self.env_vars):
            dbt = dbtRunner(callbacks=[collect_dbt_command_logs])
            with with_chdir(self.project_dir):
                res: dbtRunnerResult = dbt.invoke(dbt_command_args)
        output = "\n".join(dbt_logs) or None
        if self.raise_on_failure and not res.success:
            raise DbtCommandError(
                base_command_args=dbt_command_args,
                err_msg=(str(res.exception) if res.exception else output),
                logs=[DbtLog.from_log_line(log) for log in dbt_logs],
            )

        return APIDbtCommandResult(success=res.success, output=output, result_obj=res)

    def _parse_ls_command_result(
        self, select: Optional[str], result: DbtCommandResult
    ) -> List[str]:
        ls_result = cast(APIDbtCommandResult, result).result_obj.result
        return cast(List[str], ls_result)
