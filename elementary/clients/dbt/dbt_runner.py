import json
import os
import subprocess
from json import JSONDecodeError
from typing import Dict, List, Optional, Tuple

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.exceptions.exceptions import DbtCommandError, DbtLsCommandError
from elementary.utils.env_vars import is_debug
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class DbtLog:
    def __init__(self, log_line: str):
        log = json.loads(log_line)
        self.msg = log.get("info", {}).get("msg") or log.get("data", {}).get("msg")
        self.level = log.get("info", {}).get("level") or log.get("level")


class DbtRunner(BaseDbtRunner):
    ELEMENTARY_LOG_PREFIX = "Elementary: "

    def __init__(
        self,
        project_dir: str,
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
        raise_on_failure: bool = True,
        dbt_env_vars: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(project_dir, profiles_dir, target)
        self.raise_on_failure = raise_on_failure
        self.dbt_env_vars = dbt_env_vars

    def _run_command(
        self,
        command_args: List[str],
        capture_output: bool = False,
        log_format: str = "json",
        vars: Optional[dict] = None,
        quiet: bool = False,
    ) -> Tuple[bool, Optional[str]]:
        dbt_command = ["dbt"]
        if capture_output:
            dbt_command.extend(["--log-format", log_format])
        dbt_command.extend(command_args)
        dbt_command.extend(["--project-dir", self.project_dir])
        if self.profiles_dir:
            dbt_command.extend(["--profiles-dir", self.profiles_dir])
        if self.target:
            dbt_command.extend(["--target", self.target])
        if vars:
            json_vars = json.dumps(vars)
            dbt_command.extend(["--vars", json_vars])
        dbt_command_str = " ".join(dbt_command)
        log_msg = f"Running {dbt_command_str}"
        if not quiet:
            logger.info(log_msg)
        else:
            logger.debug(log_msg)
        try:
            result = subprocess.run(
                dbt_command,
                check=self.raise_on_failure,
                capture_output=(capture_output or quiet),
                env=self._get_command_env(),
            )
        except subprocess.CalledProcessError as err:
            err_msg = None
            if capture_output:
                err_log_msgs = []
                err_json_logs = err.output.splitlines()
                for err_log_line in err_json_logs:
                    try:
                        log = DbtLog(err_log_line)
                        if log.level == "error":
                            err_log_msgs.append(log.msg)
                    except JSONDecodeError:
                        logger.debug(
                            f"Unable to parse dbt log message: {err_log_line}",
                            exc_info=True,
                        )
                err_msg = "\n".join(err_log_msgs)
            raise DbtCommandError(err, command_args, err_msg)

        output = None
        if capture_output:
            output = result.stdout.decode("utf-8")
            if is_debug():
                logger.debug(f"Output: {output}")
            logger.debug(
                f"Result bytes size for command '{dbt_command_str}' is {len(result.stdout)}"
            )
        if result.returncode != 0:
            return False, output
        return True, output

    def deps(self, quiet: bool = False) -> bool:
        success, _ = self._run_command(command_args=["deps"], quiet=quiet)
        return success

    def seed(self, select: Optional[str] = None, full_refresh: bool = False) -> bool:
        command_args = ["seed"]
        if full_refresh:
            command_args.append("--full-refresh")
        if select:
            command_args.extend(["-s", select])
        success, _ = self._run_command(command_args)
        return success

    def snapshot(self) -> bool:
        success, _ = self._run_command(["snapshot"])
        return success

    def run_operation(
        self,
        macro_name: str,
        capture_output: bool = True,
        macro_args: Optional[dict] = None,
        log_errors: bool = True,
        vars: Optional[dict] = None,
        quiet: bool = False,
        should_log: bool = True,
    ) -> list:
        macro_to_run = macro_name
        macro_to_run_args = macro_args if macro_args else dict()
        if should_log:
            macro_to_run = "elementary.log_macro_results"
            macro_to_run_args = dict(
                macro_name=macro_name, macro_args=macro_args if macro_args else dict()
            )
        command_args = ["run-operation", macro_to_run]
        json_args = json.dumps(macro_to_run_args)
        command_args.extend(["--args", json_args])
        success, command_output = self._run_command(
            command_args=command_args,
            capture_output=capture_output,
            vars=vars,
            quiet=quiet,
        )
        if log_errors and not success:
            logger.error(
                f'Failed to run macro: "{macro_name}"\nRun output: {command_output}'
            )
        run_operation_results = []
        if capture_output and command_output is not None:
            json_messages = command_output.splitlines()
            for json_message in json_messages:
                try:
                    log = DbtLog(json_message)
                    if log_errors and log.level == "error":
                        logger.error(log.msg)
                        continue
                    if log.msg and log.msg.startswith(self.ELEMENTARY_LOG_PREFIX):
                        run_operation_results.append(
                            log.msg[len(self.ELEMENTARY_LOG_PREFIX) :]
                        )
                except JSONDecodeError:
                    logger.debug(
                        f"Unable to parse run-operation log message: {json_message}",
                        exc_info=True,
                    )
        return run_operation_results

    def run(
        self,
        models: Optional[str] = None,
        select: Optional[str] = None,
        full_refresh: bool = False,
        vars: Optional[dict] = None,
        quiet: bool = False,
    ) -> bool:
        command_args = ["run"]
        if full_refresh:
            command_args.append("--full-refresh")
        if models:
            command_args.extend(["-m", models])
        if select:
            command_args.extend(["-s", select])
        success, _ = self._run_command(
            command_args=command_args, vars=vars, quiet=quiet
        )
        return success

    def test(
        self,
        select: Optional[str] = None,
        vars: Optional[dict] = None,
        quiet: bool = False,
    ) -> bool:
        command_args = ["test"]
        if select:
            command_args.extend(["-s", select])
        success, _ = self._run_command(
            command_args=command_args, vars=vars, quiet=quiet
        )
        return success

    def _get_command_env(self):
        env = os.environ.copy()
        if self.dbt_env_vars is not None:
            env.update(self.dbt_env_vars)
        return env

    def debug(self, quiet: bool = False) -> bool:
        success, _ = self._run_command(command_args=["debug"], quiet=quiet)
        return success

    def ls(self, select: Optional[str] = None) -> list:
        command_args = ["-q", "ls"]
        if select:
            command_args.extend(["-s", select])
        try:
            success, command_output_string = self._run_command(
                command_args=command_args, capture_output=True, log_format="text"
            )
            command_outputs = (
                command_output_string.splitlines() if command_output_string else []
            )
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
        except DbtCommandError:
            raise DbtLsCommandError(select)

    def source_freshness(self):
        self._run_command(command_args=["source", "freshness"])
