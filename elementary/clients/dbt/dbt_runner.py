import json
import os
import subprocess
from json import JSONDecodeError
from typing import Dict, List, Optional, Tuple

from elementary.exceptions.exceptions import DbtCommandError, DbtLsCommandError
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class DbtLog:
    def __init__(self, log_line: str):
        log = json.loads(log_line)
        self.msg = log.get("info", {}).get("msg") or log.get("data", {}).get("msg")
        self.level = log.get("info", {}).get("level") or log.get("level")


class DbtRunner:
    ELEMENTARY_LOG_PREFIX = "Elementary: "

    def __init__(
        self,
        project_dir: str,
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
        raise_on_failure: bool = True,
        dbt_env_vars: Optional[Dict[str, str]] = None,
    ) -> None:
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target
        self.raise_on_failure = raise_on_failure
        self.dbt_env_vars = dbt_env_vars

    def _run_command(
        self,
        command_args: List[str],
        json_logs: bool = False,
        vars: Optional[dict] = None,
        quiet: bool = False,
    ) -> Tuple[bool, str]:
        dbt_command = ["dbt"]
        json_output = False
        if json_logs:
            dbt_command.extend(["--log-format", "json"])
            json_output = True
        dbt_command.extend(command_args)
        dbt_command.extend(["--project-dir", self.project_dir])
        if self.profiles_dir:
            dbt_command.extend(["--profiles-dir", self.profiles_dir])
        if self.target:
            dbt_command.extend(["--target", self.target])
        if vars:
            json_vars = json.dumps(vars)
            dbt_command.extend(["--vars", json_vars])
        log_msg = f"Running {' '.join(dbt_command)}"
        if not quiet:
            logger.info(log_msg)
        else:
            logger.debug(log_msg)
        try:
            result = subprocess.run(
                dbt_command,
                check=self.raise_on_failure,
                capture_output=(json_output or quiet),
                env=self._get_command_env(),
            )
        except subprocess.CalledProcessError as err:
            raise DbtCommandError(err, command_args)
        output = None
        if json_output:
            output = result.stdout.decode("utf-8")
            logger.debug(f"Output: {output}")
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
        json_logs: bool = True,
        macro_args: Optional[dict] = None,
        log_errors: bool = True,
        vars: Optional[dict] = None,
        quiet: bool = False,
    ) -> list:
        command_args = ["run-operation", macro_name]
        if macro_args:
            json_args = json.dumps(macro_args)
            command_args.extend(["--args", json_args])
        success, command_output = self._run_command(
            command_args=command_args, json_logs=json_logs, vars=vars, quiet=quiet
        )
        if log_errors and not success:
            logger.error(f'Failed to run macro: "{macro_name}"')
        run_operation_results = []
        if json_logs:
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
        command_args = ["ls"]
        if select:
            command_args.extend(["-s", select])
        try:
            success, command_output_string = self._run_command(
                command_args=command_args, json_logs=True
            )
            command_outputs = command_output_string.splitlines()
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
