import json
import os
import subprocess
from json import JSONDecodeError
from typing import Dict, List, Optional

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.exceptions.exceptions import DbtCommandError, DbtLsCommandError
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
        dbt_env_vars: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(project_dir, profiles_dir, target)
        self.dbt_env_vars = dbt_env_vars

    def _run_command(
        self,
        command_args: List[str],
        capture_output: bool = False,
        log_format: str = "json",
        vars: Optional[dict] = None,
        quiet: bool = False,
    ) -> Optional[str]:
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
        log_msg = f"Running {' '.join(dbt_command)}"
        if not quiet:
            logger.info(log_msg)
        else:
            logger.debug(log_msg)
        try:
            result = subprocess.run(
                dbt_command,
                check=True,
                capture_output=(capture_output or quiet),
                env=self._get_command_env(),
            )
        except subprocess.CalledProcessError as err:
            raise DbtCommandError(err, command_args)
        output = None
        if capture_output:
            output = result.stdout.decode("utf-8")
            logger.debug(f"Output: {output}")
        return output

    def deps(self, quiet: bool = False):
        self._run_command(command_args=["deps"], quiet=quiet)

    def seed(self, select: Optional[str] = None, full_refresh: bool = False):
        command_args = ["seed"]
        if full_refresh:
            command_args.append("--full-refresh")
        if select:
            command_args.extend(["-s", select])
        self._run_command(command_args)

    def snapshot(self):
        self._run_command(["snapshot"])

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
        output = self._run_command(
            command_args=command_args,
            capture_output=capture_output,
            vars=vars,
            quiet=quiet,
        )
        if log_errors:
            logger.error(f'Failed to run macro: "{macro_name}"\nRun output: {output}')
        run_operation_results = []
        if capture_output and output:
            json_messages = output.splitlines()
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
        exclude: Optional[str] = None,
        full_refresh: bool = False,
        vars: Optional[dict] = None,
        capture_output: bool = False,
        quiet: bool = False,
    ) -> Optional[str]:
        command_args = ["run"]
        if full_refresh:
            command_args.append("--full-refresh")
        if models:
            command_args.extend(["-m", models])
        if select:
            command_args.extend(["-s", select])
        if exclude:
            command_args.extend(["--exclude", exclude])
        output = self._run_command(
            command_args=command_args,
            vars=vars,
            quiet=quiet,
            capture_output=capture_output,
        )
        return output

    def test(
        self,
        select: Optional[str] = None,
        vars: Optional[dict] = None,
        quiet: bool = False,
        capture_output: bool = False,
    ) -> Optional[str]:
        command_args = ["test"]
        if select:
            command_args.extend(["-s", select])
        output = self._run_command(
            command_args=command_args,
            vars=vars,
            quiet=quiet,
            capture_output=capture_output,
        )
        return output

    def debug(self, quiet: bool = False):
        self._run_command(command_args=["debug"], quiet=quiet)

    def ls(self, select: Optional[str] = None) -> List[str]:
        command_args = ["-q", "ls"]
        if select:
            command_args.extend(["-s", select])
        try:
            output = self._run_command(
                command_args=command_args, capture_output=True, log_format="text"
            ).splitlines()
            # ls command didn't match nodes.
            # When no node is matched, ls command returns 2 dicts with warning message that there are no matches.
            if (
                len(output) == 2
                and try_load_json(output[0])
                and try_load_json(output[1])
            ):
                logger.warning(
                    f"The selection criterion '{select}' does not match any nodes"
                )
                return []
            # When nodes are matched, ls command returns strings of the node names.
            else:
                return output
        except DbtCommandError:
            raise DbtLsCommandError(select)

    def source_freshness(self):
        self._run_command(command_args=["source", "freshness"])

    def _get_command_env(self):
        env = os.environ.copy()
        if self.dbt_env_vars is not None:
            env.update(self.dbt_env_vars)
        return env
