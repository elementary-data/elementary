import json
import os
import subprocess
from typing import Any, Dict, List, Optional, Tuple

import yaml

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.dbt.dbt_log import parse_dbt_output
from elementary.exceptions.exceptions import DbtCommandError, DbtLsCommandError
from elementary.utils.env_vars import is_debug
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class DbtRunner(BaseDbtRunner):
    ELEMENTARY_LOG_PREFIX = "Elementary: "

    def __init__(
        self,
        project_dir: str,
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
        raise_on_failure: bool = True,
        env_vars: Optional[Dict[str, str]] = None,
        vars: Optional[Dict[str, Any]] = None,
        secret_vars: Optional[Dict[str, Any]] = None,
        allow_macros_without_package_prefix: bool = False,
    ) -> None:
        super().__init__(
            project_dir,
            profiles_dir,
            target,
            vars,
            secret_vars,
            allow_macros_without_package_prefix,
        )
        self.raise_on_failure = raise_on_failure
        self.env_vars = env_vars
        self._run_deps_if_needed()

    def _run_command(
        self,
        command_args: List[str],
        capture_output: bool = False,
        log_format: str = "json",
        vars: Optional[dict] = None,
        quiet: bool = False,
        log_output: bool = True,
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

        all_vars = self._get_all_vars(vars)
        if all_vars:
            log_command = dbt_command.copy()
            log_command.extend(
                [
                    "--vars",
                    json.dumps(self._get_secret_masked_vars(all_vars)),
                ]
            )
            dbt_command.extend(["--vars", json.dumps(all_vars)])
        else:
            log_command = dbt_command

        log_msg = f"Running {' '.join(log_command)}"
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
            logs = list(parse_dbt_output(err.output.decode())) if err.output else []
            if capture_output and (log_output or is_debug()):
                for log in logs:
                    logger.info(log.msg)
            raise DbtCommandError(err, command_args, logs=logs)

        output = None
        if capture_output:
            output = result.stdout.decode("utf-8")
            logger.debug(
                f"Result bytes size for command '{log_command}' is {len(result.stdout)}"
            )
            if log_output or is_debug():
                for log in parse_dbt_output(output):
                    logger.info(log.msg)

        if result.returncode != 0:
            return False, output
        return True, output

    def deps(self, quiet: bool = False, capture_output: bool = True) -> bool:
        success, _ = self._run_command(
            command_args=["deps"], quiet=quiet, capture_output=capture_output
        )
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
        log_output: bool = False,
    ) -> list:
        if "." not in macro_name and not self.allow_macros_without_package_prefix:
            raise ValueError(
                f"Macro name '{macro_name}' is missing package prefix. "
                f"Please use the following format: <package_name>.<macro_name>"
            )
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
            log_output=log_output,
        )
        if log_errors and not success:
            logger.error(
                f'Failed to run macro: "{macro_name}"\nRun output: {command_output}'
            )
        run_operation_results = []
        if capture_output and command_output is not None:
            for log in parse_dbt_output(command_output):
                if log_errors and log.level == "error":
                    logger.error(log.msg)
                    continue
                if log.msg and log.msg.startswith(self.ELEMENTARY_LOG_PREFIX):
                    run_operation_results.append(
                        log.msg[len(self.ELEMENTARY_LOG_PREFIX) :]
                    )
        return run_operation_results

    def run(
        self,
        models: Optional[str] = None,
        select: Optional[str] = None,
        selector: Optional[str] = None,
        full_refresh: bool = False,
        vars: Optional[dict] = None,
        quiet: bool = False,
        capture_output: bool = False,
    ) -> bool:
        command_args = ["run"]
        if full_refresh:
            command_args.append("--full-refresh")
        if models:
            command_args.extend(["-m", models])
        if select:
            command_args.extend(["-s", select])
        if selector:
            command_args.extend(["--selector", selector])
        success, _ = self._run_command(
            command_args=command_args,
            vars=vars,
            quiet=quiet,
            capture_output=capture_output,
        )
        return success

    def test(
        self,
        select: Optional[str] = None,
        vars: Optional[dict] = None,
        quiet: bool = False,
        capture_output: bool = False,
    ) -> bool:
        command_args = ["test"]
        if select:
            command_args.extend(["-s", select])
        success, _ = self._run_command(
            command_args=command_args,
            vars=vars,
            quiet=quiet,
            capture_output=capture_output,
        )
        return success

    def _get_command_env(self):
        env = os.environ.copy()
        if self.env_vars is not None:
            env.update(self.env_vars)
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

    def _get_installed_packages_names(self):
        packages_dir = os.path.join(
            self.project_dir, os.environ.get("DBT_PACKAGES_FOLDER", "dbt_packages")
        )
        try:
            return [
                name
                for name in os.listdir(packages_dir)
                if os.path.isdir(os.path.join(packages_dir, name))
            ]
        except FileNotFoundError:
            return []

    def _get_required_packages_names(self):
        packages_yaml_path = os.path.join(self.project_dir, "packages.yml")
        if not os.path.exists(packages_yaml_path):
            return []

        with open(packages_yaml_path) as packages_yaml_file:
            packages_data = yaml.safe_load(packages_yaml_file)
        return [
            package_entry["package"].split("/")[-1]
            for package_entry in packages_data["packages"]
            if "package" in package_entry
        ]

    def _run_deps_if_needed(self):
        installed_package_names = set(self._get_installed_packages_names())
        required_package_names = set(self._get_required_packages_names())
        if not required_package_names.issubset(installed_package_names):
            self.deps()
