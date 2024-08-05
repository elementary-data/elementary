import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.dbt.dbt_log import parse_dbt_output
from elementary.exceptions.exceptions import DbtCommandError, DbtLsCommandError
from elementary.monitor.dbt_project_utils import is_dbt_package_up_to_date
from elementary.utils.env_vars import is_debug
from elementary.utils.log import get_logger

logger = get_logger(__name__)

MACRO_RESULT_PATTERN = re.compile(
    "Elementary: --ELEMENTARY-MACRO-OUTPUT-START--(.*)--ELEMENTARY-MACRO-OUTPUT-END--"
)
RAW_EDR_LOGS_PATTERN = re.compile("Elementary: (.*)")


@dataclass
class DbtCommandResult:
    success: bool
    output: Optional[str]


class CommandLineDbtRunner(BaseDbtRunner):
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
        run_deps_if_needed: bool = True,
        force_dbt_deps: bool = False,
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
        if force_dbt_deps:
            self.deps()
        elif run_deps_if_needed:
            self._run_deps_if_needed()

    def _inner_run_command(
        self,
        dbt_command_args: List[str],
        capture_output: bool,
        quiet: bool,
        log_output: bool,
        log_format: str,
    ) -> DbtCommandResult:
        raise NotImplementedError

    def _parse_ls_command_result(
        self, select: Optional[str], result: DbtCommandResult
    ) -> List[str]:
        raise NotImplementedError

    def _run_command(
        self,
        command_args: List[str],
        capture_output: bool = False,
        log_format: str = "json",
        vars: Optional[dict] = None,
        quiet: bool = False,
        log_output: bool = True,
    ) -> DbtCommandResult:
        dbt_command_args = []
        if capture_output:
            dbt_command_args.extend(["--log-format", log_format])
        dbt_command_args.extend(command_args)
        dbt_command_args.extend(["--project-dir", os.path.abspath(self.project_dir)])
        if self.profiles_dir:
            dbt_command_args.extend(
                ["--profiles-dir", os.path.abspath(self.profiles_dir)]
            )
        if self.target:
            dbt_command_args.extend(["--target", self.target])

        all_vars = self._get_all_vars(vars)
        if all_vars:
            log_command_args = dbt_command_args.copy()
            log_command_args.extend(
                [
                    "--vars",
                    json.dumps(self._get_secret_masked_vars(all_vars)),
                ]
            )
            dbt_command_args.extend(["--vars", json.dumps(all_vars)])
        else:
            log_command_args = dbt_command_args

        log_msg = f"Running dbt command {' '.join(log_command_args)}"
        if not quiet:
            logger.info(log_msg)
        else:
            logger.debug(log_msg)

        result = self._inner_run_command(
            dbt_command_args,
            capture_output=capture_output,
            quiet=quiet,
            log_output=log_output,
            log_format=log_format,
        )

        if capture_output and result.output:
            logger.debug(
                f"Result bytes size for command '{log_command_args}' is {len(result.output)}"
            )
            if log_output or is_debug():
                for log in parse_dbt_output(result.output, log_format):
                    logger.info(log.msg)

        return result

    def deps(self, quiet: bool = False, capture_output: bool = True) -> bool:
        result = self._run_command(
            command_args=["deps"], quiet=quiet, capture_output=capture_output
        )
        return result.success

    def seed(self, select: Optional[str] = None, full_refresh: bool = False) -> bool:
        command_args = ["seed"]
        if full_refresh:
            command_args.append("--full-refresh")
        if select:
            command_args.extend(["-s", select])
        result = self._run_command(command_args)
        return result.success

    def snapshot(self) -> bool:
        result = self._run_command(["snapshot"])
        return result.success

    def run_operation(
        self,
        macro_name: str,
        capture_output: bool = True,
        macro_args: Optional[dict] = None,
        log_errors: bool = True,
        vars: Optional[dict] = None,
        quiet: bool = False,
        log_output: bool = False,
        return_raw_edr_logs: bool = False,
    ) -> list:
        if "." not in macro_name and not self.allow_macros_without_package_prefix:
            raise ValueError(
                f"Macro name '{macro_name}' is missing package prefix. "
                f"Please use the following format: <package_name>.<macro_name>"
            )
        macro_to_run = macro_name
        macro_to_run_args = macro_args if macro_args else dict()
        if not return_raw_edr_logs:
            macro_to_run = "elementary.log_macro_results"
            macro_to_run_args = dict(
                macro_name=macro_name, macro_args=macro_args if macro_args else dict()
            )
        command_args = ["run-operation", macro_to_run]
        json_args = json.dumps(macro_to_run_args, ensure_ascii=False)
        command_args.extend(["--args", json_args])
        result = self._run_command(
            command_args=command_args,
            capture_output=capture_output,
            vars=vars,
            quiet=quiet,
            log_output=log_output,
        )
        if log_errors and not result.success:
            logger.error(
                f'Failed to run macro: "{macro_name}"\nRun output: {result.output}'
            )
        run_operation_results = []

        log_pattern = (
            RAW_EDR_LOGS_PATTERN if return_raw_edr_logs else MACRO_RESULT_PATTERN
        )
        if capture_output and result.output is not None:
            for log in parse_dbt_output(result.output):
                if log_errors and log.level == "error":
                    logger.error(log.msg)
                    continue

                if log.msg:
                    match = log_pattern.match(log.msg)
                    if match:
                        run_operation_results.append(match.group(1))

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
        result = self._run_command(
            command_args=command_args,
            vars=vars,
            quiet=quiet,
            capture_output=capture_output,
        )
        return result.success

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
        result = self._run_command(
            command_args=command_args,
            vars=vars,
            quiet=quiet,
            capture_output=capture_output,
        )
        return result.success

    def debug(self, quiet: bool = False) -> bool:
        result = self._run_command(command_args=["debug"], quiet=quiet)
        return result.success

    def retry(self, quiet: bool = False) -> bool:
        result = self._run_command(command_args=["retry"], quiet=quiet)
        return result.success

    def ls(self, select: Optional[str] = None) -> list:
        command_args = ["-q", "ls"]
        if select:
            command_args.extend(["-s", select])
        try:
            result = self._run_command(
                command_args=command_args, capture_output=True, log_format="text"
            )
            return self._parse_ls_command_result(select, result)
        except DbtCommandError:
            raise DbtLsCommandError(select)

    def source_freshness(self) -> bool:
        result = self._run_command(command_args=["source", "freshness"])
        return result.success

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
        if not os.path.exists(self.project_dir):
            return

        should_run_deps = False

        installed_package_names = set(self._get_installed_packages_names())
        required_package_names = set(self._get_required_packages_names())
        if not required_package_names.issubset(installed_package_names):
            logger.info("Installing packages for edr internal dbt package...")
            should_run_deps = True
        elif not is_dbt_package_up_to_date(self.project_dir):
            # Run deps also if Elementary's dbt package is not up-to-date
            # NOTE - we can't do this check for all packages, because the version in dbt_project.yaml is not enforced to be the same
            #        as the dbt hub version (but for our package we do ensure they are aligned)
            logger.info("edr internal dbt package is not up-to-date, updating it...")
            should_run_deps = True

        if should_run_deps:
            self.deps()
