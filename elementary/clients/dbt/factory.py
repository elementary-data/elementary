import os
from enum import Enum
from typing import Any, Dict, Optional, Type

from packaging import version

from elementary.clients.dbt.command_line_dbt_runner import CommandLineDbtRunner
from elementary.clients.dbt.dbt2_runner import Dbt2Runner
from elementary.clients.dbt.dbt_installation import (
    get_dbt_core_version,
    is_dbt2_binary_available,
)
from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner


class RunnerMethod(Enum):
    SUBPROCESS = "subprocess"
    API = "api"
    DBT2 = "dbt2"
    # Legacy alias for DBT2 (dbt 2.0 is the Fusion engine).
    FUSION = "fusion"


def create_dbt_runner(
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
    runner_method: Optional[RunnerMethod] = None,
) -> CommandLineDbtRunner:
    runner_method = runner_method or get_dbt_runner_method()
    runner_class = get_dbt_runner_class(runner_method)
    return runner_class(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
        raise_on_failure=raise_on_failure,
        env_vars=env_vars,
        vars=vars,
        secret_vars=secret_vars,
        allow_macros_without_package_prefix=allow_macros_without_package_prefix,
        run_deps_if_needed=run_deps_if_needed,
        force_dbt_deps=force_dbt_deps,
    )


def get_dbt_runner_method() -> RunnerMethod:
    runner_method = os.getenv("DBT_RUNNER_METHOD")
    if runner_method:
        return RunnerMethod(runner_method)

    dbt_core_version = get_dbt_core_version()
    if dbt_core_version is not None:
        if dbt_core_version.major >= 2:
            return RunnerMethod.DBT2
        if dbt_core_version >= version.Version("1.5.0"):
            return RunnerMethod.API
        return RunnerMethod.SUBPROCESS

    if is_dbt2_binary_available():
        return RunnerMethod.DBT2

    return RunnerMethod.SUBPROCESS


def get_dbt_runner_class(runner_method: RunnerMethod) -> Type[CommandLineDbtRunner]:
    if runner_method == RunnerMethod.API:
        # Import it internally since it will fail if dbt-core is not installed
        # or its version is below 1.5.0
        from elementary.clients.dbt.api_dbt_runner import APIDbtRunner

        return APIDbtRunner
    elif runner_method == RunnerMethod.SUBPROCESS:
        return SubprocessDbtRunner
    elif runner_method in (RunnerMethod.DBT2, RunnerMethod.FUSION):
        return Dbt2Runner
    else:
        raise ValueError(f"Invalid runner method: {runner_method}")
