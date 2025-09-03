import os
from enum import Enum
from typing import Any, Dict, Optional, Type

from dbt.version import __version__ as dbt_version_string
from packaging import version

from elementary.clients.dbt.command_line_dbt_runner import CommandLineDbtRunner
from elementary.clients.dbt.dbt_fusion_runner import DbtFusionRunner
from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner

DBT_VERSION = version.Version(dbt_version_string)


class RunnerMethod(Enum):
    SUBPROCESS = "subprocess"
    API = "api"
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

    if DBT_VERSION >= version.Version("1.5.0"):
        return RunnerMethod.API
    return RunnerMethod.SUBPROCESS


def get_dbt_runner_class(runner_method: RunnerMethod) -> Type[CommandLineDbtRunner]:
    if runner_method == RunnerMethod.API:
        # Import it internally since it will fail if the dbt version is below 1.5.0
        from elementary.clients.dbt.api_dbt_runner import APIDbtRunner

        return APIDbtRunner
    elif runner_method == RunnerMethod.SUBPROCESS:
        return SubprocessDbtRunner
    elif runner_method == RunnerMethod.FUSION:
        return DbtFusionRunner
    else:
        raise ValueError(f"Invalid runner method: {runner_method}")
