import os
from typing import Any, Dict, Optional, Type

from dbt.version import __version__ as dbt_version_string
from packaging import version

from elementary.clients.dbt.command_line_dbt_runner import CommandLineDbtRunner

DBT_VERSION = version.Version(dbt_version_string)

RUNNER_CLASS: Type[CommandLineDbtRunner]
if (
    DBT_VERSION >= version.Version("1.5.0")
    and os.getenv("DBT_RUNNER_METHOD") != "subprocess"
):
    from elementary.clients.dbt.api_dbt_runner import APIDbtRunner

    RUNNER_CLASS = APIDbtRunner
else:
    from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner

    RUNNER_CLASS = SubprocessDbtRunner


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
) -> CommandLineDbtRunner:
    return RUNNER_CLASS(
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
