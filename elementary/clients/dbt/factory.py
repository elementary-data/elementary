from typing import Any, Optional

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner


def get_dbt_runner(
    project_dir: str,
    profiles_dir: Optional[str] = None,
    target: Optional[str] = None,
    raise_on_failure: bool = True,
    env_vars: Optional[dict[str, str]] = None,
    vars: Optional[dict[str, Any]] = None,
    secret_vars: Optional[dict[str, Any]] = None,
    allow_macros_without_package_prefix: bool = False,
    run_deps_if_needed: bool = True,
    force_dbt_deps: bool = False,
) -> BaseDbtRunner:
    return SubprocessDbtRunner(
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
