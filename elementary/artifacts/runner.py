import types
from typing import Optional

from elementary.clients.dbt.command_line_dbt_runner import CommandLineDbtRunner
from elementary.clients.dbt.factory import create_dbt_runner
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils


def create_artifacts_runner(
    config: Config, profile: Optional[str] = None
) -> CommandLineDbtRunner:
    runner = create_dbt_runner(
        dbt_project_utils.CLI_DBT_PROJECT_PATH,
        config.profiles_dir,
        config.profile_target,
        env_vars=config.env_vars,
        run_deps_if_needed=config.run_dbt_deps_if_needed,
    )
    if profile:
        _inject_profile_override(runner, profile)
    return runner


def _inject_profile_override(runner: CommandLineDbtRunner, profile: str) -> None:
    """Override the profile name from the internal dbt_project.yml.

    The internal elementary_cli project hardcodes `profile: elementary`, but
    artifact commands need to be runnable against any working profile in the
    user's profiles.yml (e.g. `elementary_analytics`). We inject `--profile X`
    into every dbt invocation by wrapping `_run_command`.
    """
    original = runner._run_command

    def patched(self, command_args, *args, **kwargs):
        new_args = list(command_args)
        if new_args:
            new_args = [new_args[0], "--profile", profile] + new_args[1:]
        return original(command_args=new_args, *args, **kwargs)

    runner._run_command = types.MethodType(patched, runner)
