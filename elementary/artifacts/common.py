import sys
from typing import Callable

import click

from elementary.config.config import Config


def is_artifacts_invocation() -> bool:
    """True when the current edr command is `edr artifacts ...`.

    Used at CLI bootstrap to gate stdout-polluting side effects (logo,
    version-upgrade banner, info logs) so `edr artifacts` can emit pure JSON.
    """
    argv = sys.argv[1:]
    for arg in argv:
        if arg.startswith("-"):
            continue
        return arg == "artifacts"
    return False


def common_options(func: Callable) -> Callable:
    func = click.option(
        "--output",
        "-o",
        type=click.Choice(["json", "table"]),
        default="json",
        help="Output format. JSON is default and intended for agent/script use.",
    )(func)
    func = click.option(
        "--target-path",
        type=str,
        default=Config.DEFAULT_TARGET_PATH,
        help="Absolute target path for saving edr files such as logs.",
    )(func)
    func = click.option(
        "--config-dir",
        "-c",
        type=click.Path(),
        default=Config.DEFAULT_CONFIG_DIR,
        help="Directory containing edr's config.yml.",
    )(func)
    func = click.option(
        "--profile",
        "profile_name",
        type=str,
        default=None,
        help=(
            "Override the profile name from profiles.yml (defaults to "
            "'elementary'). Useful when elementary artifact tables live "
            "in a warehouse configured under a different profile."
        ),
    )(func)
    func = click.option(
        "--profile-target",
        "-t",
        type=str,
        default=None,
        help="Which target to load from the selected profile.",
    )(func)
    func = click.option(
        "--profiles-dir",
        "-p",
        type=click.Path(exists=True),
        default=None,
        help="Directory containing profiles.yml. Defaults to CWD then HOME/.dbt/.",
    )(func)
    func = click.option(
        "--project-dir",
        type=click.Path(exists=True),
        default=None,
        help="Directory containing dbt_project.yml. Defaults to CWD.",
    )(func)
    return func


def build_config(
    config_dir: str,
    profiles_dir,
    project_dir,
    profile_target,
    target_path: str,
) -> Config:
    return Config(
        config_dir=config_dir,
        profiles_dir=profiles_dir,
        project_dir=project_dir,
        profile_target=profile_target,
        target_path=target_path,
        quiet_logs=True,
    )
