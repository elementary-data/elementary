import os
from typing import Any

import click

import elementary.cli.logo
import elementary.cli.upgrade
from elementary.artifacts.cli import artifacts
from elementary.artifacts.common import is_artifacts_invocation
from elementary.config.config import Config
from elementary.monitor.cli import monitor, report, send_report
from elementary.operations.cli import run_operation
from elementary.tracking.anonymous_tracking import AnonymousCommandLineTracking
from elementary.utils import package
from elementary.utils.log import get_logger, set_root_logger_handlers

_ARTIFACTS_MODE = is_artifacts_invocation()

if not _ARTIFACTS_MODE:
    elementary.cli.logo.print_elementary_logo()
    elementary.cli.upgrade.recommend_version_upgrade()

logger = get_logger(__name__)


def get_log_path(ctx):
    target_path = Config.DEFAULT_TARGET_PATH
    try:
        ctx_args = ctx.args
        target_path_flag = "--target-path"
        target_path = ctx_args[ctx_args.index(target_path_flag) + 1]
    except (ValueError, IndexError):
        pass
    os.makedirs(os.path.abspath(target_path), exist_ok=True)
    return os.path.join(target_path, "edr.log")


def get_quiet_logs(ctx):
    try:
        return "--quiet-logs" in ctx.args
    except (ValueError, AttributeError):
        return False


class ElementaryCLI(click.Group):
    def get_command(self, ctx, name):
        ctx.auto_envvar_prefix = "EDR"
        return super().get_command(ctx, name)

    def format_help(self, ctx, formatter):
        try:
            if not _ARTIFACTS_MODE:
                click.echo("Loading dependencies (this might take a few seconds)")
            AnonymousCommandLineTracking(config=Config()).track_cli_help()
        except Exception:
            pass
        self.format_usage(ctx, formatter)
        self.format_help_text(ctx, formatter)
        self.format_options(ctx, formatter)
        self.format_epilog(ctx, formatter)

    def invoke(self, ctx: click.Context) -> Any:
        files_target_path = get_log_path(ctx)
        quiet_logs = get_quiet_logs(ctx) or _ARTIFACTS_MODE
        set_root_logger_handlers(
            "elementary",
            files_target_path,
            quiet_logs=quiet_logs,
            use_stderr=_ARTIFACTS_MODE,
        )
        if not quiet_logs and not _ARTIFACTS_MODE:
            click.echo(
                "Any feedback and suggestions are welcomed! join our community here - "
                "https://bit.ly/slack-elementary\n"
            )
            logger.info(f"Running with edr={package.get_package_version()}")
        return super().invoke(ctx)


@click.group(
    cls=ElementaryCLI,
    help="Open source data reliability solution (https://docs.elementary-data.com/)",
)
@click.version_option(
    version=package.get_package_version(),
    message="Elementary version %(version)s.",
)
def cli():
    pass


cli.add_command(monitor)
cli.add_command(report)
cli.add_command(send_report, name="send-report")
cli.add_command(run_operation, name="run-operation")
cli.add_command(artifacts)


if __name__ == "__main__":
    cli()
