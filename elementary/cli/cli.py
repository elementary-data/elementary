import os
from typing import Any

import click

import elementary.cli.logo
import elementary.cli.upgrade
from elementary.config.config import Config
from elementary.monitor.cli import monitor, report, send_report
from elementary.operations.cli import run_operation
from elementary.tracking.anonymous_tracking import AnonymousCommandLineTracking
from elementary.utils import package
from elementary.utils.log import get_logger, set_root_logger_handlers

elementary.cli.logo.print_elementary_logo()
elementary.cli.upgrade.recommend_version_upgrade()

logger = get_logger(__name__)


def get_log_path(ctx):
    target_path = Config.DEFAULT_TARGET_PATH
    try:
        ctx_args = ctx.args
        target_path_flag = "--target-path"
        target_path = ctx_args[ctx_args.index(target_path_flag) + 1]
    finally:
        os.makedirs(os.path.abspath(target_path), exist_ok=True)
        return os.path.join(target_path, "edr.log")


class ElementaryCLI(click.MultiCommand):
    _CMD_MAP = {
        "monitor": monitor,
        "report": report,
        "send-report": send_report,
        "run-operation": run_operation,
    }

    def list_commands(self, ctx):
        return self._CMD_MAP.keys()

    def get_command(self, ctx, name):
        ctx.auto_envvar_prefix = "EDR"
        return self._CMD_MAP.get(name)

    def format_help(self, ctx, formatter):
        try:
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
        set_root_logger_handlers("elementary", files_target_path)
        click.echo(
            "Any feedback and suggestions are welcomed! join our community here - "
            "https://bit.ly/slack-elementary\n"
        )
        logger.info(f"Running with edr={package.get_package_version()}")
        return super().invoke(ctx)


@click.command(
    cls=ElementaryCLI,
    help="Open source data reliability solution (https://docs.elementary-data.com/)",
)
@click.version_option(
    version=package.get_package_version(),
    message="Elementary version %(version)s.",
)
def cli():
    pass


if __name__ == "__main__":
    cli()
