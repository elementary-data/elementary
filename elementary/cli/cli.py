from typing import Any

import click
from pyfiglet import Figlet

import elementary.cli.upgrade
from elementary.config.config import Config
from elementary.monitor.cli import monitor, report, send_report
from elementary.operations.cli import run_operation
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.utils import package
from elementary.utils.log import get_logger

f = Figlet(font="slant")
click.echo(f.renderText("Elementary"))
elementary.cli.upgrade.recommend_version_upgrade()

logger = get_logger(__name__)


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
            AnonymousTracking(config=Config()).track_cli_help()
        except Exception:
            pass
        self.format_usage(ctx, formatter)
        self.format_help_text(ctx, formatter)
        self.format_options(ctx, formatter)
        self.format_epilog(ctx, formatter)

    def invoke(self, ctx: click.Context) -> Any:
        click.echo(
            "Any feedback and suggestions are welcomed! join our community here - "
            "https://bit.ly/slack-elementary\n"
        )
        logger.info(f"Running with edr={package.get_package_version()}")
        return super().invoke(ctx)


cli = ElementaryCLI(
    help="Open source data reliability solution (https://docs.elementary-data.com/)"
)

if __name__ == "__main__":
    cli()
