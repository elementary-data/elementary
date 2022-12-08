import os
from typing import Any

import click
from pyfiglet import Figlet

import elementary.cli.upgrade
from elementary.config.config import Config
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.utils import package
from elementary.utils.log import get_logger

f = Figlet(font="slant")
click.echo(f.renderText("Elementary"))
elementary.cli.upgrade.recommend_version_upgrade()

root_folder = os.path.join(os.path.dirname(__file__), "..")
modules = ["monitor"]

logger = get_logger(__name__)


class ElementaryCLI(click.MultiCommand):
    def list_commands(self, ctx):
        rv = []
        for module in modules:
            rv.append(module)
        rv.sort()
        return rv

    def get_command(self, ctx, name):
        ns = {}
        fn = os.path.join(root_folder, name, "cli.py")
        try:
            with open(fn) as f:
                code = compile(f.read(), fn, "exec")
                eval(code, ns, ns)
        except Exception:
            logger.debug(f'Unable to load the "{name}" module.', exc_info=True)
            return None
        return ns[name]

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
            f"Any feedback and suggestions are welcomed! join our community here - "
            f"https://bit.ly/slack-elementary\n"
        )
        logger.info(f"Running with edr={package.get_package_version()}")
        return super().invoke(ctx)


cli = ElementaryCLI(
    help="Open source data reliability solution (https://docs.elementary-data.com/)"
)

if __name__ == "__main__":
    cli()
