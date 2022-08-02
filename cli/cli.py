import os

import click
from pyfiglet import Figlet

import cli.upgrade
from config.config import Config
from tracking.anonymous_tracking import AnonymousTracking

f = Figlet(font='slant')
click.echo(f.renderText('Elementary'))
cli.upgrade.recommend_version_upgrade()

root_folder = os.path.join(os.path.dirname(__file__), '..')
modules = ['lineage', 'monitor']


class ElementaryCLI(click.MultiCommand):

    def list_commands(self, ctx):
        rv = []
        for module in modules:
            rv.append(module)
        rv.sort()
        return rv

    def get_command(self, ctx, name):
        ns = {}
        fn = os.path.join(root_folder, name, 'cli.py')
        try:
            with open(fn) as f:
                code = compile(f.read(), fn, 'exec')
                eval(code, ns, ns)
        except Exception:
            return None
        return ns[name]

    def format_help(self, ctx, formatter):
        try:
            config = Config()
            AnonymousTracking(config).track_cli_help()
        except Exception:
            pass
        self.format_usage(ctx, formatter)
        self.format_help_text(ctx, formatter)
        self.format_options(ctx, formatter)
        self.format_epilog(ctx, formatter)


cli = ElementaryCLI(help='Open source data reliability solution (https://docs.elementary-data.com/)')

if __name__ == '__main__':
    cli()
