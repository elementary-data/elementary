import os
import sys
from os.path import expanduser

import click
from packaging import version
from pyfiglet import Figlet

from config.config import Config
from tracking.anonymous_tracking import AnonymousTracking, track_cli_help
from utils import package

f = Figlet(font='slant')
print(f.renderText('Elementary'))

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
            config = Config(config_dir=os.path.join(expanduser('~'), '.edr'),
                            profiles_dir=os.path.join(expanduser('~'), '.dbt'))
            self.recommend_version_upgrade()
            anonymous_tracking = AnonymousTracking(config)
            track_cli_help(anonymous_tracking)
        except Exception:
            pass
        self.format_usage(ctx, formatter)
        self.format_help_text(ctx, formatter)
        self.format_options(ctx, formatter)
        self.format_epilog(ctx, formatter)

    def recommend_version_upgrade(self):
        latest_version = package.get_latest_package_version()
        current_version = package.get_package_version()
        try:
            if version.parse(current_version) < version.parse(latest_version):
                self.epilog = click.style(
                    f'You are using Elementary {current_version}, however version {latest_version} is available.\n'
                    f'Consider upgrading by running: "{sys.executable} -m pip install --upgrade elementary-data"',
                    fg='yellow'
                )
        except Exception:
            pass


cli = ElementaryCLI(help='Open source data reliability solution (https://docs.elementary-data.com/)')

if __name__ == '__main__':
    cli()
