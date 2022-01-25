import click
from pyfiglet import Figlet
import os
from os.path import expanduser

from config.config import Config
from tracking.anonymous_tracking import AnonymousTracking, track_cli_help

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
        with open(fn) as f:
            code = compile(f.read(), fn, 'exec')
            eval(code, ns, ns)
        return ns[name]

    def format_help(self, ctx, formatter):
        try:
            config = Config(config_dir=os.path.join(expanduser('~'), '.edr'),
                            profiles_dir=os.path.join(expanduser('~'), '.dbt'),
                            profile_name='elementary')
            anonymous_tracking = AnonymousTracking(config)
            track_cli_help(anonymous_tracking)
        except Exception:
            pass
        self.format_usage(ctx, formatter)
        self.format_help_text(ctx, formatter)
        self.format_options(ctx, formatter)
        self.format_epilog(ctx, formatter)


cli = ElementaryCLI(help='Open source data reliability solution (https://docs.elementary-data.com/)')

if __name__ == '__main__':
    cli()
