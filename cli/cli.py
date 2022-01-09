import click
from pyfiglet import Figlet
import os

f = Figlet(font='slant')
print(f.renderText('Elementary'))

root_folder = os.path.join(os.path.dirname(__file__), '..')
modules = ['lineage', 'observability']


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


cli = ElementaryCLI(help='Open source data reliability solution')

if __name__ == '__main__':
    cli()
