import os
import sys

import click


@click.command()
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def code(ctx, args):
    """Launch edr-code, the data engineering CLI agent."""
    try:
        from elementary_code import _find_binary  # type: ignore[import-untyped]
    except ImportError:
        click.echo(
            "edr-code is not installed. Install it with:\n\n"
            '  pip install "elementary-data[code]"\n',
            err=True,
        )
        sys.exit(1)

    binary = _find_binary()
    os.execv(str(binary), [str(binary)] + list(args))
