import click

from elementary.config.config import Config

ELEMENTARY_LOGO = r"""
    ________                          __
   / ____/ /__  ____ ___  ___  ____  / /_____ ________  __
  / __/ / / _ \/ __ `__ \/ _ \/ __ \/ __/ __ `/ ___/ / / /
 / /___/ /  __/ / / / / /  __/ / / / /_/ /_/ / /  / /_/ /
/_____/_/\___/_/ /_/ /_/\___/_/ /_/\__/\__,_/_/   \__, /
                                                 /____/
"""


def print_elementary_logo():
    config = Config()

    if config.disable_elementary_logo_print:
        return
    else:
        click.echo(f"{ELEMENTARY_LOGO}\n")
