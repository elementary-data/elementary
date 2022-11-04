import sys

import click
from packaging import version

from elementary.utils import package


def recommend_version_upgrade():
    try:
        latest_version = package.get_latest_package_version()
        current_version = package.get_package_version()
        if version.parse(current_version) < version.parse(latest_version):
            click.secho(
                f"You are using Elementary {current_version}, however version {latest_version} is available.\n"
                f'Consider upgrading by running: "{sys.executable} -m pip install --upgrade elementary-data"\n',
                fg="yellow",
            )
    except Exception:
        pass
