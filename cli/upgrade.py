import sys

import click
from packaging import version

from utils import package


def recommend_version_upgrade():
    latest_version = package.get_latest_package_version()
    current_version = package.get_package_version()
    try:
        if version.parse(current_version) < version.parse(latest_version):
            print(click.style(
                f'You are using Elementary {current_version}, however version {latest_version} is available.\n'
                f'Consider upgrading by running: "{sys.executable} -m pip install --upgrade elementary-data"\n',
                fg='yellow'
            ))
    except Exception:
        pass
