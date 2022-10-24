from typing import Optional

import click
import pkg_resources
import requests
from packaging import version

_PYPI_URL = "https://pypi.org/pypi/elementary-data/json"


def get_package_version() -> Optional[str]:
    try:
        return pkg_resources.get_distribution("elementary-data").version
    except Exception:
        return None


def get_latest_package_version() -> Optional[str]:
    try:
        resp = requests.get(_PYPI_URL)
        resp.raise_for_status()
        return resp.json().get("info", {}).get("version")
    except Exception:
        return None


def check_dbt_pkg_compatible(dbt_pkg_ver: str):
    dbt_pkg_ver = version.parse(dbt_pkg_ver)
    py_pkg_ver = version.parse(get_package_version())
    if dbt_pkg_ver.major != py_pkg_ver.major or dbt_pkg_ver.minor != py_pkg_ver.minor:
        click.secho(
            "You are using incompatible versions between 'edr' and Elementary's dbt package.\n"
            "Please change the major and minor versions to align.\n",
            fg="yellow",
        )
