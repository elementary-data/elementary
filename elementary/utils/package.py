from typing import Optional

import click
import pkg_resources
import requests
from elementary.utils.log import get_logger
from packaging import version

_PYPI_URL = "https://pypi.org/pypi/elementary-data/json"

logger = get_logger(__name__)


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
            f"You are using incompatible versions between 'edr' ({py_pkg_ver}) and Elementary's dbt package ({dbt_pkg_ver}).\n"
            "Please change the major and minor versions to align.\n",
            fg="yellow",
        )
    else:
        logger.debug(
            f"Python ({py_pkg_ver}) and dbt ({dbt_pkg_ver}) versions are compatible."
        )
