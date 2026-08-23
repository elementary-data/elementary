import os
import shutil
from importlib import metadata
from typing import Optional

from packaging import version

DBT_FUSION_PATH_ENV_VAR = "DBT_FUSION_PATH"
DEFAULT_DBT_FUSION_PATH = "~/.local/bin/dbt"


def _get_package_version(package_name: str) -> Optional[version.Version]:
    try:
        return version.Version(metadata.version(package_name))
    except (metadata.PackageNotFoundError, version.InvalidVersion):
        return None


def get_dbt_core_version() -> Optional[version.Version]:
    """Version of the installed `dbt-core` package, or None if not installed."""
    return _get_package_version("dbt-core")


def get_dbt_package_version() -> Optional[version.Version]:
    """Version of the installed `dbt` package, or None if not installed.

    From 2.0, the `dbt` package on PyPI ships the dbt (Fusion) binary as a
    platform wheel with no importable Python module.
    """
    return _get_package_version("dbt")


def is_dbt2_binary_available() -> bool:
    dbt_package_version = get_dbt_package_version()
    if dbt_package_version is not None and dbt_package_version.major >= 2:
        return True
    return os.path.exists(os.path.expanduser(DEFAULT_DBT_FUSION_PATH))


def get_dbt2_binary_path() -> str:
    env_path = os.getenv(DBT_FUSION_PATH_ENV_VAR)
    if env_path:
        return os.path.expanduser(env_path)

    # When only dbt-core 1.x is installed, the `dbt` executable on PATH is its
    # entrypoint, so it can't be trusted to be the dbt 2.0 binary.
    dbt_core_version = get_dbt_core_version()
    dbt_package_version = get_dbt_package_version()
    dbt2_installed_via_pip = (
        dbt_package_version is not None and dbt_package_version.major >= 2
    ) or (dbt_core_version is not None and dbt_core_version.major >= 2)
    if dbt2_installed_via_pip or dbt_core_version is None:
        which_path = shutil.which("dbt")
        if which_path:
            return which_path

    return os.path.expanduser(DEFAULT_DBT_FUSION_PATH)
