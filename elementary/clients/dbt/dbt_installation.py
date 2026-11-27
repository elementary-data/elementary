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


def get_dbt2_package_version() -> Optional[version.Version]:
    """Version of the installed dbt v2 package, or None if not installed.

    dbt v2 (the Fusion engine) is distributed on PyPI as the `dbt` package (full
    feature set) and the `dbt-oss` package (Apache 2 subset); both ship the dbt
    binary. dbt-core stays on 1.x.
    """
    for package_name in ("dbt", "dbt-oss"):
        package_version = _get_package_version(package_name)
        if package_version is not None and package_version.major >= 2:
            return package_version
    return None


def is_dbt2_binary_available() -> bool:
    env_path = os.getenv(DBT_FUSION_PATH_ENV_VAR)
    if env_path and os.path.exists(os.path.expanduser(env_path)):
        return True

    if get_dbt2_package_version() is not None:
        return True
    return os.path.exists(os.path.expanduser(DEFAULT_DBT_FUSION_PATH))


def get_dbt2_binary_path() -> str:
    env_path = os.getenv(DBT_FUSION_PATH_ENV_VAR)
    if env_path:
        return os.path.expanduser(env_path)

    # When only dbt-core 1.x is installed, the `dbt` executable on PATH is its
    # entrypoint, so it can't be trusted to be the dbt 2.0 binary.
    dbt_core_version = get_dbt_core_version()
    dbt2_installed_via_pip = get_dbt2_package_version() is not None or (
        dbt_core_version is not None and dbt_core_version.major >= 2
    )
    if dbt2_installed_via_pip or dbt_core_version is None:
        which_path = shutil.which("dbt")
        if which_path:
            return which_path

    return os.path.expanduser(DEFAULT_DBT_FUSION_PATH)
