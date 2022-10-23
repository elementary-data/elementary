import bisect
from typing import Optional, Dict

import pkg_resources
import requests
from packaging import version

from elementary.exceptions.exceptions import IncompatibleDbtPackageError

_PYPI_URL = "https://pypi.org/pypi/elementary-data/json"
_COMPATIBILITY_MAP_URL = (
    "https://storage.googleapis.com/elementary_static/edr_dbt_compatibility_map.json"
)


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


def _get_compatibility_map() -> Dict[version.Version, Dict[str, version.Version]]:
    versioned_compatibility_map = {}
    str_compatibility_map = requests.get(_COMPATIBILITY_MAP_URL).json()
    for edr_ver, dbt_ver_range in str_compatibility_map.items():
        versioned_compatibility_map[version.parse(edr_ver)] = {
            "min": version.parse(dbt_ver_range["min"]),
            "max": version.parse(dbt_ver_range["max"]),
        }
    return versioned_compatibility_map


def check_dbt_pkg_compatible(current_dbt_pkg_ver: str):
    current_dbt_pkg_ver = version.parse(current_dbt_pkg_ver)
    current_py_pkg_ver = version.parse(get_package_version())
    compatibility_map = _get_compatibility_map()
    sorted_py_pkg_vers = sorted(compatibility_map)
    relevant_py_pkg_index = bisect.bisect(sorted_py_pkg_vers, current_py_pkg_ver) - 1
    if relevant_py_pkg_index == -1:
        return
    dbt_pkg_range = compatibility_map[sorted_py_pkg_vers[relevant_py_pkg_index]]
    incompatible_min = current_dbt_pkg_ver < dbt_pkg_range["min"]
    incompatible_max = (
        "max" in dbt_pkg_range and current_dbt_pkg_ver > dbt_pkg_range["max"]
    )
    if incompatible_min or incompatible_max:
        raise IncompatibleDbtPackageError(current_py_pkg_ver, dbt_pkg_range)
