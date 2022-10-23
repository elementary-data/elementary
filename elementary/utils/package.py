import bisect
from typing import Optional, Dict

import pkg_resources
import requests
from packaging import version

from elementary.exceptions.exceptions import IncompatibleDbtPackageError

_PYPI_URL = "https://pypi.org/pypi/elementary-data/json"
_COMPATIBILITY_LAYER_URL = (
    "https://storage.googleapis.com/elementary_static/edr_dbt_compatibility_matrix.json"
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


def _get_compatibility_matrix() -> Dict[version.Version, Dict[str, version.Version]]:
    versioned_compatibility_matrix = {}
    str_compatibility_matrix = requests.get(_COMPATIBILITY_LAYER_URL).json()
    for edr_ver, dbt_ver_range in str_compatibility_matrix.items():
        versioned_compatibility_matrix[version.parse(edr_ver)] = {
            "min": version.parse(dbt_ver_range["min"]),
            "max": version.parse(dbt_ver_range["max"]),
        }
    return versioned_compatibility_matrix


def check_dbt_pkg_compatible(current_dbt_pkg_ver: str):
    current_dbt_pkg_ver = version.parse(current_dbt_pkg_ver)
    current_py_pkg_ver = version.parse(get_package_version())
    compatibility_matrix = _get_compatibility_matrix()
    sorted_py_pkg_vers = sorted(compatibility_matrix)
    relevant_py_pkg_index = bisect.bisect(sorted_py_pkg_vers, current_py_pkg_ver) - 1
    if relevant_py_pkg_index == -1:
        return
    dbt_pkg_range = compatibility_matrix[sorted_py_pkg_vers[relevant_py_pkg_index]]

    if not (dbt_pkg_range["min"] <= current_dbt_pkg_ver <= dbt_pkg_range["max"]):
        raise IncompatibleDbtPackageError(current_py_pkg_ver, dbt_pkg_range)
