import importlib.metadata
from typing import Optional

import requests

_PYPI_URL = "https://pypi.org/pypi/elementary-data/json"


def get_package_version() -> str:
    return importlib.metadata.version("elementary-data")


def get_latest_package_version() -> Optional[str]:
    try:
        resp = requests.get(_PYPI_URL, timeout=5)
        resp.raise_for_status()
        return resp.json().get("info", {}).get("version")
    except Exception:
        return None
