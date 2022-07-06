from typing import Optional

import pkg_resources
import requests


def get_package_version() -> Optional[str]:
    try:
        return pkg_resources.get_distribution('elementary-data').version
    except Exception:
        pass

    return None


def get_latest_package_version() -> Optional[str]:
    resp = requests.get('https://pypi.org/pypi/elementary-data/json')
    try:
        resp.raise_for_status()
        return resp.json()['info']['version']
    except requests.HTTPError:
        return None
