import os
from pathlib import Path
from typing import Optional
import pkg_resources

DBT_DIR = ".dbt"

MILLISECONDS_IN_SEC = 1000
MILLISECONDS_IN_MIN = (1000 * 60)
MILLISECONDS_IN_HOUR = (1000 * 60 * 60)


def is_dbt_installed() -> bool:
    if os.path.exists(os.path.join(Path.home(), DBT_DIR)):
        return True
    return False


def get_package_version() -> Optional[str]:
    try:
        return pkg_resources.get_distribution('elementary-lineage').version
    except Exception:
        pass

    return None


def format_milliseconds(duration: int) -> str:

    seconds = int((duration / MILLISECONDS_IN_SEC) % 60)
    minutes = int((duration / MILLISECONDS_IN_MIN) % 60)
    hours = int(duration / MILLISECONDS_IN_HOUR)

    remaining_milliseconds = duration - (hours * MILLISECONDS_IN_HOUR + minutes * MILLISECONDS_IN_MIN +
                                         seconds * MILLISECONDS_IN_SEC)

    return f'{hours}h:{minutes}m:{seconds}s:{remaining_milliseconds}ms'


