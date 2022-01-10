import os
from pathlib import Path
from typing import Optional

import click
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


def get_run_properties() -> dict:

    click_context = click.get_current_context()
    if click_context is None:
        return dict()

    params = click_context.params
    if params is None:
        return dict()

    start_date = params.get('start_date')
    end_date = params.get('end_date')
    is_filtered = params.get('table') is not None

    start_date_str = None
    if start_date is not None:
        start_date_str = start_date.isoformat()

    end_date_str = None
    if end_date is not None:
        end_date_str = end_date.isoformat()

    return {'start_date': start_date_str,
            'end_date': end_date_str,
            'is_filtered': is_filtered,
            'open_browser': params.get('open_browser'),
            'export_query_history': params.get('export_query_history'),
            'full_table_names': params.get('full_table_names'),
            'direction': params.get('direction'),
            'depth': params.get('depth'),
            'ignore_schema': params.get('ignore_schema'),
            'dbt_installed': is_dbt_installed(),
            'version': get_package_version()}


def format_milliseconds(duration: int) -> str:

    seconds = int((duration / MILLISECONDS_IN_SEC) % 60)
    minutes = int((duration / MILLISECONDS_IN_MIN) % 60)
    hours = int(duration / MILLISECONDS_IN_HOUR)

    remaining_milliseconds = duration - (hours * MILLISECONDS_IN_HOUR + minutes * MILLISECONDS_IN_MIN +
                                         seconds * MILLISECONDS_IN_SEC)

    return f'{hours}h:{minutes}m:{seconds}s:{remaining_milliseconds}ms'


