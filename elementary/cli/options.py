import json
from typing import Any, Dict, Optional

import click

from elementary.utils.ordered_yaml import OrderedYaml


def _parse_dbt_vars(
    ctx: click.Context, param: click.Parameter, value: Optional[str]
) -> Optional[Dict[str, Any]]:
    if not value:
        return None
    try:
        parsed = OrderedYaml().loads(value)
    except Exception as exc:
        raise click.BadParameter(f"Invalid YAML: {exc}") from exc
    if parsed is None:
        return None
    if not isinstance(parsed, dict):
        raise click.BadParameter("Must be a YAML mapping of variable names to values.")
    # YAML dates/timestamps aren't JSON serializable; pass them to dbt as ISO strings.
    try:
        return json.loads(json.dumps(parsed, default=str))
    except TypeError as exc:
        raise click.BadParameter(f"Unsupported value: {exc}") from exc


def dbt_vars_option(func):
    return click.option(
        "--dbt-vars",
        type=str,
        default=None,
        callback=_parse_dbt_vars,
        help="Specify raw YAML string of dbt variables to pass to the edr internal dbt project.",
    )(func)
