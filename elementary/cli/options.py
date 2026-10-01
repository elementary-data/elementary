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
    return dict(parsed)


def dbt_vars_option(func):
    return click.option(
        "--dbt-vars",
        type=str,
        default=None,
        callback=_parse_dbt_vars,
        help="Specify raw YAML string of dbt variables to pass to the edr internal dbt project.",
    )(func)
