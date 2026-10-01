from typing import Any, Dict, Optional

import click
from click import ClickException

from elementary.utils.ordered_yaml import OrderedYaml


class RequiredIf(click.Option):
    def __init__(self, *args, **kwargs):
        self.required_if = kwargs.pop("required_if")
        if not self.required_if:
            raise ClickException("'required_if' parameter is required")

        kwargs["help"] = (
            kwargs.get("help", "")
            + " NOTE: This argument must be configured together with %s."
            % self.required_if
        ).strip()
        super(RequiredIf, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        we_are_present = self.name in opts
        other_present = self.required_if in opts

        if we_are_present and not other_present:
            raise click.UsageError(
                "Illegal usage: `%s` must be configured with `%s`"
                % (self.name, self.required_if)
            )
        else:
            self.prompt = None

        return super(RequiredIf, self).handle_parse_result(ctx, opts, args)


def _parse_dbt_vars(
    ctx: click.Context, param: click.Parameter, value: Optional[str]
) -> Optional[Dict[str, Any]]:
    if not value:
        return None
    try:
        dbt_vars = OrderedYaml().loads(value)
    except Exception as exc:
        raise click.BadParameter(f"Invalid YAML: {exc}") from exc
    if not isinstance(dbt_vars, dict):
        raise click.BadParameter(
            "Must be a YAML mapping, for example '{query_max_size: 1000000}'."
        )
    return dbt_vars


dbt_vars_option = click.option(
    "--dbt-vars",
    type=str,
    default=None,
    callback=_parse_dbt_vars,
    help="Specify raw YAML string of your dbt variables. "
    "Applied to every run of the edr internal dbt project.",
)
