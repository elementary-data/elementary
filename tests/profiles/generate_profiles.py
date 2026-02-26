#!/usr/bin/env python3
"""Generate ~/.dbt/profiles.yml from a Jinja2 template and an optional secrets JSON."""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Any

import click
import yaml
from jinja2 import BaseLoader, Environment, Undefined


class _NullUndefined(Undefined):
    """Render missing variables as empty strings so docker-only runs don't crash."""

    def __str__(self) -> str:
        return ""

    def __iter__(self):
        return iter([])

    def __bool__(self) -> bool:
        return False


def _yaml_inline(value: Any) -> str:
    """Dump *value* as a compact inline YAML scalar / mapping."""
    if isinstance(value, Undefined):
        return "{}"
    return yaml.dump(value, default_flow_style=True).strip()


@click.command()
@click.option(
    "--template",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to the Jinja2 profiles template (e.g. profiles.yml.j2).",
)
@click.option(
    "--output",
    required=True,
    type=click.Path(dir_okay=False, path_type=Path),
    help="Destination path for the rendered profiles.yml.",
)
@click.option(
    "--schema-name",
    required=True,
    help="Base schema name (e.g. dbt_pkg_<ref> or py_<ref>).",
)
@click.option(
    "--secrets-json-env",
    default="CI_WAREHOUSE_SECRETS",
    show_default=True,
    help="Name of the env-var holding the base64-encoded JSON secrets blob.",
)
@click.option(
    "--profiles-yml-env",
    default="",
    help="Name of an env-var holding a legacy base64-encoded profiles.yml (fallback).",
)
def main(
    template: Path,
    output: Path,
    schema_name: str,
    secrets_json_env: str,
    profiles_yml_env: str,
) -> None:
    """Render a Jinja2 profiles template into a dbt profiles.yml file.

    Resolution order:
      1. If the env-var named by ``--secrets-json-env`` is set, decode it and
         use its key/value pairs (plus *schema_name*) as template variables.
      2. Else if ``--profiles-yml-env`` names a non-empty env-var, decode that
         as a legacy base64 profiles.yml and write it directly (replacing
         ``<SCHEMA_NAME>`` with *schema_name*).
      3. Otherwise render the template with only *schema_name* populated (all
         other variables resolve to empty strings — suitable for docker-only
         targets on fork PRs).
    """
    output.parent.mkdir(parents=True, exist_ok=True)

    secrets_b64 = os.environ.get(secrets_json_env, "").strip()
    legacy_b64 = os.environ.get(profiles_yml_env, "").strip() if profiles_yml_env else ""

    # ── Path 2: legacy base64 profiles.yml ──────────────────────────────
    if not secrets_b64 and legacy_b64:
        click.echo("Using legacy base64 profiles.yml fallback.", err=True)
        content = base64.b64decode(legacy_b64).decode()
        content = content.replace("<SCHEMA_NAME>", schema_name)
        output.write_text(content)
        return

    # ── Build template context ──────────────────────────────────────────
    context: dict[str, object] = {"schema_name": schema_name}

    if secrets_b64:
        decoded: dict = json.loads(base64.b64decode(secrets_b64))
        for key, value in decoded.items():
            context[key.lower()] = value
        click.echo(
            f"Loaded {len(decoded)} secret(s) from ${secrets_json_env}.",
            err=True,
        )
    else:
        click.echo(
            "No secrets found — rendering template for docker-only targets.",
            err=True,
        )

    # ── Render ──────────────────────────────────────────────────────────
    env = Environment(
        loader=BaseLoader(),
        undefined=_NullUndefined,
        keep_trailing_newline=True,
    )
    env.filters["toyaml"] = _yaml_inline
    tmpl = env.from_string(template.read_text())
    rendered = tmpl.render(**context)
    output.write_text(rendered)
    click.echo(f"Wrote {output}", err=True)


if __name__ == "__main__":
    main()
