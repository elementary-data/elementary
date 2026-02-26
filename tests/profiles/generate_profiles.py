#!/usr/bin/env python3
"""Generate ~/.dbt/profiles.yml from a Jinja2 template and an optional secrets JSON."""

from __future__ import annotations

import base64
import binascii
import json
import os
from pathlib import Path
from typing import Any

import click
import yaml
from jinja2 import BaseLoader, Environment, StrictUndefined, Undefined


class _NullUndefined(Undefined):
    """Render missing variables as empty strings so docker-only runs don't crash."""

    def __str__(self) -> str:
        return ""

    def __iter__(self):
        return iter([])

    def __bool__(self) -> bool:
        return False


def _yaml_inline(value: Any) -> str:
    """Dump *value* as a YAML-safe scalar or compact inline mapping.

    For dicts (e.g. bigquery_keyfile) this produces ``{key: val, ...}``.
    For non-string scalars (int, float, bool) the value passes through
    unchanged so that YAML keeps its native type.
    For strings, values that YAML would misinterpret (e.g. ``"yes"`` as
    bool, ``"123"`` as int, ``"null"`` as None) are quoted.
    """
    if isinstance(value, Undefined):
        return "{}"
    if isinstance(value, dict):
        return yaml.dump(value, default_flow_style=True).strip()
    if not isinstance(value, str):
        # int, float, bool — pass through so YAML keeps native type
        return str(value).lower() if isinstance(value, bool) else str(value)
    # For strings, check if YAML would misinterpret the value
    loaded = yaml.safe_load(value)
    if loaded is None or not isinstance(loaded, str):
        # YAML would coerce to non-string — quote it
        dumped = yaml.dump(value, default_flow_style=True).rstrip()
        if dumped.endswith("..."):
            dumped = dumped[: -len("...")].rstrip()
        return dumped
    return value


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
def main(
    template: Path,
    output: Path,
    schema_name: str,
    secrets_json_env: str,
) -> None:
    """Render a Jinja2 profiles template into a dbt profiles.yml file.

    Resolution order:
      1. If the env-var named by ``--secrets-json-env`` is set, decode it and
         use its key/value pairs (plus *schema_name*) as template variables.
      2. Otherwise render the template with only *schema_name* populated (all
         other variables resolve to empty strings — suitable for docker-only
         targets on fork PRs).
    """
    output.parent.mkdir(parents=True, exist_ok=True)

    secrets_b64 = os.environ.get(secrets_json_env, "").strip()

    # ── Build template context ──────────────────────────────────────────
    context: dict[str, object] = {"schema_name": schema_name}

    if secrets_b64:
        try:
            decoded: dict = json.loads(base64.b64decode(secrets_b64))
        except (binascii.Error, json.JSONDecodeError) as e:
            raise click.ClickException(
                f"Failed to decode ${secrets_json_env}: {e}"
            ) from e
        if not isinstance(decoded, dict):
            raise click.ClickException(
                f"Expected JSON object for ${secrets_json_env}, "
                f"got {type(decoded).__name__}"
            )
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
    # When secrets are loaded, use StrictUndefined so typos in secret keys
    # fail fast.  For docker-only runs (no secrets) use _NullUndefined so
    # cloud placeholders silently resolve to empty strings.
    undefined_cls = StrictUndefined if secrets_b64 else _NullUndefined
    env = Environment(
        loader=BaseLoader(),
        undefined=undefined_cls,
        keep_trailing_newline=True,
    )
    env.filters["toyaml"] = _yaml_inline
    tmpl = env.from_string(template.read_text())
    rendered = tmpl.render(**context)
    output.write_text(rendered)
    click.echo(f"Wrote {output}", err=True)


if __name__ == "__main__":
    main()
