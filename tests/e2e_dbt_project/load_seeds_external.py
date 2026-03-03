#!/usr/bin/env python3
"""Load seed data via external files instead of ``dbt seed``.

Delegates to adapter-specific seeders defined in the ``external_seeders``
package.  Run ``python load_seeds_external.py --help`` for usage.
"""

from __future__ import annotations

import os
import sys

import click

# Ensure the script's directory is on sys.path so that the
# ``external_seeders`` package can be imported regardless of cwd.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from external_seeders import DremioExternalSeeder, SparkExternalSeeder  # noqa: E402

SEEDERS = {
    "dremio": DremioExternalSeeder,
    "spark": SparkExternalSeeder,
}


@click.command()
@click.argument("adapter", type=click.Choice(list(SEEDERS.keys())))
@click.argument("schema_name")
@click.argument(
    "data_dir",
    type=click.Path(exists=True, file_okay=False, resolve_path=True),
)
def main(adapter: str, schema_name: str, data_dir: str) -> None:
    """Load seed CSVs into ADAPTER using external tables.

    \b
    ADAPTER      Target warehouse adapter (dremio | spark).
    SCHEMA_NAME  Target schema / namespace for the seed tables.
                 NOTE: Spark ignores this value and always uses the fixed
                 schema defined in SparkExternalSeeder.SEED_SCHEMA (currently
                 "test_seeds") because the generate_schema_name macro returns
                 that name verbatim.
    DATA_DIR     Path to the directory containing training/ and validation/ CSVs.
    """
    seeder_cls = SEEDERS[adapter]
    seeder = seeder_cls(data_dir=data_dir, schema_name=schema_name)
    seeder.load()


if __name__ == "__main__":
    main()
