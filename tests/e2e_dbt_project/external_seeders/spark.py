"""Spark external seed loader – reads CSVs from a volume mount and creates Delta tables."""

from __future__ import annotations

import os

from external_seeders.base import ExternalSeeder


class SparkExternalSeeder(ExternalSeeder):
    """Load seeds into Spark via PyHive from CSV files mounted in the container."""

    # dbt_project.yml sets ``+schema: test_seeds`` for seeds and the default
    # ``generate_schema_name`` macro returns that verbatim, so the actual seed
    # schema is always ``test_seeds`` regardless of the target schema name.
    SEED_SCHEMA = "test_seeds"

    @staticmethod
    def _q(name: str) -> str:
        """Quote a Spark SQL identifier, escaping any embedded backticks."""
        return f"`{name.replace('`', '``')}`"

    def load(self) -> None:
        failures: list[str] = []
        q = self._q
        seed_schema = self.SEED_SCHEMA
        print(
            f"\n=== Loading Spark seeds via external CSV tables "
            f"(schema={seed_schema}) ==="
        )

        try:
            from pyhive import hive
        except ImportError as e:
            raise RuntimeError(
                "pyhive is required for SparkExternalSeeder. Install dbt-spark[PyHive] (CI does this automatically)."
            ) from e

        host = os.environ.get("SPARK_HOST", "127.0.0.1")
        port = int(os.environ.get("SPARK_PORT", "10000"))

        print(f"Connecting to Spark Thrift at {host}:{port}...")
        conn = hive.Connection(host=host, port=port, username="dbt")
        cursor = conn.cursor()
        try:
            print(f"Creating schema '{seed_schema}'...")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{seed_schema}`")

            for subdir, csv_path, table_name in self.iter_seed_csvs():
                fname = os.path.basename(csv_path)

                if not self.csv_has_data(csv_path):
                    cols = self.csv_columns(csv_path)
                    if not cols:
                        print(f"  Skipping {table_name} (completely empty file)")
                        continue
                    col_defs = ", ".join(f"{q(c)} STRING" for c in cols)
                    print(f"  Creating empty table: {table_name}")
                    try:
                        cursor.execute(
                            f"DROP TABLE IF EXISTS {q(seed_schema)}.{q(table_name)}"
                        )
                        cursor.execute(
                            f"CREATE TABLE {q(seed_schema)}.{q(table_name)} "
                            f"({col_defs}) USING delta"
                        )
                    except Exception as e:
                        failures.append(f"{table_name}: {e}")
                    continue

                container_path = f"/seed-data/{subdir}/{fname}"
                tmp_view = f"_tmp_csv_{table_name}"
                print(f"  Loading: {table_name}")
                try:
                    cursor.execute(
                        f"CREATE OR REPLACE TEMPORARY VIEW {q(tmp_view)} "
                        f"USING csv "
                        f"OPTIONS (path '{container_path}', header 'true', "
                        f"inferSchema 'true')"
                    )
                    cursor.execute(
                        f"DROP TABLE IF EXISTS {q(seed_schema)}.{q(table_name)}"
                    )
                    cursor.execute(
                        f"CREATE TABLE {q(seed_schema)}.{q(table_name)} "
                        f"USING delta AS SELECT * FROM {q(tmp_view)}"
                    )
                except Exception as e:
                    failures.append(f"{table_name}: {e}")
        finally:
            cursor.close()
            conn.close()
        if failures:
            raise RuntimeError(
                "Spark seed loading failed:\n - " + "\n - ".join(failures)
            )
        print("\nSpark seed loading complete.")
