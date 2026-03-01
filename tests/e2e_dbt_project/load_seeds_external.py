#!/usr/bin/env python3
"""Load seed data via external files instead of ``dbt seed``.

For **Dremio** the script:
  1. Uploads every seed CSV to the Dremio-MinIO bucket via a temporary
     ``minio/mc`` container.
  2. Creates an S3 source (``SeedFiles``) in Dremio with auto-promote so
     the uploaded CSVs become queryable virtual datasets.
  3. Runs ``CREATE TABLE … AS SELECT …`` to materialise each seed as an
     Iceberg table inside the NessieSource catalogue.

For **Spark** the script:
  1. Expects the ``data/`` directory to be volume-mounted into the
     Spark-Thrift container at ``/seed-data``.
  2. Connects via PyHive and creates external CSV-backed tables in the
     seed schema.

Usage::

    python load_seeds_external.py <adapter> <schema_name> <data_dir>
"""

from __future__ import annotations

import csv
import glob
import os
import subprocess
import sys
import time

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _csv_has_data(path: str) -> bool:
    """Return True when the CSV has a header AND at least one data row."""
    with open(path) as f:
        reader = csv.reader(f)
        try:
            next(reader)  # header
            next(reader)  # first data row
            return True
        except StopIteration:
            return False


def _csv_columns(path: str) -> list[str]:
    """Return the header row of a CSV (empty list when file is empty)."""
    with open(path) as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration:
            return []


def _run(cmd: str, check: bool = True, **kw):
    print(f"  ➜ {cmd}")
    return subprocess.run(cmd, shell=True, check=check, **kw)


# ---------------------------------------------------------------------------
# Dremio
# ---------------------------------------------------------------------------

DREMIO_HOST = os.environ.get("DREMIO_HOST", "localhost")
DREMIO_PORT = int(os.environ.get("DREMIO_PORT", "9047"))
DREMIO_USER = "dremio"
DREMIO_PASS = "dremio123"


def _dremio_token() -> str:
    """Authenticate and return a Dremio REST API token."""
    import requests

    resp = requests.post(
        f"http://{DREMIO_HOST}:{DREMIO_PORT}/apiv2/login",
        json={"userName": DREMIO_USER, "password": DREMIO_PASS},
    )
    resp.raise_for_status()
    return resp.json()["token"]


def _dremio_sql(token: str, sql: str, *, timeout: int = 120):
    """Execute a SQL statement on Dremio via the REST API and wait for it."""
    import requests

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"_dremio{token}",
    }

    # Submit job
    resp = requests.post(
        f"http://{DREMIO_HOST}:{DREMIO_PORT}/api/v3/sql",
        headers=headers,
        json={"sql": sql},
    )
    resp.raise_for_status()
    job_id = resp.json()["id"]

    # Poll until complete
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(
            f"http://{DREMIO_HOST}:{DREMIO_PORT}/api/v3/job/{job_id}",
            headers=headers,
        )
        resp.raise_for_status()
        state = resp.json()["jobState"]
        if state == "COMPLETED":
            return resp.json()
        if state in ("FAILED", "CANCELED", "CANCELLED"):
            detail = resp.json()
            raise RuntimeError(f"Dremio SQL failed ({state}): {detail}\nSQL: {sql}")
        time.sleep(1)
    raise TimeoutError(f"Dremio job {job_id} timed out after {timeout}s")


def _dremio_create_s3_source(token: str):
    """Create the 'SeedFiles' S3 source in Dremio pointing at MinIO."""
    import requests

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"_dremio{token}",
    }
    source_payload = {
        "name": "SeedFiles",
        "config": {
            "credentialType": "ACCESS_KEY",
            "accessKey": "admin",
            "accessSecret": "password",
            "secure": False,
            "externalBucketList": [],
            "rootPath": "/datalake",
            "propertyList": [
                {"name": "fs.s3a.path.style.access", "value": "true"},
                {"name": "fs.s3a.endpoint", "value": "dremio-storage:9000"},
            ],
            "whitelistedBuckets": [],
            "isCachingEnabled": False,
            "defaultCtasFormat": "ICEBERG",
        },
        "type": "S3",
        "metadataPolicy": {
            "deleteUnavailableDatasets": True,
            "autoPromoteDatasets": True,
            "namesRefreshMillis": 3600000,
            "datasetDefinitionRefreshAfterMillis": 3600000,
            "datasetDefinitionExpireAfterMillis": 10800000,
            "authTTLMillis": 86400000,
            "updateMode": "PREFETCH_QUERIED",
        },
    }

    resp = requests.put(
        f"http://{DREMIO_HOST}:{DREMIO_PORT}/apiv2/source/SeedFiles",
        headers=headers,
        json=source_payload,
    )
    if resp.status_code not in (200, 409):
        print(
            f"  Warning: SeedFiles source creation returned {resp.status_code}: {resp.text}"
        )
    else:
        print("  SeedFiles S3 source created/updated in Dremio")


def _upload_csvs_to_minio(data_dir: str):
    """Upload seed CSVs to the Dremio MinIO bucket using ``docker exec``
    on the running ``dremio-storage`` (MinIO) container."""
    abs_data = os.path.abspath(data_dir)

    # Copy CSVs into the running MinIO container.
    _run(f'docker cp "{abs_data}/training" dremio-storage:/tmp/seed-training')
    _run(f'docker cp "{abs_data}/validation" dremio-storage:/tmp/seed-validation')

    # Use a temporary minio/mc container on the same network.  The image's
    # default entrypoint is ``mc``, so we override with ``/bin/sh`` to chain
    # multiple mc commands together.
    network = os.environ.get("DREMIO_NETWORK", "e2e_dbt_project_dremio-lakehouse")
    mc_cmds = " && ".join(
        [
            "mc alias set myminio http://dremio-storage:9000 admin password",
            "mc mb --ignore-existing myminio/datalake/seeds/training",
            "mc mb --ignore-existing myminio/datalake/seeds/validation",
            "mc cp --recursive /tmp/seed-training/ myminio/datalake/seeds/training/",
            "mc cp --recursive /tmp/seed-validation/ myminio/datalake/seeds/validation/",
            "echo Upload complete",
        ]
    )
    _run(
        f"docker run --rm "
        f"--network {network} "
        f"--volumes-from dremio-storage "
        f"--entrypoint /bin/sh "
        f"minio/mc "
        f'-c "{mc_cmds}"'
    )


def _dremio_refresh_source(token: str):
    """Trigger a metadata refresh on the SeedFiles source."""
    import requests

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"_dremio{token}",
    }
    # Get the source to find its id
    resp = requests.get(
        f"http://{DREMIO_HOST}:{DREMIO_PORT}/apiv2/source/SeedFiles",
        headers=headers,
    )
    if resp.status_code == 200:
        source_data = resp.json()
        source_id = source_data.get("id")
        if source_id:
            # Trigger refresh
            requests.post(
                f"http://{DREMIO_HOST}:{DREMIO_PORT}/apiv2/source/SeedFiles/refresh",
                headers=headers,
            )
            print("  SeedFiles metadata refresh triggered")
            time.sleep(5)  # wait for refresh to process


def _promote_csv_dataset(token: str, path_parts: list[str]):
    """Promote a CSV file as a physical dataset in the SeedFiles source."""
    import requests

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"_dremio{token}",
    }
    # The catalog API path for the file
    full_path = ["SeedFiles"] + path_parts
    encoded_path = ".".join(f'"{p}"' for p in full_path)

    # Try to get catalog entity first
    path_param = "/".join(full_path)
    resp = requests.get(
        f"http://{DREMIO_HOST}:{DREMIO_PORT}/api/v3/catalog/by-path/{path_param}",
        headers=headers,
    )

    if resp.status_code == 200:
        entity = resp.json()
        entity_type = entity.get("entityType", "")
        if entity_type == "dataset":
            print(f"    Already promoted: {encoded_path}")
            return True

        # It's a file, promote it
        entity_id = entity.get("id")
        if entity_id:
            promote_payload = {
                "id": entity_id,
                "path": full_path,
                "type": "PHYSICAL_DATASET",
                "entityType": "dataset",
                "format": {
                    "type": "Text",
                    "fieldDelimiter": ",",
                    "lineDelimiter": "\n",
                    "quote": '"',
                    "comment": "#",
                    "extractHeader": True,
                    "trimHeader": True,
                    "autoGenerateColumnNames": False,
                },
            }
            resp2 = requests.put(
                f"http://{DREMIO_HOST}:{DREMIO_PORT}/api/v3/catalog/{entity_id}",
                headers=headers,
                json=promote_payload,
            )
            if resp2.status_code in (200, 201):
                print(f"    Promoted: {encoded_path}")
                return True
            else:
                print(f"    Promote failed ({resp2.status_code}): {resp2.text[:200]}")
                return False
    else:
        print(f"    Cannot find file at {path_param}: {resp.status_code}")
        return False


def load_dremio_seeds(data_dir: str, schema_name: str):
    """Load seeds into Dremio via MinIO external files."""
    print("\n=== Loading Dremio seeds via MinIO ===")

    # Step 1: Upload CSVs to MinIO
    print("\nStep 1: Uploading CSVs to MinIO...")
    _upload_csvs_to_minio(data_dir)

    # Step 2: Get auth token and create S3 source
    print("\nStep 2: Creating SeedFiles S3 source...")
    token = _dremio_token()
    _dremio_create_s3_source(token)

    # Step 3: Refresh source metadata and wait
    print("\nStep 3: Refreshing source metadata...")
    _dremio_refresh_source(token)
    time.sleep(5)

    # Step 4: Create Nessie namespace (folder) for seeds
    print(f"\nStep 4: Creating Nessie namespace '{schema_name}'...")
    try:
        _dremio_sql(token, f'CREATE SCHEMA IF NOT EXISTS NessieSource."{schema_name}"')
    except Exception as e:
        print(f"  Warning creating schema: {e}")

    # Step 5: Promote CSV files and create Iceberg tables
    print("\nStep 5: Creating Iceberg tables from promoted CSVs...")
    for subdir in ["training", "validation"]:
        csv_dir = os.path.join(data_dir, subdir)
        csv_files = sorted(glob.glob(os.path.join(csv_dir, "*.csv")))
        for csv_path in csv_files:
            fname = os.path.basename(csv_path)
            table_name = fname.replace(".csv", "")

            if not _csv_has_data(csv_path):
                # Empty CSV - create an empty table with just columns
                cols = _csv_columns(csv_path)
                if not cols:
                    print(f"  Skipping {table_name} (completely empty file)")
                    continue
                col_defs = ", ".join(f'"{c}" VARCHAR' for c in cols)
                sql = (
                    f'CREATE TABLE IF NOT EXISTS NessieSource."{schema_name}"."{table_name}" '
                    f"({col_defs})"
                )
                print(f"  Creating empty table: {table_name}")
                try:
                    _dremio_sql(token, sql, timeout=60)
                except Exception as e:
                    print(f"    Error: {e}")
                continue

            # Promote the CSV file first
            path_parts = ["seeds", subdir, fname]
            promoted = _promote_csv_dataset(token, path_parts)

            if not promoted:
                print(f"  Skipping CTAS for {table_name} (promotion failed)")
                continue

            # Create Iceberg table via CTAS
            s3_ref = f'"SeedFiles"."seeds"."{subdir}"."{fname}"'
            sql = (
                f'CREATE TABLE IF NOT EXISTS NessieSource."{schema_name}"."{table_name}" AS '
                f"SELECT * FROM {s3_ref}"
            )
            print(f"  CTAS: {table_name}")
            try:
                _dremio_sql(token, sql, timeout=120)
            except Exception as e:
                print(f"    Error: {e}")


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------


def load_spark_seeds(data_dir: str, schema_name: str):
    """Load seeds into Spark from CSV files mounted in the container.

    ``schema_name`` is the *target* schema (``elementary_tests``).  The dbt
    project sets ``+schema: test_seeds`` for seeds, and the default
    ``generate_schema_name`` macro returns ``custom_schema_name`` verbatim for
    seeds, so the actual seed schema is ``test_seeds``.
    """
    seed_schema = "test_seeds"
    print(
        f"\n=== Loading Spark seeds via external CSV tables (schema={seed_schema}) ==="
    )

    from pyhive import hive

    host = os.environ.get("SPARK_HOST", "127.0.0.1")
    port = int(os.environ.get("SPARK_PORT", "10000"))

    print(f"Connecting to Spark Thrift at {host}:{port}...")
    conn = hive.Connection(host=host, port=port, username="dbt")
    cursor = conn.cursor()

    # Create the seed schema
    print(f"Creating schema '{seed_schema}'...")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{seed_schema}`")

    for subdir in ["training", "validation"]:
        csv_dir = os.path.join(data_dir, subdir)
        csv_files = sorted(glob.glob(os.path.join(csv_dir, "*.csv")))
        for csv_path in csv_files:
            fname = os.path.basename(csv_path)
            table_name = fname.replace(".csv", "")

            if not _csv_has_data(csv_path):
                cols = _csv_columns(csv_path)
                if not cols:
                    print(f"  Skipping {table_name} (completely empty file)")
                    continue
                # Create empty table with schema
                col_defs = ", ".join(f"`{c}` STRING" for c in cols)
                sql = f"CREATE TABLE IF NOT EXISTS `{seed_schema}`.`{table_name}` ({col_defs}) USING delta"
                print(f"  Creating empty table: {table_name}")
                try:
                    cursor.execute(sql)
                except Exception as e:
                    print(f"    Error: {e}")
                continue

            # Container path where the data dir is mounted
            container_path = f"/seed-data/{subdir}/{fname}"

            # Create external CSV table, then convert to Delta for consistency
            tmp_view = f"_tmp_csv_{table_name}"
            print(f"  Loading: {table_name}")
            try:
                # Create temp view from CSV
                cursor.execute(
                    f"CREATE OR REPLACE TEMPORARY VIEW `{tmp_view}` "
                    f"USING csv "
                    f"OPTIONS (path '{container_path}', header 'true', inferSchema 'true')"
                )
                # Create Delta table from view (or replace if exists)
                cursor.execute(f"DROP TABLE IF EXISTS `{seed_schema}`.`{table_name}`")
                cursor.execute(
                    f"CREATE TABLE `{seed_schema}`.`{table_name}` "
                    f"USING delta AS SELECT * FROM `{tmp_view}`"
                )
            except Exception as e:
                print(f"    Error: {e}")

    cursor.close()
    conn.close()
    print("\nSpark seed loading complete.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <adapter> <schema_name> <data_dir>")
        sys.exit(1)

    adapter = sys.argv[1]
    schema_name = sys.argv[2]
    data_dir = sys.argv[3]

    if not os.path.isdir(data_dir):
        print(f"Error: data directory '{data_dir}' not found")
        sys.exit(1)

    if adapter == "dremio":
        load_dremio_seeds(data_dir, schema_name)
    elif adapter == "spark":
        load_spark_seeds(data_dir, schema_name)
    else:
        print(f"Error: unsupported adapter '{adapter}' (expected 'dremio' or 'spark')")
        sys.exit(1)


if __name__ == "__main__":
    main()
