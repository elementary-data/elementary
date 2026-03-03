"""Dremio external seed loader – uploads CSVs to MinIO and creates Iceberg tables."""

from __future__ import annotations

import os
import re
import time

import yaml
from external_seeders.base import ExternalSeeder


def _docker_defaults() -> dict[str, str]:
    """Read default credentials from docker-compose.yml and dremio-setup.sh.

    These are local Docker test credentials, not production secrets.
    """
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    defaults: dict[str, str] = {}

    # --- docker-compose.yml: MinIO credentials ---
    compose_path = os.path.join(project_dir, "docker-compose.yml")
    try:
        with open(compose_path) as fh:
            cfg = yaml.safe_load(fh)
        services = cfg.get("services", {})
        for item in services.get("dremio-minio", {}).get("environment", []):
            if isinstance(item, str) and "=" in item:
                k, v = item.split("=", 1)
                # Resolve ${VAR:-default} patterns to just the default value
                m = re.match(r"\$\{[^:}]+:-([^}]+)\}", v)
                if m:
                    v = m.group(1)
                if k == "MINIO_ROOT_USER":
                    defaults["MINIO_ACCESS_KEY"] = v
                elif k == "MINIO_ROOT_PASSWORD":
                    defaults["MINIO_SECRET_KEY"] = v
    except Exception:
        pass

    # --- dremio-setup.sh: Dremio login credentials ---
    # Extract default values from bash variable assignments like:
    #   DREMIO_PASS="${DREMIO_PASS:-dremio123}"
    setup_path = os.path.join(project_dir, "docker", "dremio", "dremio-setup.sh")
    try:
        with open(setup_path) as fh:
            content = fh.read()
        m = re.search(r'DREMIO_PASS="\$\{DREMIO_PASS:-([^}]+)\}"', content)
        if m:
            defaults["DREMIO_PASS"] = m.group(1)
        m = re.search(r'DREMIO_USER="\$\{DREMIO_USER:-([^}]+)\}"', content)
        if m:
            defaults["DREMIO_USER"] = m.group(1)
    except Exception:
        pass

    return defaults


class DremioExternalSeeder(ExternalSeeder):
    """Load seeds into Dremio via MinIO S3 + Nessie Iceberg tables.

    Credentials are read from environment variables.  If not set, defaults
    are extracted from the sibling ``docker-compose.yml`` file so the script
    works out-of-the-box in the CI Docker environment.
    """

    HTTP_TIMEOUT = (5, 30)  # (connect, read) in seconds

    def __init__(self, data_dir: str, schema_name: str) -> None:
        super().__init__(data_dir, schema_name)
        _defaults = _docker_defaults()
        self.dremio_host = os.environ.get("DREMIO_HOST", "localhost")
        self.dremio_port = int(os.environ.get("DREMIO_PORT", "9047"))
        self.dremio_user = os.environ.get(
            "DREMIO_USER", _defaults.get("DREMIO_USER", "dremio")
        )
        self.dremio_pass = os.environ.get(
            "DREMIO_PASS", _defaults.get("DREMIO_PASS", "")
        )
        self.minio_access_key = os.environ.get(
            "MINIO_ACCESS_KEY", _defaults.get("MINIO_ACCESS_KEY", "")
        )
        self.minio_secret_key = os.environ.get(
            "MINIO_SECRET_KEY", _defaults.get("MINIO_SECRET_KEY", "")
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _quote_ident(name: str) -> str:
        """Double-quote a SQL identifier, escaping embedded double-quotes."""
        return '"' + name.replace('"', '""') + '"'

    def _headers(self, token: str) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"_dremio{token}",
        }

    def _get_token(self) -> str:
        import requests

        resp = requests.post(
            f"http://{self.dremio_host}:{self.dremio_port}/apiv2/login",
            json={"userName": self.dremio_user, "password": self.dremio_pass},
            timeout=self.HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()["token"]

    def _sql(self, token: str, sql: str, *, timeout: int = 120) -> dict:
        """Execute SQL on Dremio and block until the job finishes."""
        import requests

        headers = self._headers(token)
        resp = requests.post(
            f"http://{self.dremio_host}:{self.dremio_port}/api/v3/sql",
            headers=headers,
            json={"sql": sql},
            timeout=self.HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        job_id = resp.json()["id"]

        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = requests.get(
                f"http://{self.dremio_host}:{self.dremio_port}/api/v3/job/{job_id}",
                headers=headers,
                timeout=self.HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            state = resp.json()["jobState"]
            if state == "COMPLETED":
                return resp.json()
            if state in ("FAILED", "CANCELED", "CANCELLED"):
                raise RuntimeError(
                    f"Dremio SQL failed ({state}): {resp.json()}\nSQL: {sql}"
                )
            time.sleep(1)
        raise TimeoutError(f"Dremio job {job_id} timed out after {timeout}s")

    # ------------------------------------------------------------------
    # Nessie namespace creation
    # ------------------------------------------------------------------

    def _create_nessie_namespace(self, token: str, namespace: str) -> None:
        """Create a Nessie namespace via Dremio SQL.

        Uses ``CREATE FOLDER IF NOT EXISTS`` which goes through the
        Dremio-Nessie integration and is more reliable than calling
        the Nessie REST API directly.
        """
        qi = self._quote_ident
        folder_sql = f"CREATE FOLDER IF NOT EXISTS NessieSource.{qi(namespace)}"
        try:
            self._sql(token, folder_sql, timeout=30)
            print(f"  Created Nessie namespace '{namespace}' via CREATE FOLDER")
        except Exception as e:
            # Not fatal – CREATE TABLE may implicitly create the namespace
            print(f"  Warning: CREATE FOLDER failed ({e}), continuing...")

    # ------------------------------------------------------------------
    # MinIO upload
    # ------------------------------------------------------------------

    def _upload_csvs_to_minio(self) -> None:
        """Upload seed CSVs to the Dremio MinIO bucket.

        Mounts the local ``data_dir`` into a temporary ``minio/mc`` container
        and copies files directly into the MinIO bucket.
        """
        import shlex

        network = os.environ.get("DREMIO_NETWORK", "e2e_dbt_project_dremio-lakehouse")
        mc_cmds = " && ".join(
            [
                "mc alias set myminio http://dremio-storage:9000"
                f" {shlex.quote(self.minio_access_key)}"
                f" {shlex.quote(self.minio_secret_key)}",
                "mc mb --ignore-existing myminio/datalake/seeds/training",
                "mc mb --ignore-existing myminio/datalake/seeds/validation",
                "mc cp --recursive /seed-data/training/ myminio/datalake/seeds/training/",
                "mc cp --recursive /seed-data/validation/ myminio/datalake/seeds/validation/",
                "echo Upload complete",
            ]
        )
        self.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                network,
                "-v",
                f"{self.data_dir}:/seed-data:ro",
                "--entrypoint",
                "/bin/sh",
                "minio/mc",
                "-c",
                mc_cmds,
            ]
        )

    # ------------------------------------------------------------------
    # S3 source creation
    # ------------------------------------------------------------------

    def _create_s3_source(self, token: str) -> None:
        import requests

        headers = self._headers(token)
        # Dremio OSS uses the Catalog API (v3) for source management.
        # For MinIO compatibility we must set compatibilityMode, path-style
        # access, and point the endpoint at the Docker-internal hostname.
        payload = {
            "entityType": "source",
            "name": "SeedFiles",
            "config": {
                "credentialType": "ACCESS_KEY",
                "accessKey": self.minio_access_key,
                "accessSecret": self.minio_secret_key,
                "secure": False,
                "externalBucketList": ["datalake"],
                "rootPath": "/",
                "compatibilityMode": True,
                "enableAsync": True,
                "propertyList": [
                    {"name": "fs.s3a.path.style.access", "value": "true"},
                    {"name": "fs.s3a.endpoint", "value": "dremio-storage:9000"},
                ],
                "whitelistedBuckets": ["datalake"],
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

        # Try the v3 Catalog API first (Dremio OSS ≥ 24), fall back to v2.
        for attempt in range(3):
            # v3 catalog API – POST to create
            resp = requests.post(
                f"http://{self.dremio_host}:{self.dremio_port}/api/v3/catalog",
                headers=headers,
                json=payload,
                timeout=self.HTTP_TIMEOUT,
            )
            if resp.status_code in (200, 201):
                print("  SeedFiles S3 source created via v3 Catalog API")
                return
            if resp.status_code == 409:
                # Source already exists – update it with PUT
                print("  SeedFiles source exists, updating...")
                # Get the existing source id + tag for the update
                get_resp = requests.get(
                    f"http://{self.dremio_host}:{self.dremio_port}/api/v3/catalog/by-path/SeedFiles",
                    headers=headers,
                    timeout=self.HTTP_TIMEOUT,
                )
                if get_resp.status_code == 200:
                    existing = get_resp.json()
                    payload["id"] = existing["id"]
                    payload["tag"] = existing.get("tag", "")
                    put_resp = requests.put(
                        f"http://{self.dremio_host}:{self.dremio_port}/api/v3/catalog/{existing['id']}",
                        headers=headers,
                        json=payload,
                        timeout=self.HTTP_TIMEOUT,
                    )
                    if put_resp.status_code == 200:
                        print("  SeedFiles S3 source updated")
                        return
                    print(
                        f"  Warning: update returned {put_resp.status_code}: {put_resp.text}"
                    )
                else:
                    print(
                        f"  Warning: failed fetching existing source "
                        f"({get_resp.status_code}: {get_resp.text[:200]})"
                    )
                # Continue to v2 fallback / retry.

            # v2 fallback
            resp2 = requests.put(
                f"http://{self.dremio_host}:{self.dremio_port}/apiv2/source/SeedFiles",
                headers=headers,
                json=payload,
                timeout=self.HTTP_TIMEOUT,
            )
            if resp2.status_code in (200, 409):
                print("  SeedFiles S3 source created/updated via v2 API")
                return

            print(
                f"  Attempt {attempt + 1}/3: source creation failed "
                f"(v3={resp.status_code}: {resp.text[:200]}, "
                f"v2={resp2.status_code}: {resp2.text[:200]})"
            )
            time.sleep(5)

        raise RuntimeError("Failed to create SeedFiles source after 3 attempts")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Load seeds using COPY INTO (no fragile CSV promotion needed).

        1. Upload CSVs to MinIO.
        2. Create an S3 source so Dremio can read those files.
        3. Create Nessie namespace via CREATE FOLDER.
        4. For each CSV, CREATE TABLE in Nessie + COPY INTO from S3.
        5. Refresh source metadata so the VDS validator can see new tables.

        Seeds are placed in the *same* Nessie namespace as models (the
        target schema, e.g. ``elementary_tests``) because the dbt-dremio
        REST API is stateless and the VDS view validator cannot resolve
        cross-namespace Nessie references.
        """
        failures: list[str] = []
        # For Dremio, seeds must live in the same Nessie namespace as models
        # because the REST API is stateless (no persistent USE BRANCH) and
        # the VDS view validator cannot resolve cross-namespace references.
        # self.schema_name is the target schema (e.g. "elementary_tests").
        seed_schema = self.schema_name

        print("\n=== Loading Dremio seeds via MinIO + COPY INTO ===")

        print("\nStep 1: Uploading CSVs to MinIO...")
        self._upload_csvs_to_minio()

        print("\nStep 2: Creating SeedFiles S3 source...")
        token = self._get_token()
        self._create_s3_source(token)

        # Explicitly create the Nessie namespace before creating tables.
        # Uses CREATE FOLDER via Dremio SQL (more reliable than Nessie REST API).
        print("\nStep 3: Creating Nessie namespace...")
        self._create_nessie_namespace(token, seed_schema)

        qi = self._quote_ident
        nessie_ns = f"NessieSource.{qi(seed_schema)}"

        print(f"\nStep 4: Creating Iceberg tables at '{nessie_ns}'...")
        created_tables: list[str] = []
        for subdir, csv_path, table_name in self.iter_seed_csvs():
            cols = self.csv_columns(csv_path)
            if not cols:
                print(f"  Skipping {table_name} (completely empty file)")
                continue

            fqn = f"{nessie_ns}.{qi(table_name)}"

            # Create empty Iceberg table with VARCHAR columns
            col_defs = ", ".join(f"{qi(c)} VARCHAR" for c in cols)
            create_sql = f"CREATE TABLE IF NOT EXISTS {fqn} ({col_defs})"
            try:
                self._sql(token, create_sql, timeout=60)
                created_tables.append(table_name)
            except Exception as e:
                failures.append(f"create {table_name}: {e}")
                continue

            if not self.csv_has_data(csv_path):
                print(f"  Created empty table: {table_name}")
                continue

            # Load CSV data using COPY INTO from the S3 source
            fname = os.path.basename(csv_path)
            s3_path = f"@SeedFiles/datalake/seeds/{subdir}/{fname}"
            copy_sql = (
                f"COPY INTO {fqn} "
                f"FROM '{s3_path}' "
                f"FILE_FORMAT 'csv' "
                f"(EXTRACT_HEADER 'true', TRIM_SPACE 'true')"
            )
            print(f"  COPY INTO: {table_name}")
            try:
                self._sql(token, copy_sql, timeout=120)
            except Exception as e:
                failures.append(f"copy {table_name}: {e}")

        # Force Dremio to refresh its metadata cache for the Nessie source.
        # The VDS view validator uses a separate metadata system that may not
        # immediately see tables created via the SQL API.  Without this,
        # CREATE VIEW statements fail with "Object ... not found".
        print("\nStep 5: Refreshing NessieSource metadata...")
        try:
            self._sql(token, "ALTER SOURCE NessieSource REFRESH STATUS", timeout=30)
            print("  Metadata refresh triggered")
        except Exception as e:
            failures.append(f"metadata refresh: {e}")

        # Give Dremio a moment to complete the background metadata scan.
        print("  Waiting 10s for metadata propagation...")
        time.sleep(10)

        if failures:
            raise RuntimeError("Dremio seeding failed:\n - " + "\n - ".join(failures))
        print("\nDremio seed loading complete.")
