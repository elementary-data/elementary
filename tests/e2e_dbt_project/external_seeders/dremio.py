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
                if k == "MINIO_ROOT_USER":
                    defaults["MINIO_ACCESS_KEY"] = v
                elif k == "MINIO_ROOT_PASSWORD":
                    defaults["MINIO_SECRET_KEY"] = v
    except Exception:
        pass

    # --- dremio-setup.sh: Dremio login credentials ---
    setup_path = os.path.join(project_dir, "docker", "dremio", "dremio-setup.sh")
    try:
        with open(setup_path) as fh:
            content = fh.read()
        m = re.search(r'\\?"password\\?"\s*:\s*\\?"([^"\\]+)\\?"', content)
        if m:
            defaults["DREMIO_PASS"] = m.group(1)
        m = re.search(r'\\?"userName\\?"\s*:\s*\\?"([^"\\]+)\\?"', content)
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
    # REST helpers
    # ------------------------------------------------------------------

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
        )
        resp.raise_for_status()
        job_id = resp.json()["id"]

        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = requests.get(
                f"http://{self.dremio_host}:{self.dremio_port}/api/v3/job/{job_id}",
                headers=headers,
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

    def _create_nessie_namespace(self, namespace: str) -> None:
        """Create a Nessie namespace via the Nessie REST API (v2).

        Nessie ≥ 0.52.3 requires explicit namespace creation before tables
        can be committed.  We call the Nessie API directly (not through
        Dremio) so the namespace is registered *before* any SQL runs.
        """
        import requests

        nessie_host = os.environ.get("NESSIE_HOST", "localhost")
        nessie_port = int(os.environ.get("NESSIE_PORT", "19120"))
        base = f"http://{nessie_host}:{nessie_port}"

        # Try the Iceberg REST Catalog endpoint first (newer Nessie versions)
        # POST /iceberg/v1/namespaces
        iceberg_url = f"{base}/iceberg/main/v1/namespaces"
        payload = {"namespace": [namespace], "properties": {}}
        try:
            resp = requests.post(iceberg_url, json=payload, timeout=10)
            if resp.status_code in (200, 201):
                print(f"  Created Nessie namespace '{namespace}' via Iceberg REST")
                return
            if resp.status_code == 409:
                print(f"  Nessie namespace '{namespace}' already exists")
                return
            print(f"  Iceberg REST returned {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            print(f"  Iceberg REST endpoint not available: {e}")

        # Fall back to Nessie native API v2
        # PUT /api/v2/trees/main/contents/namespace
        nessie_url = f"{base}/api/v2/trees/main/contents/{namespace}"
        nessie_payload = {
            "content": {
                "type": "NAMESPACE",
                "elements": [namespace],
                "properties": {},
            }
        }
        try:
            resp = requests.put(nessie_url, json=nessie_payload, timeout=10)
            if resp.status_code in (200, 201, 204):
                print(f"  Created Nessie namespace '{namespace}' via Nessie API v2")
                return
            if resp.status_code == 409:
                print(f"  Nessie namespace '{namespace}' already exists (v2)")
                return
            print(f"  Nessie API v2 returned {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            print(f"  Nessie API v2 failed: {e}")

        print(
            f"  Warning: could not create Nessie namespace '{namespace}' via REST API"
        )

    def _refresh_nessie_source(self, token: str) -> None:
        """Refresh NessieSource metadata so Dremio picks up new namespaces."""
        try:
            self._sql(token, "ALTER SOURCE NessieSource REFRESH STATUS", timeout=30)
            print("  NessieSource metadata refreshed")
        except Exception as e:
            print(f"  Warning: source refresh failed: {e}")
        # Also try full metadata refresh
        try:
            self._sql(
                token,
                "ALTER SOURCE NessieSource REFRESH METADATA",
                timeout=60,
            )
            print("  NessieSource full metadata refreshed")
        except Exception as e:
            print(
                f"  Warning: full metadata refresh failed (may not be supported): {e}"
            )

    # ------------------------------------------------------------------
    # MinIO upload
    # ------------------------------------------------------------------

    def _upload_csvs_to_minio(self) -> None:
        """Upload seed CSVs to the Dremio MinIO bucket.

        Mounts the local ``data_dir`` into a temporary ``minio/mc`` container
        and copies files directly into the MinIO bucket.
        """
        network = os.environ.get("DREMIO_NETWORK", "e2e_dbt_project_dremio-lakehouse")
        mc_cmds = " && ".join(
            [
                f"mc alias set myminio http://dremio-storage:9000 {self.minio_access_key} {self.minio_secret_key}",
                "mc mb --ignore-existing myminio/datalake/seeds/training",
                "mc mb --ignore-existing myminio/datalake/seeds/validation",
                "mc cp --recursive /seed-data/training/ myminio/datalake/seeds/training/",
                "mc cp --recursive /seed-data/validation/ myminio/datalake/seeds/validation/",
                "echo Upload complete",
            ]
        )
        self.run(
            f"docker run --rm "
            f"--network {network} "
            f"-v {self.data_dir}:/seed-data:ro "
            f"--entrypoint /bin/sh "
            f"minio/mc "
            f'-c "{mc_cmds}"'
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
                )
                if get_resp.status_code == 200:
                    existing = get_resp.json()
                    payload["id"] = existing["id"]
                    payload["tag"] = existing.get("tag", "")
                    put_resp = requests.put(
                        f"http://{self.dremio_host}:{self.dremio_port}/api/v3/catalog/{existing['id']}",
                        headers=headers,
                        json=payload,
                    )
                    if put_resp.status_code == 200:
                        print("  SeedFiles S3 source updated")
                        return
                    print(
                        f"  Warning: update returned {put_resp.status_code}: {put_resp.text}"
                    )
                return

            # v2 fallback
            resp2 = requests.put(
                f"http://{self.dremio_host}:{self.dremio_port}/apiv2/source/SeedFiles",
                headers=headers,
                json=payload,
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

        print("  ERROR: Failed to create SeedFiles source after 3 attempts")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Load seeds using COPY INTO (no fragile CSV promotion needed).

        1. Upload CSVs to MinIO.
        2. Create an S3 source so Dremio can read those files.
        3. For each CSV, CREATE TABLE in Nessie + COPY INTO from S3.

        With enterprise_catalog_namespace, seeds use a flat single-level
        namespace under NessieSource (e.g. ``NessieSource."test_seeds"``).
        We avoid nested namespaces because dbt-dremio skips folder creation
        for Nessie sources and Dremio cannot create folders inside a SOURCE.
        """
        # dbt_project.yml: seeds: +schema: test_seeds
        seed_schema = "test_seeds"

        print("\n=== Loading Dremio seeds via MinIO + COPY INTO ===")

        print("\nStep 1: Uploading CSVs to MinIO...")
        self._upload_csvs_to_minio()

        print("\nStep 2: Creating SeedFiles S3 source...")
        token = self._get_token()
        self._create_s3_source(token)

        # Explicitly create the Nessie namespace before creating tables.
        # Nessie >= 0.52.3 requires namespaces to exist before table commits.
        print("\nStep 3: Creating Nessie namespace...")
        self._create_nessie_namespace(seed_schema)

        nessie_ns = f'NessieSource."{seed_schema}"'
        print(f"\nStep 4: Creating Iceberg tables at '{nessie_ns}'...")
        for subdir, csv_path, table_name in self.iter_seed_csvs():
            cols = self.csv_columns(csv_path)
            if not cols:
                print(f"  Skipping {table_name} (completely empty file)")
                continue

            fqn = f'{nessie_ns}."{table_name}"'

            # Create empty Iceberg table with VARCHAR columns
            col_defs = ", ".join(f'"{c}" VARCHAR' for c in cols)
            create_sql = f"CREATE TABLE IF NOT EXISTS {fqn} ({col_defs})"
            try:
                self._sql(token, create_sql, timeout=60)
            except Exception as e:
                print(f"  Error creating table {table_name}: {e}")
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
                print(f"    Error: {e}")

        # Refresh NessieSource so Dremio sees the new namespace + tables
        print("\nStep 5: Refreshing NessieSource metadata...")
        self._refresh_nessie_source(token)

        print("\nDremio seed loading complete.")
