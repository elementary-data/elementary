"""Dremio external seed loader – uploads CSVs to MinIO and creates Iceberg tables."""

from __future__ import annotations

import os
import time

import yaml

from external_seeders.base import ExternalSeeder


def _compose_defaults() -> dict[str, str]:
    """Read default credentials from docker-compose.yml service definitions."""
    compose_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "docker-compose.yml",
    )
    defaults: dict[str, str] = {}
    try:
        with open(compose_path) as fh:
            cfg = yaml.safe_load(fh)
        services = cfg.get("services", {})
        # Dremio MinIO service stores creds as list items: "KEY=VALUE"
        for item in services.get("dremio-minio", {}).get("environment", []):
            if isinstance(item, str) and "=" in item:
                k, v = item.split("=", 1)
                if k == "MINIO_ROOT_USER":
                    defaults["MINIO_ACCESS_KEY"] = v
                elif k == "MINIO_ROOT_PASSWORD":
                    defaults["MINIO_SECRET_KEY"] = v
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
        _defaults = _compose_defaults()
        self.dremio_host = os.environ.get("DREMIO_HOST", "localhost")
        self.dremio_port = int(os.environ.get("DREMIO_PORT", "9047"))
        self.dremio_user = os.environ.get("DREMIO_USER", "dremio")
        self.dremio_pass = os.environ.get("DREMIO_PASS", _defaults.get("DREMIO_PASS", ""))
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
        payload = {
            "name": "SeedFiles",
            "config": {
                "credentialType": "ACCESS_KEY",
                "accessKey": self.minio_access_key,
                "accessSecret": self.minio_secret_key,
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
            f"http://{self.dremio_host}:{self.dremio_port}/apiv2/source/SeedFiles",
            headers=headers,
            json=payload,
        )
        if resp.status_code not in (200, 409):
            print(
                f"  Warning: SeedFiles source creation returned "
                f"{resp.status_code}: {resp.text}"
            )
        else:
            print("  SeedFiles S3 source created/updated in Dremio")

    # ------------------------------------------------------------------
    # Metadata refresh
    # ------------------------------------------------------------------

    def _refresh_source(self, token: str) -> None:
        import requests

        headers = self._headers(token)
        resp = requests.get(
            f"http://{self.dremio_host}:{self.dremio_port}/apiv2/source/SeedFiles",
            headers=headers,
        )
        if resp.status_code == 200:
            requests.post(
                f"http://{self.dremio_host}:{self.dremio_port}/apiv2/source/SeedFiles/refresh",
                headers=headers,
            )
            print("  SeedFiles metadata refresh triggered")
            time.sleep(5)

    # ------------------------------------------------------------------
    # CSV promotion
    # ------------------------------------------------------------------

    def _promote_csv(self, token: str, path_parts: list[str]) -> bool:
        import requests

        headers = self._headers(token)
        full_path = ["SeedFiles"] + path_parts
        encoded = ".".join(f'"{p}"' for p in full_path)
        path_param = "/".join(full_path)

        resp = requests.get(
            f"http://{self.dremio_host}:{self.dremio_port}/api/v3/catalog/by-path/{path_param}",
            headers=headers,
        )
        if resp.status_code != 200:
            print(f"    Cannot find file at {path_param}: {resp.status_code}")
            return False

        entity = resp.json()
        if entity.get("entityType") == "dataset":
            print(f"    Already promoted: {encoded}")
            return True

        entity_id = entity.get("id")
        if not entity_id:
            return False

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
            f"http://{self.dremio_host}:{self.dremio_port}/api/v3/catalog/{entity_id}",
            headers=headers,
            json=promote_payload,
        )
        if resp2.status_code in (200, 201):
            print(f"    Promoted: {encoded}")
            return True
        print(f"    Promote failed ({resp2.status_code}): {resp2.text[:200]}")
        return False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> None:
        print("\n=== Loading Dremio seeds via MinIO ===")

        print("\nStep 1: Uploading CSVs to MinIO...")
        self._upload_csvs_to_minio()

        print("\nStep 2: Creating SeedFiles S3 source...")
        token = self._get_token()
        self._create_s3_source(token)

        print("\nStep 3: Refreshing source metadata...")
        self._refresh_source(token)
        time.sleep(5)

        print(f"\nStep 4: Creating Nessie namespace '{self.schema_name}'...")
        try:
            self._sql(
                token,
                f'CREATE SCHEMA IF NOT EXISTS NessieSource."{self.schema_name}"',
            )
        except Exception as e:
            print(f"  Warning creating schema: {e}")

        print("\nStep 5: Creating Iceberg tables from promoted CSVs...")
        for subdir, csv_path, table_name in self.iter_seed_csvs():
            fname = os.path.basename(csv_path)

            if not self.csv_has_data(csv_path):
                cols = self.csv_columns(csv_path)
                if not cols:
                    print(f"  Skipping {table_name} (completely empty file)")
                    continue
                col_defs = ", ".join(f'"{c}" VARCHAR' for c in cols)
                sql = (
                    f"CREATE TABLE IF NOT EXISTS "
                    f'NessieSource."{self.schema_name}"."{table_name}" '
                    f"({col_defs})"
                )
                print(f"  Creating empty table: {table_name}")
                try:
                    self._sql(token, sql, timeout=60)
                except Exception as e:
                    print(f"    Error: {e}")
                continue

            promoted = self._promote_csv(token, ["seeds", subdir, fname])
            if not promoted:
                print(f"  Skipping CTAS for {table_name} (promotion failed)")
                continue

            s3_ref = f'"SeedFiles"."seeds"."{subdir}"."{fname}"'
            sql = (
                f"CREATE TABLE IF NOT EXISTS "
                f'NessieSource."{self.schema_name}"."{table_name}" AS '
                f"SELECT * FROM {s3_ref}"
            )
            print(f"  CTAS: {table_name}")
            try:
                self._sql(token, sql, timeout=120)
            except Exception as e:
                print(f"    Error: {e}")

        print("\nDremio seed loading complete.")
