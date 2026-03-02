#!/bin/bash
# Save Docker volume data to a cache directory for seed caching.
# Usage: save_seed_cache.sh <warehouse-type>
set -euo pipefail

WAREHOUSE_TYPE="${1:?Usage: save_seed_cache.sh <warehouse-type>}"
CACHE_DIR="/tmp/seed-cache-${WAREHOUSE_TYPE}"
mkdir -p "$CACHE_DIR"

# Get the Docker Compose project name and list volumes
COMPOSE_VOLUMES=$(docker compose config --volumes 2>/dev/null || true)
PROJECT=$(docker compose config 2>/dev/null | grep '^name:' | awk '{print $2}' || echo "e2e_dbt_project")

# Stop running containers so no files change while archiving
docker compose stop || true

for vol in $COMPOSE_VOLUMES; do
  FULL_VOL="${PROJECT}_${vol}"
  if docker volume inspect "$FULL_VOL" >/dev/null 2>&1; then
    echo "Saving volume $FULL_VOL..."
    docker run --rm -v "$FULL_VOL:/data:ro" -v "$CACHE_DIR:/cache" \
      alpine sh -c "cd /data && tar czf /cache/${FULL_VOL}.tar.gz ."
  fi
done

# Restart containers for the rest of the pipeline
docker compose start || true

# Wait for services to be ready after restart
case "$WAREHOUSE_TYPE" in
  clickhouse)
    for i in $(seq 1 30); do
      curl -sf http://localhost:8123/ping > /dev/null && break
      echo "Waiting for ClickHouse after restart... ($i/30)"; sleep 2
    done
    ;;
  postgres)
    for i in $(seq 1 30); do
      pg_isready -h localhost -p 5432 > /dev/null 2>&1 && break
      echo "Waiting for Postgres after restart... ($i/30)"; sleep 2
    done
    ;;
esac
echo "Seed cache saved."
