#!/bin/bash
# Restore Docker volume data from a seed cache directory.
# Usage: restore_seed_cache.sh <warehouse-type>
set -euo pipefail

WAREHOUSE_TYPE="${1:?Usage: restore_seed_cache.sh <warehouse-type>}"
CACHE_DIR="/tmp/seed-cache-${WAREHOUSE_TYPE}"

echo "Restoring seed cache for ${WAREHOUSE_TYPE}..."

# Restore each Docker volume from the cached tarballs.
# This runs BEFORE services start so containers initialise with cached data.
for archive in "$CACHE_DIR"/*.tar.gz; do
  [ -f "$archive" ] || continue
  VOLUME_NAME=$(basename "$archive" .tar.gz)
  echo "Restoring volume $VOLUME_NAME from $archive..."
  docker volume create "$VOLUME_NAME" 2>/dev/null || true
  docker run --rm -v "$VOLUME_NAME:/data" -v "$CACHE_DIR:/cache:ro" \
    alpine sh -c "cd /data && tar xzf /cache/${VOLUME_NAME}.tar.gz"
done
echo "Seed cache restored."
