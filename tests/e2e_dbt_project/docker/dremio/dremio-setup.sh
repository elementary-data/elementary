#!/bin/sh
set -e

# Install required tools
apk add --no-cache curl jq

# Wait for Dremio to be ready
until curl -s http://dremio:9047; do
  echo "Waiting for Dremio..."
  sleep 5
done

echo "Dremio is up. Proceeding with configuration..."

# Log in to Dremio to get the auth token
# Credentials are passed via environment variables from docker-compose.yml
DREMIO_USER="${DREMIO_USER:-dremio}"
DREMIO_PASS="${DREMIO_PASS:-dremio123}"
AUTH_TOKEN=$(curl -s -X POST "http://dremio:9047/apiv2/login" \
  -H "Content-Type: application/json" \
  --data "{\"userName\":\"$DREMIO_USER\", \"password\":\"$DREMIO_PASS\"}" | jq -r '.token // empty')

# Check if AUTH_TOKEN is not empty or null
if [ -z "$AUTH_TOKEN" ] || [ "$AUTH_TOKEN" = "null" ]; then
  echo "Failed to obtain Dremio auth token"
  exit 1
fi

echo "Obtained Dremio auth token"

# Create a Nessie catalog source in Dremio (used as "database" for views)
HTTP_CODE=$(curl -s -o /tmp/dremio_source_response.json -w "%{http_code}" -X PUT "http://dremio:9047/apiv2/source/NessieSource" \
  -H "Content-Type: application/json" \
  -H "Authorization: _dremio$AUTH_TOKEN" \
  --data "{\"name\":\"NessieSource\",\"config\":{\"nessieEndpoint\":\"http://catalog:19120/api/v2\",\"nessieAuthType\":\"NONE\",\"credentialType\":\"ACCESS_KEY\",\"awsAccessKey\":\"${MINIO_ROOT_USER:-admin}\",\"awsAccessSecret\":\"${MINIO_ROOT_PASSWORD:-password}\",\"awsRootPath\":\"datalake\",\"secure\":false,\"propertyList\":[{\"name\":\"fs.s3a.path.style.access\",\"value\":\"true\"},{\"name\":\"fs.s3a.endpoint\",\"value\":\"dremio-storage:9000\"},{\"name\":\"dremio.s3.compat\",\"value\":\"true\"}]},\"type\":\"NESSIE\",\"metadataPolicy\":{\"deleteUnavailableDatasets\":true,\"autoPromoteDatasets\":false,\"namesRefreshMillis\":3600000,\"datasetDefinitionRefreshAfterMillis\":3600000,\"datasetDefinitionExpireAfterMillis\":10800000,\"authTTLMillis\":86400000,\"updateMode\":\"PREFETCH_QUERIED\"}}")


if [ "$HTTP_CODE" -lt 200 ] || [ "$HTTP_CODE" -ge 300 ]; then
  echo "Failed to create Nessie Source in Dremio (HTTP $HTTP_CODE)"
  cat /tmp/dremio_source_response.json
  exit 1
fi

echo "Nessie Source created in Dremio"
