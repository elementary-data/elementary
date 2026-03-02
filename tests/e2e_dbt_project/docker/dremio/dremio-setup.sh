#!/bin/sh

# Install required tools
apk add --no-cache curl jq

# Wait for Dremio to be ready
until curl -s http://dremio:9047; do
  echo "Waiting for Dremio..."
  sleep 5
done

echo "Dremio is up. Proceeding with configuration..."

# Log in to Dremio to get the auth token
AUTH_TOKEN=$(curl -s -X POST "http://dremio:9047/apiv2/login" \
  -H "Content-Type: application/json" \
  --data "{\"userName\":\"dremio\", \"password\":\"dremio123\"}" | jq -r .token)

# Check if AUTH_TOKEN is not empty
if [ -z "$AUTH_TOKEN" ]; then
  echo "Failed to obtain Dremio auth token"
  exit 1
fi

echo "Obtained Dremio auth token"

# Create a Nessie catalog source in Dremio (used as "database" for views)
curl -s -X PUT "http://dremio:9047/apiv2/source/NessieSource" \
  -H "Content-Type: application/json" \
  -H "Authorization: _dremio$AUTH_TOKEN" \
  --data "{\"name\":\"NessieSource\",\"config\":{\"nessieEndpoint\":\"http://catalog:19120/api/v2\",\"nessieAuthType\":\"NONE\",\"credentialType\":\"ACCESS_KEY\",\"awsAccessKey\":\"admin\",\"awsAccessSecret\":\"password\",\"awsRootPath\":\"datalake\",\"secure\":false,\"propertyList\":[{\"name\":\"fs.s3a.path.style.access\",\"value\":\"true\"},{\"name\":\"fs.s3a.endpoint\",\"value\":\"dremio-storage:9000\"},{\"name\":\"dremio.s3.compat\",\"value\":\"true\"}]},\"type\":\"NESSIE\",\"metadataPolicy\":{\"deleteUnavailableDatasets\":true,\"autoPromoteDatasets\":false,\"namesRefreshMillis\":3600000,\"datasetDefinitionRefreshAfterMillis\":3600000,\"datasetDefinitionExpireAfterMillis\":10800000,\"authTTLMillis\":86400000,\"updateMode\":\"PREFETCH_QUERIED\"}}"

echo "Nessie Source created in Dremio"

# Create a Space in Dremio for views (views cannot be created in data sources)
curl -s -X POST "http://dremio:9047/api/v3/catalog" \
  -H "Content-Type: application/json" \
  -H "Authorization: _dremio$AUTH_TOKEN" \
  --data "{\"entityType\":\"space\",\"name\":\"elementary_ci\"}"

echo "Space 'elementary_ci' created in Dremio"
