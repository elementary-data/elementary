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

# Create the S3 source in Dremio
curl -s -X PUT "http://dremio:9047/apiv2/source/S3Source" \
  -H "Content-Type: application/json" \
  -H "Authorization: _dremio$AUTH_TOKEN" \
  --data "{\"name\":\"S3Source\",\"config\":{\"credentialType\":\"ACCESS_KEY\",\"accessKey\":\"admin\",\"accessSecret\":\"password\",\"secure\":false,\"externalBucketList\":[],\"enableAsync\":true,\"enableFileStatusCheck\":true,\"rootPath\":\"/\",\"defaultCtasFormat\":\"ICEBERG\",\"propertyList\":[{\"name\":\"fs.s3a.path.style.access\",\"value\":\"true\"},{\"name\":\"fs.s3a.endpoint\",\"value\":\"minio:9000\"},{\"name\":\"dremio.s3.compat\",\"value\":\"true\"}],\"whitelistedBuckets\":[],\"isCachingEnabled\":false,\"maxCacheSpacePct\":100},\"type\":\"S3\",\"metadataPolicy\":{\"deleteUnavailableDatasets\":true,\"autoPromoteDatasets\":false,\"namesRefreshMillis\":3600000,\"datasetDefinitionRefreshAfterMillis\":3600000,\"datasetDefinitionExpireAfterMillis\":10800000,\"authTTLMillis\":86400000,\"updateMode\":\"PREFETCH_QUERIED\"}}"

echo "S3 Source created in Dremio"
