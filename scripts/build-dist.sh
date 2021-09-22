#!/bin/bash

set -eo pipefail

ELEMENTARY_LINEAGE_PATH="$( cd "$(dirname "$0")/.." ; pwd -P )"

PYTHON_BIN=${PYTHON_BIN:-python}

echo "$PYTHON_BIN"

set -x

rm -rf "$ELEMENTARY_LINEAGE_PATH"/dist
rm -rf "$ELEMENTARY_LINEAGE_PATH"/build
rm -rf "$ELEMENTARY_LINEAGE_PATH"/elementary_lineage.egg-info

cd "$ELEMENTARY_LINEAGE_PATH"
$PYTHON_BIN setup.py sdist bdist_wheel

set +x
