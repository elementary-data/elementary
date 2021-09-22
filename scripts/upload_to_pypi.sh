#!/bin/bash

set -eo pipefail

ELEMENTARY_LINEAGE_PATH="$( cd "$(dirname "$0")/.." ; pwd -P )"

PYTHON_BIN=${PYTHON_BIN:-python}

echo "$PYTHON_BIN"

set -x

cd "$ELEMENTARY_LINEAGE_PATH"
[[ $1 == test ]] && $PYTHON_BIN -m twine upload --repository testpypi "$ELEMENTARY_LINEAGE_PATH"/dist/* || $PYTHON_BIN -m twine upload "$ELEMENTARY_LINEAGE_PATH"/dist/*

set +x