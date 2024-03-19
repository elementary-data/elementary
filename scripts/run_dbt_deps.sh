#!/bin/bash
set -e

# Resolving [ELE-1628] Call dbt deps on Dockerfile instead of running it on the edr run.
export ELEMENTARY_PKG_LOCATION=$(pip show elementary-data | grep -i location | awk '{print $2}')
export ELEMENTARY_DBT_PROJECT_PATH="$ELEMENTARY_PKG_LOCATION/elementary/monitor/dbt_project"

dbt deps --project-dir "$ELEMENTARY_DBT_PROJECT_PATH"

