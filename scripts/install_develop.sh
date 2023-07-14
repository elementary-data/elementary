#!/bin/bash

# This script installs the package in editable mode in a way that allows static type checkers to work.
# Information about the problem can be found in the following sources:
#   - https://github.com/python/mypy/issues/13392
#   - https://microsoft.github.io/pyright/#/import-resolution?id=editable-installs
#   - https://setuptools.pypa.io/en/latest/userguide/development_mode.html#legacy-behavior

pip install -e . --config-settings editable_mode=compat

