#!/usr/bin/env bash
# Run all code linters

set -o errexit
set -o nounset
set -o pipefail

# Move to the project root directory
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

# Sort imports
uv run isort .
# Format
uv run black -t py310 .
# Style check
uv run flake8

# Type checks
uv run mypy src/lunch/base_classes
uv run mypy src/lunch/errors
uv run mypy src/lunch/globals
uv run mypy src/lunch/import_engine
uv run mypy src/lunch/managers
uv run mypy src/lunch/model
uv run mypy src/lunch/mvcc
uv run mypy src/lunch/parser
uv run mypy src/lunch/query_engines
uv run mypy src/lunch/storage
