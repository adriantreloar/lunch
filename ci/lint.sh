#!/usr/bin/env bash
# Run all code linters

# Move to the project root directory
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

# set -o xtrace

set +o nounset
source $HOME/miniconda3/bin/activate lunch
set -o nounset

# Sort imports
isort .
# Make the code look like a Nazi would find appealing
black -t py310 .
flake8 --max-line-length=121

mypy lunch/base_classes
mypy lunch/errors
mypy lunch/globals
mypy lunch/import_engine
mypy lunch/managers
mypy lunch/model
mypy lunch/mvcc
mypy lunch/parser
mypy lunch/query_engine
mypy lunch/reference_data
mypy lunch/storage









