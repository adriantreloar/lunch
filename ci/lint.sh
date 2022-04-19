#!/usr/bin/env bash
# Run all code linters
# Always exit 0, even in the case of error

# Move to the project root directory
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

set -o xtrace

# Sort imports
isort .
# Make the code look like a Nazi would find appealing
black -t py38 .  # py39 not yet available
flake8 --max-line-length=121
mypy lunch
