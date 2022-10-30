#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

# Move to the project root directory
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

# Source the conda build environment; install it if it doesn't already exist
bash ci/make_env.sh

# Build sphinx documentation in build/html
bash ci/doc.sh
# Run all tests
bash ci/test.sh

echo
echo "Build completed successfully"
