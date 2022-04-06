#!/usr/bin/env bash
# Generate Sphinx documentation in build/html

set -o errexit
set -o nounset
set -o pipefail

# Move to the project root directory
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
export PYTHONPATH=$PWD

rm -rf build/doctrees build/html
sphinx-build -j auto -b html -d build/doctrees doc build/html

echo "Documentation generated at file://$PWD/build/html/index.html"