#!/usr/bin/env bash
# Generate Sphinx documentation in build/html

set -o errexit
set -o nounset
set -o pipefail

# Move to the project root directory
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
export PYTHONPATH=$PWD

rm -rf doc/build/doctrees doc/build/html
sphinx-build -j auto -b html doc/source doc/build

echo "Documentation generated at file://$PWD/doc/build/html/index.html"