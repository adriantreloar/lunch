#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

# Move to the project root directory
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

# Source the conda build environment; install it if it doesn't already exist
source ci/deploy_conda.sh

set +o nounset
source activate base
set -o nounset

# Install mamba
conda install mamba -n base conda-forge -y

# Create conda env for production
mamba remove --name lunch -all -y
mamba env create -n lunch --file

set +o nounset
source activate lunch
set -o nounset

# Run installer
pip install .

# Snapshot and log the installed packages, in case one of the extra packages used for
# build/dev change the versions
rm -rf build
mkdir -p build
# Log full list of packages for forensic analysis
mamba list > build/conda-1prod.list

echo
echo "Production packages"
echo "==================="
cat build/conda-1prod.list

# End of production package generation

# Add extra packages for build and dev
mamba env update -n lunch --file ci/requirements-2dev.yml

# verify that no production requirements have been changed
mamba list > build/conda-2dev.list
diff -u build/conda-1prod.list build/conda-2dev.list > build/conda.diff || true

echo
echo "Extra packages for build/dev"
echo "============================"
cat build/conda.diff
echo

if grep '^-' build/conda.diff | grep -v '^--- '
then
  echo "Some of the production packages have been changed"
  exit 1
fi

# Preserve a copy of the conda list, to help any debugging
mkdir -p ~/.lunch
cp -a build/conda-2dev.list ~/.lunch/conda.list.$(date '+%Y%m%d-%H%M%S')

# Build sphinx documentation in build/html
bash ci/doc.sh
# Run all tests
bash ci/test.sh

echo
echo "Build completed successfully"
