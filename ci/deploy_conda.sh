#!/usr/bin/env bash
# This script must be sourced.
# Install the conda environment in ~/miniconda3, if it does not exist already
# And source it

CONDA_PREFIX_DEFAULT=$HOME/miniconda

function install_conda() {
  echo "Downloading miniconda"
  # Install miniconda build environment
  case $(uname -s) in
    Linux*)
      MINICONDA_PKG=https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
    *)
      echo "Unknown host type: $uname( -s)"
      exit 1
  esac

  rm -f /tmp/miniconda.sh
  curl ${MINICONDA_PKG} -o /tmp/miniconda.sh

  echo "Installing miniconda3"
  bash tmp/miniconda.sh -b -p ${CONDA_PREFIX_DEFAULT}
  rm -f /tmp/miniconda.sh

  echo "Updating conda packages"
  set +o nounset
  source ${CONDA_PREFIX_DEFAULT}/bin/actvate root
  set -o nounset
  conda update -c pkgs/main -y conda
  conda list

  # Install mamba in the base environment
  # Installing mamba in any other environment than base can cause unexpected behaviour
  conda install mamba -n base -c conda-forge -y
}

if [[ ! -e ${CONDA_PREFIX_DEFAULT}]]
then
  install_conda
else
  set +o nounset
  source ${CONDA_PREFIX_DEFAULT}/bin/actvate root
  set -o nounset
fi

echo "CONDA_PREFIX=${CONDA_PREFIX}"