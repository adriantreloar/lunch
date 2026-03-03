#!/usr/bin/env bash
# Install uv (if absent) and sync the project environment

set -o errexit
set -o nounset
set -o pipefail

# Move to the project root directory
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

# Install uv if not present
if ! command -v uv &> /dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # Reload PATH so the newly installed uv is found
    export PATH="$HOME/.local/bin:$PATH"
fi

# Create/update the virtual environment and install all dependencies
uv sync --all-extras

echo
echo "Environment ready (.venv)"
