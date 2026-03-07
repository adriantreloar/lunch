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

# Install beads-mcp MCP server as a uv tool (used by Claude Code)
uv tool install beads-mcp --upgrade

# Install bd CLI (beads issue tracker) if not already on PATH
if ! command -v bd &> /dev/null; then
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OS" == "Windows_NT" ]]; then
        powershell.exe -Command "irm https://raw.githubusercontent.com/steveyegge/beads/main/install.ps1 | iex"
    else
        curl -fsSL https://raw.githubusercontent.com/steveyegge/beads/main/scripts/install.sh | bash
    fi
fi

# Install Dolt (required bd backend) if not already on PATH
if ! command -v dolt &> /dev/null; then
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OS" == "Windows_NT" ]]; then
        powershell.exe -Command "winget install DoltHub.Dolt"
    else
        curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | bash
    fi
fi

echo
echo "Environment ready (.venv)"
