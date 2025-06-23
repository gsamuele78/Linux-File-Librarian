#!/usr/bin/env bash
# Activates the virtual environment and runs the search GUI.

# Exit on error, on unset variables, and propagate pipeline errors.
set -euo pipefail

# Find the project root directory relative to this script.
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
PROJECT_ROOT="$SCRIPT_DIR/.."

# Activate the virtual environment.
# shellcheck source=/dev/null
source "$PROJECT_ROOT/venv/bin/activate"

echo "[INFO] Launching the search GUI..."
# Run the python script from the project root to ensure relative paths are correct.
cd "$PROJECT_ROOT"
python3 src/search_gui.py
