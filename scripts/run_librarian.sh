#!/bin/bash
# This script activates the virtual environment and runs the librarian tool.

# Find the project root directory relative to this script's location
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
PROJECT_ROOT="$SCRIPT_DIR/.."

# Activate the virtual environment
source "$PROJECT_ROOT/venv/bin/activate"

echo "[INFO] Running the librarian script..."
python3 "$PROJECT_ROOT/src/librarian.py"
