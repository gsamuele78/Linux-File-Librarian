#!/bin/bash

# Linux File Librarian - Recovery Mode Script
# Handles incomplete processing runs

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
PROJECT_ROOT="$SCRIPT_DIR/.."

# Check if we're in the right directory
if [ ! -f "$PROJECT_ROOT/conf/config.ini" ]; then
    echo "[ERROR] Please run this script from the Linux-File-Librarian directory"
    exit 1
fi

# Activate virtual environment if it exists
if [ -d "$PROJECT_ROOT/venv" ]; then
    echo "[INFO] Activating virtual environment..."
    source "$PROJECT_ROOT/venv/bin/activate"
fi

echo "[INFO] Starting recovery mode..."
cd "$PROJECT_ROOT"
PYTHONPATH="$PROJECT_ROOT" python3 src/utility/recovery_processor.py