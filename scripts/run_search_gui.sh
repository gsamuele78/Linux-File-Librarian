#!/usr/bin/env bash
# Activates the virtual environment and runs the search GUI.

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
PROJECT_ROOT="$SCRIPT_DIR/.."

if [ ! -d "$PROJECT_ROOT/venv" ]; then
  echo "[ERROR] Python virtual environment not found. Run install_linux.sh first."
  exit 1
fi

if [ ! -f "$PROJECT_ROOT/src/search_gui.py" ]; then
  echo "[ERROR] src/search_gui.py not found."
  exit 1
fi

# Check for Tkinter
if ! python3 -c "import tkinter" &>/dev/null; then
  echo "[ERROR] Tkinter not found. Please install python3-tk."
  exit 1
fi

# shellcheck source=/dev/null
source "$PROJECT_ROOT/venv/bin/activate"

echo "[INFO] Launching the search GUI..."
cd "$PROJECT_ROOT"
PYTHONPATH="$PROJECT_ROOT" python3 src/search_gui.py || {
  echo "[ERROR] search_gui.py failed!"
  exit 1
}
