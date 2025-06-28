#!/usr/bin/env bash
# Activates the virtual environment and runs the main librarian script.

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
PROJECT_ROOT="$SCRIPT_DIR/.."

if [ ! -d "$PROJECT_ROOT/venv" ]; then
  echo "[ERROR] Python virtual environment not found. Run install_linux.sh first."
  exit 1
fi

if [ ! -f "$PROJECT_ROOT/src/librarian.py" ]; then
  echo "[ERROR] src/librarian.py not found."
  exit 1
fi

# shellcheck source=/dev/null
source "$PROJECT_ROOT/venv/bin/activate"

echo "[INFO] Running the librarian script..."
cd "$PROJECT_ROOT"
PYTHONPATH="$PROJECT_ROOT" python3 src/librarian.py | tee librarian_run.log || {
  echo "[ERROR] librarian.py failed! See librarian_run.log for details."
  exit 1
}
