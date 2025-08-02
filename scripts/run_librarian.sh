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

echo "[INFO] Setting up memory constraints..."
cd "$PROJECT_ROOT"
python3 src/utility/setup_memory_constraints.py

# Check for enterprise mode
if [ -f "$PROJECT_ROOT/src/integration_adapter.py" ] && [ "${LIBRARIAN_ENTERPRISE_MODE:-false}" = "true" ]; then
    echo "[INFO] Running librarian with enterprise enhancements..."
    PYTHONPATH="$PROJECT_ROOT" python3 -c "from src.integration_adapter import integrate_with_existing_scripts; import sys; sys.exit(integrate_with_existing_scripts())" | tee librarian_run.log || {
        echo "[ERROR] Enterprise librarian failed! See librarian_run.log for details."
        exit 1
    }
else
    echo "[INFO] Running the librarian script with memory optimization..."
    PYTHONPATH="$PROJECT_ROOT" python3 src/librarian.py | tee librarian_run.log || {
        echo "[ERROR] librarian.py failed! See librarian_run.log for details."
        exit 1
    }
fi
