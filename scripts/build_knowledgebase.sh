#!/usr/bin/env bash
# Activates the virtual environment and runs the knowledge base builder.
# Only scrape sources that allow it (see robots.txt and terms).
# Throttle requests to avoid being blocked.

set -o errexit -o nounset -o pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."

if [[ ! -d "${PROJECT_ROOT}/venv" ]]; then
  echo "[ERROR] Python virtual environment not found. Run install_linux.sh first."
  exit 1
fi

if [[ ! -f "${PROJECT_ROOT}/src/build_knowledgebase.py" ]]; then
  echo "[ERROR] src/build_knowledgebase.py not found."
  exit 1
fi

# shellcheck source=/dev/null
source "${PROJECT_ROOT}/venv/bin/activate"

echo "[INFO] Running the knowledge base builder..."
cd "${PROJECT_ROOT}"
if ! PYTHONPATH="${PROJECT_ROOT}" python3 src/build_knowledgebase.py; then
  echo "[ERROR] build_knowledgebase.py failed!"
  exit 1
fi
