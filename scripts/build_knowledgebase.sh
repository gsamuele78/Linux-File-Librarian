#!/bin/bash
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
PROJECT_ROOT="$SCRIPT_DIR/.."
source "$PROJECT_ROOT/venv/bin/activate"
echo "[INFO] Running the knowledge base builder..."
python3 "$PROJECT_ROOT/src/build_knowledgebase.py"
