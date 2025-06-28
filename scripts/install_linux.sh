#!/usr/bin/env bash
set -e

echo "--- Linux File Librarian Installer ---"

# Check for sudo
if ! command -v sudo &> /dev/null; then
  echo "[ERROR] sudo command not found. Please run this script as root or install sudo."
  exit 1
fi

# Check if apt-get exists
if ! command -v apt-get &> /dev/null; then
  echo "[ERROR] This installer currently supports Debian/Ubuntu systems (apt-get required)."
  exit 1
fi

echo "[INFO] Installing system dependencies (pip, tk, venv, git, libmagic)..."
sudo apt-get update
sudo apt-get install -y python3-pip python3-tk python3-venv git libmagic1

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
PROJECT_ROOT="$SCRIPT_DIR/.."
VENV_PATH="$PROJECT_ROOT/venv"

if [ ! -f "$PROJECT_ROOT/requirements.txt" ]; then
  echo "[ERROR] requirements.txt not found in $PROJECT_ROOT."
  exit 1
fi

if [ ! -d "$VENV_PATH" ]; then
    echo "[INFO] Creating Python virtual environment..."
    python3 -m venv "$VENV_PATH"
fi

echo "[INFO] Installing Python packages..."
# shellcheck source=/dev/null
source "$VENV_PATH/bin/activate"
pip install -r "$PROJECT_ROOT/requirements.txt"
deactivate

echo "[INFO] Setting execution permissions..."
chmod +x "$PROJECT_ROOT/scripts/"*.sh

# Check for conf/config.ini, create a template if missing
if [ ! -f "$PROJECT_ROOT/conf/config.ini" ]; then
  echo "[WARN] conf/config.ini not found. Creating a default template."
  mkdir -p "$PROJECT_ROOT/conf"
  cat > "$PROJECT_ROOT/conf/config.ini" << EOF
[Paths]
library_dir = /path/to/your/library
output_dir = /path/to/output
EOF
  echo "[INFO] A default config.ini has been created at conf/config.ini. Please edit it with your custom paths."
else
  echo "[INFO] Please edit 'conf/config.ini' with your custom paths before running."
fi

echo ""
echo "--- Installation Complete! ---"
