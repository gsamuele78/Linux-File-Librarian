#!/usr/bin/env bash
# Ensures the project dependencies are installed correctly.

# Exit immediately if a command exits with a non-zero status.
set -e

echo "--- Linux File Librarian Installer ---"

echo "[INFO] Updating package lists and installing system dependencies..."
# The user will be prompted for their password here.
sudo apt-get update
sudo apt-get install -y python3-pip python3-tk python3-venv git libmagic1

# Find the project root directory, which is one level up from this script.
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
PROJECT_ROOT="$SCRIPT_DIR/.."

VENV_PATH="$PROJECT_ROOT/venv"

if [ -d "$VENV_PATH" ]; then
    echo "[INFO] Virtual environment '$VENV_PATH' already exists. Skipping creation."
else
    echo "[INFO] Creating Python virtual environment in '$VENV_PATH'..."
    python3 -m venv "$VENV_PATH"
fi

echo "[INFO] Activating virtual environment and installing packages from requirements.txt..."
# We must source from the full path to activate the venv.
# shellcheck source=/dev/null
source "$VENV_PATH/bin/activate"
pip install -r "$PROJECT_ROOT/requirements.txt"
deactivate

echo "[INFO] Setting execution permissions for run scripts..."
chmod +x "$PROJECT_ROOT/scripts/"*.sh

echo ""
echo "--- Installation Complete! ---"
echo "IMPORTANT: Please edit 'conf/config.ini' with your custom paths before running."
