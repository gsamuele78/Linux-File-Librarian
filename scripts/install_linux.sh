      
#!/usr/bin/env bash
set -e
echo "--- Linux File Librarian Installer ---"

# --- 1. System Package Installation ---
echo "[INFO] Installing system dependencies (pip, tk, venv, git, libmagic)..."
sudo apt-get update
sudo apt-get install -y python3-pip python3-tk python3-venv git libmagic1 wget

# --- 2. Selenium Dependency: Google Chrome ---
if ! command -v google-chrome &> /dev/null
then
    echo "[INFO] Google Chrome not found. Installing..."
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
    sudo apt install -y ./google-chrome-stable_current_amd64.deb
    rm google-chrome-stable_current_amd64.deb
else
    echo "[INFO] Google Chrome is already installed."
fi

# --- 3. Python Environment Setup ---
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
PROJECT_ROOT="$SCRIPT_DIR/.."
VENV_PATH="$PROJECT_ROOT/venv"

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

echo ""
echo "--- Installation Complete! ---"
echo "IMPORTANT: Please edit 'conf/config.ini' with your custom paths before running."

    
