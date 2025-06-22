#!/bin/bash
set -e

echo "--- Linux File Librarian Installer ---"

# --- 1. Install System Dependencies ---
echo "[INFO] Updating package lists and installing system dependencies (python3-pip, python3-tk, python3-venv)..."
sudo apt-get update
sudo apt-get install -y python3-pip python3-tk python3-venv git

# --- 2. Create Python Virtual Environment ---
if [ -d "venv" ]; then
    echo "[INFO] Virtual environment 'venv' already exists. Skipping creation."
else
    echo "[INFO] Creating Python virtual environment in 'venv'..."
    python3 -m venv venv
fi

# --- 3. Install Python Packages ---
echo "[INFO] Activating virtual environment and installing packages from requirements.txt..."
source venv/bin/activate
pip install -r requirements.txt
deactivate

# --- 4. Make Scripts Executable ---
echo "[INFO] Setting execution permissions for run scripts..."
chmod +x scripts/run_librarian.sh
chmod +x scripts/run_search_gui.sh

echo ""
echo "--- Installation Complete! ---"
echo "Next steps:"
echo "1. Configure your paths in 'src/librarian.py' and 'src/search_gui.py'."
echo "2. Run './scripts/run_librarian.sh' to build your library."
