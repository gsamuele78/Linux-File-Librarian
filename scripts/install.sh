#!/bin/bash
set -euo pipefail

echo "--- Linux File Librarian Installer ---"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to install system dependencies
install_system_deps() {
    echo "[INFO] Installing system dependencies..."
    
    if command_exists apt-get; then
        sudo apt-get update
        sudo apt-get install -y python3-pip python3-tk python3-venv python3-dev git libmagic1 qpdf
    elif command_exists yum; then
        sudo yum install -y python3-pip python3-tkinter python3-venv python3-devel git file-libs qpdf
    elif command_exists dnf; then
        sudo dnf install -y python3-pip python3-tkinter python3-venv python3-devel git file-libs qpdf
    else
        echo "[ERROR] Unsupported package manager. Please install dependencies manually."
        echo "Required: python3, python3-pip, python3-tk, python3-venv, git, libmagic, qpdf"
        exit 1
    fi
}

# Function to create virtual environment
create_venv() {
    if [ -d "venv" ]; then
        echo "[INFO] Virtual environment 'venv' already exists."
        read -p "Do you want to recreate it? [y/N]: " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "[INFO] Removing existing virtual environment..."
            rm -rf venv
        else
            return 0
        fi
    fi
    
    echo "[INFO] Creating Python virtual environment in 'venv'..."
    python3 -m venv venv
    
    if [ ! -d "venv" ]; then
        echo "[ERROR] Failed to create virtual environment"
        exit 1
    fi
}

# Function to install Python packages
install_python_deps() {
    echo "[INFO] Activating virtual environment and installing packages..."
    
    # shellcheck source=/dev/null
    source venv/bin/activate
    
    # Upgrade pip first
    pip install --upgrade pip setuptools wheel
    
    # Install requirements
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
    else
        echo "[WARNING] requirements.txt not found, installing basic dependencies..."
        pip install pandas beautifulsoup4 requests tqdm psutil pymupdf python-magic rapidfuzz lxml
    fi
    
    deactivate
}

# Function to set permissions
set_permissions() {
    echo "[INFO] Setting execution permissions for scripts..."
    
    find scripts/ -name "*.sh" -type f -exec chmod +x {} \;
    
    # Verify permissions were set
    if [ ! -x "scripts/run_librarian.sh" ]; then
        echo "[ERROR] Failed to set execute permissions"
        exit 1
    fi
}

# Main installation process
main() {
    # Check if running as root (not recommended)
    if [ "$EUID" -eq 0 ]; then
        echo "[WARNING] Running as root is not recommended"
        read -p "Continue anyway? [y/N]: " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    # Check Python version
    if ! command_exists python3; then
        echo "[ERROR] Python 3 is required but not installed"
        exit 1
    fi
    
    python_version=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    echo "[INFO] Found Python $python_version"
    
    if ! python3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 7) else 1)"; then
        echo "[ERROR] Python 3.7 or higher is required"
        exit 1
    fi
    
    # Install dependencies
    install_system_deps
    create_venv
    install_python_deps
    set_permissions
    
    echo ""
    echo "--- Installation Complete! ---"
    echo "Next steps:"
    echo "1. Edit 'conf/config.ini' to configure your source and destination paths"
    echo "2. (Optional) Run './scripts/build_knowledgebase.sh' to build the TTRPG knowledge base"
    echo "3. Run './scripts/run_librarian.sh' to build your library"
    echo "4. Run './scripts/run_search_gui.sh' to search your library"
    echo ""
    echo "For help, see README.md or check the logs in librarian_run.log"
}

# Run main function
main "$@"
