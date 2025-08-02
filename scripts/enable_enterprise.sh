#!/bin/bash

# Enable Enterprise Mode for Linux File Librarian
# This script activates enterprise features for existing installations

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Linux File Librarian - Enterprise Mode Activation"
echo "================================================="

# Create enterprise mode flag
echo "[INFO] Enabling enterprise mode..."
touch "$PROJECT_ROOT/.enterprise_mode"

# Set environment variable for current session
export LIBRARIAN_ENTERPRISE_MODE=true

# Add to shell profile for persistence
if [ -f "$HOME/.bashrc" ]; then
    if ! grep -q "LIBRARIAN_ENTERPRISE_MODE" "$HOME/.bashrc"; then
        echo "export LIBRARIAN_ENTERPRISE_MODE=true" >> "$HOME/.bashrc"
        echo "[INFO] Added enterprise mode to ~/.bashrc"
    fi
fi

# Check system requirements for enterprise mode
echo "[INFO] Checking system requirements for enterprise mode..."

# Check memory
MEMORY_GB=$(free -g | awk 'NR==2{print $2}')
if [ "$MEMORY_GB" -lt 4 ]; then
    echo "[WARNING] Enterprise mode recommended for systems with 4GB+ RAM (detected: ${MEMORY_GB}GB)"
else
    echo "[OK] Memory: ${MEMORY_GB}GB"
fi

# Check CPU cores
CPU_CORES=$(nproc)
if [ "$CPU_CORES" -lt 2 ]; then
    echo "[WARNING] Enterprise mode recommended for systems with 2+ CPU cores (detected: ${CPU_CORES})"
else
    echo "[OK] CPU cores: ${CPU_CORES}"
fi

# Check Python version
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "[INFO] Python version: $PYTHON_VERSION"

# Install additional enterprise dependencies if needed
if [ -f "$PROJECT_ROOT/requirements_enterprise.txt" ]; then
    echo "[INFO] Installing enterprise dependencies..."
    source "$PROJECT_ROOT/venv/bin/activate"
    pip install -r "$PROJECT_ROOT/requirements_enterprise.txt"
fi

echo ""
echo "Enterprise Mode Activated Successfully!"
echo "======================================"
echo ""
echo "Enterprise features now available:"
echo "• Advanced performance optimization"
echo "• Memory-mapped file processing"
echo "• Predictive scaling"
echo "• Circuit breaker fault tolerance"
echo "• Comprehensive monitoring"
echo "• Professional reporting"
echo ""
echo "Usage:"
echo "  ./scripts/run_librarian.sh          # Uses enterprise mode automatically"
echo "  ./scripts/run_professional.sh       # Full enterprise experience"
echo ""
echo "To disable enterprise mode:"
echo "  rm $PROJECT_ROOT/.enterprise_mode"
echo "  unset LIBRARIAN_ENTERPRISE_MODE"
echo ""