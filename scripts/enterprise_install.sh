#!/bin/bash
set -euo pipefail

# Enterprise Linux File Librarian Installation Script
# Provides comprehensive installation with validation, monitoring, and rollback capabilities

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INSTALL_LOG="$PROJECT_ROOT/logs/enterprise_install.log"
PYTHON_INSTALLER="$PROJECT_ROOT/src/enterprise_installer.py"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$INSTALL_LOG"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$INSTALL_LOG"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$INSTALL_LOG"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$INSTALL_LOG"
}

# Error handling
error_exit() {
    log_error "$1"
    log_error "Installation failed. Check $INSTALL_LOG for details."
    exit 1
}

# Cleanup function
cleanup() {
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log_error "Installation interrupted or failed"
        if [ -n "${ROLLBACK_AVAILABLE:-}" ]; then
            log_info "Rollback information available in installation log"
        fi
    fi
    exit $exit_code
}

# Setup signal handlers
trap cleanup EXIT
trap 'error_exit "Installation interrupted by user"' INT TERM

# Initialize logging
init_logging() {
    mkdir -p "$(dirname "$INSTALL_LOG")"
    echo "=== Enterprise Installation Started at $(date) ===" >> "$INSTALL_LOG"
    log_info "Installation log: $INSTALL_LOG"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if running as root (not recommended)
    if [ "$EUID" -eq 0 ]; then
        log_warning "Running as root is not recommended for security reasons"
        read -p "Continue anyway? [y/N]: " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            error_exit "Installation cancelled by user"
        fi
    fi
    
    # Check Python 3
    if ! command -v python3 >/dev/null 2>&1; then
        error_exit "Python 3 is required but not installed"
    fi
    
    local python_version
    python_version=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    log_info "Found Python $python_version"
    
    # Check Python version (3.8+)
    if ! python3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)" 2>/dev/null; then
        error_exit "Python 3.8 or higher is required, found $python_version"
    fi
    
    # Check sudo availability
    if ! command -v sudo >/dev/null 2>&1; then
        error_exit "sudo is required for system package installation"
    fi
    
    # Check internet connectivity
    if ! ping -c 1 google.com >/dev/null 2>&1; then
        log_warning "Internet connectivity check failed - package installation may fail"
    fi
    
    log_success "Prerequisites check completed"
}

# Validate system requirements using Python installer
validate_system() {
    log_info "Validating system requirements..."
    
    if [ ! -f "$PYTHON_INSTALLER" ]; then
        error_exit "Enterprise installer module not found: $PYTHON_INSTALLER"
    fi
    
    # Run system validation
    if python3 "$PYTHON_INSTALLER" --validate-only; then
        log_success "System requirements validation passed"
    else
        error_exit "System requirements validation failed"
    fi
}

# Backup existing installation
backup_existing() {
    log_info "Checking for existing installation..."
    
    local backup_needed=false
    local backup_dir="$PROJECT_ROOT/backup_$(date +%Y%m%d_%H%M%S)"
    
    # Check for existing virtual environment
    if [ -d "$PROJECT_ROOT/venv" ]; then
        log_info "Found existing virtual environment"
        backup_needed=true
    fi
    
    # Check for existing configuration
    if [ -f "$PROJECT_ROOT/conf/config.ini" ]; then
        log_info "Found existing configuration"
        backup_needed=true
    fi
    
    if [ "$backup_needed" = true ]; then
        log_info "Creating backup at $backup_dir"
        mkdir -p "$backup_dir"
        
        # Backup virtual environment
        if [ -d "$PROJECT_ROOT/venv" ]; then
            cp -r "$PROJECT_ROOT/venv" "$backup_dir/" 2>/dev/null || log_warning "Failed to backup virtual environment"
        fi
        
        # Backup configuration
        if [ -d "$PROJECT_ROOT/conf" ]; then
            cp -r "$PROJECT_ROOT/conf" "$backup_dir/" 2>/dev/null || log_warning "Failed to backup configuration"
        fi
        
        # Backup logs
        if [ -d "$PROJECT_ROOT/logs" ]; then
            cp -r "$PROJECT_ROOT/logs" "$backup_dir/" 2>/dev/null || log_warning "Failed to backup logs"
        fi
        
        log_success "Backup created successfully"
        export BACKUP_DIR="$backup_dir"
    else
        log_info "No existing installation found, skipping backup"
    fi
}

# Run enterprise installation
run_enterprise_install() {
    log_info "Starting enterprise installation process..."
    
    local install_args=("--project-root" "$PROJECT_ROOT")
    
    # Add force reinstall if requested
    if [ "${FORCE_REINSTALL:-}" = "true" ]; then
        install_args+=("--force-reinstall")
        log_info "Force reinstall enabled"
    fi
    
    # Run the Python installer
    if python3 "$PYTHON_INSTALLER" "${install_args[@]}"; then
        log_success "Enterprise installation completed successfully"
        ROLLBACK_AVAILABLE="true"
    else
        error_exit "Enterprise installation failed"
    fi
}

# Verify installation
verify_installation() {
    log_info "Verifying installation..."
    
    # Check virtual environment
    if [ ! -d "$PROJECT_ROOT/venv" ]; then
        error_exit "Virtual environment not found after installation"
    fi
    
    # Check Python executable
    local python_exe="$PROJECT_ROOT/venv/bin/python"
    if [ ! -x "$python_exe" ]; then
        error_exit "Python executable not found in virtual environment"
    fi
    
    # Test Python imports
    local test_modules=("pandas" "requests" "tqdm" "psutil")
    for module in "${test_modules[@]}"; do
        if ! "$python_exe" -c "import $module" 2>/dev/null; then
            error_exit "Failed to import required module: $module"
        fi
    done
    
    # Check configuration
    if [ ! -f "$PROJECT_ROOT/conf/config.ini" ]; then
        error_exit "Configuration file not found after installation"
    fi
    
    # Check scripts permissions
    local scripts=("run_librarian.sh" "run_search_gui.sh" "build_knowledgebase.sh")
    for script in "${scripts[@]}"; do
        local script_path="$PROJECT_ROOT/scripts/$script"
        if [ -f "$script_path" ] && [ ! -x "$script_path" ]; then
            error_exit "Script not executable: $script"
        fi
    done
    
    log_success "Installation verification completed"
}

# Setup system service (optional)
setup_system_service() {
    if [ "${SETUP_SERVICE:-}" != "true" ]; then
        return 0
    fi
    
    log_info "Setting up system service..."
    
    local service_file="/etc/systemd/system/linux-file-librarian.service"
    local service_content="[Unit]
Description=Linux File Librarian Enterprise Service
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=$PROJECT_ROOT
ExecStart=$PROJECT_ROOT/venv/bin/python $PROJECT_ROOT/src/enterprise_system_manager.py --project-root $PROJECT_ROOT --monitor
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target"
    
    # Create service file
    if echo "$service_content" | sudo tee "$service_file" >/dev/null; then
        sudo systemctl daemon-reload
        log_success "System service created: $service_file"
        log_info "To enable the service: sudo systemctl enable linux-file-librarian"
        log_info "To start the service: sudo systemctl start linux-file-librarian"
    else
        log_warning "Failed to create system service"
    fi
}

# Generate installation report
generate_report() {
    log_info "Generating installation report..."
    
    local report_file="$PROJECT_ROOT/reports/installation_report.txt"
    mkdir -p "$(dirname "$report_file")"
    
    {
        echo "=== Linux File Librarian Enterprise Installation Report ==="
        echo "Installation Date: $(date)"
        echo "Project Root: $PROJECT_ROOT"
        echo "Python Version: $(python3 --version)"
        echo "System: $(uname -a)"
        echo ""
        
        echo "=== Installation Status ==="
        if python3 "$PYTHON_INSTALLER" --project-root "$PROJECT_ROOT" --status 2>/dev/null; then
            echo "Status check completed successfully"
        else
            echo "Status check failed"
        fi
        echo ""
        
        echo "=== Next Steps ==="
        echo "1. Edit configuration: $PROJECT_ROOT/conf/config.ini"
        echo "2. Build knowledge base: $PROJECT_ROOT/scripts/build_knowledgebase.sh"
        echo "3. Run librarian: $PROJECT_ROOT/scripts/run_librarian.sh"
        echo "4. Launch search GUI: $PROJECT_ROOT/scripts/run_search_gui.sh"
        echo ""
        
        if [ -n "${BACKUP_DIR:-}" ]; then
            echo "=== Backup Information ==="
            echo "Backup created at: $BACKUP_DIR"
            echo "To restore backup if needed:"
            echo "  rm -rf $PROJECT_ROOT/venv $PROJECT_ROOT/conf"
            echo "  cp -r $BACKUP_DIR/venv $BACKUP_DIR/conf $PROJECT_ROOT/"
            echo ""
        fi
        
        echo "=== Support ==="
        echo "Installation log: $INSTALL_LOG"
        echo "For issues, check the logs directory: $PROJECT_ROOT/logs/"
        
    } > "$report_file"
    
    log_success "Installation report generated: $report_file"
}

# Main installation function
main() {
    echo "=== Linux File Librarian Enterprise Installer ==="
    echo "This installer will set up a complete enterprise-grade installation"
    echo ""
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --force-reinstall)
                export FORCE_REINSTALL="true"
                shift
                ;;
            --setup-service)
                export SETUP_SERVICE="true"
                shift
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo "Options:"
                echo "  --force-reinstall    Force reinstallation even if already installed"
                echo "  --setup-service      Setup systemd service (requires sudo)"
                echo "  --help              Show this help message"
                exit 0
                ;;
            *)
                error_exit "Unknown option: $1"
                ;;
        esac
    done
    
    # Run installation steps
    init_logging
    check_prerequisites
    validate_system
    backup_existing
    run_enterprise_install
    verify_installation
    setup_system_service
    generate_report
    
    echo ""
    log_success "=== Enterprise Installation Completed Successfully ==="
    echo ""
    echo "Next steps:"
    echo "1. Edit your configuration file:"
    echo "   nano $PROJECT_ROOT/conf/config.ini"
    echo ""
    echo "2. (Optional) Build the TTRPG knowledge base:"
    echo "   $PROJECT_ROOT/scripts/build_knowledgebase.sh"
    echo ""
    echo "3. Run the file librarian:"
    echo "   $PROJECT_ROOT/scripts/run_librarian.sh"
    echo ""
    echo "4. Launch the search interface:"
    echo "   $PROJECT_ROOT/scripts/run_search_gui.sh"
    echo ""
    echo "For detailed information, see:"
    echo "- Installation report: $PROJECT_ROOT/reports/installation_report.txt"
    echo "- Installation log: $INSTALL_LOG"
    echo "- README: $PROJECT_ROOT/README.md"
}

# Run main function
main "$@"