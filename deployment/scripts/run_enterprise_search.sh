#!/bin/bash
set -euo pipefail

# Enterprise Search GUI Launcher
# Provides comprehensive GUI launching with validation and error handling

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PYTHON_EXE="$PROJECT_ROOT/venv/bin/python"
SEARCH_GUI="$PROJECT_ROOT/src/enterprise/enterprise_search_gui.py"
LOG_FILE="$PROJECT_ROOT/logs/search_gui.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
}

# Error handling
error_exit() {
    log_error "$1"
    exit 1
}

# Initialize logging
init_logging() {
    mkdir -p "$(dirname "$LOG_FILE")"
    echo "=== Enterprise Search GUI Started at $(date) ===" >> "$LOG_FILE"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if virtual environment exists
    if [ ! -f "$PYTHON_EXE" ]; then
        error_exit "Python virtual environment not found. Please run enterprise installation first."
    fi
    
    # Check if search GUI module exists
    if [ ! -f "$SEARCH_GUI" ]; then
        error_exit "Enterprise search GUI module not found: $SEARCH_GUI"
    fi
    
    # Check configuration
    local config_file="$PROJECT_ROOT/config/config.ini"
    if [ ! -f "$config_file" ]; then
        error_exit "Configuration file not found: $config_file"
    fi
    
    # Check if library database exists
    local library_root
    library_root=$(grep "^library_root" "$config_file" | cut -d'=' -f2 | xargs)
    
    if [ -z "$library_root" ] || [ "$library_root" = "/path/to/your/library/destination" ]; then
        error_exit "Please configure library_root in $config_file before running the search GUI"
    fi
    
    local db_file="$library_root/library_index.sqlite"
    if [ ! -f "$db_file" ]; then
        log_warning "Library database not found: $db_file"
        log_warning "Please run the librarian script first to build the library index"
        
        read -p "Continue anyway? [y/N]: " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    log_success "Prerequisites check completed"
}

# Check system requirements
check_system_requirements() {
    log_info "Checking system requirements..."
    
    # Check display
    if [ -z "${DISPLAY:-}" ] && [ -z "${WAYLAND_DISPLAY:-}" ]; then
        error_exit "No display found. GUI applications require a graphical environment."
    fi
    
    # Check Python GUI libraries
    if ! "$PYTHON_EXE" -c "import tkinter" 2>/dev/null; then
        error_exit "tkinter not available. Please install python3-tk package."
    fi
    
    # Check required Python modules
    local required_modules=("sqlite3" "pathlib" "json" "csv")
    for module in "${required_modules[@]}"; do
        if ! "$PYTHON_EXE" -c "import $module" 2>/dev/null; then
            error_exit "Required Python module not available: $module"
        fi
    done
    
    log_success "System requirements check completed"
}

# Launch GUI with error handling
launch_gui() {
    log_info "Launching Enterprise Search GUI..."
    
    # Set environment variables
    export PYTHONPATH="$PROJECT_ROOT/src:${PYTHONPATH:-}"
    
    # Launch GUI with proper error handling
    if "$PYTHON_EXE" "$SEARCH_GUI" 2>&1 | tee -a "$LOG_FILE"; then
        log_success "Search GUI closed normally"
    else
        local exit_code=$?
        log_error "Search GUI exited with error code: $exit_code"
        
        # Show helpful error message
        echo ""
        echo "The search GUI encountered an error. Common solutions:"
        echo "1. Check that the library database exists and is accessible"
        echo "2. Verify configuration in config/config.ini"
        echo "3. Ensure all required dependencies are installed"
        echo "4. Check the log file for detailed error information: $LOG_FILE"
        echo ""
        echo "For support, please check the documentation or contact the development team."
        
        exit $exit_code
    fi
}

# Performance monitoring
monitor_performance() {
    if [ "${MONITOR_PERFORMANCE:-}" = "true" ]; then
        log_info "Performance monitoring enabled"
        
        # Monitor system resources while GUI is running
        {
            while pgrep -f "enterprise_search_gui.py" >/dev/null; do
                echo "$(date): $(ps -o pid,pcpu,pmem,cmd -p $(pgrep -f enterprise_search_gui.py) | tail -n +2)" >> "$PROJECT_ROOT/logs/gui_performance.log"
                sleep 30
            done
        } &
        
        local monitor_pid=$!
        
        # Cleanup monitor on exit
        trap "kill $monitor_pid 2>/dev/null || true" EXIT
    fi
}

# Show usage information
show_usage() {
    echo "Enterprise Search GUI Launcher"
    echo "=============================="
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --help              Show this help message"
    echo "  --check-only        Only check prerequisites, don't launch GUI"
    echo "  --monitor           Enable performance monitoring"
    echo "  --debug             Enable debug logging"
    echo "  --theme THEME       Set GUI theme (light, dark, high_contrast)"
    echo ""
    echo "Environment Variables:"
    echo "  MONITOR_PERFORMANCE Set to 'true' to enable performance monitoring"
    echo "  GUI_THEME          Set GUI theme (light, dark, high_contrast)"
    echo ""
    echo "Examples:"
    echo "  $0                  # Launch GUI with default settings"
    echo "  $0 --monitor        # Launch with performance monitoring"
    echo "  $0 --check-only     # Only check prerequisites"
    echo ""
    echo "Files:"
    echo "  Configuration: $PROJECT_ROOT/config/config.ini"
    echo "  Log file: $LOG_FILE"
    echo "  GUI module: $SEARCH_GUI"
}

# Main function
main() {
    # Initialize logging
    init_logging
    
    # Parse command line arguments
    local check_only=false
    local debug_mode=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --help|-h)
                show_usage
                exit 0
                ;;
            --check-only)
                check_only=true
                shift
                ;;
            --monitor)
                export MONITOR_PERFORMANCE="true"
                shift
                ;;
            --debug)
                debug_mode=true
                shift
                ;;
            --theme)
                if [ -n "${2:-}" ]; then
                    export GUI_THEME="$2"
                    shift 2
                else
                    error_exit "Theme option requires a value (light, dark, high_contrast)"
                fi
                ;;
            *)
                error_exit "Unknown option: $1. Use --help for usage information."
                ;;
        esac
    done
    
    # Enable debug logging if requested
    if [ "$debug_mode" = true ]; then
        set -x
        log_info "Debug mode enabled"
    fi
    
    # Run checks
    check_prerequisites
    check_system_requirements
    
    # Exit if only checking
    if [ "$check_only" = true ]; then
        log_success "All checks passed. GUI is ready to launch."
        exit 0
    fi
    
    # Setup performance monitoring
    monitor_performance
    
    # Launch GUI
    launch_gui
}

# Handle signals gracefully
trap 'log_info "Search GUI launcher interrupted"; exit 130' INT TERM

# Run main function
main "$@"