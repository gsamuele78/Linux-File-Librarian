#!/bin/bash
set -euo pipefail

# Enterprise System Manager Script
# Provides system management, monitoring, and maintenance capabilities

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PYTHON_EXE="$PROJECT_ROOT/venv/bin/python"
SYSTEM_MANAGER="$PROJECT_ROOT/src/enterprise_system_manager.py"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if system is properly installed
check_installation() {
    if [ ! -f "$PYTHON_EXE" ]; then
        log_error "Python virtual environment not found. Please run enterprise installation first."
        log_info "Run: $PROJECT_ROOT/scripts/enterprise_install.sh"
        exit 1
    fi
    
    if [ ! -f "$SYSTEM_MANAGER" ]; then
        log_error "System manager module not found: $SYSTEM_MANAGER"
        exit 1
    fi
}

# Show system status
show_status() {
    log_info "Getting system status..."
    
    if "$PYTHON_EXE" "$SYSTEM_MANAGER" --project-root "$PROJECT_ROOT" --status; then
        log_success "System status retrieved successfully"
    else
        log_error "Failed to get system status"
        exit 1
    fi
}

# Run system maintenance
run_maintenance() {
    local maintenance_type="${1:-full}"
    
    log_info "Running $maintenance_type maintenance..."
    
    if "$PYTHON_EXE" "$SYSTEM_MANAGER" --project-root "$PROJECT_ROOT" --maintenance "$maintenance_type"; then
        log_success "Maintenance completed successfully"
    else
        log_error "Maintenance failed"
        exit 1
    fi
}

# Start monitoring mode
start_monitoring() {
    log_info "Starting system monitoring (Press Ctrl+C to stop)..."
    
    # Check if already running
    if pgrep -f "enterprise_system_manager.py.*--monitor" >/dev/null; then
        log_warning "System monitoring is already running"
        log_info "To stop existing monitoring: pkill -f 'enterprise_system_manager.py.*--monitor'"
        exit 1
    fi
    
    "$PYTHON_EXE" "$SYSTEM_MANAGER" --project-root "$PROJECT_ROOT" --monitor
}

# Stop monitoring
stop_monitoring() {
    log_info "Stopping system monitoring..."
    
    if pgrep -f "enterprise_system_manager.py.*--monitor" >/dev/null; then
        pkill -f "enterprise_system_manager.py.*--monitor"
        log_success "System monitoring stopped"
    else
        log_warning "System monitoring is not running"
    fi
}

# Show system health summary
show_health() {
    log_info "System Health Summary"
    echo "===================="
    
    # Get system metrics using Python
    "$PYTHON_EXE" -c "
import psutil
import os
import time

# CPU
cpu_percent = psutil.cpu_percent(interval=1)
load_avg = os.getloadavg()

# Memory
memory = psutil.virtual_memory()

# Disk
disk = psutil.disk_usage('/')

# Uptime
uptime = time.time() - psutil.boot_time()

print(f'CPU Usage: {cpu_percent:.1f}%')
print(f'Load Average: {load_avg[0]:.2f}, {load_avg[1]:.2f}, {load_avg[2]:.2f}')
print(f'Memory Usage: {memory.percent:.1f}% ({memory.used // (1024**3):.1f}GB / {memory.total // (1024**3):.1f}GB)')
print(f'Disk Usage: {(disk.used / disk.total * 100):.1f}% ({disk.used // (1024**3):.1f}GB / {disk.total // (1024**3):.1f}GB)')
print(f'System Uptime: {uptime / 3600:.1f} hours')
print(f'Process Count: {len(psutil.pids())}')
"
    
    echo ""
    log_info "For detailed status, run: $0 status"
}

# Show help
show_help() {
    echo "Linux File Librarian - Enterprise System Manager"
    echo "================================================"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  status              Show detailed system status"
    echo "  health              Show system health summary"
    echo "  monitor             Start continuous monitoring (interactive)"
    echo "  stop-monitor        Stop monitoring daemon"
    echo "  maintenance [TYPE]  Run system maintenance"
    echo "                      Types: full, logs, temp, database (default: full)"
    echo "  help                Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 status                    # Show system status"
    echo "  $0 health                    # Show health summary"
    echo "  $0 monitor                   # Start monitoring"
    echo "  $0 maintenance               # Run full maintenance"
    echo "  $0 maintenance logs          # Clean up logs only"
    echo ""
    echo "System Files:"
    echo "  Project Root: $PROJECT_ROOT"
    echo "  Python: $PYTHON_EXE"
    echo "  System Manager: $SYSTEM_MANAGER"
}

# Main function
main() {
    # Check installation first
    check_installation
    
    # Parse command
    case "${1:-help}" in
        status)
            show_status
            ;;
        health)
            show_health
            ;;
        monitor)
            start_monitoring
            ;;
        stop-monitor)
            stop_monitoring
            ;;
        maintenance)
            run_maintenance "${2:-full}"
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "Unknown command: ${1:-}"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

# Handle signals for graceful shutdown
trap 'log_info "Interrupted by user"; exit 130' INT TERM

# Run main function
main "$@"