#!/bin/bash

# Professional Linux File Librarian Runner
# Implements enterprise-grade execution with comprehensive monitoring

set -euo pipefail  # Strict error handling

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VENV_PATH="$PROJECT_ROOT/venv"
LOG_DIR="$PROJECT_ROOT/logs"
REPORT_DIR="$PROJECT_ROOT/reports"

# Create necessary directories
mkdir -p "$LOG_DIR" "$REPORT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_DIR/professional_run.log"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_DIR/professional_run.log"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_DIR/professional_run.log"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_DIR/professional_run.log"
}

# System requirements check
check_system_requirements() {
    log_info "Checking system requirements..."
    
    # Check available memory
    local available_memory=$(free -m | awk 'NR==2{printf "%.0f", $7}')
    if [ "$available_memory" -lt 1000 ]; then
        log_warn "Low available memory: ${available_memory}MB. Recommended: 1GB+"
    fi
    
    # Check available disk space
    local available_disk=$(df "$PROJECT_ROOT" | awk 'NR==2{print $4}')
    local available_disk_gb=$((available_disk / 1024 / 1024))
    if [ "$available_disk_gb" -lt 5 ]; then
        log_warn "Low disk space: ${available_disk_gb}GB. Recommended: 5GB+"
    fi
    
    # Check CPU cores
    local cpu_cores=$(nproc)
    log_info "System resources: ${cpu_cores} CPU cores, ${available_memory}MB RAM, ${available_disk_gb}GB disk"
}

# Virtual environment setup
setup_virtual_environment() {
    log_info "Setting up virtual environment..."
    
    if [ ! -d "$VENV_PATH" ]; then
        log_info "Creating virtual environment..."
        python3 -m venv "$VENV_PATH"
    fi
    
    # Activate virtual environment
    source "$VENV_PATH/bin/activate"
    
    # Upgrade pip and install requirements
    pip install --upgrade pip > /dev/null 2>&1
    
    if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
        log_info "Installing Python dependencies..."
        pip install -r "$PROJECT_ROOT/requirements.txt" > /dev/null 2>&1
    fi
    
    log_success "Virtual environment ready"
}

# Configuration validation
validate_configuration() {
    log_info "Validating configuration..."
    
    local config_file="$PROJECT_ROOT/conf/config.ini"
    if [ ! -f "$config_file" ]; then
        log_error "Configuration file not found: $config_file"
        exit 1
    fi
    
    # Check if source paths are configured (look for uncommented source_paths)
    if ! grep -q "^source_paths = " "$config_file"; then
        log_error "Please configure source_paths in $config_file"
        exit 1
    fi
    
    # Check if library root is configured (look for uncommented library_root)
    if ! grep -q "^library_root = " "$config_file"; then
        log_error "Please configure library_root in $config_file"
        exit 1
    fi
    
    # Check if paths contain placeholder values (only uncommented lines)
    if grep -q "^source_paths = /path/to/" "$config_file"; then
        log_error "Please update source_paths with actual paths in $config_file"
        exit 1
    fi
    
    if grep -q "^library_root = /path/to/" "$config_file"; then
        log_error "Please update library_root with actual path in $config_file"
        exit 1
    fi
    
    log_success "Configuration validated"
}

# Performance monitoring
start_performance_monitoring() {
    log_info "Starting performance monitoring..."
    
    # Create performance monitoring script
    cat > "$LOG_DIR/monitor_performance.py" << 'EOF'
#!/usr/bin/env python3
import psutil
import time
import json
import sys

def monitor_performance(duration=3600):  # Monitor for 1 hour by default
    metrics = []
    start_time = time.time()
    
    while time.time() - start_time < duration:
        try:
            metric = {
                'timestamp': time.time(),
                'cpu_percent': psutil.cpu_percent(interval=1),
                'memory_percent': psutil.virtual_memory().percent,
                'memory_available_mb': psutil.virtual_memory().available / (1024 * 1024),
                'disk_io_read_mb': psutil.disk_io_counters().read_bytes / (1024 * 1024),
                'disk_io_write_mb': psutil.disk_io_counters().write_bytes / (1024 * 1024),
                'load_average': psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0
            }
            metrics.append(metric)
            time.sleep(10)  # Sample every 10 seconds
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Monitoring error: {e}", file=sys.stderr)
    
    # Save metrics
    with open(sys.argv[1], 'w') as f:
        json.dump(metrics, f, indent=2)

if __name__ == "__main__":
    monitor_performance()
EOF
    
    # Start monitoring in background
    python3 "$LOG_DIR/monitor_performance.py" "$LOG_DIR/performance_metrics.json" &
    MONITOR_PID=$!
    echo $MONITOR_PID > "$LOG_DIR/monitor.pid"
    
    log_info "Performance monitoring started (PID: $MONITOR_PID)"
}

# Stop performance monitoring
stop_performance_monitoring() {
    if [ -f "$LOG_DIR/monitor.pid" ]; then
        local monitor_pid=$(cat "$LOG_DIR/monitor.pid")
        if kill -0 "$monitor_pid" 2>/dev/null; then
            kill "$monitor_pid"
            log_info "Performance monitoring stopped"
        fi
        rm -f "$LOG_DIR/monitor.pid"
    fi
}

# Generate performance report
generate_performance_report() {
    log_info "Generating performance report..."
    
    # Always generate a basic report even if metrics file doesn't exist
    cat > "$LOG_DIR/generate_report.py" << 'EOF'
#!/usr/bin/env python3
import json
import sys
from pathlib import Path

def generate_report(metrics_file, report_file):
    try:
        if Path(metrics_file).exists():
            with open(metrics_file, 'r') as f:
                metrics = json.load(f)
        else:
            metrics = []
        
        if not metrics:
            # Generate basic report even without metrics
            report = """
PROFESSIONAL PERFORMANCE REPORT
================================

No performance metrics collected.
This may indicate the monitoring process failed to start or collect data.

RECOMMENDATIONS:
- Check system permissions
- Verify Python psutil module is installed
- Review logs for monitoring errors
"""
            with open(report_file, 'w') as f:
                f.write(report)
            print(report)
            return
        
        # Calculate statistics
        cpu_avg = sum(m['cpu_percent'] for m in metrics) / len(metrics)
        cpu_max = max(m['cpu_percent'] for m in metrics)
        
        memory_avg = sum(m['memory_percent'] for m in metrics) / len(metrics)
        memory_max = max(m['memory_percent'] for m in metrics)
        
        load_avg = sum(m['load_average'] for m in metrics) / len(metrics)
        
        # Generate report
        report = f"""
PROFESSIONAL PERFORMANCE REPORT
================================

Monitoring Duration: {len(metrics) * 10 / 60:.1f} minutes
Sample Count: {len(metrics)}

CPU USAGE:
- Average: {cpu_avg:.1f}%
- Peak: {cpu_max:.1f}%

MEMORY USAGE:
- Average: {memory_avg:.1f}%
- Peak: {memory_max:.1f}%

SYSTEM LOAD:
- Average: {load_avg:.2f}

RECOMMENDATIONS:
"""
        
        if cpu_max > 90:
            report += "- High CPU usage detected. Consider reducing concurrent operations.\n"
        
        if memory_max > 85:
            report += "- High memory usage detected. Consider increasing available RAM.\n"
        
        if load_avg > 2.0:
            report += "- High system load detected. Consider optimizing workload distribution.\n"
        
        if cpu_max < 50 and memory_max < 50:
            report += "- System resources are underutilized. Consider increasing parallelism.\n"
        
        with open(report_file, 'w') as f:
            f.write(report)
        
        print(report)
        
    except Exception as e:
        print(f"Report generation error: {e}", file=sys.stderr)

if __name__ == "__main__":
    generate_report(sys.argv[1], sys.argv[2])
EOF
        
    if [ -f "$LOG_DIR/performance_metrics.json" ]; then
        python3 "$LOG_DIR/generate_report.py" "$LOG_DIR/performance_metrics.json" "$REPORT_DIR/performance_report.txt"
    else
        # Generate basic report if metrics file doesn't exist
        echo "No performance metrics file found. Generating basic report..." > "$REPORT_DIR/performance_report.txt"
        log_warn "Performance metrics file not found"
    fi
}

# Cleanup function
cleanup() {
    log_info "Performing cleanup..."
    stop_performance_monitoring
    generate_performance_report
    
    # Clean up temporary files
    find "$PROJECT_ROOT" -name "*.tmp" -delete 2>/dev/null || true
    find "$PROJECT_ROOT" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    
    log_success "Cleanup completed"
}

# Signal handlers
trap cleanup EXIT
trap 'log_error "Script interrupted"; exit 130' INT TERM

# Main execution
main() {
    log_info "Starting Professional Linux File Librarian"
    log_info "=========================================="
    
    # Change to project directory
    cd "$PROJECT_ROOT"
    
    # Clean all log files first
    if [ -f "scripts/cleanup_logs.sh" ]; then
        bash scripts/cleanup_logs.sh
    fi
    
    # System checks
    check_system_requirements
    
    # Setup environment
    setup_virtual_environment
    
    # Validate configuration
    validate_configuration
    
    # Start monitoring
    start_performance_monitoring
    
    # Activate virtual environment
    source "$VENV_PATH/bin/activate"
    
    # Run professional orchestrator
    log_info "Starting professional processing..."
    
    local start_time=$(date +%s)
    
    if python3 -m src.professional_orchestrator; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log_success "Professional processing completed successfully"
        log_info "Total processing time: ${duration} seconds"
        
        # Generate final report
        if [ -f "librarian_professional_report.json" ]; then
            log_info "Professional report generated: librarian_professional_report.json"
        fi
        
        return 0
    else
        local exit_code=$?
        log_error "Professional processing failed with exit code: $exit_code"
        return $exit_code
    fi
}

# Help function
show_help() {
    cat << EOF
Professional Linux File Librarian Runner

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -h, --help      Show this help message
    -v, --verbose   Enable verbose logging
    --dry-run       Validate configuration without processing
    --monitor-only  Only run performance monitoring

EXAMPLES:
    $0                  # Run with default settings
    $0 --verbose        # Run with verbose logging
    $0 --dry-run        # Validate configuration only

REQUIREMENTS:
    - Python 3.8+
    - 1GB+ available RAM
    - 5GB+ available disk space

CONFIGURATION:
    Edit conf/config.ini to configure source paths and library destination.

REPORTS:
    - Processing report: librarian_professional_report.json
    - Performance report: reports/performance_report.txt
    - Logs: logs/professional_run.log

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -v|--verbose)
            set -x  # Enable verbose mode
            shift
            ;;
        --dry-run)
            log_info "Dry run mode - validating configuration only"
            check_system_requirements
            setup_virtual_environment
            validate_configuration
            log_success "Configuration validation completed"
            exit 0
            ;;
        --monitor-only)
            log_info "Monitor-only mode"
            start_performance_monitoring
            log_info "Monitoring started. Press Ctrl+C to stop and generate report."
            sleep infinity
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Run main function
main