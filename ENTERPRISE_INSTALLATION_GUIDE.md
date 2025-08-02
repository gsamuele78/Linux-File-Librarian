# Enterprise Installation and System Management Guide

## Overview

The Linux File Librarian now includes enterprise-grade installation and system management capabilities designed for production environments, system administrators, and organizations requiring robust, scalable file management solutions.

## Enterprise Features

### 🏗️ **Enterprise Installation System**
- **Comprehensive System Detection**: Automatically detects OS type, system resources, and compatibility
- **Dependency Management**: Advanced dependency resolution with security vulnerability checking
- **Rollback Capabilities**: Full installation rollback support in case of failures
- **Validation & Verification**: Multi-stage validation ensures installation integrity
- **Backup & Recovery**: Automatic backup of existing installations before upgrades

### 📊 **System Monitoring & Management**
- **Real-time Health Monitoring**: CPU, memory, disk, and process monitoring
- **Predictive Scaling**: Intelligent resource allocation based on workload patterns
- **Alert System**: Configurable alerts for system health issues
- **Process Management**: Enterprise-grade process lifecycle management
- **Performance Optimization**: Advanced optimization techniques including memory mapping and async I/O

### 🔧 **Maintenance & Operations**
- **Automated Maintenance**: Scheduled cleanup, optimization, and health checks
- **Log Management**: Intelligent log rotation and cleanup
- **Database Optimization**: SQLite database vacuum and analysis
- **Temporary File Cleanup**: Comprehensive temporary file management
- **Maintenance Modes**: Soft, hard, and emergency maintenance modes

## Installation Methods

### Method 1: Enterprise Installation (Recommended)

```bash
# Clone the repository
git clone https://github.com/gsamuele78/Linux-File-Librarian.git
cd Linux-File-Librarian

# Run enterprise installation
./scripts/enterprise_install.sh
```

**Enterprise Installation Features:**
- System requirements validation
- Automatic dependency resolution
- Security vulnerability checking
- Installation verification
- Comprehensive logging
- Rollback support

### Method 2: Enterprise Installation with Service Setup

```bash
# Install with systemd service setup
./scripts/enterprise_install.sh --setup-service

# Enable and start the service
sudo systemctl enable linux-file-librarian
sudo systemctl start linux-file-librarian
```

### Method 3: Force Reinstallation

```bash
# Force complete reinstallation
./scripts/enterprise_install.sh --force-reinstall
```

## System Management

### System Status and Health

```bash
# Check system status
./scripts/system_manager.sh status

# Quick health summary
./scripts/system_manager.sh health

# Start interactive monitoring
./scripts/system_manager.sh monitor
```

### Maintenance Operations

```bash
# Run full system maintenance
./scripts/system_manager.sh maintenance

# Specific maintenance tasks
./scripts/system_manager.sh maintenance logs      # Clean up logs
./scripts/system_manager.sh maintenance temp     # Clean temporary files
./scripts/system_manager.sh maintenance database # Optimize databases
```

### Process Management

```bash
# Stop monitoring daemon
./scripts/system_manager.sh stop-monitor

# View help
./scripts/system_manager.sh help
```

## Python API Usage

### Enterprise Installer

```python
from src.enterprise_installer import EnterpriseInstaller
from pathlib import Path

# Initialize installer
installer = EnterpriseInstaller(project_root=Path('/path/to/project'))

# Validate system requirements
valid, issues = installer.validate_system_requirements()
if not valid:
    print("System requirements not met:", issues)

# Perform installation
result = installer.perform_installation()
print(f"Installation {'succeeded' if result.success else 'failed'}")

# Get installation status
status = installer.get_installation_status()
print(status)
```

### System Manager

```python
from src.enterprise_system_manager import EnterpriseSystemManager
from pathlib import Path

# Initialize system manager
manager = EnterpriseSystemManager(project_root=Path('/path/to/project'))

# Initialize and start monitoring
if manager.initialize():
    # Get system status
    status = manager.get_system_status()
    print(f"System health: {status['system_health']['health_score']:.1f}%")
    
    # Perform maintenance
    maintenance_result = manager.perform_maintenance('full')
    print(f"Maintenance: {maintenance_result['status']}")
```

### Dependency Manager

```python
from src.dependency_manager import EnterpriseDependencyManager
from pathlib import Path

# Initialize dependency manager
dep_manager = EnterpriseDependencyManager(project_root=Path('/path/to/project'))

# Check all dependencies
dep_status = dep_manager.check_all_dependencies()
for name, info in dep_status.items():
    print(f"{name}: {info.status.value} (v{info.installed_version})")

# Resolve dependency issues
results = dep_manager.resolve_dependencies(auto_install=True)
print(f"Resolved {len(results['installation_results'])} dependency issues")

# Generate dependency report
report = dep_manager.generate_dependency_report()
print(f"Total dependencies: {report['summary']['total']}")
print(f"Security risks: {report['summary']['security_risks']}")
```

## Configuration

### Enterprise Configuration Structure

```ini
[Paths]
source_paths = /path/to/source1,/path/to/source2
library_root = /path/to/organized/library
temp_dir = /tmp/librarian_temp

[Processing]
max_workers = 8
batch_size = 200
memory_optimization = true
max_memory_mb = 4096

[Logging]
log_level = INFO
log_file = logs/librarian.log
audit_logging = true

[Security]
validate_paths = true
max_file_size_mb = 1000
allowed_extensions = .pdf,.txt,.doc,.docx,.epub,.mobi

[Performance]
performance_monitoring = true
report_interval = 300
enable_caching = true
```

## System Requirements

### Minimum Requirements
- **OS**: Linux (Debian/Ubuntu, RHEL/CentOS, Fedora)
- **Python**: 3.8 or higher
- **Memory**: 2GB RAM
- **Disk**: 5GB free space
- **Network**: Internet connection for initial setup

### Recommended Requirements
- **OS**: Ubuntu 20.04+ or RHEL 8+
- **Python**: 3.9 or higher
- **Memory**: 8GB RAM
- **Disk**: 20GB free space
- **CPU**: 4+ cores

### System Packages
The installer automatically installs required system packages:
- `python3-pip`, `python3-tk`, `python3-venv`, `python3-dev`
- `git`, `libmagic1`, `qpdf`, `ghostscript`, `pdftk`

## Security Features

### Dependency Security
- **Vulnerability Scanning**: Automatic checking for known security vulnerabilities
- **Version Validation**: Ensures only secure versions are installed
- **Security Alerts**: Immediate alerts for security-critical updates

### Path Validation
- **Path Traversal Protection**: Prevents directory traversal attacks
- **Input Sanitization**: All user inputs are sanitized before logging
- **File Size Limits**: Configurable limits prevent resource exhaustion

### Process Security
- **Privilege Separation**: Runs with minimal required privileges
- **Resource Limits**: Configurable memory and CPU limits
- **Secure Logging**: Log injection prevention

## Monitoring and Alerting

### Health Metrics
- **CPU Usage**: Real-time CPU utilization monitoring
- **Memory Usage**: Memory consumption tracking
- **Disk Usage**: Disk space monitoring
- **Load Average**: System load monitoring
- **Process Count**: Active process tracking

### Alert Thresholds
```python
alert_thresholds = {
    'cpu_usage': 85.0,      # CPU usage percentage
    'memory_usage': 90.0,   # Memory usage percentage
    'disk_usage': 95.0,     # Disk usage percentage
    'load_average': 10.0,   # Load average threshold
    'file_descriptors': 80.0 # File descriptor usage percentage
}
```

### Health Scoring
The system calculates a health score (0-100) based on:
- CPU utilization impact
- Memory pressure impact
- Disk space availability
- System load impact
- Resource utilization efficiency

## Maintenance Schedules

### Automated Maintenance
- **Daily**: Log cleanup, temporary file removal
- **Weekly**: Database optimization, cache cleanup
- **Monthly**: Full system health check, dependency updates

### Manual Maintenance
```bash
# Emergency maintenance mode
./scripts/system_manager.sh maintenance emergency

# Scheduled maintenance window
./scripts/system_manager.sh maintenance scheduled
```

## Troubleshooting

### Installation Issues

**Problem**: System requirements validation fails
```bash
# Check specific requirements
python3 src/enterprise_installer.py --validate-only

# View detailed system information
python3 src/enterprise_installer.py --status
```

**Problem**: Dependency installation fails
```bash
# Check dependency status
python3 src/dependency_manager.py --check

# Generate dependency report
python3 src/dependency_manager.py --report
```

### Runtime Issues

**Problem**: High resource usage
```bash
# Check system health
./scripts/system_manager.sh health

# Start monitoring mode
./scripts/system_manager.sh monitor
```

**Problem**: Process failures
```bash
# Check system status
./scripts/system_manager.sh status

# Run maintenance
./scripts/system_manager.sh maintenance
```

### Log Analysis

**Installation Logs**: `logs/enterprise_install.log`
**System Logs**: `logs/system_manager.log`
**Application Logs**: `logs/librarian.log`
**Audit Logs**: `logs/*_audit.log`

## Performance Optimization

### Memory Optimization
- **Memory Mapping**: Large files processed using memory mapping
- **Garbage Collection Tuning**: Optimized GC thresholds
- **Batch Processing**: Intelligent batch size calculation
- **Resource Pooling**: Efficient resource reuse

### I/O Optimization
- **Async Processing**: Asynchronous file operations
- **Concurrent Workers**: Optimal worker thread allocation
- **Cache Optimization**: Intelligent caching strategies
- **Predictive Scaling**: Workload-based resource allocation

### System Optimization
- **CPU Affinity**: Optimal CPU core utilization
- **Process Priorities**: Intelligent process prioritization
- **Resource Limits**: Configurable resource constraints
- **Performance Monitoring**: Real-time performance tracking

## Enterprise Support

### Logging and Auditing
- **Comprehensive Logging**: All operations logged with timestamps
- **Audit Trail**: Security-focused audit logging
- **Log Rotation**: Automatic log file management
- **Structured Logging**: JSON-formatted logs for analysis

### Backup and Recovery
- **Configuration Backup**: Automatic configuration backups
- **Installation Rollback**: Complete installation rollback capability
- **Data Recovery**: Recovery procedures for corrupted data
- **Disaster Recovery**: Enterprise disaster recovery planning

### Integration
- **API Endpoints**: RESTful API for system integration
- **Monitoring Integration**: Prometheus metrics support
- **Alert Integration**: Webhook support for external alerting
- **Configuration Management**: Integration with configuration management tools

## Migration from Basic Installation

### Upgrade Path
```bash
# Backup existing installation
cp -r venv venv_backup
cp -r conf conf_backup

# Run enterprise installation
./scripts/enterprise_install.sh

# Verify upgrade
./scripts/system_manager.sh status
```

### Configuration Migration
The enterprise installer automatically migrates existing configurations while preserving custom settings.

## Support and Documentation

- **Installation Guide**: This document
- **API Documentation**: `docs/api/`
- **Configuration Reference**: `docs/configuration/`
- **Troubleshooting Guide**: `docs/troubleshooting/`
- **Performance Tuning**: `docs/performance/`

For enterprise support and consulting services, please contact the development team.