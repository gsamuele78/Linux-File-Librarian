# Deployment Guide

## Overview

This guide covers all deployment options for Linux File Librarian, from development setup to production enterprise deployment.

## Quick Start Deployment

### Standard Installation
```bash
git clone https://github.com/gsamuele78/Linux-File-Librarian.git
cd Linux-File-Librarian
./deployment/scripts/install.sh
```

### Enhanced Installation
```bash
./deployment/scripts/install_enhanced.sh
```

### Configuration
```bash
cp config/config.ini.example config/config.ini
nano config/config.ini  # Edit paths and settings
```

### First Run
```bash
./deployment/scripts/build_knowledgebase.sh  # Optional but recommended
./deployment/scripts/run_professional.sh     # Process files
```

## Development Environment

### Prerequisites
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3 python3-venv python3-pip git

# Optional: Enhanced features
sudo apt install ffmpeg mediainfo exiftool qpdf ghostscript
```

### Setup
```bash
# Clone and setup
git clone https://github.com/gsamuele78/Linux-File-Librarian.git
cd Linux-File-Librarian

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-enhanced.txt  # For enhanced features
```

### Development Configuration
```ini
[Paths]
source_paths = /home/dev/test-files
library_root = /home/dev/test-library

[Processing]
max_workers = 2
enable_internet_enrichment = false  # Disable for faster testing
download_artwork = false

[General]
debug = true
```

## Production Deployment

### System Requirements

#### Minimum Requirements
- **OS**: Linux (Debian 12/Ubuntu 20.04+)
- **Python**: 3.10+
- **RAM**: 2GB
- **Storage**: 1GB + space for organized library
- **Network**: Internet connection for metadata enrichment

#### Recommended Requirements
- **OS**: Linux (Debian 12/Ubuntu 22.04+)
- **Python**: 3.11+
- **RAM**: 8GB
- **Storage**: SSD storage for optimal performance
- **CPU**: 4+ cores for parallel processing
- **Network**: Stable broadband connection

### Production Installation
```bash
# System dependencies
sudo apt update
sudo apt install python3 python3-venv python3-pip
sudo apt install ffmpeg mediainfo exiftool qpdf ghostscript poppler-utils
sudo apt install unrar p7zip-full sqlite3

# Application setup
git clone https://github.com/gsamuele78/Linux-File-Librarian.git
cd Linux-File-Librarian
./deployment/scripts/install_enhanced.sh

# Configuration
cp config/config.ini.example config/config.ini
# Edit configuration for production paths and settings
```

### Production Configuration
```ini
[Paths]
source_paths = /data/input,/mnt/external/files
library_root = /data/organized-library

[Processing]
max_workers = 8  # Match CPU cores
enable_internet_enrichment = true
download_artwork = true

[Repair]
enable_repair = true
repair_threshold = 0.8

[Deduplication]
enable_enhanced_dedup = true
dedup_strategies = exact,content,metadata

[APIKeys]
tmdb_api_key = your_production_key
fanart_api_key = your_production_key

[General]
debug = false
max_file_size = 5368709120  # 5GB
```

## Container Deployment

### Docker

#### Build Image
```bash
docker build -f deployment/docker/Dockerfile -t linux-file-librarian:enterprise .
```

#### Run Container
```bash
docker run -d \
  --name file-librarian \
  -v /host/input:/app/data/input \
  -v /host/output:/app/data/output \
  -v /host/config:/app/config \
  -v /host/logs:/app/monitoring/logs \
  linux-file-librarian:enterprise
```

#### Docker Compose
```yaml
version: '3.8'
services:
  file-librarian:
    build:
      context: .
      dockerfile: deployment/docker/Dockerfile
    volumes:
      - ./data/input:/app/data/input
      - ./data/output:/app/data/output
      - ./config:/app/config
      - ./logs:/app/monitoring/logs
    environment:
      - PYTHONPATH=/app/src
    restart: unless-stopped
```

### Kubernetes

#### Prerequisites
```bash
kubectl create namespace file-management
```

#### Deploy
```bash
kubectl apply -f deployment/kubernetes/deployment.yaml
```

#### Configuration
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: librarian-config
  namespace: file-management
data:
  config.ini: |
    [Paths]
    source_paths = /app/data/input
    library_root = /app/data/output
    
    [Processing]
    max_workers = 4
    enable_internet_enrichment = true
```

#### Persistent Storage
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: librarian-data-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
```

## Service Management

### Systemd Service
```ini
# /etc/systemd/system/file-librarian.service
[Unit]
Description=Linux File Librarian
After=network.target

[Service]
Type=simple
User=librarian
Group=librarian
WorkingDirectory=/opt/Linux-File-Librarian
Environment=PYTHONPATH=/opt/Linux-File-Librarian/src
ExecStart=/opt/Linux-File-Librarian/venv/bin/python -m src.enterprise.professional_orchestrator
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start service
sudo systemctl enable file-librarian
sudo systemctl start file-librarian
sudo systemctl status file-librarian
```

### Cron Scheduling
```bash
# Edit crontab
crontab -e

# Add scheduled processing (daily at 2 AM)
0 2 * * * /opt/Linux-File-Librarian/deployment/scripts/run_professional.sh

# Weekly knowledge base update (Sunday at 1 AM)
0 1 * * 0 /opt/Linux-File-Librarian/deployment/scripts/build_knowledgebase.sh

# Monthly log cleanup (1st of month at midnight)
0 0 1 * * /opt/Linux-File-Librarian/deployment/scripts/cleanup_logs.sh
```

## Monitoring and Logging

### Log Configuration
```ini
[Logging]
level = INFO
file = /var/log/file-librarian/librarian.log
max_size = 100MB
backup_count = 5
format = %(asctime)s - %(name)s - %(levelname)s - %(message)s
```

### Prometheus Metrics
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'file-librarian'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 30s
```

### Health Checks
```bash
# Manual health check
curl http://localhost:8080/health

# Automated monitoring
*/5 * * * * curl -f http://localhost:8080/health || systemctl restart file-librarian
```

## Security Configuration

### File Permissions
```bash
# Create dedicated user
sudo useradd -r -s /bin/false librarian
sudo mkdir -p /opt/Linux-File-Librarian
sudo chown -R librarian:librarian /opt/Linux-File-Librarian

# Set proper permissions
chmod 750 /opt/Linux-File-Librarian
chmod 640 /opt/Linux-File-Librarian/config/config.ini
```

### Network Security
```bash
# Firewall configuration (if using web interface)
sudo ufw allow 8080/tcp  # Application port
sudo ufw enable
```

### API Key Security
```bash
# Use environment variables for sensitive data
export TMDB_API_KEY="your_key_here"
export FANART_API_KEY="your_key_here"

# Or use a secrets file
echo "tmdb_api_key = your_key" > /etc/file-librarian/secrets.ini
chmod 600 /etc/file-librarian/secrets.ini
```

## Performance Tuning

### System Optimization
```bash
# Increase file descriptor limits
echo "librarian soft nofile 65536" >> /etc/security/limits.conf
echo "librarian hard nofile 65536" >> /etc/security/limits.conf

# Optimize for I/O intensive workloads
echo 'vm.dirty_ratio = 5' >> /etc/sysctl.conf
echo 'vm.dirty_background_ratio = 2' >> /etc/sysctl.conf
```

### Application Tuning
```ini
[Processing]
max_workers = 8  # Match CPU cores
batch_size = 100  # Files per batch
cache_size = 10000  # Metadata cache entries

[Performance]
enable_parallel_io = true
io_buffer_size = 8192
network_timeout = 30
max_retries = 3
```

## Backup and Recovery

### Configuration Backup
```bash
# Backup configuration
tar -czf config-backup-$(date +%Y%m%d).tar.gz config/

# Backup knowledge base
cp knowledge.sqlite knowledge-backup-$(date +%Y%m%d).sqlite
```

### Data Recovery
```bash
# Restore configuration
tar -xzf config-backup-20240101.tar.gz

# Restore knowledge base
cp knowledge-backup-20240101.sqlite knowledge.sqlite
```

### Disaster Recovery
```bash
# Full system backup
rsync -av /opt/Linux-File-Librarian/ /backup/file-librarian/

# Restore from backup
rsync -av /backup/file-librarian/ /opt/Linux-File-Librarian/
```

## Troubleshooting

### Common Issues

#### Permission Errors
```bash
# Fix ownership
sudo chown -R librarian:librarian /opt/Linux-File-Librarian

# Fix permissions
find /opt/Linux-File-Librarian -type f -exec chmod 644 {} \;
find /opt/Linux-File-Librarian -type d -exec chmod 755 {} \;
chmod +x /opt/Linux-File-Librarian/deployment/scripts/*.sh
```

#### Performance Issues
```bash
# Check system resources
htop
iotop
df -h

# Monitor application
tail -f monitoring/logs/librarian.log
```

#### Network Issues
```bash
# Test internet connectivity
curl -I https://api.themoviedb.org/3/
curl -I https://openlibrary.org/

# Check DNS resolution
nslookup api.themoviedb.org
```

### Log Analysis
```bash
# Error analysis
grep ERROR monitoring/logs/librarian.log | tail -20

# Performance analysis
grep "Processing time" monitoring/logs/librarian.log

# Provider analysis
grep "provider" monitoring/logs/librarian.log | grep -v DEBUG
```

## Maintenance

### Regular Maintenance Tasks
```bash
# Weekly tasks
./deployment/scripts/cleanup_logs.sh
./deployment/scripts/build_knowledgebase.sh

# Monthly tasks
# Update system packages
sudo apt update && sudo apt upgrade

# Update Python dependencies
pip install --upgrade -r requirements.txt -r requirements-enhanced.txt

# Vacuum database
sqlite3 knowledge.sqlite "VACUUM;"
```

### Monitoring Checklist
- [ ] Check disk space usage
- [ ] Review error logs
- [ ] Verify backup integrity
- [ ] Test health endpoints
- [ ] Monitor processing performance
- [ ] Update knowledge base
- [ ] Clean old logs

This deployment guide provides comprehensive coverage for all deployment scenarios from development to enterprise production environments.