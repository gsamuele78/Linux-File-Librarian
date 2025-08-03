# Linux File Librarian - User Documentation

## Quick Start Guide

Linux File Librarian is an enterprise-grade file organization system that automatically categorizes, deduplicates, and enriches your files with metadata.

### Installation

1. **Clone Repository**
   ```bash
   git clone https://github.com/gsamuele78/Linux-File-Librarian.git
   cd Linux-File-Librarian
   ```

2. **Basic Installation**
   ```bash
   ./deployment/scripts/install.sh
   ```

3. **Enhanced Features** (Optional)
   ```bash
   ./deployment/scripts/install_enhanced.sh
   ```

### Configuration

Edit `config/config.ini`:
```ini
[Paths]
source_paths = /your/source/folders
library_root = /your/organized/library
```

### Usage

1. **Build Knowledge Base** (Recommended)
   ```bash
   ./deployment/scripts/build_knowledgebase.sh
   ```

2. **Process Files**
   ```bash
   ./deployment/scripts/run_professional.sh
   ```

3. **Search Library**
   ```bash
   ./deployment/scripts/run_enterprise_search.sh
   ```

## Features

- **Intelligent Classification**: TTRPG, media, document categorization
- **Deduplication**: Advanced duplicate detection and removal
- **Metadata Enrichment**: Internet-based metadata enhancement
- **File Repair**: Automatic repair of corrupted files
- **NFO Generation**: MediaElch-style metadata files
- **Enterprise Reporting**: Comprehensive processing reports

## Support

- **User Guide**: `documentation/user/user-guide.md`
- **Troubleshooting**: Check logs in `monitoring/logs/`
- **Configuration**: See `config/config.ini.example`