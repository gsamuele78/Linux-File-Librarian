# Linux File Librarian - Developer Guide

## Project Overview

Linux File Librarian is a robust, enterprise-grade file management system designed for system administrators and power users. It provides intelligent file categorization, deduplication, metadata enrichment, and library organization with a focus on TTRPG (Tabletop Role-Playing Game) content.

### Core Architecture

The system follows a modular, enterprise-oriented architecture with clear separation of concerns:

```
src/
├── core/                    # Core functionality
│   ├── config_loader.py     # Configuration management
│   ├── database_manager.py  # SQLite database operations
│   └── file_scanner.py      # File discovery and scanning
├── classification/          # Content classification
│   ├── enhanced_classification_service.py
│   ├── enhanced_classification_engine.py
│   └── gaming_providers.py  # TTRPG-specific providers
├── processing/              # File processing pipeline
│   ├── enhanced_media_manager.py
│   ├── enhanced_repair_utils.py
│   ├── enhanced_deduplication.py
│   └── enhanced_copy_utils.py
├── providers/               # Internet metadata providers
│   ├── additional_providers.py
│   └── gaming_providers.py
├── enterprise/              # Enterprise features
│   ├── enterprise_integration.py
│   ├── enterprise_config_manager.py
│   └── professional_orchestrator.py
└── gui/                     # User interfaces
    ├── enterprise_search_gui.py
    └── enterprise_gui_framework.py
```

## Key Features

### 1. Enhanced Media Management
- **MediaElch-inspired architecture**: Comprehensive metadata extraction and NFO file generation
- **Multi-format support**: PDFs, videos, audio, images, documents
- **Intelligent categorization**: TTRPG knowledge base, media detection, folder analysis
- **Cross-platform compatibility**: Handles filename sanitization and path normalization

### 2. Internet Metadata Enrichment
- **Free API providers**: CrossRef, arXiv, Google Books, Wikipedia, Internet Archive
- **Gaming industry integration**: Paizo, Wizards of the Coast, DriveThruRPG, Giochi Uniti, Acheron Games
- **Intelligent caching**: 24-hour metadata cache with automatic expiration
- **Artwork downloading**: Poster, fanart, and banner retrieval

### 3. Enterprise Processing Pipeline
- **Quality metrics**: Comprehensive success/failure tracking
- **Error recovery**: Graceful handling of corrupted files and network issues
- **Professional reporting**: Detailed processing statistics and recommendations
- **Scalable architecture**: Multi-threaded processing with configurable worker pools

### 4. Advanced Deduplication
- **Multi-strategy detection**: Exact, content-based, and metadata-based matching
- **Intelligent file selection**: Prefers higher quality, better organized files
- **Performance optimization**: Efficient hash computation and comparison
- **Configurable thresholds**: Adjustable confidence levels for duplicate detection

## Development Setup

### Prerequisites
```bash
# System packages (Debian/Ubuntu)
sudo apt update
sudo apt install python3 python3-venv python3-pip
sudo apt install sqlite3 libsqlite3-dev
sudo apt install ffmpeg mediainfo exiftool  # For enhanced media processing

# Optional: Document repair tools
sudo apt install qpdf ghostscript poppler-utils
```

### Environment Setup
```bash
# Clone repository
git clone https://github.com/gsamuele78/Linux-File-Librarian.git
cd Linux-File-Librarian

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-enhanced.txt  # For enhanced features
```

### Configuration
```bash
# Copy example configuration
cp conf/config.ini.example conf/config.ini

# Edit configuration
nano conf/config.ini
```

## Core Components

### Configuration Management
The system uses a hierarchical configuration approach:

```python
# config_loader.py
class ConfigLoader:
    def load_config(self, config_path: str) -> Dict:
        """Load configuration with comment filtering"""
        # Supports both [Paths] and [Library] sections
        # Filters out commented lines (starting with #)
        # Handles comma-separated source paths
```

### Classification Engine
The enhanced classification engine provides multi-tiered content analysis:

```python
# enhanced_classification_engine.py
class EnhancedClassificationEngine:
    def classify_and_enrich(self, file_info: Dict) -> Dict:
        """
        1. Basic classification (file type, folder analysis)
        2. TTRPG knowledge base matching
        3. Internet metadata enrichment
        4. Artwork downloading
        """
```

### Processing Pipeline
The enterprise integration layer orchestrates all processing steps:

```python
# enterprise_integration.py
class EnterpriseProcessor:
    def process_files(self, files: List[Path]) -> ProcessingReport:
        """
        Unified processing pipeline:
        1. File discovery and filtering
        2. Repair (if enabled)
        3. Classification and enrichment
        4. Deduplication
        5. Copying with NFO generation
        """
```

## Provider System

### Metadata Providers
All providers implement the `MetadataProvider` interface:

```python
class MetadataProvider(ABC):
    @abstractmethod
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search for content"""
        pass
    
    @abstractmethod
    def get_details(self, provider_id: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get detailed metadata"""
        pass
    
    @abstractmethod
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get artwork URLs"""
        pass
```

### Gaming Providers
Specialized providers for TTRPG content:

- **PaizoProvider**: Pathfinder and Starfinder content
- **WizardsProvider**: D&D content from D&D Beyond
- **DriveThruRPGProvider**: General TTRPG marketplace
- **GiochiUnitiProvider**: Italian RPG publisher
- **AcheronGamesProvider**: Italian gaming content

### Adding New Providers
To add a new metadata provider:

1. Create provider class inheriting from `MetadataProvider`
2. Implement required methods (`search`, `get_details`, `get_artwork`)
3. Add provider to `enhanced_classification_engine.py`
4. Update configuration options in `config.ini.example`

Example:
```python
class NewProvider(MetadataProvider):
    def __init__(self):
        self.base_url = "https://api.example.com"
        self.session = requests.Session()
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        # Implementation
        pass
```

## Database Schema

The system uses SQLite for knowledge base storage:

```sql
-- TTRPG products table
CREATE TABLE ttrpg_products (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    publisher TEXT,
    game_system TEXT,
    product_type TEXT,
    description TEXT,
    image_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- File processing history
CREATE TABLE processing_history (
    id INTEGER PRIMARY KEY,
    file_path TEXT NOT NULL,
    processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT,
    metadata TEXT  -- JSON metadata
);
```

## Testing

### Unit Tests
```bash
# Run unit tests
python -m pytest tests/

# Run with coverage
python -m pytest tests/ --cov=src/
```

### Integration Tests
```bash
# Test full processing pipeline
python -m pytest tests/integration/

# Test specific providers
python -m pytest tests/providers/
```

### Manual Testing
```bash
# Test configuration loading
python -c "from src.config_loader import ConfigLoader; print(ConfigLoader().load_config('conf/config.ini'))"

# Test file scanning
python -c "from src.file_scanner import FileScanner; scanner = FileScanner(); print(list(scanner.scan_directory('/path/to/test')))"
```

## Performance Optimization

### File Processing
- **Parallel processing**: Configurable worker threads
- **Memory management**: Streaming file operations for large files
- **Caching**: Metadata and hash caching to avoid recomputation
- **Early filtering**: Skip large files (>2GB) during discovery

### Database Operations
- **Batch operations**: Bulk inserts and updates
- **Indexing**: Proper indexes on frequently queried columns
- **Connection pooling**: Reuse database connections

### Network Operations
- **Request pooling**: Session reuse for HTTP requests
- **Timeout handling**: Reasonable timeouts for external APIs
- **Rate limiting**: Respect API rate limits

## Security Considerations

### Path Traversal Prevention
```python
def _validate_path(self, path: Path) -> bool:
    """Validate path to prevent directory traversal"""
    try:
        resolved = path.resolve()
        # Ensure path is within allowed directories
        return any(str(resolved).startswith(str(allowed.resolve())) 
                  for allowed in self.allowed_paths)
    except (OSError, ValueError):
        return False
```

### Input Sanitization
- **Log injection prevention**: Sanitize all logged user input
- **Filename sanitization**: Remove dangerous characters from filenames
- **SQL injection prevention**: Use parameterized queries

### API Key Management
- **Environment variables**: Store API keys in environment variables
- **Configuration encryption**: Consider encrypting sensitive configuration
- **Key rotation**: Support for API key rotation

## Troubleshooting

### Common Issues

#### Configuration Problems
```bash
# Check configuration syntax
python -c "from src.config_loader import ConfigLoader; ConfigLoader().load_config('conf/config.ini')"

# Validate paths
python -c "from pathlib import Path; print([p.exists() for p in [Path('/your/source/path')]])"
```

#### Processing Failures
```bash
# Check logs
tail -f logs/librarian.log

# Test individual components
python -c "from src.enhanced_media_manager import EnhancedMediaManager; mgr = EnhancedMediaManager(); print(mgr.detect_media_type('/path/to/file'))"
```

#### Database Issues
```bash
# Check database integrity
sqlite3 knowledge.sqlite "PRAGMA integrity_check;"

# Reset database
rm knowledge.sqlite
python scripts/build_knowledgebase.py
```

### Performance Issues
- **Large file handling**: Files >2GB are automatically skipped
- **Memory usage**: Monitor memory usage during processing
- **Network timeouts**: Increase timeout values for slow connections

## Contributing

### Code Style
- **PEP 8 compliance**: Follow Python style guidelines
- **Type hints**: Use type hints for all public methods
- **Docstrings**: Document all classes and methods
- **Error handling**: Comprehensive exception handling

### Pull Request Process
1. Fork the repository
2. Create feature branch
3. Implement changes with tests
4. Update documentation
5. Submit pull request

### Adding Features
1. **Design document**: Create design document for major features
2. **Interface design**: Define clear interfaces
3. **Testing**: Include comprehensive tests
4. **Documentation**: Update user and developer documentation

## Future Enhancements

### Planned Features
- **Web interface**: Browser-based management interface
- **API server**: REST API for external integrations
- **Plugin system**: Extensible plugin architecture
- **Cloud storage**: Support for cloud storage backends
- **Machine learning**: AI-powered content classification

### Architecture Improvements
- **Microservices**: Split into microservices for scalability
- **Message queues**: Asynchronous processing with message queues
- **Distributed processing**: Support for distributed processing
- **Real-time monitoring**: Advanced monitoring and alerting

### Integration Opportunities
- **Plex integration**: Direct integration with Plex Media Server
- **Jellyfin support**: Support for Jellyfin media server
- **Cloud sync**: Synchronization with cloud storage services
- **Backup integration**: Integration with backup solutions

## License and Support

This project is open source and welcomes contributions. For support:

1. **Documentation**: Check this guide and user documentation
2. **Issues**: Report bugs and feature requests on GitHub
3. **Discussions**: Join community discussions
4. **Professional support**: Contact maintainers for enterprise support

## Appendix

### Useful Commands
```bash
# Full system test
./scripts/run_professional.sh

# Knowledge base rebuild
./scripts/build_knowledgebase.sh

# Search interface
./scripts/run_enterprise_search.sh

# Log cleanup
./scripts/cleanup_logs.sh
```

### Configuration Examples
See `conf/config.ini.example` for comprehensive configuration examples including:
- API key configuration
- Provider settings
- Processing options
- Performance tuning