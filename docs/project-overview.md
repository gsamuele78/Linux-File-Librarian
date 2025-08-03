# Linux File Librarian - Complete Project Overview

## Project Vision

Linux File Librarian is an enterprise-grade file management system designed with a "system engineer" philosophy. It transforms chaotic file collections into clean, searchable, and intelligently organized libraries with minimal user intervention.

## Core Philosophy

### Configuration Over Code
All user settings are centralized in a single `conf/config.ini` file. Users never need to edit Python scripts or understand the codebase to customize behavior.

### Non-Destructive Operations
Your original files are sacred. The system only copies selected files to new locations, never modifying or deleting source files.

### Intelligent Automation
The system makes smart decisions about file organization, duplicate detection, and metadata enrichment without requiring constant user input.

### Enterprise Reliability
Built for production environments with comprehensive error handling, detailed logging, and graceful degradation when external services are unavailable.

## System Architecture

### Modular Design
```
Linux-File-Librarian/
├── src/                     # Core application code
│   ├── core/               # Fundamental components
│   ├── classification/     # Content analysis and categorization
│   ├── processing/         # File processing pipeline
│   ├── providers/          # Internet metadata providers
│   ├── enterprise/         # Enterprise features and integration
│   └── gui/               # User interfaces
├── scripts/                # Executable scripts
├── conf/                   # Configuration files
├── docs/                   # Documentation
└── logs/                   # Application logs
```

### Processing Pipeline
1. **Discovery**: Scan source directories for files
2. **Filtering**: Skip hidden files, oversized files, and exclusions
3. **Repair**: Attempt to repair corrupted files (optional)
4. **Classification**: Analyze content and determine categories
5. **Enrichment**: Enhance with internet metadata (optional)
6. **Deduplication**: Identify and handle duplicate files
7. **Organization**: Copy files to organized library structure
8. **Metadata**: Generate NFO files and download artwork

## Key Features

### Enhanced Media Management
Inspired by MediaElch, the system provides comprehensive media file handling:

- **Metadata Extraction**: PDFs, videos, audio, images, documents
- **NFO File Generation**: XML metadata files for media center compatibility
- **Artwork Management**: Automatic poster, fanart, and banner downloading
- **Cross-Platform Support**: Handles filename sanitization and path normalization

### TTRPG Specialization
Unique focus on tabletop role-playing game content:

- **Knowledge Base**: Scraped product databases from major publishers
- **Game System Detection**: Automatic classification by RPG system
- **Publisher Integration**: Direct integration with Paizo, Wizards of the Coast, DriveThruRPG
- **International Support**: Italian publishers (Giochi Uniti, Acheron Games)

### Internet Metadata Enrichment
Comprehensive metadata enhancement from multiple sources:

**Free Providers (No API Keys Required)**:
- CrossRef: Academic papers and journals
- arXiv: Scientific preprints and research
- Google Books: Book metadata and covers
- Wikipedia: General knowledge and summaries
- Internet Archive: Historical documents and books

**Premium Providers (Free API Keys)**:
- TMDB: Movie and TV show metadata
- Fanart.tv: High-quality artwork and fanart

**Gaming Industry Providers**:
- Paizo: Pathfinder and Starfinder content
- Wizards of the Coast: D&D content
- DriveThruRPG: General TTRPG marketplace
- Giochi Uniti: Italian RPG publisher
- Acheron Games: Italian gaming content

### Advanced Deduplication
Multi-strategy duplicate detection system:

- **Exact Matching**: File size and hash comparison
- **Content Analysis**: Deep content comparison for similar files
- **Metadata Matching**: Duplicate detection based on metadata
- **Intelligent Selection**: Prefers higher quality, better organized files
- **Configurable Thresholds**: Adjustable confidence levels

### Enterprise Integration
Professional-grade features for production deployment:

- **Quality Metrics**: Comprehensive success/failure tracking
- **Error Recovery**: Graceful handling of network issues and corrupted files
- **Performance Monitoring**: Processing statistics and bottleneck identification
- **Scalable Architecture**: Multi-threaded processing with configurable workers
- **Professional Reporting**: Detailed processing reports with recommendations

## Technical Specifications

### Supported File Types

**Documents**:
- PDFs with metadata extraction and ISBN detection
- Office documents (Word, Excel, PowerPoint)
- Text files (plain text, markdown, code)
- E-books (EPUB, MOBI)

**Media Files**:
- Videos (MP4, AVI, MKV, MOV) with ffmpeg integration
- Audio (MP3, FLAC, WAV) with ID3 tag support
- Images (JPEG, PNG, GIF) with EXIF data extraction

**Gaming Content**:
- TTRPG PDFs with automatic game system classification
- Gaming images (character sheets, maps, artwork)
- Gaming documents (rules, adventures, supplements)

### System Requirements

**Minimum Requirements**:
- Linux (Debian 12/Ubuntu 20.04+)
- Python 3.8+
- 2GB RAM
- 1GB disk space (plus space for organized library)

**Recommended Requirements**:
- Linux (Debian 12/Ubuntu 22.04+)
- Python 3.10+
- 8GB RAM
- SSD storage for optimal performance
- Stable internet connection for metadata enrichment

**Optional Dependencies**:
- ffmpeg: Video metadata extraction and repair
- qpdf/ghostscript: PDF repair capabilities
- mediainfo: Enhanced media file analysis
- exiftool: Advanced image metadata extraction

## Installation Methods

### Basic Installation
```bash
git clone https://github.com/gsamuele78/Linux-File-Librarian.git
cd Linux-File-Librarian
bash scripts/install.sh
```

Installs core functionality with basic file organization and TTRPG classification.

### Enhanced Installation
```bash
bash scripts/install_enhanced.sh
```

Adds comprehensive media processing, document repair, internet enrichment, and enterprise features.

## Configuration System

### Hierarchical Configuration
The system supports flexible configuration through `conf/config.ini`:

```ini
[Paths]
source_paths = /path/to/source1,/path/to/source2
library_root = /path/to/organized/library

[Processing]
max_workers = 4
enable_internet_enrichment = true
download_artwork = true

[APIKeys]
tmdb_api_key = your_key_here
fanart_api_key = your_key_here

[InternetProviders]
enable_crossref = true
enable_arxiv = true
enable_paizo = true
enable_giochi_uniti = true
```

### Backward Compatibility
The system supports both `[Paths]` and `[Library]` configuration sections for compatibility with older configurations.

## Usage Workflows

### Standard Workflow
1. **Initial Setup**: Configure source and destination paths
2. **Knowledge Base**: Build TTRPG product database (optional but recommended)
3. **Processing**: Run main organization process
4. **Search**: Use enterprise search interface to browse results

### Advanced Workflow
1. **Enhanced Setup**: Install enhanced features and configure API keys
2. **Custom Configuration**: Fine-tune processing options and provider settings
3. **Batch Processing**: Process large collections with monitoring
4. **Quality Review**: Review processing reports and adjust settings
5. **Maintenance**: Regular knowledge base updates and log cleanup

## Quality Assurance

### Error Handling
- **Graceful Degradation**: System continues processing when individual files fail
- **Network Resilience**: Handles internet connectivity issues gracefully
- **File System Errors**: Robust handling of permission and disk space issues
- **Corrupted File Recovery**: Attempts to repair corrupted files when possible

### Logging and Monitoring
- **Comprehensive Logging**: Detailed logs for all operations
- **Automatic Log Cleanup**: Prevents disk space issues
- **Processing Statistics**: Detailed metrics for performance monitoring
- **Error Reporting**: Clear error messages with actionable recommendations

### Testing and Validation
- **Configuration Validation**: Automatic validation of configuration files
- **Path Verification**: Checks for valid source and destination paths
- **Permission Testing**: Verifies read/write permissions before processing
- **Dry Run Mode**: Test processing without making changes (planned feature)

## Performance Characteristics

### Scalability
- **Multi-threaded Processing**: Configurable worker threads for parallel processing
- **Memory Efficiency**: Streaming operations for large files
- **Disk I/O Optimization**: Efficient file copying and hash computation
- **Network Optimization**: Connection pooling and request batching

### Benchmarks
Typical performance on modern hardware:
- **File Discovery**: 10,000+ files/minute
- **Classification**: 1,000+ files/minute
- **Deduplication**: 500+ files/minute (with content analysis)
- **Internet Enrichment**: 100+ files/minute (network dependent)

### Resource Usage
- **CPU**: Scales with worker thread count
- **Memory**: ~100MB base + ~10MB per worker thread
- **Disk**: Temporary space for hash computation and metadata cache
- **Network**: Bandwidth dependent on internet enrichment usage

## Security and Privacy

### Security Measures
- **Path Traversal Prevention**: Validates all file paths to prevent directory traversal
- **Input Sanitization**: Sanitizes all user input and file names
- **Log Injection Prevention**: Prevents log injection attacks
- **API Key Protection**: Secure handling of API keys and credentials

### Privacy Considerations
- **Local Processing**: All file analysis happens locally
- **Optional Internet Features**: Internet enrichment is opt-in
- **No Data Collection**: System doesn't collect or transmit user data
- **Metadata Privacy**: Internet queries use generic search terms, not file paths

## Extensibility

### Plugin Architecture
The modular design allows easy extension:

- **New Providers**: Add metadata providers for additional sources
- **Custom Classifiers**: Implement domain-specific classification logic
- **Processing Stages**: Add new processing steps to the pipeline
- **Output Formats**: Support additional metadata formats

### API Integration
Future versions will include:
- **REST API**: Web API for external integrations
- **Webhook Support**: Event notifications for external systems
- **Plugin System**: Formal plugin architecture for extensions

## Comparison with Similar Tools

### vs. MediaElch
- **Broader Scope**: Handles all file types, not just media
- **TTRPG Focus**: Specialized TTRPG content handling
- **Enterprise Features**: Professional-grade processing and reporting
- **Non-Destructive**: Never modifies original files

### vs. Plex/Jellyfin
- **File Organization**: Creates organized file structure
- **Content Agnostic**: Handles documents, games, and media
- **Metadata Enhancement**: Enriches files with comprehensive metadata
- **Standalone Operation**: Doesn't require media server

### vs. Calibre
- **Multi-Format**: Handles more than just books
- **Automated Processing**: Minimal user intervention required
- **Duplicate Handling**: Advanced deduplication capabilities
- **Gaming Content**: Specialized TTRPG handling

## Roadmap and Future Development

### Short-term Goals (Next 6 months)
- **Web Interface**: Browser-based management interface
- **API Server**: REST API for external integrations
- **Enhanced Gaming Support**: More gaming publishers and platforms
- **Performance Improvements**: Optimization for large collections

### Medium-term Goals (6-12 months)
- **Machine Learning**: AI-powered content classification
- **Cloud Storage**: Support for cloud storage backends
- **Mobile App**: Mobile interface for library browsing
- **Advanced Analytics**: Processing analytics and insights

### Long-term Vision (1+ years)
- **Distributed Processing**: Support for distributed processing
- **Microservices Architecture**: Split into microservices for scalability
- **Enterprise SaaS**: Cloud-hosted enterprise solution
- **Community Marketplace**: User-contributed classifiers and providers

## Community and Support

### Open Source Community
- **GitHub Repository**: Active development and issue tracking
- **Documentation**: Comprehensive user and developer documentation
- **Contributing Guidelines**: Clear guidelines for contributions
- **Code of Conduct**: Welcoming and inclusive community

### Support Channels
- **Documentation**: Comprehensive guides and troubleshooting
- **GitHub Issues**: Bug reports and feature requests
- **Community Discussions**: User community and knowledge sharing
- **Professional Support**: Enterprise support options available

### Contributing
The project welcomes contributions in various forms:
- **Code Contributions**: Bug fixes, features, and improvements
- **Documentation**: User guides, tutorials, and examples
- **Testing**: Bug reports and testing on different systems
- **Translations**: Internationalization and localization
- **Provider Development**: New metadata providers and integrations

## Conclusion

Linux File Librarian represents a new approach to file management that combines the power of enterprise-grade processing with the intelligence of modern metadata enrichment. Its focus on TTRPG content, combined with comprehensive media management capabilities, makes it unique in the file organization space.

The system's non-destructive approach, extensive configuration options, and robust error handling make it suitable for both personal use and enterprise deployment. With its modular architecture and active development, Linux File Librarian is positioned to evolve with changing user needs and technological advances.

Whether you're a system administrator managing large file collections, a TTRPG enthusiast organizing gaming content, or a media collector seeking better organization, Linux File Librarian provides the tools and intelligence needed to transform chaos into order.