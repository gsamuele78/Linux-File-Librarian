# System Overview

## Executive Summary

Linux File Librarian is an enterprise-grade file management system that automatically organizes, classifies, and enriches file collections with minimal user intervention. Built with clean architecture principles, it provides scalable, maintainable, and reliable file processing capabilities.

## Core Capabilities

### Intelligent Classification
- **TTRPG Specialization**: Automatic detection and categorization of tabletop RPG content
- **Media Processing**: Video, audio, and image metadata extraction
- **Document Analysis**: PDF, academic paper, and book classification
- **Multi-tier Detection**: Filename, content, and metadata-based classification

### Advanced Processing
- **Smart Deduplication**: Multiple strategies for duplicate detection and resolution
- **File Repair**: Automatic repair of corrupted PDFs, videos, and archives
- **Internet Enrichment**: Metadata enhancement from 12+ online providers
- **NFO Generation**: MediaElch-compatible metadata files

### Enterprise Features
- **Scalable Architecture**: Multi-threaded processing with configurable workers
- **Quality Metrics**: Comprehensive success tracking and reporting
- **Error Recovery**: Graceful handling of failures and network issues
- **Professional Reporting**: Detailed processing statistics and recommendations

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Presentation Layer                       │
├─────────────────────────────────────────────────────────────┤
│  Enterprise GUI  │  CLI Interface  │  REST API (Future)     │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                   Application Layer                         │
├─────────────────────────────────────────────────────────────┤
│  Enterprise      │  Professional   │  Service              │
│  Integration     │  Orchestrator   │  Coordinators         │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    Domain Layer                             │
├─────────────────────────────────────────────────────────────┤
│  Classification  │  Deduplication  │  Repair    │  Copy    │
│  Services        │  Services       │  Services  │  Services │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                 Infrastructure Layer                        │
├─────────────────────────────────────────────────────────────┤
│  Metadata        │  File System    │  Database  │  External │
│  Providers       │  Access         │  Access    │  APIs     │
└─────────────────────────────────────────────────────────────┘
```

## Processing Pipeline

### 1. Discovery Phase
- Scan source directories for files
- Apply filtering rules (size, type, permissions)
- Queue files for processing

### 2. Repair Phase (Optional)
- Detect corrupted files
- Attempt automatic repair using specialized tools
- Log repair results and confidence scores

### 3. Classification Phase
- Analyze file content and metadata
- Apply TTRPG knowledge base matching
- Determine category, subcategory, and metadata

### 4. Enrichment Phase (Optional)
- Query internet providers for additional metadata
- Download artwork and supplementary information
- Cache results for future use

### 5. Deduplication Phase
- Identify duplicate files using multiple strategies
- Select best version based on quality metrics
- Remove or mark duplicates for handling

### 6. Organization Phase
- Create organized directory structure
- Copy files to appropriate locations
- Generate NFO metadata files
- Create processing reports

## Supported Content Types

### Gaming Content
- **Pathfinder**: Core books, adventures, supplements
- **D&D**: All editions, adventures, supplements
- **Italian RPGs**: Giochi Uniti, Acheron Games content
- **General TTRPG**: DriveThruRPG marketplace content

### Media Files
- **Videos**: MP4, AVI, MKV, MOV with metadata extraction
- **Audio**: MP3, FLAC, WAV with ID3 tag support
- **Images**: JPEG, PNG, GIF with EXIF data

### Documents
- **PDFs**: Academic papers, books, manuals
- **Office**: Word, Excel, PowerPoint files
- **E-books**: EPUB, MOBI formats
- **Text**: Plain text, markdown, code files

## Internet Providers

### Free Providers (No API Keys)
- **CrossRef**: Academic papers and journals
- **arXiv**: Scientific preprints
- **Google Books**: Book metadata and covers
- **Wikipedia**: General knowledge articles
- **Internet Archive**: Historical documents

### Premium Providers (Free API Keys)
- **TMDB**: Movie and TV show metadata
- **Fanart.tv**: High-quality artwork

### Gaming Providers
- **Paizo**: Pathfinder and Starfinder content
- **Wizards of the Coast**: D&D content
- **DriveThruRPG**: General TTRPG marketplace
- **Giochi Uniti**: Italian RPG publisher
- **Acheron Games**: Italian gaming content

## Quality Attributes

### Performance
- **Multi-threaded**: Configurable parallel processing
- **Caching**: Metadata and computation caching
- **Streaming**: Memory-efficient large file handling
- **Batch Processing**: Efficient bulk operations

### Reliability
- **Error Handling**: Comprehensive exception management
- **Graceful Degradation**: Continues on partial failures
- **Recovery**: Automatic retry and fallback strategies
- **Data Integrity**: Transactional operations

### Scalability
- **Horizontal**: Multi-worker processing
- **Vertical**: Configurable resource allocation
- **Modular**: Component-based architecture
- **Extensible**: Plugin-based provider system

### Security
- **Input Validation**: All inputs sanitized
- **Path Protection**: Directory traversal prevention
- **API Security**: Secure credential handling
- **Audit Logging**: Security event tracking

## Deployment Options

### Standalone
- Single-machine installation
- Local file processing
- SQLite database
- Direct file system access

### Containerized
- Docker-based deployment
- Isolated environment
- Portable configuration
- Resource management

### Distributed (Future)
- Kubernetes orchestration
- Horizontal scaling
- Load balancing
- High availability

## Monitoring and Observability

### Metrics
- Processing statistics (files, success rates, timing)
- System resources (CPU, memory, disk)
- Provider performance (response times, success rates)
- Error rates and patterns

### Logging
- Structured JSON logging
- Multiple log levels (DEBUG, INFO, WARNING, ERROR)
- Context correlation
- Security audit trail

### Health Checks
- System health monitoring
- Component status checks
- Dependency health verification
- Data integrity validation

## Integration Capabilities

### File System Integration
- Cross-platform path handling
- Permission management
- Symbolic link support
- Network drive compatibility

### External Service Integration
- RESTful API consumption
- Circuit breaker patterns
- Rate limiting compliance
- Fallback strategies

### Media Center Integration
- Plex-compatible organization
- Jellyfin support
- Kodi NFO format
- MediaElch compatibility

## Future Roadmap

### Short-term (6 months)
- Web-based management interface
- REST API for external integrations
- Enhanced gaming provider support
- Performance optimizations

### Medium-term (12 months)
- Machine learning classification
- Cloud storage backends
- Mobile management app
- Advanced analytics

### Long-term (18+ months)
- Microservices architecture
- Distributed processing
- Real-time monitoring
- Enterprise SaaS offering

This system overview provides the foundation for understanding Linux File Librarian's capabilities, architecture, and strategic direction.