# Enhanced Media Manager

The Enhanced Media Manager extends the Linux File Librarian with comprehensive media and document processing capabilities, similar to MediaElch but designed for general file organization.

## Overview

The Enhanced Media Manager integrates with popular open-source Python libraries to provide:

- **Comprehensive Metadata Extraction**: Extract rich metadata from PDFs, videos, audio files, images, and documents
- **Intelligent Classification**: Advanced file classification based on content analysis and metadata
- **Document Repair**: Automatically repair corrupted PDFs, videos, and archives using specialized tools
- **Enhanced Deduplication**: Multi-strategy duplicate detection (exact, content, metadata-based)
- **NFO File Generation**: Create MediaElch-style NFO files with extracted metadata
- **Enhanced Directory Organization**: Organize files using metadata-driven directory structures
- **Multi-Format Support**: Handle dozens of file formats with specialized detectors

## Supported File Types

### PDF Documents
- **Libraries**: PyPDF2, pdfplumber
- **Metadata**: Title, author, creator, creation date, page count, ISBN detection
- **Classification**: RPG content, technical documentation, academic papers, business documents
- **Organization**: By author, publisher, or content type

### Video Files
- **Libraries**: ffmpeg/ffprobe (system dependency)
- **Formats**: MP4, AVI, MKV, MOV, WMV, FLV, WebM, M4V
- **Metadata**: Title, duration, resolution, format, creator, genre, year
- **Classification**: Movies, TV shows, educational content, music videos
- **Organization**: By genre, year, and quality tier

### Audio Files
- **Libraries**: mutagen
- **Formats**: MP3, FLAC, WAV, OGG, M4A, AAC, WMA
- **Metadata**: Title, artist, genre, year, duration, format
- **Classification**: Music, podcasts, audiobooks, classical
- **Organization**: By genre and artist

### Image Files
- **Libraries**: Pillow, ExifRead
- **Formats**: JPG, PNG, GIF, BMP, TIFF, WebP, SVG
- **Metadata**: Resolution, format, EXIF data, creation date, artist
- **Classification**: Photos, screenshots, diagrams, icons
- **Organization**: By type and metadata

### Documents
- **Libraries**: python-docx, python-magic
- **Formats**: DOC, DOCX, ODT, RTF, TXT, MD
- **Metadata**: Title, author, creation/modification dates
- **Classification**: Word documents, text files, project documentation, manuals
- **Organization**: By document type and author

## Installation

### Basic Installation
```bash
# Install the base system first
bash scripts/install.sh

# Then install enhanced features
bash scripts/install_enhanced.sh
```

### Manual Installation
```bash
# Install Python dependencies
pip install -r requirements-enhanced.txt

# Install system dependencies (Ubuntu/Debian)
sudo apt install ffmpeg qpdf ghostscript unrar p7zip-full

# For other systems:
# CentOS/RHEL: sudo yum install ffmpeg qpdf ghostscript unrar p7zip
# macOS: brew install ffmpeg qpdf ghostscript unrar p7zip
```

## Configuration

The Enhanced Media Manager uses the same `conf/config.ini` file as the base system. No additional configuration is required - it automatically detects and uses available libraries.

## Directory Structure

The Enhanced Media Manager creates intelligent directory hierarchies based on extracted metadata:

```
Library/
├── Media/
│   ├── Video/
│   │   ├── Movies/
│   │   │   ├── 2023/
│   │   │   │   ├── Action/
│   │   │   │   └── Comedy/
│   │   │   └── 2024/
│   │   ├── TV Shows/
│   │   └── Educational/
│   ├── Audio/
│   │   ├── Music/
│   │   │   ├── Rock/
│   │   │   └── Jazz/
│   │   ├── Podcasts/
│   │   └── Audiobooks/
│   └── Images/
│       ├── Photos/
│       ├── Screenshots/
│       └── Diagrams/
├── Documents/
│   ├── PDF/
│   │   ├── Technical/
│   │   │   └── Programming/
│   │   ├── Academic/
│   │   │   └── Research/
│   │   └── Business/
│   ├── Word Documents/
│   │   ├── Author Name/
│   │   └── Publisher Name/
│   └── Text/
├── Gaming/
│   └── RPG/
│       └── Documents/
└── Technical/
    └── Programming/
        └── Documentation/
```

## NFO Files

The system generates NFO files similar to MediaElch for rich metadata storage:

### Video NFO Example
```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<movie>
    <title>Sample Movie</title>
    <year>2023</year>
    <genre>Action</genre>
    <runtime>120</runtime>
    <fileinfo>
        <streamdetails>
            <video>
                <codec>h264</codec>
                <width>1920</width>
                <height>1080</height>
            </video>
        </streamdetails>
    </fileinfo>
</movie>
```

### Document NFO Example
```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<document>
    <title>Programming Guide</title>
    <author>John Doe</author>
    <publisher>Tech Press</publisher>
    <year>2023</year>
    <pages>250</pages>
    <isbn>978-1234567890</isbn>
</document>
```

## Classification Process

The Enhanced Media Manager uses a multi-stage classification process:

1. **Enhanced Media Detection**: Specialized detectors analyze file content and extract metadata
2. **Legacy TTRPG Classification**: Falls back to existing TTRPG knowledge base for gaming content
3. **Heuristic Classification**: Basic classification based on file type and name patterns
4. **Fallback Classification**: Minimal classification for unrecognized files

## Dependencies Status

Check which dependencies are available:

```bash
# Run the professional script to see dependency status
./scripts/run_professional.sh
```

The system will report:
- Available libraries and their capabilities
- Missing optional dependencies
- Installation commands for missing packages

## Performance Considerations

### Memory Usage
- PDF processing: ~10-50MB per file
- Video analysis: ~5-20MB per file (metadata only)
- Audio processing: ~1-5MB per file
- Image processing: ~5-15MB per file

### Processing Speed
- PDF: ~2-10 files/second (depends on size and complexity)
- Video: ~5-20 files/second (metadata extraction only)
- Audio: ~10-50 files/second
- Images: ~20-100 files/second

### Disk Space
- NFO files: ~1-5KB per file
- Directory indexes: ~1-10KB per directory
- No additional space for metadata extraction

## Troubleshooting

### Common Issues

**ffmpeg not found**
```bash
# Ubuntu/Debian
sudo apt install ffmpeg

# CentOS/RHEL
sudo yum install ffmpeg

# macOS
brew install ffmpeg
```

**PDF processing fails**
```bash
pip install PyPDF2 pdfplumber
```

**Audio metadata missing**
```bash
pip install mutagen
```

**Image EXIF data not extracted**
```bash
pip install Pillow
```

### Debug Mode

Enable debug logging in `conf/config.ini`:
```ini
[General]
debug = true
```

This provides detailed information about:
- Which detector is used for each file
- Metadata extraction results
- Classification decisions
- NFO file creation

## Integration with Existing Tools

The Enhanced Media Manager is designed to work alongside existing media management tools:

- **Kodi**: NFO files are compatible with Kodi's metadata system
- **Plex**: Directory structure works well with Plex organization
- **MediaElch**: NFO format is compatible for video content
- **Calibre**: PDF organization complements e-book management

## API Reference

### EnhancedMediaManager Class

```python
from enhanced_media_manager import EnhancedMediaManager

manager = EnhancedMediaManager()
result = manager.analyze_file(Path("sample.pdf"))

print(f"Category: {result.category}")
print(f"Metadata: {result.metadata}")
print(f"Suggested path: {result.suggested_path}")
```

### Classification Results

```python
@dataclass
class EnhancedClassificationResult:
    category: str              # Top-level category
    subcategory: str          # Second-level category  
    media_type: str           # File type
    confidence: float         # Classification confidence (0.0-1.0)
    metadata: MediaMetadata   # Extracted metadata
    source: str              # Detection source
    suggested_path: List[str] # 3-level directory hierarchy
```

## Future Enhancements

Planned improvements:
- **Machine Learning Classification**: AI-based content analysis
- **Cloud Metadata Sources**: Integration with online databases
- **Custom Detector Plugins**: User-defined classification rules
- **Batch Metadata Editing**: GUI for metadata correction
- **Advanced Search**: Full-text search across NFO files