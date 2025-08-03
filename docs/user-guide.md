# Linux File Librarian - User Guide

## Quick Start

Linux File Librarian is a powerful file organization system that automatically categorizes, deduplicates, and enriches your files with metadata. This guide will help you get started quickly.

### Installation

1. **Clone and Install**
   ```bash
   git clone https://github.com/gsamuele78/Linux-File-Librarian.git
   cd Linux-File-Librarian
   bash scripts/install.sh
   ```

2. **Enhanced Features (Optional)**
   ```bash
   bash scripts/install_enhanced.sh
   ```

### Basic Configuration

**IMPORTANT**: You must configure the system before first use.

1. **Edit Configuration File**
   ```bash
   nano conf/config.ini
   ```

2. **Set Your Paths**
   ```ini
   [Paths]
   # Your source folders (comma-separated)
   source_paths = /home/user/Downloads,/home/user/Documents
   
   # Where to create your organized library
   library_root = /home/user/OrganizedLibrary
   ```

3. **Save and Exit** (Ctrl+X, then Y, then Enter)

### First Run

1. **Build Knowledge Base (Recommended)**
   ```bash
   ./scripts/build_knowledgebase.sh
   ```
   This downloads TTRPG product information for better classification.

2. **Process Your Files**
   ```bash
   ./scripts/run_professional.sh
   ```
   This scans your source folders and organizes files into your library.

3. **Browse Your Library**
   ```bash
   ./scripts/run_enterprise_search.sh
   ```
   This opens a search interface to explore your organized files.

## Understanding the System

### What It Does

The system performs these steps automatically:

1. **Scans** your source directories for files
2. **Classifies** files by type and content
3. **Removes duplicates** intelligently
4. **Enriches** files with internet metadata (if enabled)
5. **Organizes** files into a clean directory structure
6. **Creates** NFO files with metadata (like MediaElch)

### File Organization

Your organized library will look like this:
```
OrganizedLibrary/
├── TTRPG/
│   ├── Pathfinder/
│   │   ├── Adventure Paths/
│   │   ├── Player Companions/
│   │   └── Core Rulebooks/
│   ├── D&D/
│   │   ├── 5th Edition/
│   │   └── Adventures/
│   └── Other Systems/
├── Media/
│   ├── Videos/
│   ├── Audio/
│   └── Images/
├── Documents/
│   ├── PDFs/
│   ├── Academic Papers/
│   └── Books/
└── Other/
    └── Uncategorized/
```

## Configuration Options

### Basic Settings

```ini
[Paths]
# Source directories to scan (comma-separated)
source_paths = /path/to/source1,/path/to/source2

# Destination for organized library
library_root = /path/to/organized/library

[Processing]
# Number of parallel workers (adjust based on your CPU)
max_workers = 4

# Enable internet metadata enrichment
enable_internet_enrichment = false

# Download artwork/fanart
download_artwork = false
```

### Advanced Settings

```ini
[Repair]
# Automatically repair corrupted files
enable_repair = true

# Confidence threshold for repairs (0.0-1.0)
repair_threshold = 0.6

[Deduplication]
# Enable advanced duplicate detection
enable_enhanced_dedup = true

# Detection methods (exact,content,metadata)
dedup_strategies = exact,content,metadata

# Minimum confidence for duplicates (0.0-1.0)
dedup_threshold = 0.7
```

### Internet Enrichment

To enable internet metadata enrichment:

1. **Free Providers** (no API keys needed)
   ```ini
   [InternetProviders]
   enable_crossref = true          # Academic papers
   enable_arxiv = true             # Scientific preprints
   enable_google_books = true      # Book metadata
   enable_wikipedia = true         # General knowledge
   enable_internet_archive = true  # Historical documents
   ```

2. **Premium Providers** (require free API keys)
   ```ini
   [APIKeys]
   # Get free key at: https://www.themoviedb.org/settings/api
   tmdb_api_key = your_tmdb_key_here
   
   # Get free key at: https://fanart.tv/get-an-api-key/
   fanart_api_key = your_fanart_key_here
   ```

## Using the System

### Regular Processing

Run this command whenever you want to organize new files:
```bash
./scripts/run_professional.sh
```

The system will:
- Only process new or changed files
- Show progress and statistics
- Create detailed logs
- Generate a processing report

### Searching Your Library

Launch the search interface:
```bash
./scripts/run_enterprise_search.sh
```

Features:
- **Text search**: Find files by name, content, or metadata
- **Category filtering**: Browse by file type or game system
- **Advanced filters**: Filter by date, size, or quality
- **Preview**: View file details and metadata
- **Export**: Export search results

### Updating Knowledge Base

Update TTRPG product database:
```bash
./scripts/build_knowledgebase.sh
```

Run this periodically to get new product information.

### Log Management

Clean old log files:
```bash
./scripts/cleanup_logs.sh
```

Logs are automatically cleaned at startup, but you can run this manually.

## File Types Supported

### Documents
- **PDFs**: Metadata extraction, ISBN detection, academic paper classification
- **Office Documents**: Word, Excel, PowerPoint files
- **Text Files**: Plain text, markdown, code files
- **E-books**: EPUB, MOBI formats

### Media Files
- **Videos**: MP4, AVI, MKV, MOV with metadata extraction
- **Audio**: MP3, FLAC, WAV with ID3 tag support
- **Images**: JPEG, PNG, GIF with EXIF data extraction

### Gaming Content
- **TTRPG PDFs**: Automatic classification by game system
- **Gaming Images**: Character sheets, maps, artwork
- **Gaming Documents**: Rules, adventures, supplements

## Understanding Results

### Processing Report

After processing, you'll see a report like:
```
=== PROCESSING COMPLETE ===
Files Processed: 1,247
Files Copied: 892
Duplicates Removed: 355
Files Repaired: 23
Internet Enriched: 156
Processing Time: 12m 34s
```

### File Organization

Each organized file gets:
- **Clean filename**: Standardized naming
- **Proper location**: Categorized directory
- **NFO file**: Metadata in XML format (like MediaElch)
- **Artwork**: Downloaded posters/fanart (if enabled)

### Quality Indicators

The system prefers:
- **Higher resolution** videos and images
- **Better organized** files (proper names, metadata)
- **Complete files** over partial downloads
- **Original sources** over copies

## Troubleshooting

### Common Issues

#### "No files found to process"
- Check that source paths exist and contain files
- Verify paths in `conf/config.ini` are correct
- Ensure you have read permissions

#### "Permission denied" errors
- Check file and directory permissions
- Ensure destination directory is writable
- Run with appropriate user permissions

#### Processing is slow
- Reduce `max_workers` if system is overloaded
- Disable internet enrichment for faster processing
- Skip large video files by adjusting `max_file_size`

#### Internet enrichment not working
- Check internet connection
- Verify API keys are correct
- Check provider status in logs

### Log Files

Check logs for detailed information:
```bash
# View recent logs
tail -f logs/librarian.log

# Search for errors
grep ERROR logs/librarian.log

# View processing statistics
grep "Processing complete" logs/librarian.log
```

### Configuration Validation

Test your configuration:
```bash
# Check configuration syntax
python3 -c "from src.config_loader import ConfigLoader; print('Config OK')"

# Verify paths exist
ls -la /your/source/path
ls -la /your/library/path
```

## Best Practices

### File Organization
- **Use descriptive source folders**: Organize your source files logically
- **Regular processing**: Run the system regularly to keep library updated
- **Backup important files**: Always backup before first run

### Performance
- **Adjust worker count**: Match `max_workers` to your CPU cores
- **Monitor disk space**: Ensure adequate space for organized library
- **Network considerations**: Internet enrichment requires stable connection

### Maintenance
- **Update knowledge base**: Run monthly to get new TTRPG products
- **Clean logs**: Logs are cleaned automatically but monitor disk usage
- **Review duplicates**: Check duplicate detection results periodically

## Advanced Usage

### Custom Categories

You can influence categorization by:
- **Folder names**: Files in "Pathfinder" folders are classified as Pathfinder
- **File names**: Descriptive names improve classification
- **Metadata**: Files with existing metadata are better classified

### Batch Processing

For large collections:
1. **Start small**: Test with a subset of files first
2. **Monitor resources**: Watch CPU and memory usage
3. **Process in chunks**: Break large collections into smaller batches

### Integration

The system works well with:
- **Plex Media Server**: Organized media files work great with Plex
- **Calibre**: PDF books integrate well with Calibre library
- **File managers**: Browse organized files with any file manager

## Getting Help

### Documentation
- **Developer Guide**: `docs/developer-guide.md` for technical details
- **Feature Documentation**: `docs/` folder for specific features
- **Configuration Examples**: `conf/config.ini.example`

### Support Channels
1. **GitHub Issues**: Report bugs and request features
2. **Documentation**: Check all documentation first
3. **Logs**: Include relevant log excerpts when asking for help

### Contributing
- **Bug reports**: Include configuration and log files
- **Feature requests**: Describe use case and expected behavior
- **Code contributions**: Follow developer guide for setup

## Frequently Asked Questions

### Q: Will this delete my original files?
**A**: No, the system only copies files. Your originals are never touched.

### Q: How does duplicate detection work?
**A**: The system uses multiple strategies: exact file matching, content analysis, and metadata comparison to find duplicates intelligently.

### Q: Can I run this on a schedule?
**A**: Yes, you can set up a cron job to run `./scripts/run_professional.sh` automatically.

### Q: What if I don't like the organization?
**A**: You can modify the classification rules or simply delete the organized library and start over with different settings.

### Q: Does this work with network drives?
**A**: Yes, but performance may be slower. Ensure stable network connection and appropriate permissions.

### Q: How much disk space do I need?
**A**: The organized library will be roughly the same size as your source files (minus duplicates). Plan for 10-20% extra for metadata and artwork.

## Quick Reference

### Essential Commands
```bash
# First-time setup
bash scripts/install.sh
cp conf/config.ini.example conf/config.ini
nano conf/config.ini

# Regular usage
./scripts/build_knowledgebase.sh    # Update TTRPG database
./scripts/run_professional.sh       # Process files
./scripts/run_enterprise_search.sh  # Search library

# Maintenance
./scripts/cleanup_logs.sh           # Clean old logs
```

### Important Files
- `conf/config.ini` - Main configuration
- `logs/librarian.log` - Processing logs
- `knowledge.sqlite` - TTRPG database
- `metadata_cache/` - Internet metadata cache

### Key Directories
- `src/` - Program source code
- `scripts/` - Executable scripts
- `conf/` - Configuration files
- `logs/` - Log files
- `docs/` - Documentation