# Linux File Librarian

A robust, modular file management system for sysadmins, built with a "system engineer" vision. This tool scans source directories, intelligently deduplicates and categorizes every file, and builds a clean, human-browsable library.

### Core Philosophy
- **Configuration over Code:** All user settings are in a single `conf/config.ini` file. No need to edit Python scripts for setup.
- **Non-Destructive:** Your original files are never touched or deleted. The system only copies the selected files to a new location.
- **Intelligent Categorization:** Uses a multi-tiered approach (TTRPG knowledge base, enhanced media detection, folder name analysis, file type) to logically sort every file.
- **Enhanced Media Management:** Similar to MediaElch, extracts metadata from PDFs, videos, audio, images, and documents, creating NFO files for rich organization.
- **Robust & Transparent:** Handles common errors (like corrupted files or broken web links) gracefully and provides clear feedback.
- **Clean Logging:** Automatically cleans old log files at startup to prevent disk space issues and provide fresh logs for each run.

## Installation (Debian 12 / Ubuntu)

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/gsamuele78/Linux-File-Librarian.git
    cd Linux-File-Librarian
    ```

2.  **Run the Basic Installer:**
    This script installs system packages and sets up a self-contained Python virtual environment.
    ```bash
    bash scripts/install.sh
    ```

3.  **(Optional) Install Enhanced Media Manager:**
    For comprehensive media and document processing with metadata extraction:
    ```bash
    bash scripts/install_enhanced.sh
    ```
    
    This adds support for:
    - PDF metadata extraction and ISBN detection
    - Video metadata extraction (requires ffmpeg)
    - Audio metadata extraction with ID3 tags
    - Image EXIF data extraction
    - Document metadata extraction
    - NFO file generation (MediaElch-style)
    - Enhanced directory organization

## Configuration (Required First Step)

**This is the most important step.** Before running the tools, you must edit `conf/config.ini` to tell the system where to find your files.

1.  Open `conf/config.ini` in a text editor.
2.  Set `source_paths` to a comma-separated list of your source folders. If you only have one folder to organize, just list that one folder.
3.  Set `library_root` to the destination for your new organized library.
4.  (Optional) Review the URLs in the `[KnowledgeBaseURLs]` section. You can add new ones (if you create a parser for them) or comment out lines with a `#` to skip scraping that site.

## Usage Workflow

**Note:** All scripts automatically clean old log files at startup to ensure fresh logs and prevent disk space issues.

1.  **(Optional but Recommended) Build the TTRPG Knowledge Base:**
    If you have tabletop RPG files, run this first. It scrapes the web for product data to enable high-accuracy classification. This step can be run periodically to get updates.
    ```bash
    ./scripts/build_knowledgebase.sh
    ```

2.  **Build or Update Your Library:**
    This runs the main categorization, deduplication, and copying process with enterprise-grade performance monitoring. It can be run repeatedly.
    ```bash
    ./scripts/run_professional.sh
    ```

3.  **Search the Library:**
    Launch the enterprise search GUI to browse and filter your new library.
    ```bash
    ./scripts/run_enterprise_search.sh
    ```

## Manual Log Cleanup

If you need to manually clean log files without running the main scripts:
```bash
./scripts/cleanup_logs.sh
```
