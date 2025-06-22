# Linux File Librarian

A robust, KISS (Keep It Simple, Stupid) file management system for sysadmins. This tool scans multiple source directories, intelligently deduplicates files based on content and quality, and builds a single, clean library with a fast search index.

## Features

-   **Content-Based Deduplication:** Uses SHA-256 hashing to find and eliminate files with identical content, even if they have different names.
-   **Intelligent PDF Selection:** When duplicates are found, it prioritizes PDFs that are valid, have an OCR text layer, and are not empty.
-   **Safe by Design:** Never touches the original files. It copies the selected "best" files to a new library directory.
-   **Collision Handling:** If two different files have the same name, one is automatically renamed to prevent data loss.
-   **Simple Search GUI:** Includes a lightweight Tkinter-based GUI to search the finalized library by filename.
-   **Minimal & Open Source:** Built with Python and standard open-source libraries, with no complex servers or databases to maintain.

## Installation (Debian 12 / Ubuntu)

1.  **Clone the Repository:**
    ```bash
    git clone <your-github-repo-url>
    cd linux-file-librarian
    ```

2.  **Run the Installer:**
    This script will install system dependencies, create a Python virtual environment, and install the required packages.
    ```bash
    bash scripts/install.sh
    ```
    The script will ask for your password for `sudo` to install packages like `python3-tk`.

## Configuration

Before running the tools, you **must** configure your library paths.

1.  **Edit the Librarian Script:**
    Open `src/librarian.py` and set your source and destination paths.

    ```python
    # src/librarian.py

    # --- CONFIGURATION ---
    # 1. Add the full paths to your two source libraries
    SOURCE_PATHS = [
        "/path/to/your/first/library",  # <-- CHANGE THIS
        "/path/to/your/second/library" # <-- CHANGE THIS
    ]
    # 2. Set the full path for your new, merged library
    LIBRARY_ROOT = "/path/to/your/new_unified_library" # <-- CHANGE THIS
    ```

2.  **Edit the Search GUI Script:**
    Open `src/search_gui.py` and ensure `LIBRARY_ROOT` matches the one in `librarian.py`.

    ```python
    # src/search_gui.py

    # --- CONFIGURATION ---
    LIBRARY_ROOT = "/path/to/your/new_unified_library" # <-- CHANGE THIS
    ```

## Usage

Use the provided wrapper scripts to run the tools. They automatically handle the virtual environment.

1.  **Build or Update the Library:**
    This process can take a long time depending on the number and size of your files.
    ```bash
    ./scripts/run_librarian.sh
    ```

2.  **Search the Library:**
    Launch the graphical search tool.
    ```bash
    ./scripts/run_search_gui.sh
    ```

## How It Works

1.  **Scan:** The `librarian.py` script walks through all files in your `SOURCE_PATHS`.
2.  **Analyze:** It calculates a SHA-256 hash for every file and checks PDFs for validity and OCR text.
3.  **Deduplicate:** It uses the hashes to identify files with identical content. A quality score is used to decide which version to keep (e.g., an OCR'd PDF is preferred over a non-OCR'd one).
4.  **Copy & Index:** The "best" files are copied to the `LIBRARY_ROOT`. If files with different content have the same name, one is safely renamed. A lightweight `SQLite` database is created to index all files in the new library.
5.  **Search:** The `search_gui.py` application reads this SQLite database to provide fast search results.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
