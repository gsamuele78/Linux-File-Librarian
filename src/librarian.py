import os
import hashlib
import shutil
import sqlite3
import magic
import fitz  # PyMuPDF
import pandas as pd
from pathlib import Path

# --- CONFIGURATION ---
# 1. Add the full paths to your two source libraries
SOURCE_PATHS = [
    "/path/to/your/first/library",
    "/path/to/your/second/library"
]
# 2. Set the full path for your new, merged library
LIBRARY_ROOT = "/path/to/your/new_unified_library"
# 3. Set the minimum acceptable size for a PDF in bytes (e.g., 10 KB)
MIN_PDF_SIZE_BYTES = 10 * 1024
# 4. Name of the search index database
DB_FILE = "library_index.sqlite"

# --- HELPER FUNCTIONS ---

def get_file_hash(file_path):
    """Calculates the SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()
    except IOError:
        return None

def get_pdf_details(file_path):
    """Checks if a PDF is valid, not empty, and has text."""
    is_valid = False
    has_text = False
    try:
        with fitz.open(file_path) as doc:
            if doc.page_count > 0:
                is_valid = True
                for page in doc:
                    if page.get_text():
                        has_text = True
                        break # Found text, no need to check other pages
    except Exception:
        # If fitz fails to open, it's considered invalid/corrupt
        pass
    return is_valid, has_text

# --- MAIN SCRIPT LOGIC ---

def build_library():
    """Main function to scan, deduplicate, and build the library."""
    print("Starting library build process...")

    # Create library root if it doesn't exist
    os.makedirs(LIBRARY_ROOT, exist_ok=True)
    
    # --- 1. SCAN FILES AND GATHER METADATA ---
    print("Step 1: Scanning files and gathering metadata...")
    all_files = []
    for source_path in SOURCE_PATHS:
        if not os.path.isdir(source_path):
            print(f"Warning: Source path not found, skipping: {source_path}")
            continue
        for root, _, files in os.walk(source_path):
            for filename in files:
                file_path = Path(root) / filename
                file_size = file_path.stat().st_size
                all_files.append({
                    'path': str(file_path),
                    'name': filename,
                    'size': file_size,
                    'source_root': source_path
                })
    
    if not all_files:
        print("No files found in source directories. Exiting.")
        return

    df = pd.DataFrame(all_files)
    print(f"Found {len(df)} total files.")

    # --- 2. ANALYZE FILES (HASH, TYPE, PDF-SPECIFIC) ---
    print("Step 2: Analyzing files (this may take a while)...")
    
    # Using a standard loop with progress indication
    analysis_results = []
    for index, row in df.iterrows():
        path = row['path']
        mime_type = magic.from_file(path, mime=True)
        file_hash = get_file_hash(path)
        
        is_pdf_valid = False
        has_ocr = False
        if 'pdf' in mime_type:
            is_pdf_valid, has_ocr = get_pdf_details(path)

        analysis_results.append({
            'hash': file_hash,
            'mime_type': mime_type,
            'is_pdf_valid': is_pdf_valid,
            'has_ocr': has_ocr
        })
        if (index + 1) % 100 == 0:
            print(f"  ...analyzed {index + 1}/{len(df)} files")

    df = pd.concat([df, pd.DataFrame(analysis_results)], axis=1)
    df = df.dropna(subset=['hash']) # Remove files that couldn't be read

    # --- 3. DEDUPLICATION AND SELECTION LOGIC ---
    print("Step 3: Deduplicating and selecting the best files...")
    
    # **Rule 1: Same Content (Hash) -> Keep the best single file**
    # Define a "quality" score for each file to select the best one in a group.
    df['quality_score'] = 0
    df.loc[df['has_ocr'], 'quality_score'] += 4 # Big bonus for OCR
    df.loc[df['is_pdf_valid'], 'quality_score'] += 2 # Bonus for valid PDF
    df.loc[df['size'] > MIN_PDF_SIZE_BYTES, 'quality_score'] += 1 # Bonus for meeting min size
    
    # Sort by quality and then by path length (prefer shorter paths as a tie-breaker)
    df['path_len'] = df['path'].str.len()
    df = df.sort_values(by=['quality_score', 'path_len'], ascending=[False, True])
    
    # Keep the first (i.e., the best) file for each unique hash
    unique_content_files = df.drop_duplicates(subset='hash', keep='first')
    print(f"  - After content deduplication: {len(unique_content_files)} unique files remain.")

    # **Rule 2: Same Name but Different Content -> Keep all, but prepare for renaming**
    # We use cumcount() to find duplicates in the 'name' column. The first occurrence is 0, second is 1, etc.
    unique_content_files['name_occurrence'] = unique_content_files.groupby('name').cumcount()
    
    # --- 4. BUILD NEW LIBRARY AND INDEX ---
    print("Step 4: Copying selected files and building search index...")
    
    db_path = os.path.join(LIBRARY_ROOT, DB_FILE)
    if os.path.exists(db_path):
        os.remove(db_path) # Start with a fresh index
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE files (
            id INTEGER PRIMARY KEY,
            filename TEXT NOT NULL,
            path TEXT NOT NULL,
            type TEXT,
            size INTEGER
        )
    ''')
    
    copied_files = 0
    for _, row in unique_content_files.iterrows():
        original_path = Path(row['path'])
        
        # Determine the new filename to avoid collisions
        new_filename = original_path.name
        if row['name_occurrence'] > 0:
            # This file has a name conflict, so we append a suffix
            suffix = f"_{row['name_occurrence']}"
            new_filename = f"{original_path.stem}{suffix}{original_path.suffix}"

        destination_path = Path(LIBRARY_ROOT) / new_filename
        
        try:
            shutil.copy2(original_path, destination_path) # copy2 preserves metadata
            
            # Add file to the search index
            cursor.execute(
                "INSERT INTO files (filename, path, type, size) VALUES (?, ?, ?, ?)",
                (new_filename, str(destination_path), row['mime_type'], row['size'])
            )
            copied_files += 1
        except Exception as e:
            print(f"Error copying {original_path}: {e}")
            
    conn.commit()
    conn.close()

    print("\n--- Library Build Complete! ---")
    print(f"Total files selected and copied: {copied_files}")
    print(f"New library location: {LIBRARY_ROOT}")
    print(f"Search index created at: {db_path}")

if __name__ == '__main__':
    # A simple check to ensure paths are configured
    if "/path/to/" in LIBRARY_ROOT:
        print("ERROR: Please configure the SOURCE_PATHS and LIBRARY_ROOT variables before running.")
    else:
        build_library()
