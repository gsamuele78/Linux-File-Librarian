# src/librarian.py
import os
import hashlib
import shutil
import sqlite3
import magic
import fitz
import pandas as pd
from pathlib import Path
import warnings
import re

# --- CONFIGURATION ---
SOURCE_PATHS = [
    "/path/to/your/first/library",
    "/path/to/your/second/library"
]
LIBRARY_ROOT = "/path/to/your/new_unified_library"
MIN_PDF_SIZE_BYTES = 10 * 1024
DB_FILE = "library_index.sqlite"
KNOWLEDGE_DB_FILE = "knowledge.sqlite"

# --- CLASSIFIER ENGINE ---
class Classifier:
    def __init__(self, knowledge_db_path):
        self.conn = None
        self.product_cache = {}
        self.path_keywords = {}
        if os.path.exists(knowledge_db_path):
            try:
                self.conn = sqlite3.connect(f"file:{knowledge_db_path}?mode=ro", uri=True)
                self.load_products_to_cache()
                self.load_path_keywords()
            except sqlite3.Error as e:
                print(f"[WARNING] Could not connect to knowledge base at '{knowledge_db_path}'. TTRPG classification will be limited. Error: {e}")
        else:
            print("[WARNING] Knowledge base not found. Run 'build_knowledgebase.py' to enable full TTRPG classification.")

    def load_products_to_cache(self):
        """Loads product codes and titles into memory for fast filename matching."""
        if not self.conn: return
        cursor = self.conn.cursor()
        cursor.execute("SELECT product_code, title, game_system, edition, category FROM products")
        for row in cursor.fetchall():
            code, title, system, edition, category = row
            # Clean keys for better matching
            if code: self.product_cache[re.sub(r'[^a-z0-9]', '', code.lower())] = (system, edition, category)
            if title: self.product_cache[re.sub(r'[^a-z0-9]', '', title.lower())] = (system, edition, category)

    def load_path_keywords(self):
        """Loads all unique game systems and editions as keywords for path matching."""
        if not self.conn: return
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT game_system, edition FROM products WHERE game_system IS NOT NULL")
        self.path_keywords = {}
        for system, edition in cursor.fetchall():
            # Store the canonical name for each keyword
            self.path_keywords[system.lower().replace(" ", "")] = system
            if edition:
                self.path_keywords[edition.lower().replace(" ", "")] = system # An edition implies its system

    def _classify_by_filename(self, filename):
        """(Priority 1) Tries to classify based on product codes or titles in the filename."""
        clean_filename = re.sub(r'[^a-z0-9]', '', filename.lower())
        
        # Check for exact title matches (longer titles first to avoid partial matches)
        for title_key in sorted(self.product_cache.keys(), key=len, reverse=True):
            if title_key in clean_filename:
                return self.product_cache[title_key]
        return None

    def _classify_by_path(self, full_path):
        """(Priority 2) Tries to classify based on keywords found in the file's parent path."""
        if not self.path_keywords: return None
        
        path_parts = Path(full_path).parts
        # Iterate from the immediate parent backwards
        for part in reversed(path_parts[:-1]):
            clean_part = part.lower().replace(" ", "").replace("_", "").replace("-", "")
            if clean_part in self.path_keywords:
                # Found a keyword. Return its canonical system name.
                game_system = self.path_keywords[clean_part]
                return game_system, "From Folder", "Heuristic" # Edition/Category are unknown
        return None

    def _classify_by_mimetype(self, mime_type):
        """(Priority 3) Classifies based on generic file type."""
        major_type = mime_type.split('/')[0]
        if major_type == 'video': return ('Media', 'Video', None)
        if major_type == 'audio': return ('Media', 'Audio', None)
        if major_type == 'image': return ('Media', 'Images', None)
        if 'zip' in mime_type or 'rar' in mime_type or '7z' in mime_type: return ('Archives', None, None)
        if 'pdf' in mime_type: return ('Documents', 'PDF', None)
        if 'msword' in mime_type or 'officedocument' in mime_type: return ('Documents', 'Office', None)
        if major_type == 'text': return ('Documents', 'Text', None)
        if 'application' in mime_type: return ('Software & Data', None, None)
        return None

    def classify(self, filename, full_path, mime_type):
        """Runs the full classification hierarchy."""
        # Priority 1: TTRPG Filename Match
        result = self._classify_by_filename(filename)
        if result:
            return result

        # Priority 2: TTRPG Parent Path Match
        result = self._classify_by_path(full_path)
        if result:
            return result

        # Priority 3: Generic Content-Type Match
        result = self._classify_by_mimetype(mime_type)
        if result:
            return result
            
        # Priority 4: Miscellaneous (Fallback)
        return ('Miscellaneous', None, None)

    def close(self):
        if self.conn:
            self.conn.close()


# --- HELPER FUNCTIONS (get_file_hash, get_pdf_details are unchanged) ---
def get_file_hash(file_path):
    sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()
    except IOError: return None

def get_pdf_details(file_path):
    is_valid, has_text = False, False
    warnings.filterwarnings("ignore", category=UserWarning) 
    try:
        with fitz.open(file_path) as doc:
            if doc.page_count > 0:
                is_valid = True
                for page in doc:
                    if page.get_text():
                        has_text = True
                        break
    except Exception as e:
        print(f"  [Warning] Could not analyze PDF: {os.path.basename(file_path)}. Marking as invalid.", file=sys.stderr)
    return is_valid, has_text

# --- MAIN SCRIPT LOGIC ---
def build_library():
    print("--- Starting Library Build Process ---")
    os.makedirs(LIBRARY_ROOT, exist_ok=True)
    
    # Initialize the new intelligent classifier
    classifier = Classifier(KNOWLEDGE_DB_FILE)

    print("Step 1: Scanning files and gathering metadata...")
    all_files_data = []
    for source_path in SOURCE_PATHS:
        if not os.path.isdir(source_path): continue
        for root, _, files in os.walk(source_path):
            for filename in files:
                try:
                    file_path = Path(root) / filename
                    file_size = file_path.stat().st_size
                    all_files_data.append({'path': str(file_path), 'name': filename, 'size': file_size})
                except FileNotFoundError:
                    print(f"  [Warning] Broken symlink skipped: {filename}", file=sys.stderr)
                    continue

    if not all_files_data:
        print("No files found. Exiting.")
        return
    df = pd.DataFrame(all_files_data)
    print(f"Found {len(df)} total files.")

    print("Step 2: Analyzing and Classifying files (this may take a while)...")
    analysis_results = []
    for index, row in df.iterrows():
        path = row['path']
        try:
            mime_type = magic.from_file(path, mime=True)
        except magic.MagicException:
            mime_type = 'unknown/unknown'
            
        file_hash = get_file_hash(path)
        is_pdf_valid, has_ocr = (False, False)
        if 'pdf' in mime_type: is_pdf_valid, has_ocr = get_pdf_details(path)
        
        # ** NEW: Use the powerful classifier **
        game_system, edition, category = classifier.classify(row['name'], path, mime_type)

        analysis_results.append({
            'hash': file_hash, 'mime_type': mime_type, 'is_pdf_valid': is_pdf_valid, 'has_ocr': has_ocr,
            'game_system': game_system, 'edition': edition, 'category': category
        })
        if (index + 1) % 200 == 0: print(f"  ...processed {index + 1}/{len(df)} files")

    df = pd.concat([df, pd.DataFrame(analysis_results)], axis=1).dropna(subset=['hash'])

    print("Step 3: Deduplicating and selecting the best files...")
    df['quality_score'] = 0
    df.loc[df['has_ocr'], 'quality_score'] += 4
    df.loc[df['is_pdf_valid'], 'quality_score'] += 2
    df['path_len'] = df['path'].str.len()
    df = df.sort_values(by=['quality_score', 'size', 'path_len'], ascending=[False, False, True])
    unique_content_files = df.drop_duplicates(subset='hash', keep='first').copy()
    print(f"  - After content deduplication: {len(unique_content_files)} unique files remain.")
    unique_content_files['name_occurrence'] = unique_content_files.groupby('name').cumcount()

    print("Step 4: Copying selected files into new structure and building index...")
    db_path = os.path.join(LIBRARY_ROOT, DB_FILE)
    if os.path.exists(db_path): os.remove(db_path) 
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE files (
            id INTEGER PRIMARY KEY, filename TEXT NOT NULL, path TEXT NOT NULL, 
            type TEXT, size INTEGER, game_system TEXT, edition TEXT
        )
    ''')
    
    copied_files = 0
    for _, row in unique_content_files.iterrows():
        original_path = Path(row['path'])
        new_filename = original_path.name
        if row['name_occurrence'] > 0:
            new_filename = f"{original_path.stem}_{row['name_occurrence']}{original_path.suffix}"

        # Create categorized destination path dynamically from classification results
        dest_subdir = Path(row['game_system'] or 'Miscellaneous')
        if row['edition'] and pd.notna(row['edition']): dest_subdir = dest_subdir / row['edition']
        if row['category'] and pd.notna(row['category']): dest_subdir = dest_subdir / row['category']
        
        destination_path = Path(LIBRARY_ROOT) / dest_subdir / new_filename
        os.makedirs(destination_path.parent, exist_ok=True)
        
        try:
            shutil.copy2(original_path, destination_path)
            cursor.execute(
                "INSERT INTO files (filename, path, type, size, game_system, edition) VALUES (?, ?, ?, ?, ?, ?)",
                (new_filename, str(destination_path), row['mime_type'], row['size'], row['game_system'], row['edition'])
            )
            copied_files += 1
        except Exception as e:
            print(f"  [Error] Could not copy {original_path}. Reason: {e}", file=sys.stderr)
            
    conn.commit()
    conn.close()
    classifier.close()

    print("\n--- Library Build Complete! ---")
    print(f"Total files selected and copied: {copied_files}")
    print(f"New library location: {LIBRARY_ROOT}")

if __name__ == '__main__':
    if "/path/to/" in LIBRARY_ROOT or "/path/to/" in SOURCE_PATHS[0]:
        print("ERROR: Please configure the SOURCE_PATHS and LIBRARY_ROOT variables in 'src/librarian.py' before running.")
    else:
        build_library()
