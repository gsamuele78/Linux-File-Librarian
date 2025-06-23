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
SOURCE_PATHS = ["/path/to/your/first/library"]
LIBRARY_ROOT = "/path/to/your/new_unified_library"
MIN_PDF_SIZE_BYTES = 10 * 1024
DB_FILE = "library_index.sqlite"
KNOWLEDGE_DB_FILE = "knowledge.sqlite"

# --- CLASSIFIER ENGINE ---
class Classifier:
    def __init__(self, knowledge_db_path):
        self.conn = None
        self.product_cache = {}
        if os.path.exists(knowledge_db_path):
            try:
                self.conn = sqlite3.connect(knowledge_db_path)
                self.load_products_to_cache()
            except sqlite3.Error as e:
                print(f"[WARNING] Could not connect to knowledge base at '{knowledge_db_path}'. TTRPG classification will be disabled. Error: {e}")
        else:
            print("[WARNING] Knowledge base not found. Run 'build_knowledgebase.py' to enable TTRPG classification.")

    def load_products_to_cache(self):
        """Loads product codes and titles into memory for faster matching."""
        if not self.conn: return
        cursor = self.conn.cursor()
        cursor.execute("SELECT product_code, title, game_system, edition, category FROM products")
        for row in cursor.fetchall():
            code, title, system, edition, category = row
            if code: self.product_cache[code.lower()] = (system, edition, category)
            if title: self.product_cache[title.lower()] = (system, edition, category)

    def classify_by_filename(self, filename):
        """Classifies a file based on its name using the knowledge base."""
        if not self.conn:
            return "Unknown", "Unknown", "Unknown"

        clean_filename = filename.lower().replace("_", " ").replace("-", " ")
        
        # Priority 1: Match product code
        # Regex to find codes like TSR12345, PZO1110, etc.
        code_match = re.search(r'\b(tsr\s?\d{4,5}|pzo\d{4,})\b', clean_filename)
        if code_match:
            code = code_match.group(0).replace(" ", "")
            if code in self.product_cache:
                system, edition, cat = self.product_cache[code]
                return system, edition, cat

        # Priority 2: Match exact title
        for title, (system, edition, cat) in self.product_cache.items():
            if title in clean_filename:
                return system, edition, cat

        return "Uncategorized", None, None # Default if no match

    def close(self):
        if self.conn:
            self.conn.close()

# --- HELPER FUNCTIONS (get_file_hash, get_pdf_details - same as before) ---
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
        print(f"  [Warning] Could not analyze PDF: {os.path.basename(file_path)}. Reason: {e}. Marking as invalid.")
    return is_valid, has_text
# --- END HELPER FUNCTIONS ---

def build_library():
    print("--- Starting Library Build Process ---")
    os.makedirs(LIBRARY_ROOT, exist_ok=True)
    
    classifier = Classifier(KNOWLEDGE_DB_FILE)

    print("Step 1: Scanning files and gathering metadata...")
    all_files_data = []
    # ... (Scanning logic is the same as before)
    for source_path in SOURCE_PATHS:
        # ...
        for root, _, files in os.walk(source_path):
            for filename in files:
                # ...
                try:
                    file_path = Path(root) / filename
                    file_size = file_path.stat().st_size
                    all_files_data.append({'path': str(file_path), 'name': filename, 'size': file_size})
                except FileNotFoundError:
                    print(f"  [Warning] File not found during scan, possibly a broken symlink: {filename}")
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
        mime_type = magic.from_file(path, mime=True)
        file_hash = get_file_hash(path)
        
        is_pdf_valid, has_ocr = (False, False)
        if 'pdf' in mime_type:
            is_pdf_valid, has_ocr = get_pdf_details(path)
        
        # ** NEW: Classify the file **
        game_system, edition, category = classifier.classify_by_filename(row['name'])

        analysis_results.append({
            'hash': file_hash, 'mime_type': mime_type, 'is_pdf_valid': is_pdf_valid, 'has_ocr': has_ocr,
            'game_system': game_system, 'edition': edition, 'category': category
        })
        if (index + 1) % 100 == 0: print(f"  ...processed {index + 1}/{len(df)} files")

    df = pd.concat([df, pd.DataFrame(analysis_results)], axis=1).dropna(subset=['hash'])

    print("Step 3: Deduplicating and selecting the best files...")
    # ... (Quality score and sorting logic is the same)
    df['quality_score'] = 0
    df.loc[df['has_ocr'], 'quality_score'] += 4
    df.loc[df['is_pdf_valid'], 'quality_score'] += 2
    df['path_len'] = df['path'].str.len()
    df = df.sort_values(by=['quality_score', 'size', 'path_len'], ascending=[False, False, True])
    unique_content_files = df.drop_duplicates(subset='hash', keep='first').copy()
    print(f"  - After content deduplication: {len(unique_content_files)} unique files remain.")
    unique_content_files['name_occurrence'] = unique_content_files.groupby('name').cumcount()

    print("Step 4: Copying selected files and building search index...")
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

        # ** NEW: Create categorized destination path **
        dest_subdir = Path(row['game_system'] or 'Other')
        if row['edition']: dest_subdir = dest_subdir / row['edition']
        if row['category']: dest_subdir = dest_subdir / row['category']
        
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
            print(f"  [Error] Could not copy {original_path}. Reason: {e}")
            
    conn.commit()
    conn.close()
    classifier.close()

    print("\n--- Library Build Complete! ---")
    print(f"Total files selected and copied: {copied_files}")
    print(f"New library location: {LIBRARY_ROOT}")

if __name__ == '__main__':
    if "/path/to/" in LIBRARY_ROOT:
        print("ERROR: Please configure paths in 'src/librarian.py' before running.")
    else:
        build_library()
