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
import sys
from src.config_loader import load_config

# --- Classifier Engine ---
class Classifier:
    def __init__(self, knowledge_db_path):
        self.conn = None
        self.product_cache = {}
        self.path_keywords = {}
        if os.path.exists(knowledge_db_path):
            try:
                # Connect in read-only mode
                self.conn = sqlite3.connect(f"file:{knowledge_db_path}?mode=ro", uri=True)
                self.load_products_to_cache()
                self.load_path_keywords()
            except sqlite3.Error as e:
                print(f"[WARNING] Could not connect to knowledge base. TTRPG classification will be limited. Error: {e}", file=sys.stderr)
        else:
            print("[WARNING] Knowledge base not found. Run build_knowledgebase.sh for full TTRPG classification.", file=sys.stderr)

    def load_products_to_cache(self):
        if not self.conn: return
        cursor = self.conn.cursor()
        cursor.execute("SELECT product_code, title, game_system, edition, category FROM products")
        for code, title, system, edition, category in cursor.fetchall():
            if code: self.product_cache[re.sub(r'[^a-z0-9]', '', code.lower())] = (system, edition, category)
            if title: self.product_cache[re.sub(r'[^a-z0-9]', '', title.lower())] = (system, edition, category)

    def load_path_keywords(self):
        if not self.conn: return
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT game_system, edition FROM products WHERE game_system IS NOT NULL")
        for system, edition in cursor.fetchall():
            self.path_keywords[system.lower().replace(" ", "")] = system
            if edition: self.path_keywords[edition.lower().replace(" ", "")] = system

    def _classify_by_filename(self, filename):
        clean_filename = re.sub(r'[^a-z0-9]', '', filename.lower())
        for title_key in sorted(self.product_cache.keys(), key=len, reverse=True):
            if title_key in clean_filename:
                return self.product_cache[title_key]
        return None

    def _classify_by_path(self, full_path):
        if not self.path_keywords: return None
        for part in reversed(Path(full_path).parts[:-1]):
            clean_part = part.lower().replace(" ", "").replace("_", "").replace("-", "")
            if clean_part in self.path_keywords:
                game_system = self.path_keywords[clean_part]
                return game_system, "From Folder", "Heuristic"
        return None

    def _classify_by_mimetype(self, mime_type):
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
        result = self._classify_by_filename(filename)
        if result: return result
        result = self._classify_by_path(full_path)
        if result: return result
        result = self._classify_by_mimetype(mime_type)
        if result: return result
        return ('Miscellaneous', None, None)

    def close(self):
        if self.conn: self.conn.close()

# --- Helper Functions ---
def get_file_hash(file_path):
    sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
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
    except Exception:
        # Suppress detailed error, just mark as invalid
        pass
    return is_valid, has_text

# --- Main Logic ---
def build_library(config):
    SOURCE_PATHS, LIBRARY_ROOT = config['source_paths'], config['library_root']
    MIN_PDF_SIZE_BYTES = config['min_pdf_size_bytes']
    DB_FILE, KNOWLEDGE_DB_FILE = "library_index.sqlite", "knowledge.sqlite"
    
    print("--- Starting Library Build Process ---")
    os.makedirs(LIBRARY_ROOT, exist_ok=True)
    classifier = Classifier(KNOWLEDGE_DB_FILE)

    print("Step 1: Scanning files...")
    all_files = [
        {'path': str(p), 'name': p.name, 'size': p.stat().st_size}
        for src in SOURCE_PATHS if os.path.isdir(src)
        for p in Path(src).rglob('*') if p.is_file()
    ]
    if not all_files: print("No files found. Exiting."); return
    df = pd.DataFrame(all_files)
    print(f"Found {len(df)} total files.")

    print("Step 2: Analyzing and Classifying files...")
    analysis_results = []
    for index, row in df.iterrows():
        path = row['path']
        try:
            mime_type = magic.from_file(path, mime=True)
        except magic.MagicException:
            mime_type = 'unknown/unknown'
        file_hash = get_file_hash(path)
        is_pdf, has_ocr = (False, False)
        if 'pdf' in mime_type: is_pdf, has_ocr = get_pdf_details(path)
        system, edition, category = classifier.classify(row['name'], path, mime_type)
        analysis_results.append({'hash': file_hash, 'mime_type': mime_type, 'is_pdf_valid': is_pdf, 'has_ocr': has_ocr, 'game_system': system, 'edition': edition, 'category': category})
        if (index + 1) % 500 == 0: print(f"  ...processed {index + 1}/{len(df)} files")
    
    df = pd.concat([df, pd.DataFrame(analysis_results)], axis=1).dropna(subset=['hash'])

    print("Step 3: Deduplicating and selecting best files...")
    df['quality_score'] = 0
    df.loc[df['has_ocr'], 'quality_score'] += 4
    df.loc[df['is_pdf_valid'], 'quality_score'] += 2
    df.loc[df['size'] > MIN_PDF_SIZE_BYTES, 'quality_score'] += 1
    df = df.sort_values(by=['quality_score', 'size'], ascending=False)
    unique_files = df.drop_duplicates(subset='hash', keep='first').copy()
    print(f"  - After content deduplication: {len(unique_files)} unique files remain.")
    unique_files['name_occurrence'] = unique_files.groupby('name').cumcount()

    print("Step 4: Copying files and building index...")
    db_path = os.path.join(LIBRARY_ROOT, DB_FILE)
    if os.path.exists(db_path): os.remove(db_path) 
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE files (id INTEGER PRIMARY KEY, filename TEXT, path TEXT, type TEXT, size INTEGER, game_system TEXT, edition TEXT)')
    
    for _, row in unique_files.iterrows():
        original_path = Path(row['path'])
        new_filename = f"{original_path.stem}_{row['name_occurrence']}{original_path.suffix}" if row['name_occurrence'] > 0 else original_path.name
        dest_subdir = Path(row['game_system'] or 'Misc')
        if row['edition'] and pd.notna(row['edition']): dest_subdir = dest_subdir / row['edition']
        if row['category'] and pd.notna(row['category']): dest_subdir = dest_subdir / row['category']
        destination_path = Path(LIBRARY_ROOT) / dest_subdir / new_filename
        os.makedirs(destination_path.parent, exist_ok=True)
        try:
            shutil.copy2(original_path, destination_path)
            cursor.execute("INSERT INTO files (filename, path, type, size, game_system, edition) VALUES (?, ?, ?, ?, ?, ?)",(new_filename, str(destination_path), row['mime_type'], row['size'], row['game_system'], row['edition']))
        except Exception as e:
            print(f"  [Error] Could not copy {original_path}. Reason: {e}", file=sys.stderr)
            
    conn.commit()
    conn.close()
    classifier.close()
    print(f"\n--- Library Build Complete! ---")

if __name__ == '__main__':
    try:
        config = load_config()
        if not config['source_paths'] or "/path/to/" in config['source_paths'][0]:
             print("[FATAL] Configuration Error: 'source_paths' is not set in conf/config.ini.", file=sys.stderr)
             sys.exit(1)
        if not config['library_root'] or "/path/to/" in config['library_root']:
             print("[FATAL] Configuration Error: 'library_root' is not set in conf/config.ini.", file=sys.stderr)
             sys.exit(1)
        build_library(config)
    except FileNotFoundError as e:
        print(f"[FATAL] A required file was not found. Please check your setup. Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)
