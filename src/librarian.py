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
import unicodedata
from rapidfuzz import fuzz
from src.config_loader import load_config
import requests  # For optional remote knowledge base
from src.isbn_enricher import enrich_file_with_isbn_metadata

def normalize_text(text):
    """
    Lowercase, NFKD-unicode normalize, and strip accents and non-alphanumerics.
    """
    if not text:
        return ''
    text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r'[^a-z0-9]', '', text)
    return text

# --- Classifier Engine ---
class Classifier:
    """
    An intelligent engine for categorizing files using a multi-tiered approach.
    Enhanced for multilingual/Unicode support and fuzzy matching.
    """
    def __init__(self, knowledge_db_path):
        self.conn = None
        self.product_cache = {}
        self.path_keywords = {}
        # Support loading knowledge base from a web URL if provided
        if knowledge_db_path.startswith("http://") or knowledge_db_path.startswith("https://"):
            local_tmp = "/tmp/knowledge.sqlite"
            try:
                r = requests.get(knowledge_db_path, timeout=30)
                r.raise_for_status()
                with open(local_tmp, "wb") as f:
                    f.write(r.content)
                knowledge_db_path = local_tmp
            except Exception as e:
                print(f"[WARNING] Could not download remote knowledge base: {e}", file=sys.stderr)
                knowledge_db_path = None
        if knowledge_db_path and os.path.exists(knowledge_db_path):
            try:
                self.conn = sqlite3.connect(f"file:{knowledge_db_path}?mode=ro", uri=True)
                self.load_products_to_cache()
                self.load_path_keywords()
                self.load_alternate_keywords()
            except sqlite3.Error as e:
                print(f"[WARNING] Could not connect to knowledge base. TTRPG classification will be limited. Error: {e}", file=sys.stderr)
        else:
            print("[WARNING] Knowledge base not found. Run build_knowledgebase.sh for full TTRPG classification.", file=sys.stderr)

    def load_products_to_cache(self):
        """
        Loads product data into memory for extremely fast lookups during classification.
        Now normalizes keys for multilingual support.
        """
        if not self.conn: return
        cursor = self.conn.cursor()
        cursor.execute("SELECT product_code, title, game_system, edition, category FROM products")
        for code, title, system, edition, category in cursor.fetchall():
            if code:
                nkey = normalize_text(code)
                self.product_cache[nkey] = (system, edition, category)
            if title:
                nkey = normalize_text(title)
                self.product_cache[nkey] = (system, edition, category)

    def load_path_keywords(self):
        """
        Creates a dictionary of TTRPG keywords (like 'd&d', '5e') for path analysis.
        Now normalized for multilingual support.
        """
        if not self.conn: return
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT game_system, edition FROM products WHERE game_system IS NOT NULL")
        for system, edition in cursor.fetchall():
            nsys = normalize_text(system)
            self.path_keywords[nsys] = system
            if edition:
                nedit = normalize_text(edition)
                self.path_keywords[nedit] = system

    def load_alternate_keywords(self):
        """
        Optionally, loads alternate language keywords/titles from a table if present.
        The table should have: alt_title, product_code, system, edition, category
        """
        if not self.conn: return
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT alt_title, product_code, game_system, edition, category FROM alternate_titles")
            for alt_title, code, system, edition, category in cursor.fetchall():
                if alt_title:
                    nkey = normalize_text(alt_title)
                    self.product_cache[nkey] = (system, edition, category)
                    # Also add to path keywords for folder matching
                    self.path_keywords[nkey] = system
        except sqlite3.Error:
            # Table doesn't exist, skip
            pass

    def _classify_by_filename(self, filename):
        """
        (Priority 1) Tries to classify based on product codes or titles in the filename.
        Now uses normalization and fallback to fuzzy match.
        """
        clean_filename = normalize_text(filename)
        for title_key in sorted(self.product_cache.keys(), key=len, reverse=True):
            if title_key in clean_filename:
                return self.product_cache[title_key]
        # Fuzzy match fallback (threshold can be tuned)
        for title_key in self.product_cache.keys():
            if fuzz.partial_ratio(clean_filename, title_key) >= 90:
                return self.product_cache[title_key]
        return None

    def _classify_by_path(self, full_path):
        """
        (Priority 2) Tries to classify based on keywords found in the file's parent path.
        Path segments are normalized.
        """
        if not self.path_keywords: return None
        for part in reversed(Path(full_path).parts[:-1]):
            clean_part = normalize_text(part)
            if clean_part in self.path_keywords:
                game_system = self.path_keywords[clean_part]
                return game_system, "From Folder", "Heuristic"
            # Fuzzy match for path segments
            for kw in self.path_keywords.keys():
                if fuzz.partial_ratio(clean_part, kw) >= 90:
                    game_system = self.path_keywords[kw]
                    return game_system, "From Folder", "Heuristic"
        return None

    def _classify_by_mimetype(self, mime_type):
        """
        (Priority 3) Classifies based on generic file type.
        """
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
        """
        Runs the full classification hierarchy to find the best category for a file.
        """
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
    except IOError as e:
        print(f"  [Warning] Could not hash file {os.path.basename(file_path)}. Skipping. Reason: {e}", file=sys.stderr)
        return None

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
        # Silently fail. A PDF that can't be opened is simply marked as invalid.
        pass
    return is_valid, has_text

def scan_files(source_paths):
    """Generator for scanning files efficiently."""
    for src in source_paths:
        if not os.path.isdir(src):
            print(f"[Warning] Source path not found, skipping: {src}", file=sys.stderr)
            continue
        for p in Path(src).rglob('*'):
            if p.is_file():
                try:
                    yield {'path': str(p), 'name': p.name, 'size': p.stat().st_size}
                except (IOError, OSError) as e:
                    print(f"  [Warning] Could not stat file {p.name}. Skipping. Reason: {e}", file=sys.stderr)

# --- Main Logic ---
def classify_with_isbn_fallback(classifier, filename, full_path, mime_type, isbn_cache):
    """
    Try normal classification, then try ISBN enrichment if uncategorized.
    isbn_cache: dict to avoid redundant ISBN lookups.
    """
    result = classifier._classify_by_filename(filename)
    if result:
        return result
    result = classifier._classify_by_path(full_path)
    if result:
        return result
    result = classifier._classify_by_mimetype(mime_type)
    if result:
        return result
    # Try ISBN enrichment only for PDFs and text files
    if mime_type.startswith('application/pdf') or mime_type.startswith('text/'):
        if full_path in isbn_cache:
            isbn_results = isbn_cache[full_path]
        else:
            isbn_results = enrich_file_with_isbn_metadata(full_path)
            isbn_cache[full_path] = isbn_results
        if isbn_results:
            # Use the first valid ISBN metadata found
            meta = isbn_results[0]['metadata']
            title = meta.get('title')
            authors = ', '.join(a['name'] for a in meta.get('authors', [])) if meta.get('authors') else None
            # Use title as a pseudo-product for classification
            if title:
                # Try to classify by title using normalized text
                ntitle = normalize_text(title)
                if ntitle in classifier.product_cache:
                    return classifier.product_cache[ntitle]
                # Otherwise, use Open Library metadata as best guess
                return (title, None, "ISBN/Book")
    return ('Miscellaneous', None, None)

def build_library(config):
    # --- Step 1: Configuration & Setup ---
    SOURCE_PATHS, LIBRARY_ROOT = config['source_paths'], config['library_root']
    MIN_PDF_SIZE_BYTES = config['min_pdf_size_bytes']
    DB_FILE, KNOWLEDGE_DB_FILE = "library_index.sqlite", "knowledge.sqlite"
    # Optionally, get language mapping for future use
    KB_LANGS = config.get('knowledge_base_url_languages', {})
    
    print("--- Starting Library Build Process ---")
    os.makedirs(LIBRARY_ROOT, exist_ok=True)
    classifier = Classifier(KNOWLEDGE_DB_FILE)
    isbn_cache = {}  # To avoid redundant ISBN queries

    # --- Step 2: File Scanning ---
    print("Step 1: Scanning all source directories for files...")
    all_files = list(scan_files(SOURCE_PATHS))
    if not all_files:
        print("No files found in source paths. Exiting.")
        return
    df = pd.DataFrame(all_files)
    print(f"Found {len(df)} total files.")

    # --- Step 3: Analysis & Classification ---
    print("Step 2: Analyzing and Classifying files (this may take a while)...")
    analysis_results = []
    for index, row in df.iterrows():
        path = row['path']
        try:
            mime_type = magic.from_file(path, mime=True)
        except magic.MagicException as e:
            print(f"  [Warning] Could not determine MIME type for {row['name']}. Reason: {e}", file=sys.stderr)
            mime_type = 'unknown/unknown'
        file_hash = get_file_hash(path)
        is_pdf, has_ocr = (False, False)
        if 'pdf' in mime_type: is_pdf, has_ocr = get_pdf_details(path)
        # Use new classification function with ISBN fallback
        system, edition, category = classify_with_isbn_fallback(classifier, row['name'], path, mime_type, isbn_cache)
        analysis_results.append({'hash': file_hash, 'mime_type': mime_type, 'is_pdf_valid': is_pdf, 'has_ocr': has_ocr, 'game_system': system, 'edition': edition, 'category': category})
        if (index + 1) % 500 == 0: print(f"  ...processed {index + 1}/{len(df)} files")
    df = pd.concat([df, pd.DataFrame(analysis_results)], axis=1).dropna(subset=['hash'])

    # --- Step 4: Deduplication & Selection ---
    print("Step 3: Deduplicating based on content and selecting best files...")
    df['quality_score'] = 0
    df.loc[df['has_ocr'], 'quality_score'] += 4
    df.loc[df['is_pdf_valid'], 'quality_score'] += 2
    df.loc[df['size'] > MIN_PDF_SIZE_BYTES, 'quality_score'] += 1
    df = df.sort_values(by=['quality_score', 'size'], ascending=False)
    unique_files = df.drop_duplicates(subset='hash', keep='first').copy()
    print(f"  - After content deduplication: {len(unique_files)} unique files remain.")
    unique_files['name_occurrence'] = unique_files.groupby('name').cumcount()

    # --- Step 5: Copy & Index ---
    print("Step 4: Copying files into new structure and building search index...")
    db_path = os.path.join(LIBRARY_ROOT, DB_FILE)
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except Exception as e:
            print(f"  [Error] Could not remove old DB: {e}", file=sys.stderr)
            return
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('CREATE TABLE files (id INTEGER PRIMARY KEY, filename TEXT, path TEXT, type TEXT, size INTEGER, game_system TEXT, edition TEXT, language TEXT)')
            batch = []
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
                    # Add language if available, else fallback to None
                    language = getattr(row, 'language', None)
                    batch.append((new_filename, str(destination_path), row['mime_type'], row['size'], row['game_system'], row['edition'], language))
                except Exception as e:
                    print(f"  [Error] Could not copy {original_path}. Reason: {e}", file=sys.stderr)
                if len(batch) >= 100:
                    cursor.executemany(
                        "INSERT INTO files (filename, path, type, size, game_system, edition, language) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        batch
                    )
                    batch = []
            if batch:
                cursor.executemany(
                    "INSERT INTO files (filename, path, type, size, game_system, edition, language) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    batch
                )
            conn.commit()
    except Exception as e:
        print(f"  [Error] Could not build library DB: {e}", file=sys.stderr)
    finally:
        classifier.close()
    print(f"\n--- Library Build Complete! ---")
    print(f"Total unique files copied: {len(unique_files)}")
    print(f"New library location: {LIBRARY_ROOT}")

# --- Main Execution Block ---
if __name__ == '__main__':
    try:
        config = load_config()
        # Allow knowledge_base_urls to specify a remote DB file
        knowledge_db_url = config.get('knowledge_base_db_url')
        if knowledge_db_url:
            config['knowledge_base_db_url'] = knowledge_db_url.strip()
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
