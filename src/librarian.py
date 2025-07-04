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
from src.config_loader import load_config
import requests  # For optional remote knowledge base
from src.isbn_enricher import enrich_file_with_isbn_metadata
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import resource
from functools import partial

# --- Dependency Checks with auto-install for all required modules ---
REQUIRED_MODULES = [
    ("magic", "python-magic"),
    ("fitz", "pymupdf"),
    ("rapidfuzz", "rapidfuzz"),
    ("psutil", "psutil"),
    ("tqdm", "tqdm"),
]
for mod, pipname in REQUIRED_MODULES:
    try:
        globals()[mod] = __import__(mod)
    except ImportError:
        print(f"[INFO] {mod} not found. Attempting to install {pipname}...", file=sys.stderr)
        import subprocess
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', pipname])
            globals()[mod] = __import__(mod)
            print(f"[INFO] {mod} installed successfully.")
        except Exception as e:
            print(f"[FATAL] Could not install {pipname} automatically: {e}", file=sys.stderr)
            sys.exit(1)
# Now import from modules as needed
from magic import from_file as magic_from_file
from fitz import open as fitz_open
from rapidfuzz import fuzz
import psutil
from tqdm import tqdm
HAS_TQDM = True

os.environ["MU_DISABLE_WARNINGS"] = "1"  # Suppress MuPDF C-level warnings

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
skipped_appledouble_files = []
skipped_invalid_pdf_files = []
failed_pdf_repairs = []

LOG_FILE = "librarian_run.log"

def log_error(error_type, file_path, message, extra=None):
    with open(LOG_FILE, "a", encoding="utf-8") as logf:
        logf.write(f"[{error_type}] {file_path} | {message}\n")
        if extra:
            logf.write(f"    {extra}\n")

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

def qpdf_check_pdf(file_path):
    """Use qpdf --check to verify PDF integrity. Returns (is_valid, qpdf_output)."""
    try:
        result = subprocess.run(['qpdf', '--check', file_path], capture_output=True, text=True, timeout=30)
        output = result.stdout + result.stderr
        if 'no syntax or stream errors' in output.lower():
            return True, output
        return False, output
    except Exception as e:
        return False, str(e)

def get_pdf_details(file_path):
    is_valid, has_text = False, False
    pdf_version = None
    pdf_creator = None
    pdf_producer = None
    qpdf_checked = False
    qpdf_check_output = None
    warnings.filterwarnings("ignore", category=UserWarning)
    # Skip AppleDouble files (macOS resource forks)
    if os.path.basename(file_path).startswith("._"):
        msg = f"Skipping AppleDouble resource fork: {file_path}"
        print(f"  [Info] {msg}", file=sys.stderr)
        log_error("SKIPPED_APPLEDOUBLE", file_path, msg)
        skipped_appledouble_files.append(file_path)
        return False, False, pdf_version, pdf_creator, pdf_producer
    # Quick check: skip files that are not PDFs by header, but try to extract version
    try:
        with open(file_path, "rb") as f:
            header = f.read(1024)
            if not header.startswith(b'%PDF-'):
                msg = f"File does not have a valid PDF header. Skipping."
                print(f"  [Warning] {msg} {file_path}", file=sys.stderr)
                log_error("SKIPPED_INVALID_PDF_HEADER", file_path, msg)
                skipped_invalid_pdf_files.append(file_path)
                return False, False, pdf_version, pdf_creator, pdf_producer
            # Extract PDF version from header
            try:
                pdf_version = header[5:8].decode(errors='replace')
            except Exception:
                pdf_version = None
    except Exception as e:
        msg = f"Could not read file header: {e}"
        print(f"  [Warning] {msg} {file_path}", file=sys.stderr)
        log_error("HEADER_READ_ERROR", file_path, msg)
        skipped_invalid_pdf_files.append(file_path)
        return False, False, pdf_version, pdf_creator, pdf_producer
    try:
        with fitz.open(file_path) as doc:
            if doc.page_count > 0:
                is_valid = True
                # Extract metadata
                meta = doc.metadata or {}
                pdf_creator = meta.get('creator')
                pdf_producer = meta.get('producer')
                for page in doc:
                    text = ''
                    get_text_fn = getattr(page, 'get_text', None)
                    getText_fn = getattr(page, 'getText', None)
                    try:
                        if callable(get_text_fn):
                            text = get_text_fn("text")
                        elif callable(getText_fn):
                            text = getText_fn("text")
                        else:
                            msg = f"PyMuPDF page object has no get_text or getText method"
                            print(f"  [Warning] {msg} for {file_path}", file=sys.stderr)
                            log_error("MUPDF_METHOD_ERROR", file_path, msg)
                    except Exception as e:
                        msg = f"Could not extract text from page: {e}"
                        print(f"  [Warning] {msg} in {file_path}", file=sys.stderr)
                        log_error("MUPDF_TEXT_EXTRACTION_ERROR", file_path, msg)
                    if text:
                        has_text = True
                        break
    except Exception as e:
        msg = f"Could not open PDF with PyMuPDF: {e}"
        print(f"  [Warning] {msg} {file_path}", file=sys.stderr)
        log_error("MUPDF_OPEN_ERROR", file_path, msg)
        # Fallback: try qpdf --check
        is_valid, qpdf_check_output = qpdf_check_pdf(file_path)
        qpdf_checked = True
        if is_valid:
            print(f"  [INFO] qpdf --check: PDF is valid according to qpdf: {file_path}")
        else:
            msg = f"qpdf --check: PDF is invalid. qpdf output: {qpdf_check_output}"
            print(f"  [Warning] {msg} {file_path}", file=sys.stderr)
            log_error("QPDF_CHECK_ERROR", file_path, msg, extra=qpdf_check_output)
        # Optionally, log to a file for later review
        with open("bad_pdfs.log", "a") as logf:
            logf.write(f"{file_path}: PyMuPDF error: {e}\nqpdf --check: {qpdf_check_output}\n")
    return is_valid, has_text, pdf_version, pdf_creator, pdf_producer

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

def repair_pdf(input_path, output_path):
    """Attempt to repair a PDF using qpdf. Returns True if successful."""
    try:
        result = subprocess.run(['qpdf', '--repair', input_path, output_path], check=True, capture_output=True)
        return os.path.exists(output_path)
    except Exception as e:
        msg = f"Could not repair PDF: {e}"
        print(f"  [Warning] {msg} {input_path}", file=sys.stderr)
        log_error("PDF_REPAIR_ERROR", input_path, msg)
        return False

def ensure_qpdf_installed():
    """Check if qpdf is installed, and attempt to install it if not."""
    if shutil.which('qpdf') is not None:
        return True
    print("[INFO] qpdf not found. Attempting to install qpdf using system package manager...", file=sys.stderr)
    import platform
    import subprocess
    try:
        distro = platform.system().lower()
        if distro == 'linux':
            # Try apt-get, then dnf, then yum
            for cmd in [['sudo', 'apt-get', 'update'], ['sudo', 'apt-get', 'install', '-y', 'qpdf'],
                        ['sudo', 'dnf', 'install', '-y', 'qpdf'], ['sudo', 'yum', 'install', '-y', 'qpdf']]:
                try:
                    subprocess.run(cmd, check=True)
                    if shutil.which('qpdf') is not None:
                        print("[INFO] qpdf installed successfully.")
                        return True
                except Exception:
                    continue
        elif distro == 'darwin':
            # macOS
            subprocess.run(['brew', 'install', 'qpdf'], check=True)
            if shutil.which('qpdf') is not None:
                print("[INFO] qpdf installed successfully.")
                return True
        elif distro == 'windows':
            print("[FATAL] Please install qpdf manually on Windows.", file=sys.stderr)
            return False
    except Exception as e:
        print(f"[FATAL] Could not install qpdf automatically: {e}", file=sys.stderr)
        return False
    print("[FATAL] qpdf could not be installed automatically. Please install it manually.", file=sys.stderr)
    return False

# Ensure qpdf is installed before any PDF repair attempts
ensure_qpdf_installed()

# --- Parallel PDF validation and repair ---
def validate_and_repair_pdf(file_path):
    if os.path.basename(file_path).startswith("._"):
        print(f"  [Info] Skipping AppleDouble resource fork: {file_path}", file=sys.stderr)
        skipped_appledouble_files.append(file_path)
        return file_path, False, False, None, None, None
    is_valid, has_text, pdf_version, pdf_creator, pdf_producer = get_pdf_details(file_path)
    if is_valid:
        return file_path, is_valid, has_text, pdf_version, pdf_creator, pdf_producer
    try:
        with open(file_path, "rb") as f:
            header = f.read(5)
            if header != b'%PDF-':
                print(f"  [Info] Not attempting repair: {file_path} is not a valid PDF.", file=sys.stderr)
                skipped_invalid_pdf_files.append(file_path)
                return file_path, False, False, pdf_version, pdf_creator, pdf_producer
    except Exception as e:
        print(f"  [Warning] Could not read file header for repair check: {file_path}: {e}", file=sys.stderr)
        skipped_invalid_pdf_files.append(file_path)
        return file_path, False, False, pdf_version, pdf_creator, pdf_producer
    repaired_path = file_path + ".repaired.pdf"
    if repair_pdf(file_path, repaired_path):
        is_valid, has_text, pdf_version, pdf_creator, pdf_producer = get_pdf_details(repaired_path)
        if is_valid:
            print(f"  [INFO] PDF repaired: {file_path} -> {repaired_path}")
            return repaired_path, is_valid, has_text, pdf_version, pdf_creator, pdf_producer
    print(f"  [Warning] PDF repair failed or file is too corrupted: {file_path}", file=sys.stderr)
    failed_pdf_repairs.append(file_path)
    return file_path, False, False, pdf_version, pdf_creator, pdf_producer

# --- Main Logic ---
def classify_with_isbn_fallback(classifier, filename, full_path, mime_type, isbn_cache):
    """
    Try normal classification, then try ISBN enrichment if uncategorized.
    isbn_cache: dict to avoid redundant ISBN lookups.
    """
    # 1. Try filename
    result = classifier._classify_by_filename(filename)
    if result:
        return result
    # 2. Try path
    result = classifier._classify_by_path(full_path)
    if result:
        return result
    # 3. Try mimetype
    result = classifier._classify_by_mimetype(mime_type)
    if result:
        return result
    # 4. Try ISBN enrichment only for PDFs and text files
    if mime_type.startswith('application/pdf') or mime_type.startswith('text/'):
        try:
            if full_path in isbn_cache:
                isbn_results = isbn_cache[full_path]
            else:
                isbn_results = enrich_file_with_isbn_metadata(full_path)
                isbn_cache[full_path] = isbn_results
            if isbn_results:
                meta = isbn_results[0]['metadata']
                title = meta.get('title')
                authors = ', '.join(a['name'] for a in meta.get('authors', [])) if meta.get('authors') else None
                if title:
                    ntitle = normalize_text(title)
                    if ntitle in classifier.product_cache:
                        print(f"  [DEBUG] ISBN enrichment matched title in product cache: {title}")
                        return classifier.product_cache[ntitle]
                    print(f"  [DEBUG] ISBN enrichment used Open Library metadata for: {title}")
                    return (title, None, "ISBN/Book")
        except Exception as e:
            print(f"  [DEBUG] ISBN enrichment failed for {full_path}: {e}", file=sys.stderr)
    print(f"  [DEBUG] File {filename} is uncategorized after all attempts.")
    return ('Miscellaneous', None, None)

def analyze_row(row, knowledge_db_path, isbn_cache, pdf_validation):
    path = str(row.path)
    try:
        mime_type = magic.from_file(path, mime=True)
    except Exception as e:
        msg = f"Could not determine MIME type: {e}"
        print(f"  [Warning] {msg} {row.name}", file=sys.stderr)
        log_error("MIMETYPE_ERROR", path, msg)
        mime_type = 'unknown/unknown'
    file_hash = get_file_hash(path)
    is_pdf, has_ocr, pdf_version, pdf_creator, pdf_producer = pdf_validation.get(path, (False, False, None, None, None))
    try:
        classifier = Classifier(knowledge_db_path)
        system, edition, category = classify_with_isbn_fallback(classifier, row.name, path, mime_type, isbn_cache)
        classifier.close()
    except Exception as e:
        msg = f"Classification failed: {e}"
        print(f"  [Warning] {msg} {row.name}", file=sys.stderr)
        log_error("CLASSIFICATION_ERROR", path, msg)
        system, edition, category = ('Miscellaneous', None, None)
    return {'hash': file_hash, 'mime_type': mime_type, 'is_pdf_valid': is_pdf, 'has_ocr': has_ocr, 'pdf_version': pdf_version, 'pdf_creator': pdf_creator, 'pdf_producer': pdf_producer, 'game_system': system, 'edition': edition, 'category': category}

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
        classifier.close()
        return
    df = pd.DataFrame(all_files)
    print(f"Found {len(df)} total files.")

    # --- Step 3: Analysis & Classification ---
    print("Step 2: Analyzing and Classifying files (this may take a while)...")
    analysis_results = []
    pdf_paths = [row['path'] for row in all_files if row['name'].lower().endswith('.pdf')]
    pdf_validation = {}
    max_workers = min(32, (multiprocessing.cpu_count() or 1) * 2)
    # PDF validation/repair with progress
    if HAS_TQDM:
        pdf_iter = tqdm(pdf_paths, desc="Validating/Repairing PDFs", unit="pdf")
    else:
        print("Validating/Repairing PDFs...")
        pdf_iter = pdf_paths
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(validate_and_repair_pdf, path): path for path in pdf_iter}
        for i, future in enumerate(as_completed(futures)):
            path, is_valid, has_ocr, pdf_version, pdf_creator, pdf_producer = future.result()
            pdf_validation[path] = (is_valid, has_ocr, pdf_version, pdf_creator, pdf_producer)
            if not HAS_TQDM and (i+1) % 10 == 0:
                print(f"  Processed {i+1}/{len(pdf_paths)} PDFs...")
    # Classification/analysis with progress
    knowledge_db_path = config.get('knowledge_base_db_url') or "knowledge.sqlite"
    analyze_row_partial = partial(analyze_row, knowledge_db_path=knowledge_db_path, isbn_cache=isbn_cache, pdf_validation=pdf_validation)
    if HAS_TQDM:
        row_iter = tqdm(list(df.itertuples(index=False)), desc="Classifying/Hashing", unit="file")
    else:
        print("Classifying/Hashing files...")
        row_iter = list(df.itertuples(index=False))
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        analysis_results = list(executor.map(analyze_row_partial, row_iter))
    df = pd.concat([df, pd.DataFrame(analysis_results)], axis=1).dropna(subset=['hash'])
    # --- Step 4: Deduplication & Selection ---
    print("Step 3: Deduplicating based on content and selecting best files...")
    df['quality_score'] = 0
    # Fix: increment quality_score only for numeric rows
    def safe_add_quality(row, add):
        try:
            if isinstance(row['quality_score'], (int, float)):
                return row['quality_score'] + add
        except Exception:
            pass
        return row['quality_score']
    df.loc[df['has_ocr'] == True, 'quality_score'] = df.loc[df['has_ocr'] == True].apply(lambda row: safe_add_quality(row, 4), axis=1)
    df.loc[df['is_pdf_valid'] == True, 'quality_score'] = df.loc[df['is_pdf_valid'] == True].apply(lambda row: safe_add_quality(row, 2), axis=1)
    mask = (df['size'].apply(lambda x: isinstance(x, (int, float))) & (df['size'] > MIN_PDF_SIZE_BYTES))
    df.loc[mask, 'quality_score'] = df.loc[mask].apply(lambda row: safe_add_quality(row, 1), axis=1)
    df = df.sort_values(by=['quality_score', 'size'], ascending=False)
    unique_files = df.drop_duplicates(subset='hash', keep='first').copy()
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
            if HAS_TQDM:
                copy_iter = tqdm(list(unique_files.iterrows()), desc="Copying Files", unit="file")
            else:
                print("Copying files...")
                copy_iter = unique_files.iterrows()
            for _, row in copy_iter:
                # ...existing code for copying and indexing...
                original_path = Path(row['path'])
                new_filename = f"{original_path.stem}_{row['name_occurrence']}{original_path.suffix}" if row['name_occurrence'] > 0 else original_path.name
                dest_subdir = Path(row['game_system'] or 'Misc')
                if row['edition'] and pd.notna(row['edition']): dest_subdir = dest_subdir / row['edition']
                if row['category'] and pd.notna(row['category']): dest_subdir = dest_subdir / row['category']
                destination_path = Path(LIBRARY_ROOT) / dest_subdir / new_filename
                os.makedirs(destination_path.parent, exist_ok=True)
                try:
                    shutil.copy2(original_path, destination_path)
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
        try:
            classifier.close()
        except Exception as e:
            print(f"[Warning] Could not close classifier DB: {e}", file=sys.stderr)
    print(f"\n--- Library Build Complete! ---")
    print(f"Total unique files copied: {len(unique_files)}")
    print(f"New library location: {LIBRARY_ROOT}")
    # --- Print summary of skipped and failed files ---
    if skipped_appledouble_files:
        print(f"\n[SUMMARY] Skipped AppleDouble resource forks ({len(skipped_appledouble_files)}):")
        for f in skipped_appledouble_files:
            print(f"   {f}")
    if skipped_invalid_pdf_files:
        print(f"\n[SUMMARY] Skipped files with invalid PDF header ({len(skipped_invalid_pdf_files)}):")
        for f in skipped_invalid_pdf_files:
            print(f"   {f}")
    if failed_pdf_repairs:
        print(f"\n[SUMMARY] Files that failed PDF repair ({len(failed_pdf_repairs)}):")
        for f in failed_pdf_repairs:
            # Try to print version/creator if available
            try:
                details = df[df['path'] == f][['pdf_version', 'pdf_creator', 'pdf_producer']].iloc[0]
                print(f"   {f} | version: {details['pdf_version']} | creator: {details['pdf_creator']} | producer: {details['pdf_producer']}")
            except Exception:
                print(f"   {f}")

    # --- Print log summary ---
    print("\n--- Error/Warning Summary from librarian_run.log ---")
    if os.path.exists(LOG_FILE):
        from collections import Counter
        with open(LOG_FILE, "r", encoding="utf-8") as logf:
            lines = logf.readlines()
        error_types = [line.split(']')[0][1:] for line in lines if line.startswith('[')]
        counts = Counter(error_types)
        for etype, count in counts.items():
            print(f"  {etype}: {count} occurrences")
        print(f"  See {LOG_FILE} for details and file paths.")
    else:
        print("  No errors or warnings logged.")

def set_resource_limits():
    """Set resource limits to maximize open files and RAM usage, but keep system stable."""
    try:
        # Set max open files (soft, hard)
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        max_files = min(hard, max(4096, (multiprocessing.cpu_count() or 1) * 4096))
        resource.setrlimit(resource.RLIMIT_NOFILE, (max_files, hard))
        print(f"[INFO] Set max open files to {max_files}")
    except Exception as e:
        print(f"[Warning] Could not set max open files: {e}", file=sys.stderr)
    try:
        # Set max address space (RAM) to 90% of system memory, but not unlimited
        total_mem = psutil.virtual_memory().total
        max_mem = int(total_mem * 0.9)
        resource.setrlimit(resource.RLIMIT_AS, (max_mem, resource.RLIM_INFINITY))
        print(f"[INFO] Set max RAM usage to {max_mem // (1024**2)} MB")
    except Exception as e:
        print(f"[Warning] Could not set max RAM usage: {e}", file=sys.stderr)

# --- Main Execution Block ---
if __name__ == '__main__':
    set_resource_limits()
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
