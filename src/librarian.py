import traceback
# --- Robust Timeout Helper ---
import multiprocessing
import signal
import time

def run_with_timeout(func, args=(), kwargs=None, timeout=120):
    """
    Run a function in a subprocess with a hard timeout. Returns (success, result or exception).
    If the function does not finish in time, forcibly terminates the process.
    """
    if kwargs is None:
        kwargs = {}
    def target(q, *args, **kwargs):
        try:
            res = func(*args, **kwargs)
            q.put((True, res))
        except Exception as e:
            q.put((False, e))
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=target, args=(q,)+args, kwargs=kwargs)
    p.start()
    p.join(timeout)
    if p.is_alive():
        p.terminate()
        p.join(5)
        return (False, TimeoutError(f"Timeout after {timeout}s"))
    if not q.empty():
        return q.get()
    return (False, RuntimeError("No result returned from subprocess"))
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
import traceback
import unicodedata
from src.config_loader import load_config
import requests  # For optional remote knowledge base
from src.isbn_enricher import enrich_file_with_isbn_metadata
import subprocess  # Ensure subprocess is always imported at the top
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
INSTALL_LOG_FILE = "librarian_install.log"

# --- In-memory error aggregation to reduce log spam ---
_logged_errors = set()
_error_counts = {}
def log_error(error_type, file_path, message, extra=None):
    key = (error_type, file_path, message)
    if key in _logged_errors:
        _error_counts[key] = _error_counts.get(key, 1) + 1
        return
    _logged_errors.add(key)
    _error_counts[key] = 1
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

def extract_text_with_pypdf2(file_path):
    try:
        import PyPDF2
        with open(file_path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            for page in reader.pages:
                text = page.extract_text()
                if text and text.strip():
                    return True
        return False
    except Exception as e:
        print(f"  [DEBUG] PyPDF2 extraction failed for {file_path}: {e}", file=sys.stderr)
        return False

def extract_text_with_pdfminer(file_path):
    try:
        from pdfminer.high_level import extract_text
        text = extract_text(file_path, maxpages=1)
        if text and text.strip():
            return True
        return False
    except Exception as e:
        print(f"  [DEBUG] pdfminer.six extraction failed for {file_path}: {e}", file=sys.stderr)
        return False

def get_pdf_details(file_path):
    is_valid, has_text = False, False
    pdf_version = None
    pdf_creator = None
    pdf_producer = None
    qpdf_checked = False
    qpdf_check_output = None
    warnings.filterwarnings("ignore", category=UserWarning)
    # --- MuPDF error aggregation ---
    global _logged_errors, _error_counts
    mupdf_error_seen = set()
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
    # --- Main text extraction with MuPDF, fallback to qpdf, then PyPDF2/pdfminer ---
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
                            if msg not in mupdf_error_seen:
                                print(f"  [Warning] {msg} for {file_path}", file=sys.stderr)
                                log_error("MUPDF_METHOD_ERROR", file_path, msg)
                                mupdf_error_seen.add(msg)
                    except Exception as e:
                        msg = f"Could not extract text from page: {e}"
                        if msg not in mupdf_error_seen:
                            print(f"  [Warning] {msg} in {file_path}", file=sys.stderr)
                            log_error("MUPDF_TEXT_EXTRACTION_ERROR", file_path, msg)
                            mupdf_error_seen.add(msg)
                    if text:
                        has_text = True
                        break
    except Exception as e:
        msg = f"Could not open PDF with PyMuPDF: {e}"
        if msg not in mupdf_error_seen:
            print(f"  [Warning] {msg} {file_path}", file=sys.stderr)
            log_error("MUPDF_OPEN_ERROR", file_path, msg)
            mupdf_error_seen.add(msg)
        # Fallback: try qpdf --check
        is_valid, qpdf_check_output = qpdf_check_pdf(file_path)
        qpdf_checked = True
        if is_valid:
            print(f"  [INFO] qpdf --check: PDF is valid according to qpdf: {file_path}")
        else:
            msg = f"qpdf --check: PDF is invalid. qpdf output: {qpdf_check_output}"
            if msg not in mupdf_error_seen:
                print(f"  [Warning] {msg} {file_path}", file=sys.stderr)
                log_error("QPDF_CHECK_ERROR", file_path, msg, extra=qpdf_check_output)
                mupdf_error_seen.add(msg)
        # Fallback: try PyPDF2/pdfminer.six for text extraction if MuPDF and qpdf fail
        if not has_text:
            print(f"  [DEBUG] Trying PyPDF2/pdfminer.six fallback for {file_path}", file=sys.stderr)
            has_text = extract_text_with_pypdf2(file_path)
            if not has_text:
                has_text = extract_text_with_pdfminer(file_path)
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
    """Attempt to repair a PDF using qpdf. Tries --repair, then --linearize, then PyMuPDF, then Ghostscript, then pdftocairo as fallback. Returns True if successful.
    Example Ghostscript command:
      gs -o repaired.pdf -sDEVICE=pdfwrite -dPDFSETTINGS=/prepress corrupted.pdf
    Example pdftocairo command:
      pdftocairo -pdf corrupted.pdf repaired.pdf
    """
    import shutil
    import subprocess
    # Remove output_path if it exists to avoid stale file issues
    if os.path.exists(output_path):
        try:
            os.remove(output_path)
        except Exception:
            pass
    # Helper for subprocess with timeout and error logging
    def run_subprocess(cmd, timeout=60, **kwargs):
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, timeout=timeout, **kwargs)
            return result
        except subprocess.TimeoutExpired as e:
            msg = f"Timeout running: {' '.join(cmd)}"
            print(f"  [Timeout] {msg}", file=sys.stderr)
            log_error("PDF_REPAIR_TIMEOUT", input_path, msg)
        except Exception as e:
            msg = f"Error running: {' '.join(cmd)}: {e}"
            print(f"  [Warning] {msg}", file=sys.stderr)
            log_error("PDF_REPAIR_ERROR", input_path, msg)
        return None
    # Try qpdf --repair
    result = run_subprocess(['qpdf', '--repair', input_path, output_path], timeout=60)
    if result and os.path.exists(output_path):
        print(f"  [INFO] PDF repaired with qpdf --repair: {input_path} -> {output_path}")
        log_error("PDF_REPAIR_QPDF", input_path, "Repaired with qpdf --repair", extra=result.stderr.decode(errors='ignore') if result.stderr else None)
        return True
    # Try qpdf --linearize as a fallback
    result = run_subprocess(['qpdf', '--linearize', input_path, output_path], timeout=60)
    if result and os.path.exists(output_path):
        print(f"  [INFO] PDF repaired with qpdf --linearize: {input_path} -> {output_path}")
        log_error("PDF_REPAIR_LINEARIZE", input_path, "Repaired with qpdf --linearize", extra=result.stderr.decode(errors='ignore') if result.stderr else None)
        return True
    # Try to extract pages with PyMuPDF as a last resort
    try:
        doc = fitz.open(input_path)
        if doc.page_count > 0:
            doc.save(output_path, garbage=4, deflate=True, clean=True)
            print(f"  [INFO] PDF re-saved with PyMuPDF: {input_path} -> {output_path}")
            log_error("PDF_REPAIR_PYMUPDF", input_path, "Re-saved with PyMuPDF")
            return True
    except Exception as e:
        msg = f"Could not re-save PDF with PyMuPDF: {e}"
        print(f"  [Warning] {msg} {input_path}", file=sys.stderr)
        log_error("PDF_REPAIR_ERROR", input_path, msg)
    # Try Ghostscript as a fallback (with aggressive repair options)
    gs_cmd = ['gs', '-o', output_path, '-sDEVICE=pdfwrite', '-dPDFSETTINGS=/prepress', '-dSAFER', '-dBATCH', '-dNOPAUSE', '-dQUIET', input_path]
    result = run_subprocess(gs_cmd, timeout=120)
    if result and os.path.exists(output_path):
        print(f"  [INFO] PDF repaired with Ghostscript: {input_path} -> {output_path}")
        log_error("PDF_REPAIR_GHOSTSCRIPT", input_path, "Repaired with Ghostscript", extra=result.stderr.decode(errors='ignore') if result.stderr else None)
        return True
    # Try pdftocairo as a final fallback (with -origpagesizes and -antialias)
    pdftocairo_cmd = ['pdftocairo', '-pdf', '-origpagesizes', '-antialias', 'gray', input_path, output_path]
    result = run_subprocess(pdftocairo_cmd, timeout=120)
    if result and os.path.exists(output_path):
        print(f"  [INFO] PDF repaired with pdftocairo: {input_path} -> {output_path}")
        log_error("PDF_REPAIR_PDFTOCAIRO", input_path, "Repaired with pdftocairo", extra=result.stderr.decode(errors='ignore') if result.stderr else None)
        return True
    # Try mutool clean as a last resort (if available)
    if shutil.which('mutool'):
        mutool_cmd = ['mutool', 'clean', '-gggg', input_path, output_path]
        result = run_subprocess(mutool_cmd, timeout=60)
        if result and os.path.exists(output_path):
            print(f"  [INFO] PDF repaired with mutool clean: {input_path} -> {output_path}")
            log_error("PDF_REPAIR_MUTOOL", input_path, "Repaired with mutool clean", extra=result.stderr.decode(errors='ignore') if result.stderr else None)
            return True
    # Try pdfcpu (if available)
    if shutil.which('pdfcpu'):
        pdfcpu_cmd = ['pdfcpu', 'validate', '-mode', 'relaxed', input_path]
        result = run_subprocess(pdfcpu_cmd, timeout=60)
        if result and result.returncode == 0:
            pdfcpu_cmd = ['pdfcpu', 'optimize', input_path, output_path]
            result2 = run_subprocess(pdfcpu_cmd, timeout=120)
            if result2 and os.path.exists(output_path):
                print(f"  [INFO] PDF repaired with pdfcpu: {input_path} -> {output_path}")
                log_error("PDF_REPAIR_PDFCPU", input_path, "Repaired with pdfcpu", extra=result2.stderr.decode(errors='ignore') if result2.stderr else None)
                return True
    return False

def ensure_system_tool_installed(tool_name, install_cmds):
    # Special handling for pdfcpu on Debian/Ubuntu (install from GitHub release if not present)
    import shutil
    install_log_file = "librarian_install.log"
    # --- Robust pdfcpu install: handle 404 and avoid repeated attempts ---
    if tool_name == 'pdfcpu' and shutil.which('pdfcpu') is None:
        import platform
        import tempfile
        import tarfile
        import urllib.request
        import urllib.error
        # subprocess is already imported at the top
        # Use a temp file as a flag to avoid repeated attempts in the same run
        pdfcpu_failed_flag = '/tmp/pdfcpu_install_failed.flag'
        if os.path.exists(pdfcpu_failed_flag):
            print("[INFO] Skipping pdfcpu install: previous attempt failed.", file=sys.stderr)
            with open(install_log_file, "a", encoding="utf-8") as ilog:
                ilog.write(f"[INFO] Skipping pdfcpu install: previous attempt failed.\n")
            return False
        try:
            distro = ''
            try:
                with open('/etc/os-release') as f:
                    for line in f:
                        if line.startswith('ID='):
                            distro = line.strip().split('=')[1].strip('"')
                            break
            except Exception:
                pass
            if 'debian' in distro or 'ubuntu' in distro:
                print("[INFO] Attempting to install pdfcpu from GitHub release for Debian/Ubuntu...", file=sys.stderr)
                with open(install_log_file, "a", encoding="utf-8") as ilog:
                    ilog.write(f"[INFO] Attempting to install pdfcpu from GitHub release for Debian/Ubuntu...\n")
                try:
                    # Get latest version
                    version_cmd = ["curl", "-s", "https://api.github.com/repos/pdfcpu/pdfcpu/releases/latest"]
                    version_out = subprocess.check_output(version_cmd).decode()
                    import re
                    m = re.search(r'"tag_name":\s*"v([0-9.]+)"', version_out)
                    if not m:
                        raise Exception("Could not determine latest pdfcpu version from GitHub API.")
                    pdfcpu_version = m.group(1)
                    archive_url = f"https://github.com/pdfcpu/pdfcpu/releases/download/v{pdfcpu_version}/pdfcpu_{pdfcpu_version}_Linux_x86_64.tar.xz"
                    # Download archive
                    archive_path = os.path.join(tempfile.gettempdir(), "pdfcpu.tar.xz")
                    wget_cmd = ["wget", "-qO", archive_path, archive_url]
                    ret = subprocess.run(wget_cmd)
                    if ret.returncode != 0 or not os.path.exists(archive_path):
                        raise Exception(f"Failed to download pdfcpu binary from {archive_url}")
                    # Extract
                    extract_dir = os.path.join(tempfile.gettempdir(), "pdfcpu-temp")
                    if os.path.exists(extract_dir):
                        shutil.rmtree(extract_dir)
                    os.makedirs(extract_dir, exist_ok=True)
                    tar_cmd = ["tar", "xf", archive_path, "--strip-components=1", "-C", extract_dir]
                    ret = subprocess.run(tar_cmd)
                    if ret.returncode != 0:
                        raise Exception("Failed to extract pdfcpu archive.")
                    pdfcpu_bin = os.path.join(extract_dir, "pdfcpu")
                    if not os.path.exists(pdfcpu_bin):
                        raise Exception("pdfcpu binary not found after extraction.")
                    # Move to /usr/local/bin
                    import getpass
                    print("[INFO] pdfcpu install requires sudo. You may be prompted for your password.")
                    sudo_pw = getpass.getpass(prompt='Enter your sudo password for pdfcpu install (leave blank to try without): ')
                    mv_cmd = ['sudo', '-S', 'mv', pdfcpu_bin, '/usr/local/bin/pdfcpu']
                    chmod_cmd = ['sudo', '-S', 'chmod', '+x', '/usr/local/bin/pdfcpu']
                    if sudo_pw:
                        mv_proc = subprocess.run(mv_cmd, input=(sudo_pw+'\n').encode(), check=True, timeout=30)
                        chmod_proc = subprocess.run(chmod_cmd, input=(sudo_pw+'\n').encode(), check=True, timeout=10)
                    else:
                        mv_proc = subprocess.run(mv_cmd, check=True, timeout=30)
                        chmod_proc = subprocess.run(chmod_cmd, check=True, timeout=10)
                    with open(install_log_file, "a", encoding="utf-8") as ilog:
                        ilog.write(f"[INFO] pdfcpu binary moved and permissions set.\n")
                    # Clean up
                    try:
                        os.remove(archive_path)
                        shutil.rmtree(extract_dir)
                    except Exception:
                        pass
                    if shutil.which('pdfcpu'):
                        with open(install_log_file, "a", encoding="utf-8") as ilog:
                            ilog.write(f"[INFO] pdfcpu installed successfully.\n")
                        print("[INFO] pdfcpu installed successfully.")
                        return True
                    else:
                        with open(install_log_file, "a", encoding="utf-8") as ilog:
                            ilog.write(f"[FATAL] pdfcpu binary was not found in /usr/local/bin after install.\n")
                        print("[FATAL] pdfcpu binary was not found in /usr/local/bin after install.", file=sys.stderr)
                except Exception as e:
                    msg = f"[FATAL] Could not auto-install pdfcpu: {e}"
                    print(msg, file=sys.stderr)
                    with open(install_log_file, "a", encoding="utf-8") as ilog:
                        ilog.write(msg + "\n")
                    # Try Go install if go is available
                    go_path = shutil.which('go')
                    if go_path:
                        try:
                            print("[INFO] Attempting to install pdfcpu using Go toolchain...", file=sys.stderr)
                            with open(install_log_file, "a", encoding="utf-8") as ilog:
                                ilog.write("[INFO] Attempting to install pdfcpu using Go toolchain...\n")
                            go_install_cmd = [go_path, 'install', 'github.com/pdfcpu/pdfcpu/cmd/pdfcpu@latest']
                            env = os.environ.copy()
                            # Ensure GOBIN is set to $HOME/go/bin if not already
                            gobin = env.get('GOBIN') or os.path.join(env.get('HOME', ''), 'go', 'bin')
                            env['GOBIN'] = gobin
                            ret = subprocess.run(go_install_cmd, env=env)
                            pdfcpu_bin = os.path.join(gobin, 'pdfcpu')
                            if os.path.exists(pdfcpu_bin):
                                import getpass
                                print("[INFO] pdfcpu (Go) install requires sudo to move binary. You may be prompted for your password.")
                                sudo_pw = getpass.getpass(prompt='Enter your sudo password for pdfcpu install (leave blank to try without): ')
                                mv_cmd = ['sudo', '-S', 'mv', pdfcpu_bin, '/usr/local/bin/pdfcpu']
                                chmod_cmd = ['sudo', '-S', 'chmod', '+x', '/usr/local/bin/pdfcpu']
                                if sudo_pw:
                                    mv_proc = subprocess.run(mv_cmd, input=(sudo_pw+'\n').encode(), check=True, timeout=30)
                                    chmod_proc = subprocess.run(chmod_cmd, input=(sudo_pw+'\n').encode(), check=True, timeout=10)
                                else:
                                    mv_proc = subprocess.run(mv_cmd, check=True, timeout=30)
                                    chmod_proc = subprocess.run(chmod_cmd, check=True, timeout=10)
                                with open(install_log_file, "a", encoding="utf-8") as ilog:
                                    ilog.write(f"[INFO] pdfcpu binary moved from Go build and permissions set.\n")
                                if shutil.which('pdfcpu'):
                                    with open(install_log_file, "a", encoding="utf-8") as ilog:
                                        ilog.write(f"[INFO] pdfcpu installed successfully via Go.\n")
                                    print("[INFO] pdfcpu installed successfully via Go.")
                                    return True
                                else:
                                    with open(install_log_file, "a", encoding="utf-8") as ilog:
                                        ilog.write(f"[FATAL] pdfcpu binary was not found in /usr/local/bin after Go install.\n")
                                    print("[FATAL] pdfcpu binary was not found in /usr/local/bin after Go install.", file=sys.stderr)
                        except Exception as go_e:
                            go_msg = f"[FATAL] Go install of pdfcpu failed: {go_e}"
                            print(go_msg, file=sys.stderr)
                            with open(install_log_file, "a", encoding="utf-8") as ilog:
                                ilog.write(go_msg + "\n")
                    # Write flag to avoid repeated attempts
                    with open(pdfcpu_failed_flag, "w") as flagf:
                        flagf.write("failed\n")
                    print("[SYSADMIN] pdfcpu could not be installed automatically. Please install it manually from https://github.com/pdfcpu/pdfcpu/releases or your package manager.", file=sys.stderr)
                    return False
        except Exception as e:
            msg = f"[FATAL] Could not auto-install pdfcpu: {e}"
            print(msg, file=sys.stderr)
            with open(install_log_file, "a", encoding="utf-8") as ilog:
                ilog.write(msg + "\n")
            # Write flag to avoid repeated attempts
            with open(pdfcpu_failed_flag, "w") as flagf:
                flagf.write("failed\n")
            print("[SYSADMIN] pdfcpu could not be installed automatically. Please install it manually from https://github.com/pdfcpu/pdfcpu/releases or your package manager.", file=sys.stderr)
            return False
    # Log all other system tool install attempts and errors
    if shutil.which(tool_name) is not None:
        with open(install_log_file, "a", encoding="utf-8") as ilog:
            ilog.write(f"[INFO] {tool_name} already installed.\n")
        return True
    print(f"[INFO] {tool_name} not found. Attempting to install...", file=sys.stderr)
    with open(install_log_file, "a", encoding="utf-8") as ilog:
        ilog.write(f"[INFO] {tool_name} not found. Attempting to install...\n")
    for cmd in install_cmds:
        try:
            subprocess.run(cmd, check=True)
            if shutil.which(tool_name) is not None:
                with open(install_log_file, "a", encoding="utf-8") as ilog:
                    ilog.write(f"[INFO] {tool_name} installed successfully.\n")
                print(f"[INFO] {tool_name} installed successfully.")
                return True
        except Exception as e:
            with open(install_log_file, "a", encoding="utf-8") as ilog:
                ilog.write(f"[FATAL] Could not install {tool_name} with command {cmd}: {e}\n")
            continue
    with open(install_log_file, "a", encoding="utf-8") as ilog:
        ilog.write(f"[FATAL] Could not install {tool_name} automatically. Please install it manually.\n")
    print(f"[FATAL] Could not install {tool_name} automatically. Please install it manually.", file=sys.stderr)
    return False


# Ensure qpdf is installed before any PDF repair attempts
def ensure_qpdf_installed():
    """Ensure qpdf is installed, try to install if missing."""
    import shutil
    import subprocess
    if shutil.which('qpdf') is not None:
        return True
    print("[INFO] qpdf not found. Attempting to install...", file=sys.stderr)
    install_cmds = [
        ['sudo', 'apt-get', 'install', '-y', 'qpdf'],
        ['sudo', 'dnf', 'install', '-y', 'qpdf'],
        ['sudo', 'yum', 'install', '-y', 'qpdf']
    ]
    for cmd in install_cmds:
        try:
            subprocess.run(cmd, check=True)
            if shutil.which('qpdf') is not None:
                print("[INFO] qpdf installed successfully.")
                return True
        except Exception:
            continue
    print("[FATAL] Could not install qpdf automatically. Please install it manually.", file=sys.stderr)
    return False

ensure_qpdf_installed()

# --- Parallel PDF validation and repair ---
def validate_and_repair_pdf(file_path):
    if os.path.basename(file_path).startswith("._"):
        print(f"  [Info] Skipping AppleDouble resource fork: {file_path}", file=sys.stderr)
        skipped_appledouble_files.append(file_path)
        return file_path, False, False, None, None, None
    # Run get_pdf_details with a hard timeout
    success, result = run_with_timeout(get_pdf_details, args=(file_path,), timeout=120)
    if not success or isinstance(result, (TimeoutError, RuntimeError, Exception)):
        print(f"  [Timeout] PDF validation timed out or failed: {file_path}", file=sys.stderr)
        log_error("PDF_VALIDATION_TIMEOUT", file_path, str(result))
        failed_pdf_repairs.append(file_path)
        return file_path, False, False, None, None, None
    is_valid, has_text, pdf_version, pdf_creator, pdf_producer = result
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
    # Run repair_pdf with a hard timeout
    success, repair_result = run_with_timeout(repair_pdf, args=(file_path, repaired_path), timeout=180)
    if not success:
        print(f"  [Timeout] PDF repair timed out: {file_path}", file=sys.stderr)
        log_error("PDF_REPAIR_TIMEOUT", file_path, str(repair_result))
        failed_pdf_repairs.append(file_path)
        return file_path, False, False, pdf_version, pdf_creator, pdf_producer
    if repair_result:
        # Validate the repaired file before accepting it
        success, repaired_details = run_with_timeout(get_pdf_details, args=(repaired_path,), timeout=60)
        if success and not isinstance(repaired_details, (TimeoutError, RuntimeError, Exception)):
            is_valid, has_text, pdf_version, pdf_creator, pdf_producer = repaired_details
            if is_valid:
                print(f"  [INFO] PDF repaired and validated: {file_path} -> {repaired_path}")
                return repaired_path, is_valid, has_text, pdf_version, pdf_creator, pdf_producer
        print(f"  [Warning] Repaired PDF is still invalid: {repaired_path}", file=sys.stderr)
        log_error("PDF_REPAIR_INVALID", repaired_path, "Repaired file is still invalid")
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

def build_library(config):
    import traceback
    # --- Step 1: Configuration & Setup ---
    SOURCE_PATHS, LIBRARY_ROOT = config['source_paths'], config['library_root']
    MIN_PDF_SIZE_BYTES = config['min_pdf_size_bytes']
    DB_FILE, KNOWLEDGE_DB_FILE = "library_index.sqlite", "knowledge.sqlite"
    # Optionally, get language mapping for future use
    KB_LANGS = config.get('knowledge_base_url_languages', {})
    
    print("--- Starting Library Build Process ---")
    print("[DEBUG] Entered build_library()")
    import psutil
    def print_resource_usage(phase):
        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss / (1024*1024)
        open_files = len(process.open_files()) if hasattr(process, 'open_files') else 'N/A'
        children = len(process.children(recursive=True))
        print(f"[RESOURCE] {phase}: RAM={mem:.1f}MB, OpenFiles={open_files}, Children={children}")
    print_resource_usage('Start')
    os.makedirs(LIBRARY_ROOT, exist_ok=True)
    classifier = Classifier(KNOWLEDGE_DB_FILE)
    isbn_cache = {}  # To avoid redundant ISBN queries

    # --- Step 2: File Scanning ---
    print("Step 1: Scanning all source directories for files...")
    print("[DEBUG] Starting file scan phase")
    print_resource_usage('Before Scan')
    all_files = list(scan_files(SOURCE_PATHS))
    if not all_files:
        print("No files found in source paths. Exiting.")
        classifier.close()
        return
    df = pd.DataFrame(all_files)
    print(f"Found {len(df)} total files.")
    # --- LIMIT FILES FOR TESTING ---
    # Remove file limit for production run
    # MAX_TEST_FILES = int(os.environ.get('LIBRARIAN_MAX_TEST_FILES', '1000'))
    # if len(df) > MAX_TEST_FILES:
    #     print(f"[DEBUG] Limiting to first {MAX_TEST_FILES} files for testing.")
    #     df = df.head(MAX_TEST_FILES)
    print(f"[DEBUG] DataFrame shape: {df.shape}")
    print("[DEBUG] Starting PDF validation/repair phase")
    print_resource_usage('Before PDF Validation')

    # --- Step 3: Analysis & Classification ---
    print("Step 2: Analyzing and Classifying files (this may take a while)...")
    print("[DEBUG] Starting classification/hashing phase")
    analysis_results = []
    pdf_paths = [row['path'] for row in all_files if row['name'].lower().endswith('.pdf')]
    pdf_validation = {}
    # Lower worker count to avoid resource exhaustion
    max_workers = min(8, (multiprocessing.cpu_count() or 1))
    # PDF validation/repair with progress
    if HAS_TQDM:
        pdf_iter = tqdm(pdf_paths, desc="Validating/Repairing PDFs", unit="pdf")
    else:
        print("Validating/Repairing PDFs...")
        pdf_iter = pdf_paths
    try:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(validate_and_repair_pdf, path): path for path in pdf_iter}
            for i, future in enumerate(as_completed(futures)):
                try:
                    path, is_valid, has_ocr, pdf_version, pdf_creator, pdf_producer = future.result(timeout=300)
                    pdf_validation[path] = (is_valid, has_ocr, pdf_version, pdf_creator, pdf_producer)
                except Exception as e:
                    print(f"[ERROR] PDF validation/repair failed or hung for a file: {e}", file=sys.stderr)
                if (i+1) % 100 == 0:
                    print_resource_usage(f'PDF Validation {i+1}')
                if not HAS_TQDM and (i+1) % 10 == 0:
                    print(f"  Processed {i+1}/{len(pdf_paths)} PDFs...")
    except Exception as e:
        print(f"[FATAL] Exception in PDF validation/repair phase: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    print_resource_usage('After PDF Validation')
    # --- Wait for all child processes to exit before next phase ---
    import gc, time
    process = psutil.Process(os.getpid())
    max_wait = 30
    waited = 0
    while True:
        children = process.children(recursive=True)
        if not children:
            break
        print(f"[DEBUG] Waiting for {len(children)} child processes to exit before classification... (waited {waited}s)")
        time.sleep(1)
        waited += 1
        if waited >= max_wait:
            print(f"[WARNING] Some child processes still running after {max_wait}s, proceeding anyway.", file=sys.stderr)
            break
    gc.collect()
    print("[DEBUG] All child processes from PDF validation/repair cleaned up. Proceeding to classification/hashing.")
    # Classification/analysis with progress
    knowledge_db_path = config.get('knowledge_base_db_url') or "knowledge.sqlite"
    print("[DEBUG] Preparing analyze_row_partial for classification/hashing phase")
    analyze_row_partial = partial(analyze_row, knowledge_db_path=knowledge_db_path, isbn_cache=isbn_cache, pdf_validation=pdf_validation)
    print("[DEBUG] analyze_row_partial created")
    print_resource_usage('Before Hashing/Classification')
    # --- Step 3b: Hashing/Classification Phase ---
    print("[DEBUG] Creating row_iter for classification/hashing phase")
    try:
        # Convert DataFrame to list of dicts for picklable row objects
        row_dicts = df.to_dict('records')
        # Ensure all rows have a 'path' key
        for r in row_dicts:
            if 'path' not in r:
                print(f"[FATAL] Row missing 'path' key: {r}", file=sys.stderr)
        if HAS_TQDM:
            row_iter = tqdm(row_dicts, desc="Classifying/Hashing", unit="file")
        else:
            print("Classifying/Hashing files...")
            row_iter = row_dicts
        print(f"[DEBUG] row_iter created, length: {len(row_iter)}")
    except Exception as e:
        print(f"[FATAL] Exception while creating row_iter: {e}", file=sys.stderr)
        traceback.print_exc()
        return
    # --- Hashing/analysis with robust error handling and progress ---
    analysis_results = []
    error_log_path = "hashing_analysis_errors.log"
    print("[DEBUG] Entering classification/hashing ProcessPoolExecutor")
    try:
        # Reduce max_workers for lower resource usage
        pool_workers = min(2, max_workers)
        print(f"[DEBUG] Using max_workers={pool_workers} for ProcessPoolExecutor")
        with ProcessPoolExecutor(max_workers=pool_workers) as executor:
            print("[DEBUG] ProcessPoolExecutor created for hashing/classification")
            future_to_row = {}
            for idx, row in enumerate(row_iter):
                if idx % 100 == 0:
                    print(f"[DEBUG] Submitting analyze_row_partial for row {idx} path={row.get('path', 'unknown')}")
                future = executor.submit(analyze_row_partial, row)
                future_to_row[future] = row
            print(f"[DEBUG] All {len(future_to_row)} futures submitted to pool")
            completed = 0
            total = len(future_to_row)
            for future in as_completed(future_to_row):
                row = future_to_row[future]
                try:
                    result = future.result(timeout=300)
                    analysis_results.append(result)
                    print(f"[DEBUG] analyze_row_partial completed for {row.get('path', 'unknown')}")
                except Exception as e:
                    with open(error_log_path, "a", encoding="utf-8") as elog:
                        elog.write(f"[ERROR] During hashing/analysis of file: {row.get('path', 'unknown')}\n  Exception: {e}\n")
                    print(f"[ERROR] Hashing/analysis failed or hung for {row.get('path', 'unknown')}: {e}", file=sys.stderr)
                completed += 1
                if (completed) % 100 == 0:
                    print_resource_usage(f'Hashing/Classification {completed}')
                    print(f"[DEBUG] {completed}/{total} files classified/hashed")
                if HAS_TQDM:
                    tqdm.write(f"[Progress] Hashing/analysis: {completed}/{total} ({(completed/total)*100:.1f}%)")
                elif completed % max(1, total//10) == 0:
                    print(f"[Progress] Hashing/analysis: {completed}/{total} ({(completed/total)*100:.1f}%)", file=sys.stderr)
    except Exception as e:
        print(f"[FATAL] Exception in classification/hashing phase: {e}", file=sys.stderr)
        traceback.print_exc()
    print_resource_usage('After Hashing/Classification')
    print(f"[DEBUG] Hashing/Classification phase complete. analysis_results length: {len(analysis_results)}")
    if os.path.exists(error_log_path):
        print(f"[SYSADMIN] See {error_log_path} for details on hashing/analysis errors.", file=sys.stderr)
        # Also append these errors to librarian_run.log for unified sysadmin review
        try:
            with open(error_log_path, "r", encoding="utf-8") as elog, open(LOG_FILE, "a", encoding="utf-8") as llog:
                llog.write("\n[SYSADMIN] Hashing/analysis errors (copied from hashing_analysis_errors.log):\n")
                for line in elog:
                    llog.write(line)
        except Exception as e:
            print(f"[SYSADMIN] Could not append hashing/analysis errors to {LOG_FILE}: {e}", file=sys.stderr)
    if not analysis_results:
        print("[FATAL] No analysis results produced. Skipping deduplication and copy/index phases.", file=sys.stderr)
        return
    df = pd.concat([df, pd.DataFrame(analysis_results)], axis=1).dropna(subset=['hash'])
def analyze_row(row, knowledge_db_path, isbn_cache, pdf_validation):
    """
    Analyze a file row: detect mime, hash, PDF validity, OCR, classify, and extract metadata.
    Returns a dict with analysis results for DataFrame concat.
    """
    import mimetypes
    from pathlib import Path
    # Defensive: row may be a namedtuple or dict
    path = getattr(row, 'path', None) or row.get('path')
    name = getattr(row, 'name', None) or row.get('name')
    size = getattr(row, 'size', None) or row.get('size')
    mime_type = None
    try:
        mime_type = magic_from_file(path)
    except Exception:
        mime_type = mimetypes.guess_type(path)[0] or 'application/octet-stream'
    # Hashing
    file_hash = get_file_hash(path)
    # PDF validation/repair results
    is_pdf_valid = None
    has_ocr = None
    pdf_version = None
    pdf_creator = None
    pdf_producer = None
    if name.lower().endswith('.pdf'):
        # Use precomputed validation if available
        pdf_info = pdf_validation.get(path)
        if pdf_info:
            is_pdf_valid, has_ocr, pdf_version, pdf_creator, pdf_producer = pdf_info
        else:
            # Defensive: run validation if not present
            _, is_pdf_valid, has_ocr, pdf_version, pdf_creator, pdf_producer = validate_and_repair_pdf(path)
    # Classification
    classifier = Classifier(knowledge_db_path)
    game_system, edition, category = classify_with_isbn_fallback(classifier, name, path, mime_type, isbn_cache)
    # Language detection (optional, placeholder)
    language = None
    # Clean up classifier connection
    try:
        classifier.close()
    except Exception:
        pass
    return {
        'mime_type': mime_type,
        'hash': file_hash,
        'is_pdf_valid': is_pdf_valid,
        'has_ocr': has_ocr,
        'pdf_version': pdf_version,
        'pdf_creator': pdf_creator,
        'pdf_producer': pdf_producer,
        'game_system': game_system,
        'edition': edition,
        'category': category,
        'language': language
    }
    print("[DEBUG] Creating row_iter for classification/hashing phase")
    try:
        if HAS_TQDM:
            row_iter = tqdm(list(df.itertuples(index=False)), desc="Classifying/Hashing", unit="file")
        else:
            print("Classifying/Hashing files...")
            row_iter = list(df.itertuples(index=False))
        print(f"[DEBUG] row_iter created, length: {len(row_iter)}")
    except Exception as e:
        print(f"[FATAL] Exception while creating row_iter: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return
    # --- Hashing/analysis with robust error handling and progress ---
    analysis_results = []
    error_log_path = "hashing_analysis_errors.log"
    print("[DEBUG] Entering classification/hashing ProcessPoolExecutor")
    try:
        # Reduce max_workers for lower resource usage
        pool_workers = min(2, max_workers)
        print(f"[DEBUG] Using max_workers={pool_workers} for ProcessPoolExecutor")
        with ProcessPoolExecutor(max_workers=pool_workers) as executor:
            print("[DEBUG] ProcessPoolExecutor created for hashing/classification")
            future_to_row = {}
            for idx, row in enumerate(row_iter):
                if idx % 100 == 0:
                    print(f"[DEBUG] Submitting analyze_row_partial for row {idx}")
                future = executor.submit(analyze_row_partial, row)
                future_to_row[future] = row
            print(f"[DEBUG] All {len(future_to_row)} futures submitted to pool")
            completed = 0
            total = len(future_to_row)
            for future in as_completed(future_to_row):
                row = future_to_row[future]
                try:
                    result = future.result(timeout=300)
                    analysis_results.append(result)
                    print(f"[DEBUG] analyze_row_partial completed for {getattr(row, 'path', 'unknown')}")
                except Exception as e:
                    with open(error_log_path, "a", encoding="utf-8") as elog:
                        elog.write(f"[ERROR] During hashing/analysis of file: {getattr(row, 'path', 'unknown')}\n  Exception: {e}\n")
                    print(f"[ERROR] Hashing/analysis failed or hung for {getattr(row, 'path', 'unknown')}: {e}", file=sys.stderr)
                completed += 1
                if (completed) % 100 == 0:
                    print_resource_usage(f'Hashing/Classification {completed}')
                    print(f"[DEBUG] {completed}/{total} files classified/hashed")
                if HAS_TQDM:
                    tqdm.write(f"[Progress] Hashing/analysis: {completed}/{total} ({(completed/total)*100:.1f}%)")
                elif completed % max(1, total//10) == 0:
                    print(f"[Progress] Hashing/analysis: {completed}/{total} ({(completed/total)*100:.1f}%)", file=sys.stderr)
    except Exception as e:
        print(f"[FATAL] Exception in classification/hashing phase: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    print_resource_usage('After Hashing/Classification')
    if os.path.exists(error_log_path):
        print(f"[SYSADMIN] See {error_log_path} for details on hashing/analysis errors.", file=sys.stderr)
        # Also append these errors to librarian_run.log for unified sysadmin review
        try:
            with open(error_log_path, "r", encoding="utf-8") as elog, open(LOG_FILE, "a", encoding="utf-8") as llog:
                llog.write("\n[SYSADMIN] Hashing/analysis errors (copied from hashing_analysis_errors.log):\n")
                for line in elog:
                    llog.write(line)
        except Exception as e:
            print(f"[SYSADMIN] Could not append hashing/analysis errors to {LOG_FILE}: {e}", file=sys.stderr)
    df = pd.concat([df, pd.DataFrame(analysis_results)], axis=1).dropna(subset=['hash'])
    # --- Step 4: Deduplication & Selection ---
    print("Step 3: Deduplicating based on content and selecting best files...")
    print("[DEBUG] Starting deduplication/selection phase")
    print_resource_usage('Before Deduplication')
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
    print("[DEBUG] Starting copy/index phase")
    print_resource_usage('Before Copy/Index')
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
    # Merge install log into run log for sysadmin review
    try:
        merge_install_log_into_run_log()
    except Exception as e:
        print(f"[SYSADMIN] Could not merge install log into run log: {e}", file=sys.stderr)

# --- Print log summary (aggregated, in-memory) ---
    print("\n--- Error/Warning Summary (Aggregated) ---")
    if _error_counts:
        from collections import defaultdict
        type_counts = defaultdict(int)
        for (etype, fpath, msg), count in _error_counts.items():
            type_counts[etype] += count
        for etype, count in type_counts.items():
            print(f"  {etype}: {count} occurrences")
        print(f"  See {LOG_FILE} for details and file paths.")
        # Optionally, print most frequent errors/files
        most_common = sorted(_error_counts.items(), key=lambda x: -x[1])[:5]
        if most_common:
            print("  Most frequent errors:")
            for (etype, fpath, msg), count in most_common:
                print(f"    [{etype}] {fpath} | {msg} (x{count})")
    else:
        print("  No errors or warnings logged.")

def merge_install_log_into_run_log():
    """Append the install log to the run log for unified sysadmin review, if not already present."""
    try:
        if not os.path.exists(INSTALL_LOG_FILE) or not os.path.exists(LOG_FILE):
            return
        with open(INSTALL_LOG_FILE, "r", encoding="utf-8") as ilog:
            install_lines = ilog.readlines()
        with open(LOG_FILE, "r", encoding="utf-8") as rlog:
            run_lines = rlog.readlines()
        # Only append lines not already present
        new_lines = [line for line in install_lines if line not in run_lines]
        if new_lines:
            with open(LOG_FILE, "a", encoding="utf-8") as rlog:
                rlog.writelines(["\n[INSTALL LOG MERGED]\n"] + new_lines)
    except Exception as e:
        print(f"[SYSADMIN] Error merging install log: {e}", file=sys.stderr)

if __name__ == "__main__":
    import traceback
    try:
        config_path = os.environ.get("LIBRARIAN_CONFIG", os.path.join(os.path.dirname(__file__), "../conf/config.ini"))
        print(f"[INFO] Loading config from: {config_path}")
        config = load_config()
        build_library(config)
    except Exception as e:
        print(f"[FATAL] Unhandled exception in librarian.py: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
