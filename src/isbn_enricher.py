# --- Dependency Checks with auto-install for all required modules ---
REQUIRED_MODULES = [
    ("fitz", "pymupdf"),
    ("requests", "requests"),
]
import sys
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

import re
import requests
import sys
import os
from pathlib import Path

ISBN_REGEX = re.compile(r'\b(?:ISBN(?:-1[03])?:?\s*)?((?:97[89][- ]?)?\d{1,5}[- ]?\d{1,7}[- ]?\d{1,7}[- ]?[\dXx])\b')

def extract_isbns_from_text(text):
    """Extracts all ISBNs from a given text."""
    return [isbn.replace('-', '').replace(' ', '') for isbn in ISBN_REGEX.findall(text)]

def extract_isbns_from_file(filepath):
    """Extracts ISBNs from a file (tries text extraction for PDFs and plain text files)."""
    try:
        if filepath.lower().endswith('.pdf'):
            import fitz
            with fitz.open(filepath) as doc:
                text = ""
                for page in doc:
                    page_text = ''
                    get_text_fn = getattr(page, 'get_text', None)
                    getText_fn = getattr(page, 'getText', None)
                    try:
                        if callable(get_text_fn):
                            page_text = get_text_fn("text")
                        elif callable(getText_fn):
                            page_text = getText_fn("text")
                        else:
                            print(f"[WARN] PyMuPDF page object has no get_text or getText method for {filepath}", file=sys.stderr)
                    except Exception as e:
                        print(f"[WARN] Could not extract text from page in {filepath}: {e}", file=sys.stderr)
                        page_text = ''
                    if isinstance(page_text, str):
                        text += page_text
                    elif page_text is not None:
                        text += str(page_text)
                return extract_isbns_from_text(text)
        else:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
                return extract_isbns_from_text(text)
    except Exception as e:
        print(f"[WARN] Could not extract ISBN from {filepath}: {e}", file=sys.stderr)
        return []

def query_openlibrary(isbn):
    """Queries Open Library for ISBN metadata."""
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get(f"ISBN:{isbn}", {})
    except Exception as e:
        print(f"[WARN] Could not query Open Library for ISBN {isbn}: {e}", file=sys.stderr)
        return {}

def enrich_file_with_isbn_metadata(filepath):
    """Extracts ISBNs from file and queries Open Library for metadata."""
    isbns = extract_isbns_from_file(filepath)
    results = []
    for isbn in isbns:
        meta = query_openlibrary(isbn)
        if meta:
            results.append({'isbn': isbn, 'metadata': meta})
    return results

def main_scan_folder(folder):
    """Scans a folder for files, extracts ISBNs, and prints metadata for each file."""
    for p in Path(folder).rglob('*'):
        if p.is_file():
            enriched = enrich_file_with_isbn_metadata(str(p))
            if enriched:
                print(f"\n[INFO] File: {p}")
                for entry in enriched:
                    print(f"  ISBN: {entry['isbn']}")
                    meta = entry['metadata']
                    title = meta.get('title', 'N/A')
                    authors = ', '.join(a['name'] for a in meta.get('authors', [])) if meta.get('authors') else 'N/A'
                    publishers = ', '.join(p['name'] for p in meta.get('publishers', [])) if meta.get('publishers') else 'N/A'
                    print(f"    Title: {title}")
                    print(f"    Authors: {authors}")
                    print(f"    Publishers: {publishers}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python isbn_enricher.py <folder_or_file>")
        sys.exit(1)
    target = sys.argv[1]
    if os.path.isdir(target):
        main_scan_folder(target)
    elif os.path.isfile(target):
        enriched = enrich_file_with_isbn_metadata(target)
        if enriched:
            print(f"\n[INFO] File: {target}")
            for entry in enriched:
                print(f"  ISBN: {entry['isbn']}")
                meta = entry['metadata']
                title = meta.get('title', 'N/A')
                authors = ', '.join(a['name'] for a in meta.get('authors', [])) if meta.get('authors') else 'N/A'
                publishers = ', '.join(p['name'] for p in meta.get('publishers', [])) if meta.get('publishers') else 'N/A'
                print(f"    Title: {title}")
                print(f"    Authors: {authors}")
                print(f"    Publishers: {publishers}")
        else:
            print(f"No ISBN metadata found for {target}")
    else:
        print("Target is not a valid file or directory.")
