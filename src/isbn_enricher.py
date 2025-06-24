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
                    text += page.get_text()
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
