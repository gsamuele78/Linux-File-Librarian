import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import os
import time
import sys
from collections import defaultdict
from src.config_loader import load_config

# --- Configuration ---
DB_FILE = "knowledge.sqlite"
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'}

def init_db():
    """Initializes a fresh database, deleting any existing one to ensure a clean build."""
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Add a 'language' column to track the source and add a UNIQUE constraint to prevent duplicates.
    cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY, product_code TEXT, title TEXT NOT NULL,
            game_system TEXT NOT NULL, edition TEXT, category TEXT, language TEXT, source_url TEXT,
            UNIQUE(product_code, title, game_system, edition, language)
        )
    ''')
    conn.commit()
    return conn

def safe_request(url):
    """Makes a web request and handles potential network errors gracefully."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        return BeautifulSoup(response.content, 'lxml')
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Could not fetch {url}. Reason: {e}", file=sys.stderr)
        return None

# --- Specialized Parsers (Rewritten for Robustness) ---

def parse_tsr_archive(conn, url, lang):
    """(FIXED) Parser for tsrarchive.com. Now correctly avoids navigation links."""
    print(f"\n[+] Parsing tsrarchive.com from {url}...")
    base_url = "http://www.tsrarchive.com/"
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    # FIX: The selector now targets links within table cells (<td>) to avoid nav bars.
    for link in soup.select('td a[href*=".html"]'):
        sub_page_url = base_url + link['href']
        game_system = link.get_text(strip=True)
        # FIX: More robust filtering of irrelevant links like "Back to..."
        if not game_system or "Back to" in game_system or "Home" in game_system: continue
        print(f"  -> Scraping system: {game_system}")
        sub_soup = safe_request(sub_page_url)
        if not sub_soup: continue
        for item in sub_soup.select('b a[href*=".html"]'):
            title = item.get_text(strip=True)
            code_match = re.search(r'\((TSR\s?\d{4,5})\)', item.parent.get_text())
            if title and code_match:
                code = code_match.group(1).replace(" ", "")
                cursor.execute("INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)", (code, title, game_system, "1e/2e", "Module/Adventure", lang, sub_page_url))
                total_added += cursor.rowcount
    conn.commit()
    time.sleep(1) # Be a good internet citizen.
    print(f"[SUCCESS] tsrarchive.com parsing complete. Added {total_added} unique entries.")

def parse_wikipedia_generic(conn, url, system, category, lang, description):
    """(FIXED & GENERALIZED) A more robust parser for Wikipedia tables that works for multiple languages."""
    print(f"\n[+] Parsing Wikipedia: {description}...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    # Strategy: Find all valid tables and extract data based on header names.
    for table in soup.find_all('table', class_='wikitable'):
        headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
        try:
            # FIX: More flexible header finding for both English and Italian pages.
            title_idx = headers.index('title') if 'title' in headers else headers.index('titolo')
            code_idx = headers.index('code') if 'code' in headers else headers.index('codice') if 'codice' in headers else -1
            edition_idx = headers.index('edition') if 'edition' in headers else headers.index('edizione') if 'edizione' in headers else -1
        except ValueError:
            continue # Skip tables that don't have the columns we need.
        
        for row in table.find_all('tr')[1:]:
            cols = row.find_all(['td', 'th']) # Some rows use <th> for the title.
            if len(cols) > title_idx:
                title = cols[title_idx].get_text(strip=True)
                code = cols[code_idx].get_text(strip=True) if code_idx != -1 and len(cols) > code_idx else None
                edition = cols[edition_idx].get_text(strip=True) if edition_idx != -1 and len(cols) > edition_idx else "N/A"
                if title and "List of" not in title:
                    cursor.execute("INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)", (code, title, system, edition, category, lang, url))
                    total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] Wikipedia ({description}) parsing complete. Added {total_added} unique entries.")

def parse_dndwiki_35e(conn, url, lang):
    """(FIXED) Parser for dnd-wiki.org's 3.5e Adventures list."""
    print(f"\n[+] Parsing dnd-wiki.org for 3.5e Adventures...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    # FIX: Selector is now more specific to target only the main content lists within the body.
    content_div = soup.find('div', id='mw-content-text')
    if not content_div: return
    for li in content_div.select('ul > li'):
        # Get the first link in the list item, which is usually the title.
        title_tag = li.find('a')
        if title_tag:
            title = title_tag.get_text(strip=True)
            # FIX: Added filter to avoid non-adventure and homebrew links.
            if title and len(title) > 2 and "Homebrew" not in li.get_text() and "Category:" not in title:
                cursor.execute("INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)", (None, title, "D&D", "3.5e", "Adventure", lang, url))
                total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] dnd-wiki.org parsing complete. Added {total_added} unique entries.")

def parse_drivethrurpg(conn, url, system, lang):
    """(UPDATED) This parser now gracefully handles the expected 403 error from DriveThruRPG."""
    print(f"\n[+] Parsing DriveThruRPG {system} products from {url}...")
    print("  [INFO] DriveThruRPG actively blocks automated scripts (HTTP 403 Error).")
    print("  [INFO] This parser will likely fail, which is expected. Bypassing this requires advanced techniques not suitable for this project.")
    soup = safe_request(url)
    if not soup:
        print(f"[SKIPPED] Could not access DriveThruRPG for {system}, as expected.")
        return
    # If by some miracle it works, the logic would go here. For now, it just reports the failure.
    print("[SUCCESS] DriveThruRPG parsing step finished (likely with an error, which is expected).")

# --- Main Execution Block ---
PARSER_MAPPING = {
    # Key from config.ini: (function_to_call, language_of_content)
    "tsr_archive": (parse_tsr_archive, "English"),
    "wiki_dnd_modules": (lambda c, u, l: parse_wikipedia_generic(c, u, "D&D", "Module", l, "D&D Modules"), "English"),
    "wiki_dnd_adventures": (lambda c, u, l: parse_wikipedia_generic(c, u, "D&D", "Adventure", l, "D&D Adventures"), "English"),
    "dndwiki_35e": (parse_dndwiki_35e, "English"),
    "wiki_pathfinder": (lambda c, u, l: parse_wikipedia_generic(c, u, "Pathfinder", "Book", l, "Pathfinder Books"), "English"),
    "italian_dnd_wiki": (lambda c, u, l: parse_wikipedia_generic(c, u, "D&D", "Modulo", l, "D&D Italian"), "Italian"),
    "italian_pathfinder_wiki": (lambda c, u, l: parse_wikipedia_generic(c, u, "Pathfinder", "Manuale", l, "Pathfinder Italian"), "Italian"),
    "drivethrurpg_dnd": (lambda c, u, l: parse_drivethrurpg(c, u, "D&D", l), "English"),
    "drivethrurpg_pathfinder": (lambda c, u, l: parse_drivethrurpg(c, u, "Pathfinder", l), "English")
}

if __name__ == "__main__":
    print("--- Building Enhanced Knowledge Base from Online Sources ---")
    try:
        config = load_config()
        urls_to_scrape = config['knowledge_base_urls']
    except Exception as e:
        print(f"[FATAL] Could not load configuration. Error: {e}", file=sys.stderr)
        sys.exit(1)
        
    connection = init_db()
    for key, url in urls_to_scrape.items():
        print(f"\n[INFO] Processing: {key}")
        if key in PARSER_MAPPING:
            parser_func, lang = PARSER_MAPPING[key]
            try:
                parser_func(connection, url, lang)
            except Exception as e:
                 print(f"  [CRITICAL] Parser '{key}' failed unexpectedly: {e}", file=sys.stderr)
        else:
            print(f"  [WARNING] No parser available for config key '{key}'. Skipping.", file=sys.stderr)
    
    # --- Final Statistics Report (Enhanced) ---
    print("\n--- Knowledge Base Statistics ---")
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    print(f"Total unique products: {cursor.fetchone()[0]}")

    # By Game System
    print("\nBy Game System:")
    stats_system = defaultdict(int)
    cursor.execute("SELECT game_system, COUNT(*) FROM products GROUP BY game_system")
    for system, count in cursor.fetchall():
        stats_system[system] = count
    for system, count in sorted(stats_system.items()): print(f"  {system}: {count}")

    # By Language
    print("\nBy Language:")
    stats_lang = defaultdict(int)
    cursor.execute("SELECT language, COUNT(*) FROM products GROUP BY language")
    for lang, count in cursor.fetchall():
        stats_lang[lang] = count
    for lang, count in sorted(stats_lang.items()): print(f"  {lang}: {count}")

    # D&D by Edition
    print("\nD&D by Edition:")
    stats_dnd = defaultdict(int)
    cursor.execute("SELECT edition, COUNT(*) FROM products WHERE game_system = 'D&D' GROUP BY edition")
    for edition, count in cursor.fetchall():
        stats_dnd[edition] = count
    for edition, count in sorted(stats_dnd.items()): print(f"  {edition or 'N/A'}: {count}")
        
    connection.close()
    print("\n--- Knowledge Base Build Complete! ---")
    print(f"Enhanced database saved to '{DB_FILE}'")
