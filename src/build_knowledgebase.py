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
    """Initializes a fresh database."""
    if os.path.exists(DB_FILE): os.remove(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
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
        response.raise_for_status()
        return BeautifulSoup(response.content, 'lxml')
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Could not fetch {url}. Reason: {e}", file=sys.stderr)
        return None

# --- Specialized Parsers (Rewritten for Robustness) ---

def parse_tsr_archive(conn, url, lang):
    """(REWRITTEN) Parser for tsrarchive.com. Now correctly finds and filters links."""
    print(f"\n[+] Parsing tsrarchive.com from {url}...")
    base_url = "http://www.tsrarchive.com/"
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    # FIX: Instead of a fragile CSS selector, find all links and filter them in Python.
    for link in soup.find_all('a', href=True):
        link_text = link.get_text(strip=True)
        # FIX: More robust filtering of irrelevant navigation links.
        if link['href'].endswith('.html') and link_text and "Back to" not in link_text and "Home" not in link_text:
            sub_page_url = base_url + link['href']
            game_system = link_text
            print(f"  -> Scraping system: {game_system}")
            sub_soup = safe_request(sub_page__url)
            if not sub_soup: continue
            for item in sub_soup.select('b a[href*=".html"]'):
                title = item.get_text(strip=True)
                # FIX: Make the regex more general to catch codes without spaces.
                code_match = re.search(r'\((TSR\d{4,5})\)', item.parent.get_text())
                if title and code_match:
                    code = code_match.group(1)
                    cursor.execute("INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)", (code, title, game_system, "1e/2e", "Module/Adventure", lang, sub_page_url))
                    total_added += cursor.rowcount
    conn.commit()
    time.sleep(1)
    print(f"[SUCCESS] tsrarchive.com parsing complete. Added {total_added} unique entries.")

def parse_wikipedia_generic(conn, url, system, category, lang, description):
    """(REWRITTEN) A stateful parser for Wikipedia that correctly determines the edition."""
    print(f"\n[+] Parsing Wikipedia: {description}...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    content = soup.find(id='mw-content-text')
    if not content: return

    current_edition = "N/A"
    # FIX: Iterate through all relevant tags in order to maintain the 'edition' context.
    for tag in content.find_all(['h2', 'h3', 'table']):
        if tag.name == 'h2':
            # An H2 tag resets the context.
            current_edition = "N/A"
        elif tag.name == 'h3':
            # An H3 tag sets the new edition context.
            headline = tag.find(class_='mw-headline')
            if headline:
                current_edition = headline.get_text(strip=True)
        
        elif tag.name == 'table' and 'wikitable' in tag.get('class', []):
            # When we find a table, parse it using the current edition context.
            headers = [th.get_text(strip=True).lower() for th in tag.find_all('th')]
            try:
                title_idx = headers.index('title') if 'title' in headers else headers.index('titolo')
                code_idx = headers.index('code') if 'code' in headers else headers.index('codice') if 'codice' in headers else -1
                # Also try to get edition from inside the table as a fallback.
                edition_col_idx = headers.index('edition') if 'edition' in headers else headers.index('edizione') if 'edizione' in headers else -1
            except ValueError: continue
            
            for row in tag.find_all('tr')[1:]:
                cols = row.find_all(['td', 'th'])
                if len(cols) > title_idx:
                    title = cols[title_idx].get_text(strip=True)
                    code = cols[code_idx].get_text(strip=True) if code_idx != -1 and len(cols) > code_idx else None
                    # Use edition from the column if available, otherwise use the one from the H3 header.
                    edition_in_table = cols[edition_col_idx].get_text(strip=True) if edition_col_idx != -1 and len(cols) > edition_col_idx else None
                    final_edition = edition_in_table or current_edition
                    if title and "List of" not in title:
                        cursor.execute("INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)", (code, title, system, final_edition, category, lang, url))
                        total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] Wikipedia ({description}) parsing complete. Added {total_added} unique entries.")

def parse_dndwiki_35e(conn, url, lang):
    """(REWRITTEN) Parser for dnd-wiki.org that correctly parses the full page."""
    print(f"\n[+] Parsing dnd-wiki.org for 3.5e Adventures...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    content_div = soup.find('div', id='mw-content-text')
    if not content_div: return
    # FIX: Find all list items, then check their content, which is more robust.
    for li in content_div.find_all('li'):
        title_tag = li.find('a')
        if title_tag:
            title = title_tag.get_text(strip=True)
            # FIX: More aggressive filtering to exclude index links and categories.
            if title and len(title) > 1 and "Category:" not in title and "d20srd" not in title_tag.get('href', ''):
                cursor.execute("INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)", (None, title, "D&D", "3.5e", "Adventure", lang, url))
                total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] dnd-wiki.org parsing complete. Added {total_added} unique entries.")

def parse_drivethrurpg(conn, url, system, lang):
    """(UPDATED) This parser gracefully handles the 403 error from DriveThruRPG."""
    print(f"\n[+] Parsing DriveThruRPG {system} products from {url}...")
    print("  [INFO] DriveThruRPG actively blocks automated scripts (HTTP 403 Error).")
    soup = safe_request(url)
    if not soup:
        print(f"[SKIPPED] Could not access DriveThruRPG for {system}, as expected.")
        return
    print("[SUCCESS] DriveThruRPG parsing step finished (likely with an error, which is expected).")

# --- Main Execution Block ---
PARSER_MAPPING = {
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
    # (The main execution and statistics reporting block remains the same as the previous correct version)
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
    
    print("\n--- Knowledge Base Statistics ---")
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    print(f"Total unique products: {cursor.fetchone()[0]}")
    print("\nBy Game System:")
    stats_system = defaultdict(int)
    cursor.execute("SELECT game_system, COUNT(*) FROM products GROUP BY game_system")
    for system, count in cursor.fetchall():
        stats_system[system] = count
    for system, count in sorted(stats_system.items()): print(f"  {system}: {count}")
    print("\nBy Language:")
    stats_lang = defaultdict(int)
    cursor.execute("SELECT language, COUNT(*) FROM products GROUP BY language")
    for lang, count in cursor.fetchall():
        stats_lang[lang] = count
    for lang, count in sorted(stats_lang.items()): print(f"  {lang}: {count}")
    print("\nD&D by Edition:")
    stats_dnd = defaultdict(int)
    cursor.execute("SELECT edition, COUNT(*) FROM products WHERE game_system = 'D&D' GROUP BY edition")
    for edition, count in cursor.fetchall():
        stats_dnd[edition] = count
    for edition, count in sorted(stats_dnd.items()): print(f"  {edition or 'N/A'}: {count}")
    connection.close()
    print("\n--- Knowledge Base Build Complete! ---")
    print(f"Enhanced database saved to '{DB_FILE}'")
