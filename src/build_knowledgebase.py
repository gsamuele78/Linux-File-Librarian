import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import os
import time
import sys
from src.config_loader import load_config

# --- Configuration ---
DB_FILE = "knowledge.sqlite"
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'}

def init_db():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY, product_code TEXT, title TEXT NOT NULL,
            game_system TEXT NOT NULL, edition TEXT, category TEXT, source_url TEXT,
            UNIQUE(product_code, title, game_system, edition)
        )
    ''')
    conn.commit()
    return conn

def safe_request(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'lxml')
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Could not fetch {url}. Reason: {e}", file=sys.stderr)
        return None

# --- Specialized Parsers ---
def parse_tsr_archive(conn, url):
    print(f"\n[+] Parsing tsrarchive.com from {url}...")
    base_url = "http://www.tsrarchive.com/"
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    for link in soup.select('a[href*=".html"]'):
        sub_page_url = base_url + link['href']
        game_system = link.get_text(strip=True)
        if not game_system or game_system == "Home": continue
        print(f"  -> Scraping system: {game_system}")
        sub_soup = safe_request(sub_page_url)
        if not sub_soup: continue
        for item in sub_soup.select('b a[href*=".html"]'):
            title = item.get_text(strip=True)
            code_match = re.search(r'\((TSR\s?\d{4,5})\)', item.parent.get_text())
            if title and code_match:
                code = code_match.group(1).replace(" ", "")
                cursor.execute( "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, source_url) VALUES (?, ?, ?, ?, ?, ?)", (code, title, game_system, "1e/2e", "Module/Adventure", sub_page_url))
                total_added += cursor.rowcount
        conn.commit()
        time.sleep(1)
    print(f"[SUCCESS] tsrarchive.com parsing complete. Added {total_added} entries.")

def parse_wikipedia_dnd(conn, url, description):
    print(f"\n[+] Parsing Wikipedia: {description}...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    for header in soup.find_all('h2'):
        edition_text = header.find(class_='mw-headline')
        if not edition_text: continue
        edition = edition_text.get_text(strip=True)
        for table in header.find_next_siblings('table', class_='wikitable'):
            if table.find_previous_sibling('h2') != header: break
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            try:
                title_idx, code_idx = headers.index('title'), headers.index('code')
            except ValueError: continue
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) > max(title_idx, code_idx):
                    title, code = cols[title_idx].get_text(strip=True), cols[code_idx].get_text(strip=True)
                    if title:
                        cursor.execute("INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, source_url) VALUES (?, ?, ?, ?, ?, ?)",(code or None, title, "D&D", edition, "Module/Adventure", url))
                        total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] Wikipedia ({description}) parsing complete. Added {total_added} entries.")

def parse_dndwiki_35e(conn, url):
    print(f"\n[+] Parsing dnd-wiki.org for 3.5e Adventures...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    content = soup.find(id='bodyContent')
    if not content: return
    for li in content.find_all('li'):
        if li.find(class_='tocnumber'): continue
        title = li.get_text(strip=True).split('(')[0].strip()
        if title and len(title) > 2:
             cursor.execute("INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, source_url) VALUES (?, ?, ?, ?, ?, ?)",(None, title, "D&D", "3.5e", "Adventure", url))
             total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] dnd-wiki.org parsing complete. Added {total_added} entries.")

def parse_wikipedia_pathfinder(conn, url):
    print(f"\n[+] Parsing Wikipedia for Pathfinder books...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    for header in soup.find_all('h2'):
        edition_text = header.find(class_='mw-headline')
        if not edition_text: continue
        edition_str = edition_text.get_text(strip=True)
        edition = '1e' if 'First edition' in edition_str else '2e' if 'Second edition' in edition_str else None
        if not edition: continue
        for table in header.find_next_siblings('table', class_='wikitable'):
            if table.find_previous_sibling('h2') != header: break
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            try:
                title_idx = headers.index('title')
                code_idx = headers.index('product code') if 'product code' in headers else headers.index('isbn')
            except ValueError: continue
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) > max(title_idx, code_idx):
                    title, code = cols[title_idx].get_text(strip=True), cols[code_idx].get_text(strip=True)
                    if title:
                        cursor.execute("INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, source_url) VALUES (?, ?, ?, ?, ?, ?)",(code, title, "Pathfinder", edition, "Book", url))
                        total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] Wikipedia (Pathfinder) parsing complete. Added {total_added} entries.")

# --- Main Execution Block ---
PARSER_MAPPING = {
    "tsr_archive": parse_tsr_archive,
    "wiki_dnd_modules": lambda c, u: parse_wikipedia_dnd(c, u, "D&D Modules"),
    "wiki_dnd_adventures": lambda c, u: parse_wikipedia_dnd(c, u, "D&D Adventures"),
    "dndwiki_35e": parse_dndwiki_35e,
    "wiki_pathfinder": parse_wikipedia_pathfinder
}

if __name__ == "__main__":
    print("--- Building Knowledge Base from Online Sources ---")
    try:
        config = load_config()
        urls_to_scrape = config['knowledge_base_urls']
    except Exception as e:
        print(f"[FATAL] Could not load configuration. Error: {e}", file=sys.stderr)
        sys.exit(1)
        
    connection = init_db()
    for key, url in urls_to_scrape.items():
        if key in PARSER_MAPPING:
            try:
                PARSER_MAPPING[key](connection, url)
            except Exception as e:
                 print(f"  [CRITICAL] Parser '{key}' failed unexpectedly: {e}", file=sys.stderr)
        else:
            print(f"  [WARNING] No parser available for config key '{key}'. Skipping.", file=sys.stderr)
    
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    final_count = cursor.fetchone()[0]
    connection.close()
    
    print("\n--- Knowledge Base Build Complete! ---")
    print(f"Database saved to '{DB_FILE}' with a total of {final_count} unique product entries.")
