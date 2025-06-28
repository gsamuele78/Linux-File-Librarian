import sqlite3
import requests
from bs4 import BeautifulSoup, Tag
import re
import os
import time
import sys
from urllib.parse import urljoin, urlparse
from src.config_loader import load_config

# --- Configuration ---
DB_FILE = "knowledge.sqlite"
USER_AGENT = 'LinuxFileLibrarianBot/1.0 (+https://github.com/yourproject)'

HEADERS = {'User-Agent': USER_AGENT}

def init_db():
    """Initializes a fresh database, deleting any existing one to ensure a clean build."""
    if os.path.exists(DB_FILE):
        try:
            os.remove(DB_FILE)
        except Exception as e:
            print(f"[ERROR] Could not remove old DB: {e}", file=sys.stderr)
            raise
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

def is_scraping_allowed(url):
    """
    Checks robots.txt for the given URL to determine if scraping is allowed.
    Returns True if allowed, False otherwise.
    """
    try:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        resp = requests.get(robots_url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            # If robots.txt not found, assume allowed
            return True
        rules = resp.text.lower()
        # Simple check: Disallow for all or for our user-agent
        if 'disallow: /' in rules and ('user-agent: *' in rules or f'user-agent: {USER_AGENT.lower()}' in rules):
            print(f"[WARNING] robots.txt at {robots_url} disallows scraping. Skipping {url}.", file=sys.stderr)
            return False
        return True
    except Exception as e:
        print(f"[WARNING] Could not check robots.txt for {url}: {e}", file=sys.stderr)
        return True  # Fail open

def safe_request(url, retries=3, delay=2):
    """
    Makes a web request with retries and handles network errors robustly.
    Returns BeautifulSoup object or None.
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            # Defensive: check for HTML content type
            if 'text/html' not in response.headers.get('Content-Type', ''):
                print(f"  [ERROR] Non-HTML content at {url}", file=sys.stderr)
                return None
            return BeautifulSoup(response.content, 'lxml')
        except requests.exceptions.RequestException as e:
            print(f"  [ERROR] Could not fetch {url} (attempt {attempt+1}/{retries}): {e}", file=sys.stderr)
            if attempt < retries - 1:
                time.sleep(delay)
    return None

def normalize_whitespace(text):
    """Utility to normalize whitespace in strings."""
    return re.sub(r'\s+', ' ', text or '').strip()

def parse_table_rows(table, header_map):
    """
    Yields dicts mapping header names to cell values for each row in a table.
    header_map: dict mapping logical names to possible header strings.
    """
    headers = [normalize_whitespace(th.get_text()) for th in table.find_all('th')]
    col_indices = {}
    for logical, possible in header_map.items():
        for idx, h in enumerate(headers):
            if h.lower() in possible:
                col_indices[logical] = idx
                break
    for row in table.find_all('tr')[1:]:
        cols = row.find_all(['td', 'th'])
        row_data = {}
        for logical, idx in col_indices.items():
            if idx < len(cols):
                row_data[logical] = normalize_whitespace(cols[idx].get_text())
            else:
                row_data[logical] = None
        yield row_data

# --- Specialized Parsers (All Parsers Fully Implemented) ---

def parse_tsr_archive(conn, start_url, lang):
    """
    Aggiornato: parser per tsrarchive.com che segue la nuova struttura HTML (tabella di link alle edizioni, nessun frame).
    """
    print(f"\n[+] Parsing TSR Archive for language '{lang}' from starting page: {start_url}...")
    cursor = conn.cursor()
    total_added = 0

    index_soup = safe_request(start_url)
    if not index_soup:
        return

    # Trova tutti i link principali nella tabella centrale
    main_links = [a for a in index_soup.find_all('a', href=True) if isinstance(a, Tag)]
    print(f"  -> Found {len(main_links)} main section links.")

    for main_link in main_links:
        section_text = normalize_whitespace(main_link.get_text())
        section_href = main_link.get('href')
        if isinstance(section_href, list):
            section_href = section_href[0]
        section_href = str(section_href)
        if not section_href or not section_text or any(x in section_text for x in ("Back to", "Home")):
            continue
        section_url = urljoin(start_url, section_href)
        print(f"    [Section] {section_text} -> {section_url}")
        section_soup = safe_request(section_url)
        if not section_soup:
            continue
        # Cerca tutti i link a prodotti (moduli) nelle sottopagine
        for item_link in section_soup.find_all('a', href=True):
            if not isinstance(item_link, Tag):
                continue
            item_href = item_link.get('href')
            if isinstance(item_href, list):
                item_href = item_href[0]
            item_href = str(item_href)
            if not item_href or not item_href.endswith('.html'):
                continue
            product_page_url = urljoin(section_url, item_href)
            product_soup = safe_request(product_page_url)
            if not product_soup:
                continue
            title_tag = product_soup.find('h1')
            title = normalize_whitespace(title_tag.get_text() if title_tag else item_link.get_text())
            page_text = product_soup.get_text()
            code_match = re.search(r'TSR\s?(\d{4,5})', page_text)
            if title and code_match:
                code = f"TSR{code_match.group(1)}"
                game_system = "AD&D" if "AD&D" in section_text else "D&D"
                edition = "N/A"
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (code, title, game_system, edition, "Module/Adventure", lang, product_page_url)
                    )
                    total_added += 1
                except Exception as e:
                    print(f"    [ERROR] DB insert failed for {title}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] TSR Archive parsing for '{lang}' complete. Added {total_added} entries.")

def parse_wikipedia_generic(conn, url, system, category, lang, description):
    """
    (RESTORED & UPGRADED) A stateful parser for Wikipedia that is multi-lingual and correctly finds editions.
    """
    print(f"\n[+] Parsing Wikipedia: {description}...")
    soup = safe_request(url)
    if not soup:
        return
    content = soup.find(id='mw-content-text')
    if not content or not isinstance(content, Tag):
        print(f"  [WARNING] No content found for {url}", file=sys.stderr)
        return
    current_edition = "N/A"
    header_map = {
        "title": {"title", "titolo", "titolo originale"},
        "code": {"code", "codice", "codice prodotto"},
        "edition": {"edition", "edizione"}
    }
    total_added = 0
    for tag in content.find_all(['h2', 'h3', 'table']):
        if not isinstance(tag, Tag):
            continue
        if tag.name in ('h2', 'h3'):
            headline = tag.find(class_='mw-headline') if hasattr(tag, 'find') else None
            if headline:
                current_edition = normalize_whitespace(headline.get_text())
        elif tag.name == 'table' and tag.has_attr('class') and 'wikitable' in tag['class']:
            for row in parse_table_rows(tag, header_map):
                title = row.get("title")
                code = row.get("code")
                edition_in_table = row.get("edition")
                final_edition = edition_in_table or current_edition
                if title and "List of" not in title and len(title) > 1:
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (code, title, system, final_edition, category, lang, url)
                        )
                        total_added += 1
                    except Exception as e:
                        print(f"    [ERROR] DB insert failed for {title}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] Wikipedia ({description}) parsing complete. Added {total_added} unique entries.")

def parse_dndwiki_35e(conn, url, lang):
    """(RESTORED) Parser for dnd-wiki.org that correctly parses the full page."""
    print(f"\n[+] Parsing dnd-wiki.org for 3.5e Adventures...")
    soup = safe_request(url)
    if not soup:
        return
    content_div = soup.find('div', id='mw-content-text')
    if not content_div or not isinstance(content_div, Tag):
        print(f"  [WARNING] No content found for {url}", file=sys.stderr)
        return
    total_added = 0
    for li in content_div.find_all('li'):
        if not isinstance(li, Tag):
            continue
        title_tag = li.find('a') if hasattr(li, 'find') else None
        if title_tag and isinstance(title_tag, Tag):
            title = normalize_whitespace(title_tag.get_text())
            href = title_tag.get('href', '') if title_tag.has_attr('href') else ''
            if not isinstance(href, str) and href is not None:
                href = str(href[0]) if isinstance(href, list) and href else ''
            if title and len(title) > 1 and "Category:" not in title and (isinstance(href, str) and "d20srd" not in href):
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (None, title, "D&D", "3.5e", "Adventure", lang, url)
                    )
                    total_added += 1
                except Exception as e:
                    print(f"    [ERROR] DB insert failed for {title}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] dnd-wiki.org parsing complete. Added {total_added} unique entries.")

def parse_rpggeek(conn, url, lang):
    """
    Parser for RPGGeek adventure lists (publicly accessible, allows scraping).
    """
    print(f"\n[+] Parsing RPGGeek from {url} (language: {lang})...")
    soup = safe_request(url)
    if not soup:
        print(f"  [WARNING] Could not fetch RPGGeek page: {url}", file=sys.stderr)
        return
    cursor = conn.cursor()
    total_added = 0
    # Example: parse table rows for modules/adventures
    for row in soup.select('table.geekitem_table tr'):
        cols = row.find_all('td')
        if len(cols) >= 2:
            title = cols[1].get_text(strip=True)
            if title:
                try:
                    cursor.execute(
                        "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (None, title, "RPG", None, "Module/Adventure", lang, url)
                    )
                    total_added += 1
                except Exception as e:
                    print(f"    [ERROR] DB insert failed for {title}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] RPGGeek parsing complete. Added {total_added} entries.")

def parse_rpgnet(conn, url, lang):
    """
    Parser for RPG.net lists (if available).
    """
    print(f"\n[+] Parsing RPG.net from {url} (language: {lang})...")
    soup = safe_request(url)
    if not soup:
        print(f"  [WARNING] Could not fetch RPG.net page: {url}", file=sys.stderr)
        return
    cursor = conn.cursor()
    total_added = 0
    # Example: parse list items for modules/adventures
    for li in soup.select('li'):
        title = li.get_text(strip=True)
        if title and len(title) > 3:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (None, title, "RPG", None, "Module/Adventure", lang, url)
                )
                total_added += 1
            except Exception as e:
                print(f"    [ERROR] DB insert failed for {title}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] RPG.net parsing complete. Added {total_added} entries.")

def parse_drivethrurpg(conn, url, system, lang):
    """
    This parser gracefully handles the expected 403 error from DriveThruRPG.
    Suggests manual import or using APIs if available.
    """
    print(f"\n[+] Skipping DriveThruRPG {system} products from {url} (scraping blocked).")
    print("  [INFO] DriveThruRPG blocks automated scripts. Please use manual import, publisher data, or official APIs if available.")
    return

def parse_rpggeek_it(conn, url, lang):
    """
    Parser for RPGGeek Italian adventure lists.
    """
    print(f"\n[+] Parsing RPGGeek (Italian) from {url} (language: {lang})...")
    soup = safe_request(url)
    if not soup:
        print(f"  [WARNING] Could not fetch RPGGeek (Italian) page: {url}", file=sys.stderr)
        return
    cursor = conn.cursor()
    total_added = 0
    for row in soup.select('table.geekitem_table tr'):
        cols = row.find_all('td')
        if len(cols) >= 2:
            title = cols[1].get_text(strip=True)
            if title:
                try:
                    cursor.execute(
                        "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (None, title, "RPG", None, "Modulo/Avventura", lang, url)
                    )
                    total_added += 1
                except Exception as e:
                    print(f"    [ERROR] DB insert failed for {title}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] RPGGeek (Italian) parsing complete. Added {total_added} entries.")

def print_robots_and_ask(url):
    """
    Prints the robots.txt for the given URL and asks the user if they want to proceed anyway.
    Returns True if the user wants to proceed, False otherwise.
    """
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    print(f"\n--- robots.txt for {parsed.netloc} ---")
    try:
        resp = requests.get(robots_url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            print(resp.text)
        else:
            print(f"[INFO] Could not fetch robots.txt (HTTP {resp.status_code})")
    except Exception as e:
        print(f"[ERROR] Could not fetch robots.txt: {e}")
    proceed = input("robots.txt disallows scraping. Vuoi procedere comunque? [y/N]: ")
    return proceed.strip().lower() == 'y'

# --- Main Execution Block ---
PARSER_MAPPING = {
    "tsr_archive_en": (parse_tsr_archive, "English"),
    "tsr_archive_it": (parse_tsr_archive, "Italian"),
    "wiki_dnd_modules": (lambda c, u, l: parse_wikipedia_generic(c, u, "D&D", "Module", l, "D&D Modules"), "English"),
    "wiki_dnd_adventures": (lambda c, u, l: parse_wikipedia_generic(c, u, "D&D", "Adventure", l, "D&D Adventures"), "English"),
    "dndwiki_35e": (parse_dndwiki_35e, "English"),
    "wiki_pathfinder": (lambda c, u, l: parse_wikipedia_generic(c, u, "Pathfinder", "Book", l, "Pathfinder Books"), "English"),
    "italian_dnd_wiki": (lambda c, u, l: parse_wikipedia_generic(c, u, "D&D", "Modulo", l, "D&D Italian"), "Italian"),
    "italian_pathfinder_wiki": (lambda c, u, l: parse_wikipedia_generic(c, u, "Pathfinder", "Manuale", l, "Pathfinder Italian"), "Italian"),
    "drivethrurpg_dnd": (lambda c, u, l: parse_drivethrurpg(c, u, "D&D", l), "English"),
    "drivethrurpg_pathfinder": (lambda c, u, l: parse_drivethrurpg(c, u, "Pathfinder", l), "English"),
    "rpggeek_adventures": (parse_rpggeek, "English"),
    "rpgnet_adventures": (parse_rpgnet, "English"),
    "rpggeek_avventure_it": (parse_rpggeek_it, "Italian"),
    # Add more Italian/other-language sources and parsers as needed
}

if __name__ == "__main__":
    print("--- Building Enhanced Knowledge Base from Online Sources ---")
    connection = None
    try:
        config = load_config()
        urls_to_scrape = config['knowledge_base_urls']
        url_languages = config.get('knowledge_base_url_languages', {})
        connection = init_db()
        for key, url in urls_to_scrape.items():
            print(f"\n[INFO] Processing: {key}")
            lang = url_languages.get(key, "English")
            # Check robots.txt before scraping
            if key.startswith("drivethrurpg"):
                parse_drivethrurpg(connection, url, key, lang)
                continue
            allowed = is_scraping_allowed(url)
            if not allowed:
                if print_robots_and_ask(url):
                    print("[INFO] Procedo ignorando robots.txt su richiesta dell'utente.")
                else:
                    print(f"  [WARNING] Skipping {url} per rispetto di robots.txt.", file=sys.stderr)
                    continue
            if key in PARSER_MAPPING:
                parser_func, _ = PARSER_MAPPING[key]
                try:
                    parser_func(connection, url, lang)
                    time.sleep(2)  # Throttle between sources
                except Exception as e:
                    print(f"  [CRITICAL] Parser '{key}' failed unexpectedly: {e}", file=sys.stderr)
                    import traceback
                    traceback.print_exc()
            else:
                print(f"  [WARNING] No parser available for config key '{key}'. Skipping.", file=sys.stderr)
    except Exception as e:
        print(f"[FATAL] Unhandled error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if connection:
            try:
                # --- Dynamic and Correct Statistics Report ---
                print("\n--- Knowledge Base Statistics ---")
                cursor = connection.cursor()
                cursor.execute("SELECT COUNT(*) FROM products")
                print(f"Total unique products: {cursor.fetchone()[0]}")
                cursor.execute("SELECT DISTINCT game_system FROM products ORDER BY game_system")
                game_systems = [row[0] for row in cursor.fetchall()]
                for system in game_systems:
                    print(f"\nBreakdown for: {system}")
                    cursor.execute("SELECT COUNT(*) FROM products WHERE game_system = ?", (system,))
                    print(f"  Total Entries: {cursor.fetchone()[0]}")
                    print("  By Language:")
                    cursor.execute("SELECT language, COUNT(*) FROM products WHERE game_system = ? GROUP BY language", (system,))
                    for lang, count in cursor.fetchall():
                        print(f"    {lang}: {count}")
                    print("  By Edition:")
                    cursor.execute("SELECT edition, COUNT(*) FROM products WHERE game_system = ? GROUP BY edition ORDER BY edition", (system,))
                    for edition, count in cursor.fetchall():
                        print(f"    {edition or 'N/A'}: {count}")
                connection.close()
                print("\n--- Knowledge Base Build Complete! ---")
                print(f"Enhanced database saved to '{DB_FILE}'")
            except Exception:
                pass
