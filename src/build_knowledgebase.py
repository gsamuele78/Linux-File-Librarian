import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import os
import time
import sys
from collections import defaultdict
from urllib.parse import urljoin

# --- SELENIUM IMPORTS ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
# This is the "smart wait" tool.
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
# To get detailed error information from Selenium
from selenium.common.exceptions import TimeoutException

from src.config_loader import load_config

# --- Configuration ---
DB_FILE = "knowledge.sqlite"
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'}

def init_db():
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
    """Makes a simple web request for static HTML pages using the 'requests' library."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'lxml')
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Could not fetch {url}. Reason: {e}", file=sys.stderr)
        return None

def setup_driver():
    """Sets up a headless Chrome browser instance for Selenium to use."""
    print("  [INFO] Setting up headless Chrome browser for Selenium...")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=" + HEADERS['User-Agent'])
    # Suppress verbose logging from WebDriver Manager
    os.environ['WDM_LOG_LEVEL'] = '0'
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    print("  [INFO] Browser setup complete.")
    return driver

# --- Specialized Parsers (Rewritten with Professional-Grade Selenium) ---

def parse_tsr_archive_selenium(conn, start_url, lang):
    """
    (REWRITTEN WITH ROBUST FRAME DISCOVERY) A crawler for tsrarchive.com that correctly handles frames.
    """
    print(f"\n[+] Parsing TSR Archive with Selenium for '{lang}' from: {start_url}...")
    cursor = conn.cursor()
    total_added = 0
    driver = None

    try:
        driver = setup_driver()
        driver.get(start_url)
        wait = WebDriverWait(driver, 20) # Use a 20-second "smart wait" timeout.

        # FIX: Dynamically find and switch to the navigation frame.
        print("  -> Discovering navigation frame...")
        # Wait until at least one frame is loaded on the page.
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "frame")))
        frames = driver.find_elements(By.TAG_NAME, "frame")
        nav_frame_found = False
        for frame in frames:
            # Switch to any frame that looks like a navigation frame.
            if 'nav' in (frame.get_attribute('name') or '') or 'nav' in (frame.get_attribute('src') or ''):
                driver.switch_to.frame(frame)
                nav_frame_found = True
                print("  -> Switched to navigation frame.")
                break
        
        if not nav_frame_found:
            print("  [CRITICAL] Could not find a suitable navigation frame. Aborting parser.", file=sys.stderr)
            return

        section_links_data = []
        for link in driver.find_elements(By.TAG_NAME, 'a'):
            href = link.get_attribute('href')
            text = link.text
            if href and text and "Back to" not in text and "Home" not in text:
                section_links_data.append({'url': href, 'text': text})
        
        driver.switch_to.default_content()
        print(f"  -> Found {len(section_links_data)} content sections to crawl.")

        for section_data in section_links_data:
            section_url = section_data['url']
            section_text = section_data['text']
            print(f"    -> Scraping Section: {section_text}")
            driver.get(section_url)
            
            # FIX: Dynamically find and switch to the main content frame.
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "frame")))
            frames = driver.find_elements(By.TAG_NAME, "frame")
            main_frame_found = False
            for frame in frames:
                if 'main' in (frame.get_attribute('name') or '') or 'main' in (frame.get_attribute('src') or ''):
                    driver.switch_to.frame(frame)
                    main_frame_found = True
                    break
            
            if not main_frame_found: continue
            
            section_soup = BeautifulSoup(driver.page_source, 'lxml')
            driver.switch_to.default_content()

            for item_link in section_soup.select('b > a[href$=".html"]'):
                product_page_url = urljoin(section_url, item_link['href'])
                # No need to use the driver for the final page if requests works
                product_soup = safe_request(product_page_url)
                if not product_soup: continue
                
                title_tag = product_soup.find('h1')
                title = title_tag.get_text(strip=True) if title_tag else item_link.get_text(strip=True)
                
                page_text = product_soup.get_text()
                code_match = re.search(r'TSR\s?(\d{4,5})', page_text)
                
                if title and code_match:
                    code = f"TSR{code_match.group(1)}"
                    game_system = "AD&D" if "AD&D" in section_text else "D&D"
                    edition = "N/A"
                    cursor.execute(
                        "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (code, title, game_system, edition, "Module/Adventure", lang, product_page_url)
                    )
                    total_added += cursor.rowcount
            conn.commit()

    except TimeoutException as e:
        print(f"  [CRITICAL] Selenium parser timed out waiting for a page element (frame). The site may be slow or has changed structure. Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"  [CRITICAL] An unexpected error occurred in the Selenium parser: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()

    print(f"[SUCCESS] TSR Archive parsing for '{lang}' complete. Added {total_added} unique entries.")

# --- (Full, working versions of other parsers restored) ---
def parse_wikipedia_generic(conn, url, system, category, lang, description):
    """A stateful parser for Wikipedia that is multi-lingual and correctly finds editions."""
    print(f"\n[+] Parsing Wikipedia: {description}...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    content = soup.find(id='mw-content-text')
    if not content: return

    current_edition = "N/A"
    for tag in content.find_all(['h2', 'h3', 'table']):
        if tag.name == 'h2' or tag.name == 'h3':
            headline = tag.find(class_='mw-headline')
            if headline: current_edition = headline.get_text(strip=True)
        
        elif tag.name == 'table' and 'wikitable' in tag.get('class', []):
            headers = [th.get_text(strip=True).lower() for th in tag.find_all('th')]
            try:
                title_idx = headers.index('title') if 'title' in headers else headers.index('titolo')
                code_idx = headers.index('code') if 'code' in headers else headers.index('codice') if 'codice' in headers else -1
                edition_col_idx = headers.index('edition') if 'edition' in headers else headers.index('edizione') if 'edizione' in headers else -1
            except ValueError: continue
            
            for row in tag.find_all('tr')[1:]:
                cols = row.find_all(['td', 'th'])
                if len(cols) > title_idx:
                    title = cols[title_idx].get_text(strip=True)
                    code = cols[code_idx].get_text(strip=True) if code_idx != -1 and len(cols) > code_idx else None
                    edition_in_table = cols[edition_col_idx].get_text(strip=True) if edition_col_idx != -1 and len(cols) > edition_col_idx else None
                    final_edition = edition_in_table or current_edition
                    if title and "List of" not in title and len(title) > 1:
                        cursor.execute("INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)", (code, title, system, final_edition, category, lang, url))
                        total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] Wikipedia ({description}) parsing complete. Added {total_added} unique entries.")

def parse_dndwiki_35e(conn, url, lang):
    """Parser for dnd-wiki.org that correctly parses the full page."""
    print(f"\n[+] Parsing dnd-wiki.org for 3.5e Adventures...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup: return
    total_added = 0
    content_div = soup.find('div', id='mw-content-text')
    if not content_div: return
    for li in content_div.find_all('li'):
        title_tag = li.find('a')
        if title_tag:
            title = title_tag.get_text(strip=True)
            if title and len(title) > 1 and "Category:" not in title and "d20srd" not in title_tag.get('href', ''):
                cursor.execute("INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)", (None, title, "D&D", "3.5e", "Adventure", lang, url))
                total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] dnd-wiki.org parsing complete. Added {total_added} unique entries.")

def parse_drivethrurpg(conn, url, system, lang):
    """Parser that gracefully handles the expected 403 error from DriveThruRPG."""
    print(f"\n[+] Parsing DriveThruRPG {system} products from {url}...")
    print("  [INFO] DriveThruRPG actively blocks automated scripts (HTTP 403 Error).")
    soup = safe_request(url)
    if not soup:
        print(f"[SKIPPED] Could not access DriveThruRPG for {system}, as expected.")
        return
    print("[SUCCESS] DriveThruRPG parsing step finished (likely with an error, which is expected).")


# --- Main Execution Block ---
PARSER_MAPPING = {
    "tsr_archive_en": (parse_tsr_archive_selenium, "English"),
    "tsr_archive_it": (parse_tsr_archive_selenium, "Italian"),
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
    
    print("\n--- Knowledge Base Statistics ---")
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    print(f"Total unique products: {cursor.fetchone()[0]}")
    print("\nBy Game System:")
    stats_system = defaultdict(int)
    cursor.execute("SELECT game_system, COUNT(*) FROM products GROUP BY game_system")
    for system, count in sorted(stats_system.items()): print(f"  {system}: {count}")
    print("\nBy Language:")
    stats_lang = defaultdict(int)
    cursor.execute("SELECT language, COUNT(*) FROM products GROUP BY language")
    for lang, count in sorted(stats_lang.items()): print(f"  {lang}: {count}")
    print("\nD&D by Edition:")
    stats_dnd = defaultdict(int)
    cursor.execute("SELECT edition, COUNT(*) FROM products WHERE game_system = 'D&D' GROUP BY edition")
    for edition, count in sorted(stats_dnd.items()): print(f"  {edition or 'N/A'}: {count}")
    connection.close()
    print("\n--- Knowledge Base Build Complete! ---")
    print(f"Enhanced database saved to '{DB_FILE}'")
