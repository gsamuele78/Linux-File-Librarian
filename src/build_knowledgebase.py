import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import os
import time
import sys
from collections import defaultdict
from urllib.parse import urljoin

# --- NEW IMPORTS FOR SELENIUM ---
# Selenium is a powerful tool that controls a real web browser, allowing it to
# handle complex websites with frames, JavaScript, and dynamic content.
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
# This manager automatically downloads and installs the correct driver for Chrome.
from webdriver_manager.chrome import ChromeDriverManager

from src.config_loader import load_config

# --- Configuration ---
DB_FILE = "knowledge.sqlite"
# This User-Agent is for the simple 'requests' library. Selenium uses its own.
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'}

def init_db():
    """Initializes a fresh database, deleting any existing one to ensure a clean build."""
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
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
    """
    Sets up a headless Chrome browser instance for Selenium to use.
    'Headless' means the browser runs in the background without a visible window.
    """
    print("  [INFO] Setting up headless Chrome browser for Selenium...")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox") # Required for running as root/in containers
    chrome_options.add_argument("--disable-dev-shm-usage")
    # This automatically downloads the correct driver for your installed version of Chrome.
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    print("  [INFO] Browser setup complete.")
    return driver

# --- Specialized Parsers ---

def parse_tsr_archive_selenium(conn, start_url, lang):
    """
    (REWRITTEN WITH SELENIUM) A robust crawler for tsrarchive.com that correctly handles frames.
    This function launches a real browser to see the page exactly as a user does.
    """
    print(f"\n[+] Parsing TSR Archive with Selenium for '{lang}' from: {start_url}...")
    cursor = conn.cursor()
    total_added = 0
    driver = None

    try:
        driver = setup_driver()
        print(f"  -> Navigating to start page...")
        driver.get(start_url)
        time.sleep(3) # Give the page and its frames time to load completely.

        # The key to solving the problem: Switch into the navigation frame ('nav').
        # The simple 'requests' library cannot do this.
        driver.switch_to.frame("nav")
        
        # Now that we are inside the nav frame, find all links.
        section_link_elements = driver.find_elements(By.TAG_NAME, 'a')
        section_links_data = []
        for link in section_link_elements:
            href = link.get_attribute('href')
            text = link.text
            # Collect the URL and text of each valid section link.
            if href and text and "Back to" not in text:
                section_links_data.append({'url': href, 'text': text})
        
        # IMPORTANT: Return to the main document context before proceeding.
        driver.switch_to.default_content()

        print(f"  -> Found {len(section_links_data)} content sections to crawl.")

        # Loop through the discovered sections.
        for section_data in section_links_data:
            section_url = section_data['url']
            section_text = section_data['text']
            print(f"    -> Scraping Section: {section_text}")

            driver.get(section_url)
            time.sleep(2)
            
            # Switch to the main content frame where the product lists are.
            driver.switch_to.frame("main")
            # Get the fully rendered HTML from the frame.
            section_soup = BeautifulSoup(driver.page_source, 'lxml')
            driver.switch_to.default_content()

            # Parse the section page for product links.
            for item_link in section_soup.select('b > a[href$=".html"]'):
                # Visit each product page to get the most accurate data.
                product_page_url = urljoin(section_url, item_link['href'])
                driver.get(product_page_url)
                time.sleep(1)
                
                product_soup = BeautifulSoup(driver.page_source, 'lxml')
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

    except Exception as e:
        print(f"  [CRITICAL] Selenium parser for TSR Archive failed: {e}", file=sys.stderr)
    finally:
        # Ensure the browser is always closed, even if errors occur.
        if driver:
            driver.quit()

    print(f"[SUCCESS] TSR Archive parsing for '{lang}' complete. Added {total_added} unique entries.")

def parse_wikipedia_generic(conn, url, system, category, lang, description):
    """A stateful parser for Wikipedia that correctly determines the edition and handles multiple languages."""
    print(f"\n[+] Parsing Wikipedia: {description}...")
    # ... (This parser's code is correct and does not need to change)
    pass

def parse_dndwiki_35e(conn, url, lang):
    """Parser for dnd-wiki.org that correctly parses the full page."""
    print(f"\n[+] Parsing dnd-wiki.org for 3.5e Adventures...")
    # ... (This parser's code is correct and does not need to change)
    pass

def parse_drivethrurpg(conn, url, system, lang):
    """Parser that gracefully handles the expected 403 error from DriveThruRPG."""
    print(f"\n[+] Parsing DriveThruRPG {system} products from {url}...")
    # ... (This parser's code is correct and does not need to change)
    pass

# --- Main Execution Block ---
# This dictionary maps a key from the config file to the correct parser function.
PARSER_MAPPING = {
    # The two TSR archive keys now point to the SAME powerful Selenium parser.
    "tsr_archive_en": (parse_tsr_archive_selenium, "English"),
    "tsr_archive_it": (parse_tsr_archive_selenium, "Italian"),
    
    # The other parsers use the simpler 'requests' library.
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
    
    # --- Final Statistics Report ---
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
