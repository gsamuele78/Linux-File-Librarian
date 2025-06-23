import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import os
import time

# --- Configuration ---
# The name of the database file that will be created in the project root.
DB_FILE = "knowledge.sqlite"
# Headers to mimic a web browser and avoid being blocked.
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'}

def init_db():
    """Initializes a fresh database, deleting any existing one."""
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            product_code TEXT,
            title TEXT NOT NULL,
            game_system TEXT NOT NULL,
            edition TEXT,
            category TEXT,
            source_url TEXT,
            UNIQUE(product_code, title, game_system, edition)
        )
    ''')
    conn.commit()
    return conn

def safe_request(url):
    """Makes a request and handles potential errors, returning a BeautifulSoup object."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        # Use lxml for performance
        soup = BeautifulSoup(response.content, 'lxml')
        return soup
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Could not fetch {url}. Reason: {e}")
        return None

def parse_tsr_archive(conn):
    """
    Parser for tsrarchive.com. It first finds all sub-pages from the index
    and then scrapes each one for product information.
    """
    print("\n[+] Parsing tsrarchive.com...")
    base_url = "http://www.tsrarchive.com/"
    index_url = base_url + "index3.html"
    cursor = conn.cursor()
    
    soup = safe_request(index_url)
    if not soup:
        return

    # Find links to the different game systems (D&D, AD&D, etc.)
    game_system_links = soup.select('a[href*=".html"]')
    total_added = 0

    for link in game_system_links:
        sub_page_url = base_url + link['href']
        # Infer game system from the link text (e.g., "D&D", "AD&D")
        game_system = link.get_text(strip=True)
        if not game_system or game_system == "Home":
            continue

        print(f"  -> Scraping system: {game_system} from {sub_page_url}")
        sub_soup = safe_request(sub_page_url)
        if not sub_soup:
            continue

        # Find all module links, which are typically bolded
        for item in sub_soup.select('b a[href*=".html"]'):
            title = item.get_text(strip=True)
            # The product code is usually in the text following the link
            product_code_match = re.search(r'\((TSR\s?\d{4,5})\)', item.parent.get_text())
            
            if title and product_code_match:
                product_code = product_code_match.group(1).replace(" ", "")
                cursor.execute(
                    "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, source_url) VALUES (?, ?, ?, ?, ?, ?)",
                    (product_code, title, game_system, "1e/2e", "Module/Adventure", sub_page_url)
                )
                total_added += cursor.rowcount
        conn.commit()
        time.sleep(1) # Be respectful to the server

    print(f"[SUCCESS] tsrarchive.com parsing complete. Added {total_added} entries.")

def parse_wikipedia_dnd(conn, url, description):
    """
    A generic parser for Wikipedia's D&D adventure/module lists.
    It identifies sections by H2 tags and scrapes the tables within them.
    """
    print(f"\n[+] Parsing Wikipedia: {description}...")
    cursor = conn.cursor()
    
    soup = safe_request(url)
    if not soup:
        return

    total_added = 0
    # Find all section headers
    for header in soup.find_all('h2'):
        edition_text = header.find(class_='mw-headline')
        if not edition_text:
            continue
        
        # Infer edition from the header text
        edition = edition_text.get_text(strip=True)
        
        # Find all tables between this header and the next
        for table in header.find_next_siblings('table', class_='wikitable'):
            # Stop if we've entered a new section
            if table.find_previous_sibling('h2') != header:
                break
            
            # The first row is the header, find the 'Code' and 'Title' column indices
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            try:
                title_idx = headers.index('title')
                code_idx = headers.index('code')
            except ValueError:
                # This table doesn't have the required columns, skip it
                continue
            
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) > max(title_idx, code_idx):
                    title = cols[title_idx].get_text(strip=True)
                    code = cols[code_idx].get_text(strip=True)
                    if title:
                        cursor.execute(
                            "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, source_url) VALUES (?, ?, ?, ?, ?, ?)",
                            (code or None, title, "D&D", edition, "Module/Adventure", url)
                        )
                        total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] Wikipedia ({description}) parsing complete. Added {total_added} entries.")

def parse_dndwiki_35e(conn):
    """
    Parser for dnd-wiki.org's 3.5e Adventures list.
    This site does not have product codes, so we focus on titles.
    """
    print("\n[+] Parsing dnd-wiki.org for 3.5e Adventures...")
    url = "https://dnd-wiki.org/wiki/3.5e_Adventures"
    cursor = conn.cursor()
    
    soup = safe_request(url)
    if not soup:
        return

    total_added = 0
    content = soup.find(id='bodyContent')
    if not content:
        print("  [ERROR] Could not find main content block.")
        return

    # Find all list items (li) which contain adventure titles
    for li in content.find_all('li'):
        # Filter out navigation links or other junk `li` elements
        if li.find(class_='tocnumber'):
            continue
            
        title = li.get_text(strip=True).split('(')[0].strip() # Get title, remove parenthetical notes
        if title and len(title) > 2:
             cursor.execute(
                "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, source_url) VALUES (?, ?, ?, ?, ?, ?)",
                (None, title, "D&D", "3.5e", "Adventure", url)
            )
             total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] dnd-wiki.org parsing complete. Added {total_added} entries.")

def parse_wikipedia_pathfinder(conn):
    """
    Parses the Wikipedia list of Pathfinder books, handling 1e and 2e.
    """
    print("\n[+] Parsing Wikipedia for Pathfinder books...")
    url = "https://en.wikipedia.org/wiki/List_of_Pathfinder_books"
    cursor = conn.cursor()
    
    soup = safe_request(url)
    if not soup:
        return

    total_added = 0
    # Find the H2 for "First edition" and "Second edition"
    for header in soup.find_all('h2'):
        edition_text = header.find(class_='mw-headline')
        if not edition_text:
            continue
        
        edition_str = edition_text.get_text(strip=True)
        if 'First edition' in edition_str:
            edition = '1e'
        elif 'Second edition' in edition_str:
            edition = '2e'
        else:
            continue

        # Find all tables for that edition
        for table in header.find_next_siblings('table', class_='wikitable'):
            if table.find_previous_sibling('h2') != header:
                break

            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            try:
                # Column names can vary slightly
                title_idx = headers.index('title')
                code_idx = -1
                if 'product code' in headers:
                    code_idx = headers.index('product code')
                elif 'isbn' in headers:
                     code_idx = headers.index('isbn')
                else:
                    continue # Skip if no usable code column
            except ValueError:
                continue

            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) > max(title_idx, code_idx):
                    title = cols[title_idx].get_text(strip=True)
                    code = cols[code_idx].get_text(strip=True) if code_idx != -1 else None
                    if title:
                        cursor.execute(
                            "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, source_url) VALUES (?, ?, ?, ?, ?, ?)",
                            (code, title, "Pathfinder", edition, "Book", url)
                        )
                        total_added += cursor.rowcount
    conn.commit()
    print(f"[SUCCESS] Wikipedia (Pathfinder) parsing complete. Added {total_added} entries.")


if __name__ == "__main__":
    print("--- Building Knowledge Base from Online Sources ---")
    print("This may take several minutes. Please be patient.")
    connection = init_db()
    
    # --- Execute all parsers ---
    parse_tsr_archive(connection)
    parse_wikipedia_dnd(connection, 'https://en.wikipedia.org/wiki/List_of_Dungeons_%26_Dragons_modules', "D&D Modules")
    parse_wikipedia_dnd(connection, 'https://en.wikipedia.org/wiki/List_of_Dungeons_%26_Dragons_adventures', "D&D Adventures")
    parse_dndwiki_35e(connection)
    parse_wikipedia_pathfinder(connection)
    
    # --- Finalize ---
    print("\n[+] Finalizing database...")
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    final_count = cursor.fetchone()[0]
    connection.close()
    
    print("\n--- Knowledge Base Build Complete! ---")
    print(f"Database saved to '{DB_FILE}' with a total of {final_count} unique product entries.")
