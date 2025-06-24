import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import os
import time
import sys
from difflib import SequenceMatcher
from src.config_loader import load_config

# --- Configuration ---
DB_FILE = "knowledge.sqlite"
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'}

def init_db():
    """
    Initializes a fresh database with enhanced deduplication structure.
    """
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Enhanced table structure for better deduplication
    cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            product_code TEXT,
            title TEXT NOT NULL,
            title_normalized TEXT NOT NULL,
            game_system TEXT NOT NULL,
            edition TEXT,
            category TEXT,
            language TEXT DEFAULT 'en',
            year INTEGER,
            isbn TEXT,
            source_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(title_normalized, game_system, edition, language)
        )
    ''')
    
    # Index for faster similarity searches
    cursor.execute('CREATE INDEX idx_title_normalized ON products(title_normalized)')
    cursor.execute('CREATE INDEX idx_game_system_edition ON products(game_system, edition)')
    
    conn.commit()
    return conn

def normalize_title(title):
    """
    Normalizes titles for better deduplication by removing common variations.
    """
    if not title:
        return ""
    
    # Convert to lowercase and remove special characters
    normalized = re.sub(r'[^\w\s]', ' ', title.lower())
    
    # Remove common words that don't affect uniqueness
    stop_words = ['the', 'a', 'an', 'and', 'or', 'of', 'in', 'on', 'at', 'to', 'for', 'with', 'by']
    words = [word for word in normalized.split() if word not in stop_words]
    
    # Remove edition markers for better matching
    edition_patterns = [r'\b\d+e\b', r'\bedition\b', r'\bed\b', r'\brev\b', r'\brevised\b']
    text = ' '.join(words)
    for pattern in edition_patterns:
        text = re.sub(pattern, '', text)
    
    return ' '.join(text.split())  # Remove extra spaces

def similarity_ratio(a, b):
    """Calculate similarity ratio between two strings."""
    return SequenceMatcher(None, a, b).ratio()

def is_duplicate(cursor, title, game_system, edition, language, threshold=0.85):
    """
    Check if a product is likely a duplicate based on title similarity.
    """
    normalized_title = normalize_title(title)
    
    # First check exact match
    cursor.execute(
        "SELECT id, title FROM products WHERE title_normalized = ? AND game_system = ? AND edition = ? AND language = ?",
        (normalized_title, game_system, edition, language)
    )
    if cursor.fetchone():
        return True
    
    # Then check similarity within same system/edition
    cursor.execute(
        "SELECT title, title_normalized FROM products WHERE game_system = ? AND edition = ? AND language = ?",
        (game_system, edition, language)
    )
    
    for existing_title, existing_normalized in cursor.fetchall():
        if similarity_ratio(normalized_title, existing_normalized) >= threshold:
            return True
    
    return False

def safe_request(url):
    """
    Makes a web request with error handling and rate limiting.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'lxml')
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Could not fetch {url}. Reason: {e}", file=sys.stderr)
        return None

def extract_year(text):
    """Extract year from text if present."""
    year_match = re.search(r'\b(19|20)\d{2}\b', text)
    return int(year_match.group()) if year_match else None

def extract_isbn(text):
    """Extract ISBN from text if present."""
    isbn_match = re.search(r'ISBN[:\s]*([0-9-]+)', text, re.IGNORECASE)
    return isbn_match.group(1) if isbn_match else None

# --- Enhanced Parsers ---

def parse_tsr_archive(conn, url):
    """Enhanced TSR Archive parser with better deduplication."""
    print(f"\n[+] Parsing tsrarchive.com from {url}...")
    base_url = "http://www.tsrarchive.com/"
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup:
        return
    
    total_added = 0
    
    for link in soup.select('a[href*=".html"]'):
        sub_page_url = base_url + link['href']
        game_system = link.get_text(strip=True)
        
        if not game_system or game_system == "Home":
            continue
        
        print(f"  -> Scraping system: {game_system}")
        sub_soup = safe_request(sub_page_url)
        if not sub_soup:
            continue
        
        for item in sub_soup.select('b a[href*=".html"]'):
            title = item.get_text(strip=True)
            parent_text = item.parent.get_text()
            
            # Extract product code
            code_match = re.search(r'\((TSR\s?\d{4,5})\)', parent_text)
            code = code_match.group(1).replace(" ", "") if code_match else None
            
            # Extract year
            year = extract_year(parent_text)
            
            if title and not is_duplicate(cursor, title, game_system, "1e/2e", "en"):
                cursor.execute(
                    """INSERT INTO products 
                       (product_code, title, title_normalized, game_system, edition, category, language, year, source_url) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (code, title, normalize_title(title), game_system, "1e/2e", "Module/Adventure", "en", year, sub_page_url)
                )
                total_added += 1
    
    conn.commit()
    time.sleep(1)
    print(f"[SUCCESS] tsrarchive.com parsing complete. Added {total_added} unique entries.")

def parse_wikipedia_dnd(conn, url, description):
    """Enhanced Wikipedia D&D parser with better edition detection."""
    print(f"\n[+] Parsing Wikipedia: {description}...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup:
        return
    
    total_added = 0
    
    for header in soup.find_all('h2'):
        edition_text = header.find(class_='mw-headline')
        if not edition_text:
            continue
        
        edition = edition_text.get_text(strip=True)
        
        # Better edition normalization
        if 'first' in edition.lower() or '1st' in edition.lower():
            edition = "1e"
        elif 'second' in edition.lower() or '2nd' in edition.lower():
            edition = "2e"
        elif 'third' in edition.lower() or '3rd' in edition.lower() or '3.5' in edition:
            edition = "3e/3.5e"
        elif 'fourth' in edition.lower() or '4th' in edition.lower():
            edition = "4e"
        elif 'fifth' in edition.lower() or '5th' in edition.lower():
            edition = "5e"
        
        for table in header.find_next_siblings('table', class_='wikitable'):
            if table.find_previous_sibling('h2') != header:
                break
            
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            
            try:
                title_idx = headers.index('title')
                code_idx = headers.index('code') if 'code' in headers else -1
            except ValueError:
                continue
            
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) > title_idx:
                    title = cols[title_idx].get_text(strip=True)
                    code = cols[code_idx].get_text(strip=True) if code_idx >= 0 and len(cols) > code_idx else None
                    
                    # Extract additional info
                    row_text = row.get_text()
                    year = extract_year(row_text)
                    isbn = extract_isbn(row_text)
                    
                    if title and not is_duplicate(cursor, title, "D&D", edition, "en"):
                        cursor.execute(
                            """INSERT INTO products 
                               (product_code, title, title_normalized, game_system, edition, category, language, year, isbn, source_url) 
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (code, title, normalize_title(title), "D&D", edition, "Module/Adventure", "en", year, isbn, url)
                        )
                        total_added += 1
    
    conn.commit()
    print(f"[SUCCESS] Wikipedia ({description}) parsing complete. Added {total_added} unique entries.")

def parse_dndwiki_35e(conn, url):
    """Enhanced DnD Wiki parser."""
    print(f"\n[+] Parsing dnd-wiki.org for 3.5e Adventures...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup:
        return
    
    total_added = 0
    content = soup.find(id='bodyContent')
    if not content:
        return
    
    for li in content.find_all('li'):
        if li.find(class_='tocnumber'):
            continue
        
        text = li.get_text(strip=True)
        title = text.split('(')[0].strip()
        
        if title and len(title) > 2 and not is_duplicate(cursor, title, "D&D", "3.5e", "en"):
            cursor.execute(
                """INSERT INTO products 
                   (title, title_normalized, game_system, edition, category, language, source_url) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (title, normalize_title(title), "D&D", "3.5e", "Adventure", "en", url)
            )
            total_added += 1
    
    conn.commit()
    print(f"[SUCCESS] dnd-wiki.org parsing complete. Added {total_added} unique entries.")

def parse_wikipedia_pathfinder(conn, url):
    """Enhanced Pathfinder parser with better edition handling."""
    print(f"\n[+] Parsing Wikipedia for Pathfinder books...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup:
        return
    
    total_added = 0
    
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
        
        for table in header.find_next_siblings('table', class_='wikitable'):
            if table.find_previous_sibling('h2') != header:
                break
            
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            
            try:
                title_idx = headers.index('title')
                code_idx = -1
                if 'product code' in headers:
                    code_idx = headers.index('product code')
                elif 'isbn' in headers:
                    code_idx = headers.index('isbn')
            except ValueError:
                continue
            
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) > title_idx:
                    title = cols[title_idx].get_text(strip=True)
                    code = cols[code_idx].get_text(strip=True) if code_idx >= 0 and len(cols) > code_idx else None
                    
                    # Extract additional info
                    row_text = row.get_text()
                    year = extract_year(row_text)
                    isbn = extract_isbn(row_text)
                    
                    if title and not is_duplicate(cursor, title, "Pathfinder", edition, "en"):
                        cursor.execute(
                            """INSERT INTO products 
                               (product_code, title, title_normalized, game_system, edition, category, language, year, isbn, source_url) 
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (code, title, normalize_title(title), "Pathfinder", edition, "Book", "en", year, isbn, url)
                        )
                        total_added += 1
    
    conn.commit()
    print(f"[SUCCESS] Wikipedia (Pathfinder) parsing complete. Added {total_added} unique entries.")

def parse_italian_dnd_wiki(conn, url):
    """Parser for Italian D&D Wiki."""
    print(f"\n[+] Parsing Italian D&D content from {url}...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup:
        return
    
    total_added = 0
    
    # Look for Italian content patterns
    content_divs = soup.find_all('div', class_=['mw-content-ltr', 'content'])
    
    for div in content_divs:
        for link in div.find_all('a'):
            title = link.get_text(strip=True)
            if not title or len(title) < 3:
                continue
            
            # Detect D&D edition from context
            context = link.parent.get_text() if link.parent else ""
            edition = "5e"  # Default to most common
            
            if any(marker in context.lower() for marker in ['3.5', '3e', 'terza']):
                edition = "3.5e"
            elif any(marker in context.lower() for marker in ['4e', 'quarta']):
                edition = "4e"
            elif any(marker in context.lower() for marker in ['5e', 'quinta']):
                edition = "5e"
            
            if not is_duplicate(cursor, title, "D&D", edition, "it"):
                cursor.execute(
                    """INSERT INTO products 
                       (title, title_normalized, game_system, edition, category, language, source_url) 
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (title, normalize_title(title), "D&D", edition, "Adventure", "it", url)
                )
                total_added += 1
    
    conn.commit()
    print(f"[SUCCESS] Italian D&D parsing complete. Added {total_added} unique entries.")

def parse_italian_pathfinder_wiki(conn, url):
    """Parser for Italian Pathfinder Wiki."""
    print(f"\n[+] Parsing Italian Pathfinder content from {url}...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup:
        return
    
    total_added = 0
    
    # Look for Italian Pathfinder content
    content_divs = soup.find_all('div', class_=['mw-content-ltr', 'content'])
    
    for div in content_divs:
        for link in div.find_all('a'):
            title = link.get_text(strip=True)
            if not title or len(title) < 3:
                continue
            
            # Detect Pathfinder edition
            context = link.parent.get_text() if link.parent else ""
            edition = "1e"  # Default
            
            if any(marker in context.lower() for marker in ['2e', 'seconda', 'second']):
                edition = "2e"
            
            if not is_duplicate(cursor, title, "Pathfinder", edition, "it"):
                cursor.execute(
                    """INSERT INTO products 
                       (title, title_normalized, game_system, edition, category, language, source_url) 
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (title, normalize_title(title), "Pathfinder", edition, "Book", "it", url)
                )
                total_added += 1
    
    conn.commit()
    print(f"[SUCCESS] Italian Pathfinder parsing complete. Added {total_added} unique entries.")

def parse_drivethrurpg_dnd(conn, url):
    """Parser for DriveThruRPG D&D products (both languages)."""
    print(f"\n[+] Parsing DriveThruRPG D&D products from {url}...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup:
        return
    
    total_added = 0
    
    # Look for product listings
    products = soup.find_all('div', class_=['product-row', 'product-item'])
    
    for product in products:
        title_elem = product.find(['h3', 'h4', 'a'], class_=['product-title', 'title'])
        if not title_elem:
            continue
        
        title = title_elem.get_text(strip=True)
        if not title:
            continue
        
        # Detect language
        language = "it" if any(word in title.lower() for word in ['italiano', 'italiana', 'ita']) else "en"
        
        # Detect edition
        edition = "5e"  # Default
        if any(marker in title.lower() for marker in ['3.5', '3e']):
            edition = "3.5e"
        elif '4e' in title.lower():
            edition = "4e"
        
        # Extract product code if present
        code_match = re.search(r'[A-Z]{2,}\d{3,}', title)
        code = code_match.group() if code_match else None
        
        if not is_duplicate(cursor, title, "D&D", edition, language):
            cursor.execute(
                """INSERT INTO products 
                   (product_code, title, title_normalized, game_system, edition, category, language, source_url) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (code, title, normalize_title(title), "D&D", edition, "Adventure", language, url)
            )
            total_added += 1
    
    conn.commit()
    print(f"[SUCCESS] DriveThruRPG D&D parsing complete. Added {total_added} unique entries.")

def parse_drivethrurpg_pathfinder(conn, url):
    """Parser for DriveThruRPG Pathfinder products (both languages)."""
    print(f"\n[+] Parsing DriveThruRPG Pathfinder products from {url}...")
    cursor = conn.cursor()
    soup = safe_request(url)
    if not soup:
        return
    
    total_added = 0
    
    # Look for product listings
    products = soup.find_all('div', class_=['product-row', 'product-item'])
    
    for product in products:
        title_elem = product.find(['h3', 'h4', 'a'], class_=['product-title', 'title'])
        if not title_elem:
            continue
        
        title = title_elem.get_text(strip=True)
        if not title:
            continue
        
        # Detect language
        language = "it" if any(word in title.lower() for word in ['italiano', 'italiana', 'ita']) else "en"
        
        # Detect edition
        edition = "1e"  # Default
        if any(marker in title.lower() for marker in ['2e', 'second']):
            edition = "2e"
        
        if not is_duplicate(cursor, title, "Pathfinder", edition, language):
            cursor.execute(
                """INSERT INTO products 
                   (title, title_normalized, game_system, edition, category, language, source_url) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (title, normalize_title(title), "Pathfinder", edition, "Book", language, url)
            )
            total_added += 1
    
    conn.commit()
    print(f"[SUCCESS] DriveThruRPG Pathfinder parsing complete. Added {total_added} unique entries.")

# --- Enhanced Parser Mapping ---
PARSER_MAPPING = {
    # English sources
    "tsr_archive": parse_tsr_archive,
    "wiki_dnd_modules": lambda c, u: parse_wikipedia_dnd(c, u, "D&D Modules"),
    "wiki_dnd_adventures": lambda c, u: parse_wikipedia_dnd(c, u, "D&D Adventures"),
    "dndwiki_35e": parse_dndwiki_35e,
    "wiki_pathfinder": parse_wikipedia_pathfinder,
    
    # Italian sources
    "italian_dnd_wiki": parse_italian_dnd_wiki,
    "italian_pathfinder_wiki": parse_italian_pathfinder_wiki,
    
    # DriveThruRPG sources (both languages)
    "drivethrurpg_dnd": parse_drivethrurpg_dnd,
    "drivethrurpg_pathfinder": parse_drivethrurpg_pathfinder,
}

def print_statistics(conn):
    """Print comprehensive statistics about the knowledge base."""
    cursor = conn.cursor()
    
    print("\n--- Knowledge Base Statistics ---")
    
    # Total products
    cursor.execute("SELECT COUNT(*) FROM products")
    total = cursor.fetchone()[0]
    print(f"Total unique products: {total}")
    
    # By game system
    cursor.execute("SELECT game_system, COUNT(*) FROM products GROUP BY game_system ORDER BY COUNT(*) DESC")
    print("\nBy Game System:")
    for system, count in cursor.fetchall():
        print(f"  {system}: {count}")
    
    # By language
    cursor.execute("SELECT language, COUNT(*) FROM products GROUP BY language")
    print("\nBy Language:")
    for lang, count in cursor.fetchall():
        lang_name = "English" if lang == "en" else "Italian" if lang == "it" else lang
        print(f"  {lang_name}: {count}")
    
    # By edition (for D&D)
    cursor.execute("SELECT edition, COUNT(*) FROM products WHERE game_system = 'D&D' GROUP BY edition ORDER BY edition")
    print("\nD&D by Edition:")
    for edition, count in cursor.fetchall():
        print(f"  {edition}: {count}")
    
    # By edition (for Pathfinder)
    cursor.execute("SELECT edition, COUNT(*) FROM products WHERE game_system = 'Pathfinder' GROUP BY edition ORDER BY edition")
    print("\nPathfinder by Edition:")
    for edition, count in cursor.fetchall():
        print(f"  {edition}: {count}")

if __name__ == "__main__":
    print("--- Building Enhanced Knowledge Base from Online Sources ---")
    
    try:
        config = load_config()
        urls_to_scrape = config['knowledge_base_urls']
    except Exception as e:
        print(f"[FATAL] Could not load configuration. Please check 'conf/config.ini'.\nError: {e}", file=sys.stderr)
        sys.exit(1)
    
    connection = init_db()
    
    # Process all configured URLs
    for key, url in urls_to_scrape.items():
        if key in PARSER_MAPPING:
            try:
                print(f"\n[INFO] Processing: {key}")
                PARSER_MAPPING[key](connection, url)
                time.sleep(2)  # Be respectful to servers
            except Exception as e:
                print(f"  [CRITICAL] Parser '{key}' failed unexpectedly: {e}", file=sys.stderr)
        else:
            print(f"  [WARNING] No parser available for config key '{key}'. Skipping.", file=sys.stderr)
    
    # Print comprehensive statistics
    print_statistics(connection)
    
    connection.close()
    print(f"\n--- Knowledge Base Build Complete! ---")
    print(f"Enhanced database saved to '{DB_FILE}'")
