# src/build_knowledgebase.py
import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import os

DB_FILE = "knowledge.sqlite"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

def init_db():
    """Initializes a fresh database."""
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

def parse_tsrarchive(conn):
    """Parses D&D modules from tsrarchive.com."""
    print("[INFO] Parsing tsrarchive.com for D&D modules...")
    cursor = conn.cursor()
    url = "http://www.tsrarchive.com/dd/dd.html"
    try:
        response = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Find all links in the main content tables
        for link in soup.select('table a[href*=".html"]'):
            title = link.get_text(strip=True)
            product_code_match = re.search(r'\((TSR\s?\d{4,5})\)', link.next_sibling)
            if title and product_code_match:
                product_code = product_code_match.group(1).replace(" ", "")
                cursor.execute(
                    "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, source_url) VALUES (?, ?, ?, ?, ?, ?)",
                    (product_code, title, "D&D", "1e/2e", "Module/Adventure", url)
                )
    except Exception as e:
        print(f"[ERROR] Could not parse TSR Archive: {e}")
    conn.commit()
    print(f"[INFO] Finished tsrarchive.com. Found {cursor.rowcount} new entries.")

def parse_wikipedia_pathfinder(conn):
    """Parses Pathfinder books from Wikipedia."""
    print("[INFO] Parsing Wikipedia for Pathfinder books...")
    cursor = conn.cursor()
    url = "https://en.wikipedia.org/wiki/List_of_Pathfinder_books"
    try:
        response = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(response.content, 'lxml')
        
        for h2 in soup.find_all('h2'):
            # First Edition Adventures
            if 'Adventure Paths' in h2.get_text():
                table = h2.find_next_sibling('table', class_='wikitable')
                if table:
                    for row in table.find_all('tr')[1:]:
                        cols = row.find_all('td')
                        if len(cols) > 2:
                            title = cols[0].get_text(strip=True)
                            code = cols[2].get_text(strip=True)
                            cursor.execute(
                                "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, source_url) VALUES (?, ?, ?, ?, ?, ?)",
                                (code, title, "Pathfinder", "1e", "Adventure Path", url)
                            )
            # Second Edition Adventures
            if 'Second edition' in h2.get_text():
                 h3 = h2.find_next_sibling('h3')
                 if h3 and 'Adventure Paths' in h3.get_text():
                    table = h3.find_next_sibling('table', class_='wikitable')
                    if table:
                         for row in table.find_all('tr')[1:]:
                            cols = row.find_all('td')
                            if len(cols) > 2:
                                title = cols[0].get_text(strip=True)
                                code = cols[2].get_text(strip=True)
                                cursor.execute(
                                "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, source_url) VALUES (?, ?, ?, ?, ?, ?)",
                                (code, title, "Pathfinder", "2e", "Adventure Path", url)
                            )
    except Exception as e:
        print(f"[ERROR] Could not parse Wikipedia Pathfinder list: {e}")
    conn.commit()
    print(f"[INFO] Finished Pathfinder list. Found {cursor.rowcount} new entries.")

# Add more parsers here for the other URLs if desired, following the same pattern.
# For brevity, I have implemented two examples.

if __name__ == "__main__":
    print("--- Building Knowledge Base ---")
    connection = init_db()
    
    # Run all parsers
    parse_tsrarchive(connection)
    parse_wikipedia_pathfinder(connection)
    
    connection.close()
    print("\n--- Knowledge Base Build Complete! ---")
    print(f"Database saved to '{DB_FILE}'.")
