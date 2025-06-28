import sqlite3
import requests
from bs4 import BeautifulSoup, Tag
import re
import os
import time
import sys
from urllib.parse import urljoin, urlparse
from src.config_loader import load_config
import concurrent.futures

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

def safe_request(url, retries=5, delay=2):
    """
    Makes a web request with retries and handles network errors robustly.
    Returns BeautifulSoup object or None.
    Now with exponential backoff.
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            if 'text/html' not in response.headers.get('Content-Type', ''):
                print(f"  [ERROR] Non-HTML content at {url}", file=sys.stderr)
                return None
            return BeautifulSoup(response.content, 'lxml')
        except requests.exceptions.RequestException as e:
            print(f"  [ERROR] Could not fetch {url} (attempt {attempt+1}/{retries}): {e}", file=sys.stderr)
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
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

# --- Utility: robots.txt check and user prompt (shared) ---
def check_robots_and_prompt(url):
    """
    Checks robots.txt for the given URL. If scraping is disallowed, ask user if they want to proceed.
    Returns True if allowed, False if user declines.
    """
    allowed = is_scraping_allowed(url)
    if allowed:
        return True
    # Print robots.txt and ask user
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

def parallel_map_parse(parse_func, url_list, *args, **kwargs):
    """
    Utility to parallelize parsing of multiple URLs/sections.
    parse_func: function to call for each url/section, must return a list of products or None.
    url_list: list of URLs or (section, url) tuples.
    Returns: list of all products found.
    """
    results = []
    errors = []
    def safe_parse(item):
        try:
            return parse_func(item, *args, **kwargs)
        except Exception as e:
            print(f"  [ERROR] Exception in parallel parse for {item}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            errors.append((item, str(e)))
            return []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_item = {executor.submit(safe_parse, item): item for item in url_list}
        for future in concurrent.futures.as_completed(future_to_item):
            res = future.result()
            if res:
                results.extend(res)
    return results, errors

# --- Specialized Parsers (All Parsers Fully Implemented) ---
def parse_tsr_archive(conn, start_url, lang):
    """
    Updated: parser for tsrarchive.com that follows the new HTML structure (main tables, no frames),
    with robust English table parsing and debug output.
    Now always parses the main product tables from the five key URLs for English.
    Uses parallel fetching/parsing for speedup.
    """
    print(f"\n[+] Parsing TSR Archive for language '{lang}' from starting page: {start_url}...")
    cursor = conn.cursor()
    total_added = 0
    ENGLISH_MAIN_URLS = [
        "http://www.tsrarchive.com/dd/dd.html",
        "http://www.tsrarchive.com/add/add.html",
        "http://www.tsrarchive.com/3e/3e2.html",
        "http://www.tsrarchive.com/4e/4e.html",
        "http://www.tsrarchive.com/5e/5e.html",
    ]
    if lang.lower() == "english":
        results = []
        def fetch_and_parse(url):
            print(f"  [EN] Parsing main product table: {url} [LANG: {lang}]")
            main_soup = safe_request(url)
            if not main_soup:
                print(f"    [SKIP] Could not fetch: {url} [LANG: {lang}]")
                return []
            return (url, parse_tsr_table_products(main_soup, "Main Table", lang, url, None))
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(fetch_and_parse, url): url for url in ENGLISH_MAIN_URLS}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    url, products = future.result()
                    if products:
                        for prod in products:
                            try:
                                conn.execute(
                                    "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                    prod
                                )
                                total_added += 1
                            except Exception as e:
                                print(f"    [ERROR] DB insert failed for {prod}: {e}", file=sys.stderr)
                        print(f"    [EN] Added {len(products)} products from {url} [LANG: {lang}]")
                except Exception as exc:
                    print(f"    [ERROR] Exception for {url}: {exc}", file=sys.stderr)
        conn.commit()
        print(f"[SUCCESS] TSR Archive parsing for '{lang}' complete. Added {total_added} entries.")
        return
    # --- Parallelized Italian section parsing ---
    if lang.lower() == "italian":
        index_soup = safe_request(start_url)
        if not index_soup:
            return
        # Find all main section links in the central table, excluding anchor-only links
        main_links = []
        for a in index_soup.find_all('a', href=True):
            if not isinstance(a, Tag):
                continue
            href = a.get('href')
            if isinstance(href, list):
                href = href[0]
            href = str(href)
            if href and not href.startswith('#'):
                main_links.append(a)
        print(f"  -> Found {len(main_links)} main section links.")

        def fetch_and_parse_section(main_link):
            section_text = normalize_whitespace(main_link.get_text())
            section_href = main_link.get('href')
            if isinstance(section_href, list):
                section_href = section_href[0]
            section_href = str(section_href)
            if not section_href or not section_text or any(x in section_text for x in ("Back to", "Home")):
                return (None, [])
            section_url = urljoin(start_url, section_href)
            print(f"    [Section] {section_text} -> {section_url}")
            section_soup = safe_request(section_url)
            if not section_soup:
                return (section_url, [])
            # Always try to parse tables of products in the section page (English and Italian)
            products = parse_tsr_table_products(section_soup, section_text, lang, section_url, None)
            # Fallback: parse individual product pages as before (mainly for Italian, but keep for completeness)
            product_links = []
            for item_link in section_soup.find_all('a', href=True):
                if not isinstance(item_link, Tag):
                    continue
                item_href = item_link.get('href')
                if isinstance(item_href, list):
                    item_href = item_href[0]
                item_href = str(item_href)
                product_page_url = urljoin(section_url, item_href) if item_href else None
                skip_reason = None
                if not item_href:
                    skip_reason = 'empty href'
                elif item_href.startswith('#'):
                    skip_reason = 'anchor link'
                elif product_page_url == section_url:
                    skip_reason = 'same as section'
                if skip_reason:
                    continue
                if not item_href.endswith('.html'):
                    continue
                product_links.append((item_link, product_page_url))
            for item_link, product_page_url in product_links:
                product_soup = safe_request(product_page_url)
                if not product_soup:
                    continue
                title_tag = product_soup.find('h1')
                title = normalize_whitespace(title_tag.get_text() if title_tag else item_link.get_text())
                page_text = product_soup.get_text()
                code_match = re.search(r'TSR\s?(\d{4,5})', page_text)
                if not code_match:
                    code_match = re.search(r'Item Code:\s*(\d{4,5})', page_text, re.IGNORECASE)
                if not code_match:
                    code_match = re.search(r'Product Code:\s*(\d{4,5})', page_text, re.IGNORECASE)
                if code_match:
                    code = code_match.group(1)
                else:
                    code = None
                if lang.lower() == "italian":
                    m = re.match(r'^(.*?)(?:\s*\(([^\"]+)\))?$', title)
                    if m:
                        italian_title = m.group(1).strip()
                        english_title = m.group(2).strip() if m.group(2) else None
                        if english_title:
                            full_title = f"{italian_title} ({english_title})"
                        else:
                            full_title = italian_title
                    else:
                        full_title = title
                    game_system = "AD&D" if "AD&D" in section_text else "D&D"
                    edition = "N/A"
                    if full_title:
                        products.append((code, full_title, game_system, edition, "Module/Adventure", "Italian", product_page_url))
                else:
                    game_system = "AD&D" if "AD&D" in section_text else "D&D"
                    edition = "N/A"
                    if title:
                        products.append((code, title, game_system, edition, "Module/Adventure", lang, product_page_url))
            return (section_url, products)

        all_products = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_section = {executor.submit(fetch_and_parse_section, main_link): main_link for main_link in main_links}
            for future in concurrent.futures.as_completed(future_to_section):
                section_url, products = future.result()
                if products:
                    for prod in products:
                        try:
                            conn.execute(
                                "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                prod
                            )
                            total_added += 1
                        except Exception as e:
                            print(f"    [ERROR] DB insert failed for {prod}: {e}", file=sys.stderr)
        conn.commit()
        print(f"[SUCCESS] TSR Archive parsing for '{lang}' complete. Added {total_added} entries.")
        return
    # --- Fallback: parse from the index/start URL for other languages ---
    index_soup = safe_request(start_url)
    if not index_soup:
        return

    # Find all main section links in the central table, excluding anchor-only links
    main_links = []
    for a in index_soup.find_all('a', href=True):
        if not isinstance(a, Tag):
            continue
        href = a.get('href')
        if isinstance(href, list):
            href = href[0]
        href = str(href)
        if href and not href.startswith('#'):
            main_links.append(a)
    print(f"  -> Found {len(main_links)} main section links.")

    # --- Parallel Italian section parsing ---
    def fetch_and_parse_section(main_link):
        section_text = normalize_whitespace(main_link.get_text())
        section_href = main_link.get('href')
        if isinstance(section_href, list):
            section_href = section_href[0]
        section_href = str(section_href)
        if not section_href or not section_text or any(x in section_text for x in ("Back to", "Home")):
            return (None, [])
        section_url = urljoin(start_url, section_href)
        print(f"    [Section] {section_text} -> {section_url}")
        section_soup = safe_request(section_url)
        if not section_soup:
            return (section_url, [])
        # Always try to parse tables of products in the section page (English and Italian)
        products = parse_tsr_table_products(section_soup, section_text, lang, section_url, None)
        # Fallback: parse individual product pages as before (mainly for Italian, but keep for completeness)
        product_links = []
        for item_link in section_soup.find_all('a', href=True):
            if not isinstance(item_link, Tag):
                continue
            item_href = item_link.get('href')
            if isinstance(item_href, list):
                item_href = item_href[0]
            item_href = str(item_href)
            product_page_url = urljoin(section_url, item_href) if item_href else None
            skip_reason = None
            if not item_href:
                skip_reason = 'empty href'
            elif item_href.startswith('#'):
                skip_reason = 'anchor link'
            elif product_page_url == section_url:
                skip_reason = 'same as section'
            if skip_reason:
                continue
            if not item_href.endswith('.html'):
                continue
            product_links.append((item_link, product_page_url))
        for item_link, product_page_url in product_links:
            product_soup = safe_request(product_page_url)
            if not product_soup:
                continue
            title_tag = product_soup.find('h1')
            title = normalize_whitespace(title_tag.get_text() if title_tag else item_link.get_text())
            page_text = product_soup.get_text()
            code_match = re.search(r'TSR\s?(\d{4,5})', page_text)
            if not code_match:
                code_match = re.search(r'Item Code:\s*(\d{4,5})', page_text, re.IGNORECASE)
            if not code_match:
                code_match = re.search(r'Product Code:\s*(\d{4,5})', page_text, re.IGNORECASE)
            if code_match:
                code = code_match.group(1)
            else:
                code = None
            if lang.lower() == "italian":
                m = re.match(r'^(.*?)(?:\s*\(([^\"]+)\))?$', title)
                if m:
                    italian_title = m.group(1).strip()
                    english_title = m.group(2).strip() if m.group(2) else None
                    if english_title:
                        full_title = f"{italian_title} ({english_title})"
                    else:
                        full_title = italian_title
                else:
                    full_title = title
                game_system = "AD&D" if "AD&D" in section_text else "D&D"
                edition = "N/A"
                if full_title:
                    products.append((code, full_title, game_system, edition, "Module/Adventure", "Italian", product_page_url))
            else:
                game_system = "AD&D" if "AD&D" in section_text else "D&D"
                edition = "N/A"
                if title:
                    products.append((code, title, game_system, edition, "Module/Adventure", lang, product_page_url))
        return (section_url, products)

    all_products = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_section = {executor.submit(fetch_and_parse_section, main_link): main_link for main_link in main_links}
        for future in concurrent.futures.as_completed(future_to_section):
            section_url, products = future.result()
            if products:
                for prod in products:
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            prod
                        )
                        total_added += 1
                    except Exception as e:
                        print(f"    [ERROR] DB insert failed for {prod}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] TSR Archive parsing for '{lang}' complete. Added {total_added} entries.")
    return

def parse_tsr_table_products(soup, section_text, lang, section_url, conn):
    """
    Enhanced: Parse all <td> in main tables, extract product code, module code, title, and category from column header.
    Returns a list of tuples for DB insertion if conn is None, else inserts directly and returns count.
    Now with improved debug output, robust English table parsing, fallback for missing headers, better product splitting/normalization, and multi-product extraction per cell.
    """
    total_added = 0
    table_count = 0
    seen_products = set()
    products = []
    for table in soup.find_all('table'):
        headers = [normalize_whitespace(th.get_text()) for th in table.find_all('th')]
        # Fallback: If no <th>, try to use first row as headers, or use default categories
        if len(headers) < 2:
            first_row = table.find('tr')
            if first_row:
                first_row_cells = [normalize_whitespace(td.get_text()) for td in first_row.find_all(['td', 'th'])]
                if len(first_row_cells) >= 2 and any(first_row_cells):
                    headers = first_row_cells
                    print(f"      [DEBUG] Using first row as headers: {headers} [LANG: {lang}]")
                else:
                    headers = [f"Category {i+1}" for i in range(len(first_row_cells))]
                    print(f"      [DEBUG] Using fallback headers: {headers} [LANG: {lang}]")
            else:
                print(f"      [DEBUG] Skipping table: no headers and no first row [LANG: {lang}]")
                continue
        else:
            print(f"      [DEBUG] Table #{table_count+1} headers: {headers} [LANG: {lang}]" )
        table_count += 1
        rows = table.find_all('tr')
        data_rows = rows[1:] if len(rows) > 1 else []
        for row_idx, row in enumerate(data_rows, 1):
            cols = row.find_all(['td', 'th'])
            if not cols or all(not normalize_whitespace(cell.get_text()) for cell in cols):
                print(f"        [SKIP ROW] Empty row at idx {row_idx} [LANG: {lang}]")
                continue
            for col_idx, cell in enumerate(cols):
                cell_text = normalize_whitespace(cell.get_text())
                if not cell_text:
                    continue
                # --- Italian logic unchanged ---
                if lang.lower() == "italian":
                    lines = re.split(r'\s{2,}|\t|\n|\|', cell_text)
                    for line in lines:
                        line = normalize_whitespace(line)
                        if not line or line.lower() in ("code", "tsr", "module", "product"):
                            continue
                        if not ("it" in section_url.lower() or "italian" in section_text.lower()):
                            print(f"        [SKIP ROW] Not Italian section: '{section_url}' [LANG: {lang}]")
                            continue
                        m = re.match(r'^(.*?)(?:\s*\(([^)]+)\))?$', line)
                        if m:
                            italian_title = m.group(1).strip()
                            english_title = m.group(2).strip() if m.group(2) else None
                            code_match = re.match(r'^(\S+)\s+(.*)', italian_title)
                            if code_match:
                                product_code = code_match.group(1).upper()
                                title_it = code_match.group(2).strip()
                            else:
                                product_code = None
                                title_it = italian_title.strip()
                            if english_title:
                                full_title = f"{title_it} ({english_title})"
                            else:
                                full_title = title_it
                            category = headers[col_idx] if col_idx < len(headers) and headers[col_idx] != f"Category {col_idx+1}" else section_text
                            game_system = "AD&D" if "AD&D" in section_text else (
                                "3E" if "3e" in section_url.lower() else (
                                "4E" if "4e" in section_url.lower() else (
                                "5E" if "5e" in section_url.lower() else "D&D")))
                            edition = "N/A"
                            key = (product_code, full_title, game_system, edition, "Italian")
                            if key in seen_products:
                                print(f"        [DUPLICATE] {full_title} | Code: {product_code} | Cat: {category} [LANG: {lang}]")
                                continue
                            if conn:
                                try:
                                    conn.execute(
                                        "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                        (product_code, full_title, game_system, edition, category, "Italian", section_url)
                                    )
                                    print(f"        [INSERTED] {full_title} | Code: {product_code} | Cat: {category} [LANG: {lang}]")
                                    total_added += 1
                                    seen_products.add(key)
                                except Exception as e:
                                    print(f"    [ERROR] DB insert failed for {full_title}: {e}", file=sys.stderr)
                            else:
                                products.append((product_code, full_title, game_system, edition, category, "Italian", section_url))
                                seen_products.add(key)
                    continue
                # --- English/Default logic: extract all product code+title pairs in cell ---
                # Regex: product code (optionally module code) + title, repeated
                # Example: 9193 GAZ1 The Grand Duchy of Karameikos 9194 GAZ2 The Emirates of Ylaruam
                # Pattern: (code) (optional module code) (title until next code or end)
                product_pattern = re.compile(r'(\S+)(?:\s+([A-Z]{1,4}\d{1,3}))?\s+([^\d]+?)(?=\s+\S+\s+[A-Z]{0,4}\d{1,5}\s+|\s+\S+\s+[^A-Z\d]|$)')
                matches = list(product_pattern.finditer(cell_text))
                if not matches:
                    # fallback: try to match code + title at start
                    m = re.match(r'^(\S+)\s+(.*)', cell_text)
                    if m and len(m.groups()) >= 2:
                        matches = [m]
                if len(matches) > 1:
                    print(f"        [MULTI-PRODUCT] Found {len(matches)} products in one cell [LANG: {lang}]")
                for match in matches:
                    if hasattr(match, 'groups'):
                        product_code = match.group(1).upper()
                        module_code = match.group(2) if len(match.groups()) > 2 else None
                        title = match.group(3).strip() if len(match.groups()) > 2 else match.group(2).strip()
                    else:
                        product_code = match.group(1).upper()
                        title = match.group(2).strip()
                        module_code = None
                    if not title or len(title) < 2:
                        print(f"        [SKIP ROW] No valid title in match: '{cell_text}' [LANG: {lang}]")
                        continue
                    category = headers[col_idx] if col_idx < len(headers) and headers[col_idx] != f"Category {col_idx+1}" else section_text
                    game_system = "AD&D" if "AD&D" in section_text or "add" in section_url.lower() else (
                        "3E" if "3e" in section_url.lower() else (
                        "4E" if "4e" in section_url.lower() else (
                        "5E" if "5e" in section_url.lower() else "D&D")))
                    edition = "N/A"
                    key = (product_code, title, game_system, edition, "English")
                    if key in seen_products:
                        print(f"        [DUPLICATE] {title} | Code: {product_code} | Cat: {category} [LANG: {lang}]")
                        continue
                    if conn:
                        try:
                            conn.execute(
                                "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (product_code, title, game_system, edition, category, "English", section_url)
                            )
                            print(f"        [INSERTED] {title} | Code: {product_code} | Cat: {category} [LANG: {lang}]")
                            total_added += 1
                            seen_products.add(key)
                        except Exception as e:
                            print(f"    [ERROR] DB insert failed for {title}: {e}", file=sys.stderr)
                    else:
                        products.append((product_code, title, game_system, edition, category, "English", section_url))
                        seen_products.add(key)
    if table_count == 0:
        print(f"      [DEBUG] No tables found in section page [LANG: {lang}]" )
    if conn:
        print(f"      [SUMMARY] Inserted: {total_added} unique products from tables in {section_url} [LANG: {lang}]\n")
        return total_added
    else:
        print(f"      [SUMMARY] Parsed {len(products)} unique products from tables in {section_url} [LANG: {lang}]\n")
        return products

# --- Update Wikipedia parser to parallelize tables if multiple tables exist ---
def parse_wikipedia_generic(conn, url, system, category, lang, description):
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
    tables = []
    for tag in content.find_all('table'):
        if isinstance(tag, Tag):
            cls = tag.get('class')
            if isinstance(cls, list) and 'wikitable' in cls:
                tables.append(tag)
    def parse_table(table):
        nonlocal current_edition
        products = []
        # Find preceding h2/h3 for edition
        prev = table.find_previous(['h2', 'h3'])
        if prev:
            headline = prev.find(class_='mw-headline') if hasattr(prev, 'find') else None
            if headline:
                current_edition = normalize_whitespace(headline.get_text())
        for row in parse_table_rows(table, header_map):
            title = row.get("title")
            code = row.get("code")
            edition_in_table = row.get("edition")
            final_edition = edition_in_table or current_edition
            if title and "List of" not in title and len(title) > 1:
                products.append((code, title, system, final_edition, category, lang, url))
        return products
    all_products, errors = parallel_map_parse(parse_table, tables)
    for prod in all_products:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                prod
            )
            total_added += 1
        except Exception as e:
            print(f"    [ERROR] DB insert failed for {prod}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] Wikipedia ({description}) parsing complete. Added {total_added} unique entries. Errors: {len(errors)}")

# --- RPGGeek parser parallelization ---
def parse_rpggeek(conn, url, lang):
    print(f"\n[+] Parsing RPGGeek from {url} (language: {lang})...")
    soup = safe_request(url)
    if not soup:
        print(f"  [WARNING] Could not fetch RPGGeek page: {url}", file=sys.stderr)
        return
    cursor = conn.cursor()
    total_added = 0
    rows = soup.select('table.geekitem_table tr')
    def parse_row(row):
        cols = row.find_all('td')
        if len(cols) >= 2:
            title = cols[1].get_text(strip=True)
            if title:
                return (None, title, "RPG", None, "Module/Adventure", lang, url)
        return None
    products, errors = parallel_map_parse(parse_row, rows)
    for prod in products:
        if prod:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    prod
                )
                total_added += 1
            except Exception as e:
                print(f"    [ERROR] DB insert failed for {prod}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] RPGGeek parsing complete. Added {total_added} entries. Errors: {len(errors)}")

# --- RPGNet parser parallelization ---
def parse_rpgnet(conn, url, lang):
    print(f"\n[+] Parsing RPG.net from {url} (language: {lang})...")
    soup = safe_request(url)
    if not soup:
        print(f"  [WARNING] Could not fetch RPG.net page: {url}", file=sys.stderr)
        return
    cursor = conn.cursor()
    total_added = 0
    items = soup.select('li')
    def parse_li(li):
        title = li.get_text(strip=True)
        if title and len(title) > 3:
            return (None, title, "RPG", None, "Module/Adventure", lang, url)
        return None
    products, errors = parallel_map_parse(parse_li, items)
    for prod in products:
        if prod:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    prod
                )
                total_added += 1
            except Exception as e:
                print(f"    [ERROR] DB insert failed for {prod}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] RPG.net parsing complete. Added {total_added} entries. Errors: {len(errors)}")

# --- RPGGeek Italian parser parallelization ---
def parse_rpggeek_it(conn, url, lang):
    print(f"\n[+] Parsing RPGGeek (Italian) from {url} (language: {lang})...")
    soup = safe_request(url)
    if not soup:
        print(f"  [WARNING] Could not fetch RPGGeek (Italian) page: {url}", file=sys.stderr)
        return
    cursor = conn.cursor()
    total_added = 0
    rows = soup.select('table.geekitem_table tr')
    def parse_row(row):
        cols = row.find_all('td')
        if len(cols) >= 2:
            title = cols[1].get_text(strip=True)
            if title:
                return (None, title, "RPG", None, "Modulo/Avventura", lang, url)
        return None
    products, errors = parallel_map_parse(parse_row, rows)
    for prod in products:
        if prod:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    prod
                )
                total_added += 1
            except Exception as e:
                print(f"    [ERROR] DB insert failed for {prod}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] RPGGeek (Italian) parsing complete. Added {total_added} entries. Errors: {len(errors)}")

# --- dnd-wiki.org parser parallelization ---
def parse_dndwiki_35e(conn, url, lang):
    print(f"\n[+] Parsing dnd-wiki.org for 3.5e Adventures...")
    soup = safe_request(url)
    if not soup:
        return
    content_div = soup.find('div', id='mw-content-text')
    if not content_div or not isinstance(content_div, Tag):
        print(f"  [WARNING] No content found for {url}", file=sys.stderr)
        return
    total_added = 0
    items = content_div.find_all('li')
    def parse_li(li):
        title_tag = li.find('a') if hasattr(li, 'find') else None
        if title_tag and isinstance(title_tag, Tag):
            title = normalize_whitespace(title_tag.get_text())
            href = title_tag.get('href', '') if title_tag.has_attr('href') else ''
            if not isinstance(href, str) and href is not None:
                href = str(href[0]) if isinstance(href, list) and href else ''
            if title and len(title) > 1 and "Category:" not in title and (isinstance(href, str) and "d20srd" not in href):
                return (None, title, "D&D", "3.5e", "Adventure", lang, url)
        return None
    products, errors = parallel_map_parse(parse_li, items)
    for prod in products:
        if prod:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    prod
                )
                total_added += 1
            except Exception as e:
                print(f"    [ERROR] DB insert failed for {prod}: {e}", file=sys.stderr)
    conn.commit()
    print(f"[SUCCESS] dnd-wiki.org parsing complete. Added {total_added} unique entries. Errors: {len(errors)}")

def parse_drivethrurpg(conn, url, system, lang):
    """
    This parser gracefully handles the expected 403 error from DriveThruRPG.
    Suggests manual import or using APIs if available.
    """
    print(f"\n[+] Skipping DriveThruRPG {system} products from {url} (scraping blocked).")
    print("  [INFO] DriveThruRPG blocks automated scripts. Please use manual import, publisher data, or official APIs if available.")
    return

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
