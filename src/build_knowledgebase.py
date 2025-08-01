import sqlite3
import requests
from bs4 import BeautifulSoup, Tag
import re
import os
import time
import sys
import gc
from urllib.parse import urljoin, urlparse
from src.config_loader import load_config
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

# Global session with connection pooling
_session = None

def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        _session.mount("http://", adapter)
        _session.mount("https://", adapter)
        _session.headers.update(HEADERS)
    return _session

def _cleanup_temp_objects():
    """Clean up temporary objects and force garbage collection"""
    gc.collect()

def safe_request(url, retries=3, delay=2):
    """
    Makes a web request with retries and handles network errors robustly.
    Returns BeautifulSoup object or None.
    Uses connection pooling for better resource management.
    """
    session = get_session()
    
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()
            
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' not in content_type and 'text/xml' not in content_type:
                print(f"  [ERROR] Non-HTML/XML content at {url}: {content_type}", file=sys.stderr)
                return None
            
            # Use lxml parser with error handling
            try:
                soup = BeautifulSoup(response.content, 'lxml')
            except Exception:
                # Fallback to html.parser
                soup = BeautifulSoup(response.content, 'html.parser')
            
            return soup
            
        except requests.exceptions.RequestException as e:
            print(f"  [ERROR] Could not fetch {url} (attempt {attempt+1}/{retries}): {e}", file=sys.stderr)
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
        except Exception as e:
            print(f"  [ERROR] Unexpected error fetching {url}: {e}", file=sys.stderr)
            break
    
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
    Enhanced with resource management and memory cleanup.
    """
    results = []
    errors = []
    
    # Limit concurrent workers based on list size
    max_workers = min(3, len(url_list), os.cpu_count() or 2)
    
    def safe_parse(item):
        try:
            result = parse_func(item, *args, **kwargs)
            # Force cleanup after each parse
            _cleanup_temp_objects()
            return result
        except Exception as e:
            print(f"  [ERROR] Exception in parallel parse for {item}: {e}", file=sys.stderr)
            errors.append((item, str(e)))
            return []
    
    # Process in smaller batches to prevent memory buildup
    batch_size = max(1, len(url_list) // max_workers)
    
    for i in range(0, len(url_list), batch_size):
        batch = url_list[i:i + batch_size]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(batch))) as executor:
            future_to_item = {executor.submit(safe_parse, item): item for item in batch}
            
            for future in concurrent.futures.as_completed(future_to_item, timeout=300):
                try:
                    res = future.result(timeout=60)
                    if res:
                        results.extend(res)
                except concurrent.futures.TimeoutError:
                    item = future_to_item[future]
                    print(f"  [ERROR] Timeout parsing {item}", file=sys.stderr)
                    errors.append((item, "Timeout"))
                except Exception as e:
                    item = future_to_item[future]
                    print(f"  [ERROR] Future exception for {item}: {e}", file=sys.stderr)
                    errors.append((item, str(e)))
        
        # Cleanup between batches
        self._cleanup_temp_objects()
        time.sleep(0.5)  # Brief pause between batches
    
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
    Enhanced with better pattern matching and error recovery.
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
                # --- Enhanced English/Default logic: multiple pattern attempts ---
                patterns = [
                    # Pattern 1: code + optional module + title
                    re.compile(r'(\S+)(?:\s+([A-Z]{1,4}\d{1,3}))?\s+([^\d]+?)(?=\s+\S+\s+[A-Z]{0,4}\d{1,5}\s+|\s+\S+\s+[^A-Z\d]|$)'),
                    # Pattern 2: simple code + title
                    re.compile(r'(\d{4,5}|[A-Z]{1,4}\d{1,5})\s+(.+?)(?=\s+\d{4,5}|\s+[A-Z]{1,4}\d{1,5}|$)'),
                    # Pattern 3: any word + title
                    re.compile(r'(\S+)\s+([A-Z][^\n]{5,100})'),
                ]
                
                matches = []
                for pattern in patterns:
                    matches = list(pattern.finditer(cell_text))
                    if matches:
                        break
                
                # Enhanced fallback with multiple attempts
                if not matches:
                    fallback_patterns = [
                        r'^(\S+)\s+(.*)',
                        r'(\d+)\s*[:-]\s*(.+)',
                        r'([A-Z]+\d+)\s+(.+)',
                    ]
                    for fb_pattern in fallback_patterns:
                        m = re.match(fb_pattern, cell_text)
                        if m and len(m.groups()) >= 2:
                            matches = [m]
                            break
                if len(matches) > 1:
                    print(f"        [MULTI-PRODUCT] Found {len(matches)} products in one cell [LANG: {lang}]")
                for match in matches:
                    try:
                        if hasattr(match, 'groups'):
                            product_code = match.group(1).upper() if match.group(1) else None
                            if len(match.groups()) > 2 and match.group(3):
                                module_code = match.group(2) if match.group(2) else None
                                title = match.group(3).strip()
                            else:
                                module_code = None
                                title = match.group(2).strip() if len(match.groups()) > 1 else match.group(1)
                        else:
                            product_code = match.group(1).upper() if match.group(1) else None
                            title = match.group(2).strip() if len(match.groups()) > 1 else cell_text.strip()
                            module_code = None
                        
                        # Enhanced title validation and cleanup
                        if not title or len(title) < 2:
                            print(f"        [SKIP ROW] No valid title in match: '{cell_text}' [LANG: {lang}]")
                            continue
                        
                        # Clean up title
                        title = re.sub(r'^[\s\-:]+|[\s\-:]+$', '', title)
                        title = re.sub(r'\s+', ' ', title)
                        
                        if len(title) < 3:
                            continue
                    except Exception as e:
                        print(f"        [ERROR] Failed to parse match: {e} [LANG: {lang}]")
                        continue
                    category = headers[col_idx] if col_idx < len(headers) and headers[col_idx] != f"Category {col_idx+1}" else section_text
                    
                    # Enhanced game system detection
                    game_system = "D&D"  # default
                    edition = "N/A"
                    
                    # Check URL and section text for system/edition info
                    url_lower = section_url.lower()
                    section_lower = section_text.lower()
                    
                    if "add" in url_lower or "ad&d" in section_lower:
                        game_system = "AD&D"
                        if "1e" in url_lower or "first" in section_lower:
                            edition = "1E"
                        elif "2e" in url_lower or "second" in section_lower:
                            edition = "2E"
                    elif "3e" in url_lower or "3.5" in url_lower:
                        game_system = "D&D"
                        edition = "3E" if "3e" in url_lower else "3.5E"
                    elif "4e" in url_lower:
                        game_system = "D&D"
                        edition = "4E"
                    elif "5e" in url_lower:
                        game_system = "D&D"
                        edition = "5E"
                    elif "basic" in section_lower:
                        game_system = "D&D"
                        edition = "Basic"
                    # Enhanced duplicate detection
                    normalized_title = re.sub(r'[^a-zA-Z0-9\s]', '', title.lower()).strip()
                    key = (product_code, normalized_title, game_system, edition, lang)
                    if key in seen_products:
                        print(f"        [DUPLICATE] {title} | Code: {product_code} | Cat: {category} [LANG: {lang}]")
                        continue
                    if conn:
                        try:
                            conn.execute(
                                "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (product_code, title, game_system, edition, category, lang, section_url)
                            )
                            print(f"        [INSERTED] {title} | Code: {product_code} | Cat: {category} [LANG: {lang}]")
                            total_added += 1
                            seen_products.add(key)
                        except Exception as e:
                            print(f"    [ERROR] DB insert failed for {title}: {e}", file=sys.stderr)
                    else:
                        products.append((product_code, title, game_system, edition, category, lang, section_url))
                        seen_products.add(key)
    if table_count == 0:
        print(f"      [DEBUG] No tables found in section page [LANG: {lang}]")
        # Try to extract from plain text as fallback
        text = soup.get_text()
        text_patterns = [
            r'(\d{4,5})\s+([A-Z][^\n]{10,100})',
            r'([A-Z]{1,4}\d{1,5})\s+([A-Z][^\n]{10,100})',
        ]
        for pattern in text_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                code = match.group(1)
                title = normalize_whitespace(match.group(2))
                if len(title) > 5 and len(title) < 150:
                    if conn:
                        try:
                            conn.execute(
                                "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (code, title, "D&D", "N/A", section_text, lang, section_url)
                            )
                            total_added += 1
                        except Exception:
                            pass
                    else:
                        products.append((code, title, "D&D", "N/A", section_text, lang, section_url))
    
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
        "title": {"title", "titolo", "titolo originale", "name", "nome"},
        "code": {"code", "codice", "codice prodotto", "product code", "isbn"},
        "edition": {"edition", "edizione", "version", "versione"}
    }
    total_added = 0
    
    # Enhanced table detection - include more table types
    tables = []
    for tag in content.find_all('table'):
        if isinstance(tag, Tag):
            cls = tag.get('class', [])
            if isinstance(cls, str):
                cls = [cls]
            if any(c in cls for c in ['wikitable', 'sortable', 'navbox', 'infobox']):
                tables.append(tag)
    
    # Also parse lists if no tables found
    if not tables:
        for ul in content.find_all(['ul', 'ol']):
            if isinstance(ul, Tag):
                tables.append(ul)
    
    def parse_table(table):
        nonlocal current_edition
        products = []
        # Find preceding h2/h3 for edition
        prev = table.find_previous(['h2', 'h3', 'h4'])
        if prev:
            headline = prev.find(class_='mw-headline') if hasattr(prev, 'find') else None
            if headline:
                edition_text = normalize_whitespace(headline.get_text())
                # Extract edition info from headers
                edition_patterns = [r'(\d+(?:\.\d+)?e?)', r'(first|second|third|fourth|fifth|1st|2nd|3rd|4th|5th)', r'(basic|expert|companion|master|immortal)']
                for pattern in edition_patterns:
                    match = re.search(pattern, edition_text, re.IGNORECASE)
                    if match:
                        current_edition = match.group(1)
                        break
        
        if table.name in ['ul', 'ol']:
            # Parse list items
            for li in table.find_all('li'):
                text = normalize_whitespace(li.get_text())
                if len(text) > 3 and not any(skip in text.lower() for skip in ['list of', 'category:', 'see also']):
                    # Try to extract code and title
                    code_match = re.search(r'\b([A-Z]{1,4}\d{1,5}|\d{4,5})\b', text)
                    code = code_match.group(1) if code_match else None
                    title = re.sub(r'\b([A-Z]{1,4}\d{1,5}|\d{4,5})\b', '', text).strip()
                    if not title:
                        title = text
                    products.append((code, title, system, current_edition, category, lang, url))
        else:
            # Parse table rows
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
    
    # Enhanced selectors for different RPGGeek layouts
    selectors = [
        'table.geekitem_table tr',
        '.collection_table tr',
        '.game_table tr',
        'tr.collection_objectname',
        '.objectname a'
    ]
    
    rows = []
    for selector in selectors:
        found = soup.select(selector)
        if found:
            rows.extend(found)
            break
    
    def parse_row(row):
        if row.name == 'a':
            title = normalize_whitespace(row.get_text())
            if title and len(title) > 2:
                return (None, title, "RPG", None, "Module/Adventure", lang, url)
        else:
            cols = row.find_all(['td', 'th'])
            if len(cols) >= 2:
                # Try different column positions for title
                for i in range(min(3, len(cols))):
                    title_elem = cols[i].find('a') or cols[i]
                    title = normalize_whitespace(title_elem.get_text())
                    if title and len(title) > 2 and not title.isdigit():
                        # Extract system info if available
                        system = "RPG"
                        if any(sys in title.lower() for sys in ['d&d', 'dungeons', 'dragons']):
                            system = "D&D"
                        elif 'pathfinder' in title.lower():
                            system = "Pathfinder"
                        return (None, title, system, None, "Module/Adventure", lang, url)
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

# Enhanced generic parser for unknown sources
def parse_generic_fallback(conn, url, lang):
    """Fallback parser that attempts to extract RPG-related content from any page"""
    print(f"\n[+] Parsing with generic fallback from {url} (language: {lang})...")
    soup = safe_request(url)
    if not soup:
        return
    
    cursor = conn.cursor()
    total_added = 0
    
    # Look for common RPG patterns in text
    text = soup.get_text()
    rpg_patterns = [
        r'([A-Z]{1,4}\d{1,5})\s+([^\n]{10,100})',  # Product codes
        r'ISBN[:\s]*(\d{10,13})\s+([^\n]{10,100})',  # ISBN codes
        r'(\d{4,5})\s+([A-Z][^\n]{10,100})',  # Numeric codes
    ]
    
    for pattern in rpg_patterns:
        matches = re.finditer(pattern, text)
        for match in matches:
            code = match.group(1)
            title = normalize_whitespace(match.group(2))
            if len(title) > 10 and len(title) < 200:
                system = "RPG"
                if any(term in title.lower() for term in ['d&d', 'dungeons', 'dragons']):
                    system = "D&D"
                elif 'pathfinder' in title.lower():
                    system = "Pathfinder"
                
                try:
                    cursor.execute(
                        "INSERT OR IGNORE INTO products (product_code, title, game_system, edition, category, language, source_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (code, title, system, None, "Module/Adventure", lang, url)
                    )
                    total_added += 1
                except Exception as e:
                    print(f"    [ERROR] DB insert failed: {e}", file=sys.stderr)
    
    conn.commit()
    print(f"[SUCCESS] Generic fallback parsing complete. Added {total_added} entries.")

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

def main():
    print("--- Building Enhanced Knowledge Base from Online Sources ---")
    connection = None
    
    try:
        # Load configuration
        config = load_config()
        urls_to_scrape = config['knowledge_base_urls']
        url_languages = config.get('knowledge_base_url_languages', {})
        
        if not urls_to_scrape:
            print("[WARNING] No URLs configured for scraping")
            return 0
        
        # Initialize database
        connection = init_db()
        
        # Process URLs with resource management
        processed_count = 0
        failed_count = 0
        
        for key, url in urls_to_scrape.items():
            print(f"\n[INFO] Processing: {key} ({processed_count + 1}/{len(urls_to_scrape)})")
            lang = url_languages.get(key, "English")
            
            try:
                if key in PARSER_MAPPING:
                    parser_func, _ = PARSER_MAPPING[key]
                    parser_func(connection, url, lang)
                else:
                    print(f"  [WARNING] No specific parser for '{key}'. Trying generic fallback...")
                    parse_generic_fallback(connection, url, lang)
                
                processed_count += 1
                
                # Resource management between sources
                gc.collect()
                time.sleep(2)  # Throttle between sources
                
            except KeyboardInterrupt:
                print("\n[INFO] Process interrupted by user")
                break
            except Exception as e:
                print(f"  [ERROR] Failed to process '{key}': {e}", file=sys.stderr)
                failed_count += 1
                
                # Try generic fallback if specific parser failed
                if key in PARSER_MAPPING:
                    print(f"  [INFO] Attempting generic fallback for '{key}'...")
                    try:
                        parse_generic_fallback(connection, url, lang)
                        processed_count += 1
                    except Exception as fallback_e:
                        print(f"  [ERROR] Generic fallback also failed: {fallback_e}", file=sys.stderr)
        
        print(f"\n[SUMMARY] Processed: {processed_count}, Failed: {failed_count}")
        
        return 0
        
    except Exception as e:
        print(f"[FATAL] Unhandled error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        # Cleanup and statistics
        try:
            if connection:
                print_statistics(connection)
                connection.close()
            
            # Close global session
            global _session
            if _session:
                _session.close()
                _session = None
                
        except Exception as cleanup_error:
            print(f"[WARNING] Error during cleanup: {cleanup_error}", file=sys.stderr)

def print_statistics(connection):
    """Print database statistics"""
    try:
        print("\n--- Knowledge Base Statistics ---")
        cursor = connection.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM products")
        total_count = cursor.fetchone()[0]
        print(f"Total unique products: {total_count}")
        
        if total_count == 0:
            print("No products found in database.")
            return
        
        cursor.execute("SELECT DISTINCT game_system FROM products ORDER BY game_system")
        game_systems = [row[0] for row in cursor.fetchall()]
        
        for system in game_systems[:10]:  # Limit to first 10 systems
            print(f"\nBreakdown for: {system}")
            
            cursor.execute("SELECT COUNT(*) FROM products WHERE game_system = ?", (system,))
            system_count = cursor.fetchone()[0]
            print(f"  Total Entries: {system_count}")
            
            # Language breakdown
            cursor.execute("SELECT language, COUNT(*) FROM products WHERE game_system = ? GROUP BY language LIMIT 5", (system,))
            lang_results = cursor.fetchall()
            if lang_results:
                print("  By Language:")
                for lang, count in lang_results:
                    print(f"    {lang or 'N/A'}: {count}")
            
            # Edition breakdown
            cursor.execute("SELECT edition, COUNT(*) FROM products WHERE game_system = ? GROUP BY edition ORDER BY COUNT(*) DESC LIMIT 5", (system,))
            edition_results = cursor.fetchall()
            if edition_results:
                print("  By Edition:")
                for edition, count in edition_results:
                    print(f"    {edition or 'N/A'}: {count}")
        
        print(f"\n--- Knowledge Base Build Complete! ---")
        print(f"Enhanced database saved to '{DB_FILE}'")
        
    except Exception as e:
        print(f"[ERROR] Could not generate statistics: {e}", file=sys.stderr)

if __name__ == "__main__":
    sys.exit(main())
