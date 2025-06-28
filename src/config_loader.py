import configparser
from pathlib import Path

class CaseConfigParser(configparser.ConfigParser):
    def optionxform(self, optionstr):
        return optionstr

def load_config():
    """
    Reads the central conf/config.ini file and returns a structured dictionary of settings.
    Robustly parses all expected sections and handles missing or malformed entries gracefully.

    Returns:
        dict: A dictionary containing parsed and cleaned configuration values.

    Raises:
        FileNotFoundError: If the config.ini file cannot be found.
        ValueError: If required configuration values are missing or invalid.
    """
    config = CaseConfigParser(interpolation=None)

    config_path = Path(__file__).parent.parent / 'conf' / 'config.ini'
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    try:
        config.read(config_path)
    except configparser.Error as e:
        raise ValueError(f"Error parsing configuration file: {e}")

    # --- Section [Paths] ---
    source_paths = []
    library_root = ""
    if config.has_section('Paths'):
        source_paths_raw = config.get('Paths', 'source_paths', fallback='').strip()
        # Support both comma and semicolon as separators for flexibility
        if ',' in source_paths_raw:
            sep = ','
        elif ';' in source_paths_raw:
            sep = ';'
        else:
            sep = ','  # fallback to comma if neither found
        source_paths = [p.strip() for p in source_paths_raw.split(sep) if p.strip()]
        library_root = config.get('Paths', 'library_root', fallback='').strip()
    else:
        raise ValueError("Missing required [Paths] section in config.ini")

    # --- Section [Settings] ---
    min_pdf_size = 10240
    if config.has_section('Settings'):
        try:
            min_pdf_size = config.getint('Settings', 'min_pdf_size_bytes', fallback=10240)
        except ValueError:
            min_pdf_size = 10240
    # else: use default

    # --- Section [KnowledgeBaseURLs] ---
    urls = {}
    url_languages = {}
    if config.has_section('KnowledgeBaseURLs'):
        for key, url in config.items('KnowledgeBaseURLs'):
            url = url.strip()
            if url and not url.startswith('#'):
                # Detect language from key suffix or default to English
                if key.endswith('_it'):
                    urls[key] = url
                    url_languages[key] = 'Italian'
                elif key.endswith('_en'):
                    urls[key] = url
                    url_languages[key] = 'English'
                else:
                    urls[key] = url
                    url_languages[key] = 'English'
    # else: leave urls empty

    # Validate required fields
    if not source_paths or not library_root:
        raise ValueError("Config must specify at least one source_path and a library_root in [Paths]")

    return {
        "source_paths": source_paths,
        "library_root": library_root,
        "min_pdf_size_bytes": min_pdf_size,
        "knowledge_base_urls": urls,
        "knowledge_base_url_languages": url_languages
    }
