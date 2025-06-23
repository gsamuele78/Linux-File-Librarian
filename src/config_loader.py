import configparser
from pathlib import Path

def load_config():
    """Reads conf/config.ini and returns a structured dictionary of settings."""
    config = configparser.ConfigParser()
    config_path = Path(__file__).parent.parent / 'conf' / 'config.ini'

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    config.read(config_path)

    # --- Paths ---
    source_paths_raw = config.get('Paths', 'source_paths', fallback='').strip()
    source_paths = [p.strip() for p in source_paths_raw.split(',') if p.strip()]
    library_root = config.get('Paths', 'library_root', fallback='').strip()

    # --- Settings ---
    min_pdf_size = config.getint('Settings', 'min_pdf_size_bytes', fallback=10240)
    
    # --- URLs ---
    urls = {key: url for key, url in config.items('KnowledgeBaseURLs')}

    return {
        "source_paths": source_paths,
        "library_root": library_root,
        "min_pdf_size_bytes": min_pdf_size,
        "knowledge_base_urls": urls
    }
