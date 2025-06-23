import configparser
from pathlib import Path

def load_config():
    """
    Reads the central conf/config.ini file and returns a structured dictionary of settings.
    This function is the single source of truth for all configuration in the application.

    Returns:
        dict: A dictionary containing parsed and cleaned configuration values.

    Raises:
        FileNotFoundError: If the config.ini file cannot be found.
    """
    config = configparser.ConfigParser()
    
    # Build a path to conf/config.ini relative to this source file's location.
    # This ensures it can be found regardless of where the script is run from.
    config_path = Path(__file__).parent.parent / 'conf' / 'config.ini'

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    config.read(config_path)

    # --- Section [Paths] ---
    # Read the raw string from the config file.
    source_paths_raw = config.get('Paths', 'source_paths', fallback='').strip()
    # Split the comma-separated string into a list of cleaned-up paths.
    # The 'if p.strip()' ensures that empty items (e.g., from a trailing comma) are ignored.
    source_paths = [p.strip() for p in source_paths_raw.split(',') if p.strip()]
    
    # Read the library root path.
    library_root = config.get('Paths', 'library_root', fallback='').strip()

    # --- Section [Settings] ---
    # Read the minimum PDF size, defaulting to 10240 bytes if not specified.
    min_pdf_size = config.getint('Settings', 'min_pdf_size_bytes', fallback=10240)
    
    # --- Section [KnowledgeBaseURLs] ---
    # Read all key-value pairs from the URLs section into a dictionary.
    urls = {key: url for key, url in config.items('KnowledgeBaseURLs')}

    # Return a single, clean dictionary containing all parsed settings.
    return {
        "source_paths": source_paths,
        "library_root": library_root,
        "min_pdf_size_bytes": min_pdf_size,
        "knowledge_base_urls": urls
    }
