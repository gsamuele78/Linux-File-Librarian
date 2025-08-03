# Integration Guide

## Overview

This guide covers all integration capabilities of Linux File Librarian, including internet providers, gaming industry sources, and external system integrations.

## Internet Metadata Providers

### Free Providers (No API Keys Required)

#### CrossRef - Academic Papers
```python
from src.providers.additional_providers import CrossRefProvider

provider = CrossRefProvider()
results = provider.search("machine learning", "academic")
```

**Capabilities:**
- Academic paper metadata
- DOI resolution
- Author and publication information
- Citation data

#### arXiv - Scientific Preprints
```python
from src.providers.additional_providers import ArxivProvider

provider = ArxivProvider()
results = provider.search("neural networks", "academic")
```

**Capabilities:**
- Preprint paper metadata
- Author information
- Abstract and categories
- PDF download links

#### Google Books
```python
from src.providers.additional_providers import GoogleBooksProvider

provider = GoogleBooksProvider()
results = provider.search("Python programming", "book")
```

**Capabilities:**
- Book metadata and descriptions
- Author and publisher information
- Cover images
- ISBN data

#### Wikipedia
```python
from src.providers.additional_providers import WikipediaProvider

provider = WikipediaProvider()
results = provider.search("artificial intelligence", "general")
```

**Capabilities:**
- General knowledge articles
- Summaries and descriptions
- Related topics
- Reference links

#### Internet Archive
```python
from src.providers.additional_providers import InternetArchiveProvider

provider = InternetArchiveProvider()
results = provider.search("historical documents", "document")
```

**Capabilities:**
- Historical document metadata
- Book and media information
- Archive-specific data
- Download links

### Premium Providers (Free API Keys)

#### TMDB - Movies and TV
```python
from src.providers.enhanced_classification_engine import TMDBProvider

provider = TMDBProvider(api_key="your_key")
results = provider.search("The Matrix", "movie")
```

**Setup:**
1. Register at https://www.themoviedb.org/
2. Get API key from account settings
3. Add to `config/config.ini`:
   ```ini
   [APIKeys]
   tmdb_api_key = your_key_here
   ```

**Capabilities:**
- Movie and TV show metadata
- Cast and crew information
- Posters and backdrops
- Ratings and reviews

#### Fanart.tv - High-Quality Artwork
```python
from src.providers.enhanced_classification_engine import FanartTVProvider

provider = FanartTVProvider(api_key="your_key")
artwork = provider.get_movie_artwork("tmdb_id")
```

**Setup:**
1. Register at https://fanart.tv/
2. Get API key
3. Add to configuration:
   ```ini
   [APIKeys]
   fanart_api_key = your_key_here
   ```

**Capabilities:**
- High-resolution posters
- Background artwork
- Banners and logos
- Multiple art types

## Gaming Industry Providers

### International Publishers

#### Paizo - Pathfinder/Starfinder
```python
from src.providers.gaming_providers import PaizoProvider

provider = PaizoProvider()
results = provider.search("Core Rulebook", "gaming")
```

**Capabilities:**
- Pathfinder product metadata
- Starfinder content information
- Product descriptions and artwork
- Game system classification

#### Wizards of the Coast - D&D
```python
from src.providers.gaming_providers import WizardsProvider

provider = WizardsProvider()
results = provider.search("Player's Handbook", "gaming")
```

**Capabilities:**
- D&D product information
- Edition-specific metadata
- Product type classification
- Official content verification

#### DriveThruRPG - General TTRPG
```python
from src.providers.gaming_providers import DriveThruRPGProvider

provider = DriveThruRPGProvider()
results = provider.search("RPG adventure", "gaming")
```

**Capabilities:**
- General TTRPG marketplace content
- Publisher information
- Product ratings and reviews
- Category classification

### Italian Publishers

#### Giochi Uniti
```python
from src.providers.gaming_providers import GiochiUnitiProvider

provider = GiochiUnitiProvider()
results = provider.search("Vampiri", "gaming")
```

**Capabilities:**
- Italian RPG content
- Localized game systems
- Publisher-specific metadata
- Italian language support

#### Acheron Games
```python
from src.providers.gaming_providers import AcheronGamesProvider

provider = AcheronGamesProvider()
results = provider.search("cyberpunk", "gaming")
```

**Capabilities:**
- Italian gaming publisher content
- Original Italian RPG systems
- Genre-specific classification
- Artwork and descriptions

## Configuration

### Provider Configuration
```ini
[InternetProviders]
# Free providers (no API keys needed)
enable_crossref = true
enable_arxiv = true
enable_google_books = true
enable_wikipedia = true
enable_internet_archive = true

# Gaming providers
enable_paizo = true
enable_wizards = true
enable_drivethrurpg = true
enable_giochi_uniti = true
enable_acheron_games = true

[APIKeys]
# Premium providers (free API keys)
tmdb_api_key = your_tmdb_key
fanart_api_key = your_fanart_key
```

### Processing Configuration
```ini
[Processing]
enable_internet_enrichment = true
download_artwork = true
provider_timeout = 30
max_retries = 3
cache_duration = 86400  # 24 hours
```

## Custom Provider Development

### Creating a New Provider
```python
from src.interfaces.base import IMetadataProvider
from typing import Dict, List, Optional

class CustomProvider(IMetadataProvider):
    def __init__(self):
        self.base_url = "https://api.example.com"
        self.session = requests.Session()
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search for content"""
        # Implementation
        pass
    
    def get_details(self, provider_id: str, media_type: str) -> Optional[Dict]:
        """Get detailed metadata"""
        # Implementation
        pass
    
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get artwork URLs"""
        # Implementation
        pass
```

### Registering Custom Providers
```python
# In enhanced_classification_engine.py
from .custom_providers import CustomProvider

def _initialize_providers(self):
    # Add custom provider
    self.providers.append(CustomProvider())
    logger.info("Custom provider initialized")
```

## External System Integration

### Media Center Integration

#### Plex Media Server
- Organized directory structure compatible with Plex
- NFO files for metadata import
- Artwork in standard locations
- Proper naming conventions

#### Jellyfin
- MediaElch-compatible NFO format
- Artwork organization
- Library structure optimization

#### Kodi
- NFO metadata support
- Fanart integration
- Library scanning optimization

### File Manager Integration
- Cross-platform path handling
- Symbolic link support
- Network drive compatibility
- Permission management

### Backup System Integration
- Non-destructive processing (originals preserved)
- Organized library structure for backup
- Metadata preservation
- Incremental backup support

## API Integration Patterns

### Error Handling
```python
from src.enterprise.enterprise_error_handling import with_error_handling

@with_error_handling(operation="provider_search", component="CustomProvider")
def search_with_retry(self, query: str, media_type: str):
    # Implementation with automatic retry and error handling
    pass
```

### Caching
```python
from functools import lru_cache
import time

@lru_cache(maxsize=1000)
def cached_search(self, query: str, media_type: str, timestamp: int):
    # Cache results for specified duration
    pass

def search(self, query: str, media_type: str):
    # Use timestamp for cache invalidation
    timestamp = int(time.time() / 3600)  # Hourly cache
    return self.cached_search(query, media_type, timestamp)
```

### Rate Limiting
```python
import time
from threading import Lock

class RateLimitedProvider:
    def __init__(self):
        self.last_request = 0
        self.min_interval = 1.0  # Minimum seconds between requests
        self.lock = Lock()
    
    def make_request(self, url: str):
        with self.lock:
            elapsed = time.time() - self.last_request
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            
            response = requests.get(url)
            self.last_request = time.time()
            return response
```

## Monitoring Integration

### Metrics Collection
```python
from src.interfaces.base import IMetricsCollector

def track_provider_performance(provider_name: str, response_time: float, success: bool):
    metrics.record_metric(f"provider.{provider_name}.response_time", response_time)
    metrics.increment_counter(f"provider.{provider_name}.requests")
    if success:
        metrics.increment_counter(f"provider.{provider_name}.success")
    else:
        metrics.increment_counter(f"provider.{provider_name}.errors")
```

### Health Checks
```python
def check_provider_health(provider):
    try:
        # Test basic functionality
        results = provider.search("test", "test")
        return {"status": "healthy", "response_time": response_time}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

## Best Practices

### Provider Implementation
- Implement proper error handling and timeouts
- Use session pooling for HTTP requests
- Implement exponential backoff for retries
- Cache results appropriately
- Respect rate limits and terms of service

### Configuration Management
- Use environment variables for sensitive data
- Provide sensible defaults
- Validate configuration on startup
- Support configuration reloading

### Performance Optimization
- Use connection pooling
- Implement proper caching strategies
- Batch requests when possible
- Monitor and optimize slow operations

### Security Considerations
- Validate all external data
- Use HTTPS for all external communications
- Implement proper authentication
- Log security-relevant events

This integration guide provides comprehensive coverage of all integration capabilities and patterns used in Linux File Librarian.