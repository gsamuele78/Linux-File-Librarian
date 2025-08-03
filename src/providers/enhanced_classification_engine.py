#!/usr/bin/env python3
"""
Enhanced Classification Engine

Advanced classification system inspired by MediaElch with internet-based
metadata enrichment, fanart downloading, and comprehensive content analysis.
"""

import logging
import requests
import json
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
import time

logger = logging.getLogger(__name__)


@dataclass
class EnhancedMetadata:
    """Comprehensive metadata container"""
    # Core identification
    title: Optional[str] = None
    original_title: Optional[str] = None
    year: Optional[int] = None
    imdb_id: Optional[str] = None
    tmdb_id: Optional[str] = None
    isbn: Optional[str] = None
    
    # Content details
    plot: Optional[str] = None
    tagline: Optional[str] = None
    genres: List[str] = None
    tags: List[str] = None
    rating: Optional[float] = None
    votes: Optional[int] = None
    
    # People
    director: Optional[str] = None
    writers: List[str] = None
    actors: List[str] = None
    author: Optional[str] = None
    publisher: Optional[str] = None
    
    # Technical
    runtime: Optional[int] = None
    resolution: Optional[str] = None
    audio_codec: Optional[str] = None
    video_codec: Optional[str] = None
    file_size: Optional[int] = None
    
    # Artwork URLs
    poster_url: Optional[str] = None
    fanart_url: Optional[str] = None
    banner_url: Optional[str] = None
    thumb_url: Optional[str] = None
    
    # Classification
    content_rating: Optional[str] = None
    country: Optional[str] = None
    language: Optional[str] = None
    
    def __post_init__(self):
        if self.genres is None:
            self.genres = []
        if self.tags is None:
            self.tags = []
        if self.writers is None:
            self.writers = []
        if self.actors is None:
            self.actors = []


class MetadataProvider(ABC):
    """Abstract metadata provider"""
    
    @abstractmethod
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search for content"""
        pass
    
    @abstractmethod
    def get_details(self, provider_id: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get detailed metadata"""
        pass
    
    @abstractmethod
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get artwork URLs"""
        pass


class TMDBProvider(MetadataProvider):
    """The Movie Database provider"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or "demo_key"  # Use demo key or configure
        self.base_url = "https://api.themoviedb.org/3"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search TMDB"""
        if not self.api_key or self.api_key == "demo_key":
            return []
        
        try:
            endpoint = "search/movie" if media_type == "movie" else "search/tv"
            url = f"{self.base_url}/{endpoint}"
            
            params = {
                'api_key': self.api_key,
                'query': query,
                'language': 'en-US'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return data.get('results', [])[:5]  # Top 5 results
            
        except Exception as e:
            logger.debug(f"TMDB search failed: {e}")
            return []
    
    def get_details(self, provider_id: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get detailed metadata from TMDB"""
        if not self.api_key or self.api_key == "demo_key":
            return None
        
        try:
            endpoint = "movie" if media_type == "movie" else "tv"
            url = f"{self.base_url}/{endpoint}/{provider_id}"
            
            params = {
                'api_key': self.api_key,
                'language': 'en-US',
                'append_to_response': 'credits,images'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_tmdb_data(data, media_type)
            
        except Exception as e:
            logger.debug(f"TMDB details failed: {e}")
            return None
    
    def _parse_tmdb_data(self, data: Dict, media_type: str) -> EnhancedMetadata:
        """Parse TMDB response to EnhancedMetadata"""
        metadata = EnhancedMetadata()
        
        # Basic info
        metadata.title = data.get('title') or data.get('name')
        metadata.original_title = data.get('original_title') or data.get('original_name')
        metadata.plot = data.get('overview')
        metadata.tagline = data.get('tagline')
        metadata.rating = data.get('vote_average')
        metadata.votes = data.get('vote_count')
        metadata.tmdb_id = str(data.get('id'))
        
        # Year extraction
        release_date = data.get('release_date') or data.get('first_air_date')
        if release_date:
            try:
                metadata.year = int(release_date.split('-')[0])
            except (ValueError, IndexError):
                pass
        
        # Genres
        genres = data.get('genres', [])
        metadata.genres = [g['name'] for g in genres]
        
        # Runtime
        if media_type == "movie":
            metadata.runtime = data.get('runtime')
        
        # Credits
        credits = data.get('credits', {})
        crew = credits.get('crew', [])
        cast = credits.get('cast', [])
        
        # Director
        directors = [c['name'] for c in crew if c.get('job') == 'Director']
        if directors:
            metadata.director = directors[0]
        
        # Writers
        writers = [c['name'] for c in crew if c.get('job') in ['Writer', 'Screenplay']]
        metadata.writers = writers[:3]  # Top 3
        
        # Actors
        actors = [c['name'] for c in cast[:5]]  # Top 5
        metadata.actors = actors
        
        # Artwork
        if data.get('poster_path'):
            metadata.poster_url = f"https://image.tmdb.org/t/p/w500{data['poster_path']}"
        if data.get('backdrop_path'):
            metadata.fanart_url = f"https://image.tmdb.org/t/p/w1280{data['backdrop_path']}"
        
        return metadata
    
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get artwork URLs"""
        artwork = {}
        
        try:
            endpoint = "movie" if media_type == "movie" else "tv"
            url = f"{self.base_url}/{endpoint}/{provider_id}/images"
            
            params = {'api_key': self.api_key}
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Posters
            posters = data.get('posters', [])
            if posters:
                artwork['poster'] = f"https://image.tmdb.org/t/p/w500{posters[0]['file_path']}"
            
            # Backdrops
            backdrops = data.get('backdrops', [])
            if backdrops:
                artwork['fanart'] = f"https://image.tmdb.org/t/p/w1280{backdrops[0]['file_path']}"
            
        except Exception as e:
            logger.debug(f"TMDB artwork failed: {e}")
        
        return artwork


class OpenLibraryProvider(MetadataProvider):
    """Open Library provider for books"""
    
    def __init__(self):
        self.base_url = "https://openlibrary.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search Open Library"""
        if media_type != "book":
            return []
        
        try:
            url = f"{self.base_url}/search.json"
            params = {
                'q': query,
                'limit': 5
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return data.get('docs', [])
            
        except Exception as e:
            logger.debug(f"OpenLibrary search failed: {e}")
            return []
    
    def get_details(self, provider_id: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get book details from Open Library"""
        try:
            url = f"{self.base_url}/works/{provider_id}.json"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_openlibrary_data(data)
            
        except Exception as e:
            logger.debug(f"OpenLibrary details failed: {e}")
            return None
    
    def _parse_openlibrary_data(self, data: Dict) -> EnhancedMetadata:
        """Parse Open Library response"""
        metadata = EnhancedMetadata()
        
        metadata.title = data.get('title')
        metadata.plot = data.get('description', {}).get('value') if isinstance(data.get('description'), dict) else data.get('description')
        
        # Authors
        authors = data.get('authors', [])
        if authors:
            # Get author details
            author_key = authors[0].get('author', {}).get('key')
            if author_key:
                try:
                    author_url = f"{self.base_url}{author_key}.json"
                    author_response = self.session.get(author_url, timeout=5)
                    author_data = author_response.json()
                    metadata.author = author_data.get('name')
                except Exception:
                    pass
        
        # Subjects as genres
        subjects = data.get('subjects', [])
        metadata.genres = subjects[:5]  # Top 5 subjects
        
        return metadata
    
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get book cover artwork"""
        artwork = {}
        
        try:
            # Open Library covers
            cover_url = f"https://covers.openlibrary.org/b/olid/{provider_id}-L.jpg"
            
            # Check if cover exists
            response = self.session.head(cover_url, timeout=5)
            if response.status_code == 200:
                artwork['poster'] = cover_url
                
        except Exception as e:
            logger.debug(f"OpenLibrary artwork failed: {e}")
        
        return artwork


class FanartTVProvider:
    """Fanart.tv provider for high-quality artwork"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.base_url = "https://webservice.fanart.tv/v3"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
        if self.api_key:
            self.session.headers.update({
                'api-key': self.api_key
            })
    
    def get_movie_artwork(self, tmdb_id: str) -> Dict[str, str]:
        """Get movie artwork from Fanart.tv"""
        if not self.api_key:
            return {}
        
        try:
            url = f"{self.base_url}/movies/{tmdb_id}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            artwork = {}
            
            # Movie poster
            if 'movieposter' in data:
                posters = data['movieposter']
                if posters:
                    artwork['poster'] = posters[0]['url']
            
            # Movie background
            if 'moviebackground' in data:
                backgrounds = data['moviebackground']
                if backgrounds:
                    artwork['fanart'] = backgrounds[0]['url']
            
            # Movie banner
            if 'moviebanner' in data:
                banners = data['moviebanner']
                if banners:
                    artwork['banner'] = banners[0]['url']
            
            return artwork
            
        except Exception as e:
            logger.debug(f"Fanart.tv movie artwork failed: {e}")
            return {}


class EnhancedClassificationEngine:
    """Advanced classification engine with internet enrichment"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.providers = []
        self.fanart_provider = None
        self.cache = {}
        self.cache_dir = Path('metadata_cache')
        self.cache_dir.mkdir(exist_ok=True)
        
        # Initialize providers
        self._initialize_providers()
    
    def _initialize_providers(self):
        """Initialize metadata providers"""
        
        # TMDB for movies/TV
        tmdb_key = self.config.get('tmdb_api_key')
        if tmdb_key:
            self.providers.append(TMDBProvider(tmdb_key))
            logger.info("TMDB provider initialized")
        else:
            logger.info("TMDB API key not configured - movie/TV enrichment disabled")
        
        # Open Library for books
        self.providers.append(OpenLibraryProvider())
        logger.info("OpenLibrary provider initialized")
        
        # Additional free providers
        from src.providers.additional_providers import (
            CrossRefProvider, ArxivProvider, GoogleBooksProvider, 
            WikipediaProvider, InternetArchiveProvider
        )
        
        # CrossRef for academic papers
        self.providers.append(CrossRefProvider())
        logger.info("CrossRef provider initialized")
        
        # arXiv for preprints
        self.providers.append(ArxivProvider())
        logger.info("arXiv provider initialized")
        
        # Google Books
        self.providers.append(GoogleBooksProvider())
        logger.info("Google Books provider initialized")
        
        # Wikipedia for general knowledge
        self.providers.append(WikipediaProvider())
        logger.info("Wikipedia provider initialized")
        
        # Internet Archive for books/documents
        self.providers.append(InternetArchiveProvider())
        logger.info("Internet Archive provider initialized")
        
        # Gaming industry providers
        from src.providers.gaming_providers import PaizoProvider, WizardsProvider, DriveThruRPGProvider, GiochiUnitiProvider, AcheronGamesProvider
        
        # Paizo for Pathfinder/Starfinder
        self.providers.append(PaizoProvider())
        logger.info("Paizo provider initialized")
        
        # Wizards of the Coast for D&D
        self.providers.append(WizardsProvider())
        logger.info("Wizards of the Coast provider initialized")
        
        # DriveThruRPG for general TTRPG
        self.providers.append(DriveThruRPGProvider())
        logger.info("DriveThruRPG provider initialized")
        
        # Italian gaming providers
        self.providers.append(GiochiUnitiProvider())
        logger.info("Giochi Uniti provider initialized")
        
        self.providers.append(AcheronGamesProvider())
        logger.info("Acheron Games provider initialized")
        
        # Fanart.tv for high-quality artwork
        fanart_key = self.config.get('fanart_api_key')
        if fanart_key:
            self.fanart_provider = FanartTVProvider(fanart_key)
            logger.info("Fanart.tv provider initialized")
    
    def classify_and_enrich(self, file_info: Dict) -> Dict:
        """Enhanced classification with internet enrichment"""
        
        # Start with basic classification
        from src.services.enhanced_classification_service import EnhancedClassificationService
        basic_service = EnhancedClassificationService(self.config)
        classified = basic_service.classify_file(file_info)
        
        # Determine if we should enrich with internet data
        if self._should_enrich(classified):
            enriched_metadata = self._enrich_with_internet(classified)
            if enriched_metadata:
                classified['enhanced_metadata'].update(asdict(enriched_metadata))
                classified['internet_enriched'] = True
                
                # Download artwork if enabled
                if self.config.get('download_artwork', False):
                    self._download_artwork(classified, enriched_metadata)
        
        return classified
    
    def _should_enrich(self, file_info: Dict) -> bool:
        """Determine if file should be enriched with internet data"""
        
        # Skip if internet enrichment disabled
        if not self.config.get('enable_internet_enrichment', False):
            return False
        
        # Enrich media files
        media_type = file_info.get('media_type', '').lower()
        if media_type in ['video', 'pdf']:
            return True
        
        # Enrich based on categories
        category = file_info.get('game_system', '').lower()
        if category in ['media', 'documents']:
            return True
        
        return False
    
    def _enrich_with_internet(self, file_info: Dict) -> Optional[EnhancedMetadata]:
        """Enrich file with internet metadata"""
        
        # Generate search query
        query = self._generate_search_query(file_info)
        if not query:
            return None
        
        # Check cache first
        cache_key = hashlib.md5(query.encode()).hexdigest()
        cached_result = self._get_cached_metadata(cache_key)
        if cached_result:
            return cached_result
        
        # Determine media type for providers
        media_type = self._determine_provider_media_type(file_info)
        
        # Search providers
        for provider in self.providers:
            try:
                search_results = provider.search(query, media_type)
                
                if search_results:
                    # Get details for best match
                    best_match = self._select_best_match(search_results, query)
                    if best_match:
                        provider_id = str(best_match.get('id'))
                        metadata = provider.get_details(provider_id, media_type)
                        
                        if metadata:
                            # Get additional artwork
                            artwork = provider.get_artwork(provider_id, media_type)
                            if artwork:
                                if 'poster' in artwork:
                                    metadata.poster_url = artwork['poster']
                                if 'fanart' in artwork:
                                    metadata.fanart_url = artwork['fanart']
                                if 'banner' in artwork:
                                    metadata.banner_url = artwork['banner']
                            
                            # Cache result
                            self._cache_metadata(cache_key, metadata)
                            
                            logger.info(f"Enriched {file_info.get('path')} with {provider.__class__.__name__}")
                            return metadata
                            
            except Exception as e:
                logger.debug(f"Provider {provider.__class__.__name__} failed: {e}")
        
        return None
    
    def _generate_search_query(self, file_info: Dict) -> Optional[str]:
        """Generate search query from file info"""
        
        # Try enhanced metadata first
        enhanced_metadata = file_info.get('enhanced_metadata', {})
        
        # Use title if available
        title = enhanced_metadata.get('title')
        if title and title != 'Unknown':
            return title
        
        # Use filename without extension
        file_path = Path(file_info.get('path', ''))
        filename = file_path.stem
        
        # Clean filename for search
        query = filename.replace('_', ' ').replace('-', ' ')
        
        # Remove common patterns
        import re
        query = re.sub(r'\b(19|20)\d{2}\b', '', query)  # Remove years
        query = re.sub(r'\b(720p|1080p|4K|HD|BluRay|DVD|WEB-DL)\b', '', query, flags=re.IGNORECASE)
        query = re.sub(r'\s+', ' ', query).strip()
        
        return query if len(query) > 3 else None
    
    def _determine_provider_media_type(self, file_info: Dict) -> str:
        """Determine media type for provider search"""
        
        media_type = file_info.get('media_type', '').lower()
        category = file_info.get('category', '').lower()
        filename = Path(file_info.get('path', '')).name.lower()
        
        # Academic/research papers
        if any(term in filename for term in ['paper', 'research', 'study', 'journal', 'arxiv', 'doi']):
            return 'academic'
        
        # Video content
        if media_type == 'video':
            if 'movie' in category.lower():
                return 'movie'
            elif 'tv' in category.lower() or 'series' in category.lower():
                return 'tv'
            else:
                return 'movie'
        
        # Document types
        elif media_type == 'pdf' or 'document' in category.lower():
            # Check for academic indicators
            if any(term in category.lower() for term in ['academic', 'research', 'technical']):
                return 'academic'
            else:
                return 'book'
        
        # Books
        elif 'book' in category.lower() or media_type == 'book':
            return 'book'
        
        # General documents
        elif media_type in ['document', 'text']:
            return 'document'
        
        # Gaming/TTRPG content
        elif any(term in filename for term in ['pathfinder', 'starfinder', 'd&d', 'dungeons', 'dragons']):
            return 'gaming'
        elif any(term in category.lower() for term in ['gaming', 'rpg', 'ttrpg']):
            return 'gaming'
        
        return 'movie'  # Default
    
    def _select_best_match(self, results: List[Dict], query: str) -> Optional[Dict]:
        """Select best match from search results"""
        
        if not results:
            return None
        
        # Simple scoring based on title similarity
        def score_match(result):
            title = result.get('title') or result.get('name') or ''
            
            # Exact match gets highest score
            if title.lower() == query.lower():
                return 100
            
            # Partial match scoring
            query_words = set(query.lower().split())
            title_words = set(title.lower().split())
            
            if query_words and title_words:
                intersection = query_words.intersection(title_words)
                union = query_words.union(title_words)
                similarity = len(intersection) / len(union) if union else 0
                return similarity * 80
            
            return 0
        
        # Return highest scoring result
        scored_results = [(score_match(r), r) for r in results]
        scored_results.sort(key=lambda x: x[0], reverse=True)
        
        if scored_results[0][0] > 30:  # Minimum similarity threshold
            return scored_results[0][1]
        
        return None
    
    def _download_artwork(self, file_info: Dict, metadata: EnhancedMetadata):
        """Download artwork files"""
        
        artwork_dir = Path(file_info.get('destination_path', '')).parent / 'artwork'
        artwork_dir.mkdir(exist_ok=True)
        
        base_name = Path(file_info.get('path', '')).stem
        
        # Download poster
        if metadata.poster_url:
            self._download_image(metadata.poster_url, artwork_dir / f"{base_name}-poster.jpg")
        
        # Download fanart
        if metadata.fanart_url:
            self._download_image(metadata.fanart_url, artwork_dir / f"{base_name}-fanart.jpg")
        
        # Download banner
        if metadata.banner_url:
            self._download_image(metadata.banner_url, artwork_dir / f"{base_name}-banner.jpg")
    
    def _download_image(self, url: str, output_path: Path) -> bool:
        """Download image from URL"""
        
        try:
            response = requests.get(url, timeout=30, stream=True)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.debug(f"Downloaded artwork: {output_path}")
            return True
            
        except Exception as e:
            logger.debug(f"Artwork download failed for {url}: {e}")
            return False
    
    def _get_cached_metadata(self, cache_key: str) -> Optional[EnhancedMetadata]:
        """Get cached metadata"""
        
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        try:
            if cache_file.exists():
                # Check if cache is recent (24 hours)
                if time.time() - cache_file.stat().st_mtime < 86400:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        return EnhancedMetadata(**data)
        except Exception as e:
            logger.debug(f"Cache read failed: {e}")
        
        return None
    
    def _cache_metadata(self, cache_key: str, metadata: EnhancedMetadata):
        """Cache metadata"""
        
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(asdict(metadata), f, indent=2, default=str)
        except Exception as e:
            logger.debug(f"Cache write failed: {e}")
    
    def get_provider_status(self) -> Dict:
        """Get status of all providers"""
        
        status = {
            'providers_available': len(self.providers),
            'internet_enrichment_enabled': self.config.get('enable_internet_enrichment', False),
            'artwork_download_enabled': self.config.get('download_artwork', False),
            'cache_entries': len(list(self.cache_dir.glob('*.json'))),
            'providers': []
        }
        
        for provider in self.providers:
            provider_info = {
                'name': provider.__class__.__name__,
                'available': True
            }
            status['providers'].append(provider_info)
        
        if self.fanart_provider:
            status['providers'].append({
                'name': 'FanartTVProvider',
                'available': True
            })
        
        return status