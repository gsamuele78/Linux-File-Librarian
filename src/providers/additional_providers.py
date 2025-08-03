#!/usr/bin/env python3
"""
Additional Free Internet Providers for Document Classification

Integrates with free APIs and services for enhanced document metadata enrichment.
"""

import logging
import requests
import json
import re
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass
from src.providers.enhanced_classification_engine import MetadataProvider, EnhancedMetadata

logger = logging.getLogger(__name__)


class CrossRefProvider(MetadataProvider):
    """CrossRef provider for academic papers and DOI resolution"""
    
    def __init__(self):
        self.base_url = "https://api.crossref.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0 (mailto:admin@example.com)'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search CrossRef for academic papers"""
        if media_type not in ["book", "document", "academic"]:
            return []
        
        try:
            url = f"{self.base_url}/works"
            params = {
                'query': query,
                'rows': 5,
                'sort': 'relevance'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return data.get('message', {}).get('items', [])
            
        except Exception as e:
            logger.debug(f"CrossRef search failed: {e}")
            return []
    
    def get_details(self, doi: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get paper details by DOI"""
        try:
            url = f"{self.base_url}/works/{doi}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_crossref_data(data.get('message', {}))
            
        except Exception as e:
            logger.debug(f"CrossRef details failed: {e}")
            return None
    
    def _parse_crossref_data(self, data: Dict) -> EnhancedMetadata:
        """Parse CrossRef response"""
        metadata = EnhancedMetadata()
        
        # Title
        titles = data.get('title', [])
        if titles:
            metadata.title = titles[0]
        
        # Authors
        authors = data.get('author', [])
        if authors:
            author_names = []
            for author in authors[:3]:  # Top 3 authors
                given = author.get('given', '')
                family = author.get('family', '')
                if given and family:
                    author_names.append(f"{given} {family}")
                elif family:
                    author_names.append(family)
            if author_names:
                metadata.author = author_names[0]
                metadata.writers = author_names
        
        # Publisher
        metadata.publisher = data.get('publisher')
        
        # Year
        published = data.get('published-print') or data.get('published-online')
        if published and 'date-parts' in published:
            date_parts = published['date-parts'][0]
            if date_parts:
                metadata.year = date_parts[0]
        
        # Abstract as plot
        abstract = data.get('abstract')
        if abstract:
            # Clean HTML tags from abstract
            metadata.plot = re.sub(r'<[^>]+>', '', abstract)
        
        # Subject as genres
        subjects = data.get('subject', [])
        metadata.genres = subjects[:5]
        
        return metadata
    
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """CrossRef doesn't provide artwork"""
        return {}


class ArxivProvider(MetadataProvider):
    """arXiv provider for preprint papers"""
    
    def __init__(self):
        self.base_url = "http://export.arxiv.org/api/query"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search arXiv"""
        if media_type not in ["book", "document", "academic"]:
            return []
        
        try:
            params = {
                'search_query': f'all:{query}',
                'start': 0,
                'max_results': 5,
                'sortBy': 'relevance',
                'sortOrder': 'descending'
            }
            
            response = self.session.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            # Parse XML response
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            
            entries = []
            for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
                entries.append(self._parse_arxiv_entry(entry))
            
            return entries
            
        except Exception as e:
            logger.debug(f"arXiv search failed: {e}")
            return []
    
    def get_details(self, arxiv_id: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get arXiv paper details"""
        try:
            params = {
                'id_list': arxiv_id,
                'max_results': 1
            }
            
            response = self.session.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            
            entry = root.find('{http://www.w3.org/2005/Atom}entry')
            if entry is not None:
                return self._parse_arxiv_entry(entry, detailed=True)
            
        except Exception as e:
            logger.debug(f"arXiv details failed: {e}")
        
        return None
    
    def _parse_arxiv_entry(self, entry, detailed=False) -> Dict:
        """Parse arXiv XML entry"""
        ns = {'atom': 'http://www.w3.org/2005/Atom', 'arxiv': 'http://arxiv.org/schemas/atom'}
        
        data = {}
        
        # ID
        id_elem = entry.find('atom:id', ns)
        if id_elem is not None:
            data['id'] = id_elem.text.split('/')[-1]  # Extract arXiv ID
        
        # Title
        title_elem = entry.find('atom:title', ns)
        if title_elem is not None:
            data['title'] = title_elem.text.strip()
        
        # Authors
        authors = []
        for author in entry.findall('atom:author', ns):
            name_elem = author.find('atom:name', ns)
            if name_elem is not None:
                authors.append(name_elem.text)
        data['authors'] = authors
        
        # Summary
        summary_elem = entry.find('atom:summary', ns)
        if summary_elem is not None:
            data['summary'] = summary_elem.text.strip()
        
        # Categories
        categories = []
        for cat in entry.findall('atom:category', ns):
            term = cat.get('term')
            if term:
                categories.append(term)
        data['categories'] = categories
        
        # Published date
        published_elem = entry.find('atom:published', ns)
        if published_elem is not None:
            data['published'] = published_elem.text
        
        if detailed:
            return self._convert_to_metadata(data)
        
        return data
    
    def _convert_to_metadata(self, data: Dict) -> EnhancedMetadata:
        """Convert arXiv data to EnhancedMetadata"""
        metadata = EnhancedMetadata()
        
        metadata.title = data.get('title')
        
        authors = data.get('authors', [])
        if authors:
            metadata.author = authors[0]
            metadata.writers = authors[:3]
        
        metadata.plot = data.get('summary')
        metadata.genres = data.get('categories', [])
        
        # Extract year from published date
        published = data.get('published', '')
        if published:
            try:
                metadata.year = int(published[:4])
            except (ValueError, IndexError):
                pass
        
        return metadata
    
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """arXiv doesn't provide artwork"""
        return {}


class GoogleBooksProvider(MetadataProvider):
    """Google Books API provider"""
    
    def __init__(self):
        self.base_url = "https://www.googleapis.com/books/v1"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search Google Books"""
        if media_type != "book":
            return []
        
        try:
            url = f"{self.base_url}/volumes"
            params = {
                'q': query,
                'maxResults': 5,
                'orderBy': 'relevance'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return data.get('items', [])
            
        except Exception as e:
            logger.debug(f"Google Books search failed: {e}")
            return []
    
    def get_details(self, volume_id: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get book details from Google Books"""
        try:
            url = f"{self.base_url}/volumes/{volume_id}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_google_books_data(data)
            
        except Exception as e:
            logger.debug(f"Google Books details failed: {e}")
            return None
    
    def _parse_google_books_data(self, data: Dict) -> EnhancedMetadata:
        """Parse Google Books response"""
        metadata = EnhancedMetadata()
        
        volume_info = data.get('volumeInfo', {})
        
        metadata.title = volume_info.get('title')
        metadata.plot = volume_info.get('description')
        metadata.publisher = volume_info.get('publisher')
        metadata.language = volume_info.get('language')
        metadata.pages = volume_info.get('pageCount')
        
        # Authors
        authors = volume_info.get('authors', [])
        if authors:
            metadata.author = authors[0]
            metadata.writers = authors
        
        # Published date
        published_date = volume_info.get('publishedDate', '')
        if published_date:
            try:
                metadata.year = int(published_date[:4])
            except (ValueError, IndexError):
                pass
        
        # Categories as genres
        categories = volume_info.get('categories', [])
        metadata.genres = categories
        
        # ISBN
        identifiers = volume_info.get('industryIdentifiers', [])
        for identifier in identifiers:
            if identifier.get('type') in ['ISBN_13', 'ISBN_10']:
                metadata.isbn = identifier.get('identifier')
                break
        
        # Rating
        average_rating = volume_info.get('averageRating')
        if average_rating:
            metadata.rating = float(average_rating)
        
        return metadata
    
    def get_artwork(self, volume_id: str, media_type: str) -> Dict[str, str]:
        """Get book cover from Google Books"""
        artwork = {}
        
        try:
            url = f"{self.base_url}/volumes/{volume_id}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            volume_info = data.get('volumeInfo', {})
            image_links = volume_info.get('imageLinks', {})
            
            # Get highest quality cover available
            for size in ['extraLarge', 'large', 'medium', 'small', 'thumbnail']:
                if size in image_links:
                    artwork['poster'] = image_links[size].replace('http://', 'https://')
                    break
                    
        except Exception as e:
            logger.debug(f"Google Books artwork failed: {e}")
        
        return artwork


class WikipediaProvider(MetadataProvider):
    """Wikipedia provider for general knowledge"""
    
    def __init__(self):
        self.base_url = "https://en.wikipedia.org/api/rest_v1"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search Wikipedia"""
        try:
            url = f"{self.base_url}/page/search/{query}"
            params = {'limit': 5}
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return data.get('pages', [])
            
        except Exception as e:
            logger.debug(f"Wikipedia search failed: {e}")
            return []
    
    def get_details(self, page_title: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get Wikipedia page details"""
        try:
            # Get page summary
            url = f"{self.base_url}/page/summary/{page_title}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_wikipedia_data(data)
            
        except Exception as e:
            logger.debug(f"Wikipedia details failed: {e}")
            return None
    
    def _parse_wikipedia_data(self, data: Dict) -> EnhancedMetadata:
        """Parse Wikipedia response"""
        metadata = EnhancedMetadata()
        
        metadata.title = data.get('title')
        metadata.plot = data.get('extract')  # Summary text
        
        # Extract year from title if present
        title = data.get('title', '')
        year_match = re.search(r'\b(19|20)\d{2}\b', title)
        if year_match:
            metadata.year = int(year_match.group())
        
        return metadata
    
    def get_artwork(self, page_title: str, media_type: str) -> Dict[str, str]:
        """Get Wikipedia page image"""
        artwork = {}
        
        try:
            url = f"{self.base_url}/page/summary/{page_title}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            thumbnail = data.get('thumbnail')
            if thumbnail and 'source' in thumbnail:
                artwork['poster'] = thumbnail['source']
                
        except Exception as e:
            logger.debug(f"Wikipedia artwork failed: {e}")
        
        return artwork


class InternetArchiveProvider(MetadataProvider):
    """Internet Archive provider for books and documents"""
    
    def __init__(self):
        self.base_url = "https://archive.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search Internet Archive"""
        if media_type not in ["book", "document"]:
            return []
        
        try:
            url = f"{self.base_url}/advancedsearch.php"
            params = {
                'q': f'title:({query}) AND mediatype:(texts)',
                'fl': 'identifier,title,creator,date,description,subject',
                'rows': 5,
                'page': 1,
                'output': 'json'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return data.get('response', {}).get('docs', [])
            
        except Exception as e:
            logger.debug(f"Internet Archive search failed: {e}")
            return []
    
    def get_details(self, identifier: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get Internet Archive item details"""
        try:
            url = f"{self.base_url}/metadata/{identifier}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_ia_data(data)
            
        except Exception as e:
            logger.debug(f"Internet Archive details failed: {e}")
            return None
    
    def _parse_ia_data(self, data: Dict) -> EnhancedMetadata:
        """Parse Internet Archive response"""
        metadata = EnhancedMetadata()
        
        item_data = data.get('metadata', {})
        
        metadata.title = item_data.get('title')
        metadata.plot = item_data.get('description')
        metadata.publisher = item_data.get('publisher')
        
        # Creator/Author
        creator = item_data.get('creator')
        if creator:
            if isinstance(creator, list):
                metadata.author = creator[0]
                metadata.writers = creator
            else:
                metadata.author = creator
        
        # Date
        date = item_data.get('date')
        if date:
            try:
                metadata.year = int(date[:4])
            except (ValueError, IndexError):
                pass
        
        # Subject as genres
        subject = item_data.get('subject')
        if subject:
            if isinstance(subject, list):
                metadata.genres = subject[:5]
            else:
                metadata.genres = [subject]
        
        return metadata
    
    def get_artwork(self, identifier: str, media_type: str) -> Dict[str, str]:
        """Get Internet Archive item thumbnail"""
        artwork = {}
        
        try:
            # Internet Archive thumbnail URL pattern
            thumb_url = f"{self.base_url}/services/img/{identifier}"
            
            # Check if thumbnail exists
            response = self.session.head(thumb_url, timeout=5)
            if response.status_code == 200:
                artwork['poster'] = thumb_url
                
        except Exception as e:
            logger.debug(f"Internet Archive artwork failed: {e}")
        
        return artwork