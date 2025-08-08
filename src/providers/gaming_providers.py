#!/usr/bin/env python3
"""
Gaming Industry Providers for TTRPG Content

Specialized providers for Paizo, Wizards of the Coast, and other gaming publishers.
"""

import logging
import requests
import re
from pathlib import Path
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from bs4.element import Tag
from src.providers.enhanced_classification_engine import MetadataProvider, EnhancedMetadata

logger = logging.getLogger(__name__)


class PaizoProvider(MetadataProvider):
    """Paizo Publishing provider for Pathfinder content"""
    
    def __init__(self):
        self.base_url = "https://paizo.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search Paizo products"""
        if not self._is_paizo_content(query):
            return []
        
        try:
            url = f"{self.base_url}/search"
            params = {'q': query, 'what': 'products'}
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            return self._parse_search_results(response.text)
            
        except Exception as e:
            logger.debug(f"Paizo search failed: {e}")
            return []
    
    def _is_paizo_content(self, query: str) -> bool:
        """Check if content is likely Paizo-related"""
        paizo_keywords = [
            'pathfinder', 'starfinder', 'paizo', 'golarion', 'absalom',
            'adventure path', 'pf2e', 'pf1e', 'society'
        ]
        query_lower = query.lower()
        return any(keyword in query_lower for keyword in paizo_keywords)
    
    def _parse_search_results(self, html: str) -> List[Dict]:
        """Parse Paizo search results"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []

            for item in soup.find_all('div', class_='product-item')[:5]:
                if not isinstance(item, Tag):
                    continue
                title_elem = item.find('h3') or item.find('a')
                if isinstance(title_elem, Tag):
                    title = title_elem.get_text(strip=True)
                    link = ""
                    if title_elem.name == 'a':
                        href = title_elem.get('href')
                        if isinstance(href, str):
                            link = href
                        elif isinstance(href, list) and href:
                            link = str(href[0])

                    results.append({
                        'title': title,
                        'link': link,
                        'id': link.split('/')[-1] if link else title.replace(' ', '-').lower()
                    })

            return results

        except Exception as e:
            logger.debug(f"Paizo search parsing failed: {e}")
            return []
    
    def get_details(self, provider_id: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get Paizo product details"""
        try:
            url = f"{self.base_url}/products/{provider_id}"
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            return self._parse_product_page(response.text)
            
        except Exception as e:
            logger.debug(f"Paizo details failed: {e}")
            return None
    
    def _parse_product_page(self, html: str) -> EnhancedMetadata:
        """Parse Paizo product page"""
        metadata = EnhancedMetadata()

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Title
            title_elem = soup.find('h1') or soup.find('title')
            if isinstance(title_elem, Tag):
                metadata.title = title_elem.get_text(strip=True)

            # Description
            desc_elem = soup.find('div', class_='product-description') or soup.find('meta', {'name': 'description'})
            if isinstance(desc_elem, Tag):
                if desc_elem.name == 'meta':
                    content = desc_elem.get('content', '')
                    if isinstance(content, list):
                        metadata.plot = ' '.join(content)
                    else:
                        metadata.plot = str(content)
                else:
                    metadata.plot = desc_elem.get_text(strip=True)

            # Publisher is always Paizo
            metadata.publisher = "Paizo Publishing"

            # Extract game system
            if metadata.title and 'pathfinder' in metadata.title.lower():
                metadata.genres = ['Pathfinder']
            elif metadata.title and 'starfinder' in metadata.title.lower():
                metadata.genres = ['Starfinder']
            else:
                metadata.genres = ['Paizo']

            # Extract product type
            title_lower = metadata.title.lower() if metadata.title else ''
            if 'adventure path' in title_lower:
                metadata.tags = ['Adventure Path']
            elif 'player companion' in title_lower:
                metadata.tags = ['Player Companion']
            elif 'campaign setting' in title_lower:
                metadata.tags = ['Campaign Setting']
            elif 'module' in title_lower:
                metadata.tags = ['Module']

        except Exception as e:
            logger.debug(f"Paizo parsing failed: {e}")

        return metadata
    
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get Paizo product artwork"""
        artwork = {}

        try:
            url = f"{self.base_url}/products/{provider_id}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find product image
            img_elem = soup.find('img', class_='product-image') or soup.find('img', {'alt': re.compile(r'cover', re.I)})
            if isinstance(img_elem, Tag):
                src = img_elem.get('src')
                if src:
                    img_url = ""
                    if isinstance(src, str):
                        img_url = src
                    elif isinstance(src, list) and src:
                        img_url = str(src[0])

                    if img_url:
                        if not img_url.startswith('http'):
                            img_url = f"{self.base_url}{img_url}"
                        artwork['poster'] = img_url

        except Exception as e:
            logger.debug(f"Paizo artwork failed: {e}")

        return artwork


class WizardsProvider(MetadataProvider):
    """Wizards of the Coast provider for D&D content"""
    
    def __init__(self):
        self.base_url = "https://dnd.wizards.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search D&D products"""
        if not self._is_dnd_content(query):
            return []
        
        try:
            # Use DDB search as fallback
            url = "https://www.dndbeyond.com/search"
            params = {'q': query}
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            return self._parse_ddb_results(response.text)
            
        except Exception as e:
            logger.debug(f"Wizards search failed: {e}")
            return []
    
    def _is_dnd_content(self, query: str) -> bool:
        """Check if content is D&D-related"""
        dnd_keywords = [
            'd&d', 'dungeons', 'dragons', 'wizards', 'coast', 'forgotten realms',
            'eberron', 'ravenloft', 'spelljammer', 'planescape', 'dragonlance',
            'player handbook', 'dungeon master', 'monster manual', 'dnd', '5e', '3.5e'
        ]
        query_lower = query.lower()
        return any(keyword in query_lower for keyword in dnd_keywords)
    
    def _parse_ddb_results(self, html: str) -> List[Dict]:
        """Parse D&D Beyond search results"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []

            for item in soup.find_all('div', class_='search-result-item')[:5]:
                if not isinstance(item, Tag):
                    continue
                title_elem = item.find('h3') or item.find('a')
                if isinstance(title_elem, Tag):
                    title = title_elem.get_text(strip=True)
                    link = ""
                    if title_elem.name == 'a':
                        href = title_elem.get('href')
                        if isinstance(href, str):
                            link = href
                        elif isinstance(href, list) and href:
                            link = str(href[0])

                    results.append({
                        'title': title,
                        'link': link,
                        'id': link.split('/')[-1] if link else title.replace(' ', '-').lower()
                    })

            return results

        except Exception as e:
            logger.debug(f"D&D Beyond parsing failed: {e}")
            return []
    
    def get_details(self, provider_id: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get D&D product details"""
        metadata = EnhancedMetadata()
        
        # Set default D&D metadata
        metadata.publisher = "Wizards of the Coast"
        metadata.genres = ['Dungeons & Dragons']
        metadata.tags = []
        
        # Determine edition and product type from ID/title
        if '5e' in provider_id or 'fifth' in provider_id:
            metadata.tags.append('5th Edition')
        elif '3.5' in provider_id or 'third' in provider_id:
            metadata.tags.append('3.5 Edition')
        elif '4e' in provider_id or 'fourth' in provider_id:
            metadata.tags.append('4th Edition')
        
        # Product type classification
        if 'player' in provider_id and 'handbook' in provider_id:
            metadata.tags.append('Core Rulebook')
        elif 'monster' in provider_id and 'manual' in provider_id:
            metadata.tags.append('Bestiary')
        elif 'dungeon' in provider_id and 'master' in provider_id:
            metadata.tags.append('Core Rulebook')
        elif 'adventure' in provider_id:
            metadata.tags.append('Adventure')
        elif 'campaign' in provider_id:
            metadata.tags.append('Campaign Setting')
        
        return metadata
    
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get D&D product artwork"""
        return {}  # D&D Beyond has complex artwork access


class DriveThruRPGProvider(MetadataProvider):
    """DriveThruRPG provider for general TTRPG content"""
    
    def __init__(self):
        self.base_url = "https://www.drivethrurpg.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search DriveThruRPG"""
        try:
            url = f"{self.base_url}/browse.php"
            params = {
                'keywords': query,
                'x': 0,
                'y': 0,
                'author': '',
                'artist': '',
                'pfrom': '',
                'pto': ''
            }
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            return self._parse_dtrpg_results(response.text)
            
        except Exception as e:
            logger.debug(f"DriveThruRPG search failed: {e}")
            return []
    
    def _parse_dtrpg_results(self, html: str) -> List[Dict]:
        """Parse DriveThruRPG search results"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []

            for item in soup.find_all('div', class_='product-row')[:5]:
                if not isinstance(item, Tag):
                    continue
                title_elem = item.find('a', class_='product-title')
                if isinstance(title_elem, Tag):
                    title = title_elem.get_text(strip=True)
                    link_val = title_elem.get('href', '')
                    link = ""
                    if isinstance(link_val, str):
                        link = link_val
                    elif isinstance(link_val, list) and link_val:
                        link = str(link_val[0])

                    # Extract product ID from link
                    product_id = ''
                    if link and '/product/' in link:
                        product_id = link.split('/product/')[1].split('/')[0]

                    results.append({
                        'title': title,
                        'link': link,
                        'id': product_id
                    })

            return results

        except Exception as e:
            logger.debug(f"DriveThruRPG parsing failed: {e}")
            return []
    
    def get_details(self, provider_id: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get DriveThruRPG product details"""
        try:
            url = f"{self.base_url}/product/{provider_id}"
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            return self._parse_dtrpg_product(response.text)
            
        except Exception as e:
            logger.debug(f"DriveThruRPG details failed: {e}")
            return None
    
    def _parse_dtrpg_product(self, html: str) -> EnhancedMetadata:
        """Parse DriveThruRPG product page"""
        metadata = EnhancedMetadata()

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Title
            title_elem = soup.find('h1', class_='product-title')
            if isinstance(title_elem, Tag):
                metadata.title = title_elem.get_text(strip=True)

            # Publisher
            pub_elem = soup.find('a', class_='publisher-name')
            if isinstance(pub_elem, Tag):
                metadata.publisher = pub_elem.get_text(strip=True)

            # Description
            desc_elem = soup.find('div', class_='product-description')
            if isinstance(desc_elem, Tag):
                metadata.plot = desc_elem.get_text(strip=True)

            # Game system from categories
            cat_elems = soup.find_all('a', class_='category-link')
            categories = [elem.get_text(strip=True) for elem in cat_elems if isinstance(elem, Tag)]
            metadata.genres = categories[:3]  # Top 3 categories

            # Rating
            rating_elem = soup.find('div', class_='product-rating')
            if isinstance(rating_elem, Tag):
                rating_text = rating_elem.get_text()
                rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                if rating_match:
                    metadata.rating = float(rating_match.group(1))

        except Exception as e:
            logger.debug(f"DriveThruRPG parsing failed: {e}")

        return metadata
    
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get DriveThruRPG product artwork"""
        artwork = {}

        try:
            url = f"{self.base_url}/product/{provider_id}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find product cover
            img_elem = soup.find('img', class_='product-image') or soup.find('img', {'alt': re.compile(r'cover', re.I)})
            if isinstance(img_elem, Tag):
                src = img_elem.get('src')
                if isinstance(src, str):
                    artwork['poster'] = src
                elif isinstance(src, list) and src:
                    artwork['poster'] = str(src[0])

        except Exception as e:
            logger.debug(f"DriveThruRPG artwork failed: {e}")

        return artwork


class GiochiUnitiProvider(MetadataProvider):
    """Giochi Uniti (Italian gaming publisher) provider"""
    
    def __init__(self):
        self.base_url = "https://www.giochiuniti.it"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search Giochi Uniti products"""
        if not self._is_italian_content(query):
            return []
        
        try:
            url = f"{self.base_url}/ricerca"
            params = {'q': query}
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            return self._parse_gu_results(response.text)
            
        except Exception as e:
            logger.debug(f"Giochi Uniti search failed: {e}")
            return []
    
    def _is_italian_content(self, query: str) -> bool:
        """Check if content is Italian RPG-related"""
        italian_keywords = [
            'giochi uniti', 'gu', 'italian', 'italiano', 'italia',
            'dungeons dragons', 'pathfinder', 'vampiri', 'lupo mannaro',
            'mondo di tenebra', 'cyberpunk', 'shadowrun'
        ]
        query_lower = query.lower()
        return any(keyword in query_lower for keyword in italian_keywords)
    
    def _parse_gu_results(self, html: str) -> List[Dict]:
        """Parse Giochi Uniti search results"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []

            for item in soup.find_all('div', class_='product-item')[:5]:
                if not isinstance(item, Tag):
                    continue
                title_elem = item.find('h3') or item.find('a')
                if isinstance(title_elem, Tag):
                    title = title_elem.get_text(strip=True)
                    link = ""
                    if title_elem.name == 'a':
                        href = title_elem.get('href')
                        if isinstance(href, str):
                            link = href
                        elif isinstance(href, list) and href:
                            link = str(href[0])

                    results.append({
                        'title': title,
                        'link': link,
                        'id': link.split('/')[-1] if link else title.replace(' ', '-').lower()
                    })

            return results

        except Exception as e:
            logger.debug(f"Giochi Uniti parsing failed: {e}")
            return []
    
    def get_details(self, provider_id: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get Giochi Uniti product details"""
        try:
            url = f"{self.base_url}/prodotto/{provider_id}"
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            return self._parse_gu_product(response.text)
            
        except Exception as e:
            logger.debug(f"Giochi Uniti details failed: {e}")
            return None
    
    def _parse_gu_product(self, html: str) -> EnhancedMetadata:
        """Parse Giochi Uniti product page"""
        metadata = EnhancedMetadata()

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Title
            title_elem = soup.find('h1', class_='product-title') or soup.find('h1')
            if isinstance(title_elem, Tag):
                metadata.title = title_elem.get_text(strip=True)

            # Publisher is always Giochi Uniti
            metadata.publisher = "Giochi Uniti"

            # Description
            desc_elem = soup.find('div', class_='product-description') or soup.find('div', class_='description')
            if isinstance(desc_elem, Tag):
                metadata.plot = desc_elem.get_text(strip=True)

            # Extract game system
            system_elem = soup.find(text=re.compile(r'Sistema:', re.I))
            if system_elem and system_elem.parent:
                system = system_elem.parent.get_text(strip=True).replace('Sistema:', '').strip()
                metadata.genres = [system]

            # Italian RPG tag
            metadata.tags = ['Italian RPG']

        except Exception as e:
            logger.debug(f"Giochi Uniti parsing failed: {e}")

        return metadata
    
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get Giochi Uniti product artwork"""
        artwork = {}

        try:
            url = f"{self.base_url}/prodotto/{provider_id}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find product image
            img_elem = soup.find('img', class_='product-image') or soup.find('img')
            if isinstance(img_elem, Tag):
                src = img_elem.get('src')
                if src:
                    img_url = ""
                    if isinstance(src, str):
                        img_url = src
                    elif isinstance(src, list) and src:
                        img_url = str(src[0])

                    if img_url:
                        if not img_url.startswith('http'):
                            img_url = f"{self.base_url}{img_url}"
                        artwork['poster'] = img_url

        except Exception as e:
            logger.debug(f"Giochi Uniti artwork failed: {e}")

        return artwork


class AcheronGamesProvider(MetadataProvider):
    """Acheron Games (Italian gaming publisher) provider"""
    
    def __init__(self):
        self.base_url = "https://www.acherongames.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Linux-File-Librarian/1.0'
        })
    
    def search(self, query: str, media_type: str) -> List[Dict]:
        """Search Acheron Games products"""
        if not self._is_acheron_content(query):
            return []
        
        try:
            url = f"{self.base_url}/search"
            params = {'q': query}
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            return self._parse_acheron_results(response.text)
            
        except Exception as e:
            logger.debug(f"Acheron Games search failed: {e}")
            return []
    
    def _is_acheron_content(self, query: str) -> bool:
        """Check if content is Acheron Games-related"""
        acheron_keywords = [
            'acheron', 'games', 'italian', 'italiano', 'italia',
            'cyberpunk', 'horror', 'fantasy', 'sci-fi', 'steampunk'
        ]
        query_lower = query.lower()
        return any(keyword in query_lower for keyword in acheron_keywords)
    
    def _parse_acheron_results(self, html: str) -> List[Dict]:
        """Parse Acheron Games search results"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []

            for item in soup.find_all('div', class_='product-item')[:5]:
                if not isinstance(item, Tag):
                    continue
                title_elem = item.find('h3') or item.find('a')
                if isinstance(title_elem, Tag):
                    title = title_elem.get_text(strip=True)
                    link = ""
                    if title_elem.name == 'a':
                        href = title_elem.get('href')
                        if isinstance(href, str):
                            link = href
                        elif isinstance(href, list) and href:
                            link = str(href[0])

                    results.append({
                        'title': title,
                        'link': link,
                        'id': link.split('/')[-1] if link else title.replace(' ', '-').lower()
                    })

            return results

        except Exception as e:
            logger.debug(f"Acheron Games parsing failed: {e}")
            return []
    
    def get_details(self, provider_id: str, media_type: str) -> Optional[EnhancedMetadata]:
        """Get Acheron Games product details"""
        try:
            url = f"{self.base_url}/prodotto/{provider_id}"
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            return self._parse_acheron_product(response.text)
            
        except Exception as e:
            logger.debug(f"Acheron Games details failed: {e}")
            return None
    
    def _parse_acheron_product(self, html: str) -> EnhancedMetadata:
        """Parse Acheron Games product page"""
        metadata = EnhancedMetadata()

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Title
            title_elem = soup.find('h1', class_='product-title') or soup.find('h1')
            if isinstance(title_elem, Tag):
                metadata.title = title_elem.get_text(strip=True)

            # Publisher is always Acheron Games
            metadata.publisher = "Acheron Games"

            # Description
            desc_elem = soup.find('div', class_='product-description') or soup.find('div', class_='description')
            if isinstance(desc_elem, Tag):
                metadata.plot = desc_elem.get_text(strip=True)

            # Extract game system
            system_elem = soup.find(text=re.compile(r'Sistema?:', re.I))
            if system_elem and system_elem.parent:
                system = system_elem.parent.get_text(strip=True)
                system = re.sub(r'Sistema?:', '', system, flags=re.I).strip()
                metadata.genres = [system]

            # Italian RPG tag
            metadata.tags = ['Italian RPG']

        except Exception as e:
            logger.debug(f"Acheron Games parsing failed: {e}")

        return metadata
    
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get Acheron Games product artwork"""
        artwork = {}

        try:
            url = f"{self.base_url}/prodotto/{provider_id}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find product image
            img_elem = soup.find('img', class_='product-image') or soup.find('img')
            if isinstance(img_elem, Tag):
                src = img_elem.get('src')
                if src:
                    img_url = ""
                    if isinstance(src, str):
                        img_url = src
                    elif isinstance(src, list) and src:
                        img_url = str(src[0])

                    if img_url:
                        if not img_url.startswith('http'):
                            img_url = f"{self.base_url}{img_url}"
                        artwork['poster'] = img_url

        except Exception as e:
            logger.debug(f"Acheron Games artwork failed: {e}")

        return artwork