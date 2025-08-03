#!/usr/bin/env python3
"""
Enterprise Classification Engine
Implements enterprise software engineering best practices for file classification
"""

import logging
import sqlite3
import unicodedata
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, Union
import re
import gc

try:
    from rapidfuzz.fuzz import partial_ratio
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False
    from difflib import SequenceMatcher
    
    def partial_ratio(a: str, b: str) -> int:
        """Fallback fuzzy matching"""
        if not a or not b:
            return 0
        return int(SequenceMatcher(None, a, b).ratio() * 100)

logger = logging.getLogger(__name__)


@dataclass
class ClassificationResult:
    """Enterprise classification result with confidence scoring"""
    game_system: str
    edition: Optional[str]
    category: Optional[str]
    confidence: float
    source: str


class ClassificationStrategy(ABC):
    """Strategy pattern for classification methods"""
    
    @abstractmethod
    def classify(self, filename: str, full_path: str, mime_type: str) -> Optional[ClassificationResult]:
        """Classify file using this strategy"""
        pass


class DatabaseConnectionManager:
    """Enterprise database connection management with pooling"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection = None
    
    @contextmanager
    def get_connection(self):
        """Get database connection with proper resource management"""
        try:
            if not self._connection:
                self._connection = sqlite3.connect(
                    f"file:{self.db_path}?mode=ro", 
                    uri=True,
                    timeout=30.0
                )
                self._connection.row_factory = sqlite3.Row
            
            yield self._connection
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            # Connection remains open for reuse
            pass
    
    def close(self):
        """Close database connection"""
        if self._connection:
            self._connection.close()
            self._connection = None


class KnowledgeBaseStrategy(ClassificationStrategy):
    """Knowledge base classification with enterprise patterns"""
    
    def __init__(self, db_manager: DatabaseConnectionManager):
        self.db_manager = db_manager
        self._product_cache: Dict[str, Tuple[str, str, str]] = {}
        self._path_keywords: Dict[str, str] = {}
        self._cache_loaded = False
    
    def _ensure_cache_loaded(self):
        """Lazy loading of classification cache"""
        if self._cache_loaded:
            return
        
        try:
            with self.db_manager.get_connection() as conn:
                self._load_products_cache(conn)
                self._load_path_keywords(conn)
                self._load_alternate_keywords(conn)
            
            self._cache_loaded = True
            logger.info(f"Loaded {len(self._product_cache)} products and {len(self._path_keywords)} keywords")
            
        except Exception as e:
            logger.error(f"Failed to load classification cache: {e}")
    
    def _load_products_cache(self, conn: sqlite3.Connection):
        """Load products into memory cache"""
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT product_code, title, game_system, edition, category 
                FROM products 
                WHERE product_code IS NOT NULL OR title IS NOT NULL
            """)
            
            for row in cursor.fetchall():
                code, title, system, edition, category = row
                
                if code:
                    key = self._normalize_text(code)
                    self._product_cache[key] = (system, edition, category)
                
                if title:
                    key = self._normalize_text(title)
                    self._product_cache[key] = (system, edition, category)
                    
        finally:
            cursor.close()
    
    def _load_path_keywords(self, conn: sqlite3.Connection):
        """Load path keywords for directory-based classification"""
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT DISTINCT game_system, edition 
                FROM products 
                WHERE game_system IS NOT NULL
            """)
            
            for row in cursor.fetchall():
                system, edition = row
                
                if system:
                    key = self._normalize_text(system)
                    self._path_keywords[key] = system
                
                if edition:
                    key = self._normalize_text(edition)
                    self._path_keywords[key] = system
                    
        finally:
            cursor.close()
    
    def _load_alternate_keywords(self, conn: sqlite3.Connection):
        """Load alternate titles and keywords"""
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT alt_title, game_system, edition, category 
                FROM alternate_titles 
                WHERE alt_title IS NOT NULL
            """)
            
            for row in cursor.fetchall():
                alt_title, system, edition, category = row
                
                if alt_title:
                    key = self._normalize_text(alt_title)
                    self._product_cache[key] = (system, edition, category)
                    self._path_keywords[key] = system
                    
        except sqlite3.OperationalError:
            # Table might not exist
            pass
        finally:
            cursor.close()
    
    @staticmethod
    def _normalize_text(text: str) -> str:
        """Enterprise text normalization with Unicode support"""
        if not text:
            return ''
        
        # Convert to lowercase
        text = text.lower()
        
        # Unicode normalization
        text = unicodedata.normalize('NFKD', text)
        
        # Remove combining characters
        text = ''.join(c for c in text if not unicodedata.combining(c))
        
        # Keep only alphanumeric characters
        text = re.sub(r'[^a-z0-9]', '', text)
        
        return text
    
    def classify(self, filename: str, full_path: str, mime_type: str) -> Optional[ClassificationResult]:
        """Classify using knowledge base with confidence scoring"""
        self._ensure_cache_loaded()
        
        # Try filename classification first
        result = self._classify_by_filename(filename)
        if result:
            return result
        
        # Try path-based classification
        result = self._classify_by_path(full_path)
        if result:
            return result
        
        return None
    
    def _classify_by_filename(self, filename: str) -> Optional[ClassificationResult]:
        """Classify by filename with fuzzy matching"""
        clean_filename = self._normalize_text(filename)
        
        # Exact substring matching (highest confidence)
        for title_key in sorted(self._product_cache.keys(), key=len, reverse=True):
            if len(title_key) >= 4 and title_key in clean_filename:
                system, edition, category = self._product_cache[title_key]
                return ClassificationResult(
                    game_system=system,
                    edition=edition,
                    category=category,
                    confidence=0.95,
                    source='knowledge_base_exact'
                )
        
        # Fuzzy matching (medium confidence)
        best_match = None
        best_ratio = 0
        
        for title_key in self._product_cache.keys():
            if len(title_key) >= 4:
                ratio = partial_ratio(clean_filename, title_key)
                if ratio >= 85 and ratio > best_ratio:
                    best_ratio = ratio
                    best_match = title_key
        
        if best_match:
            system, edition, category = self._product_cache[best_match]
            confidence = min(0.9, best_ratio / 100.0)
            return ClassificationResult(
                game_system=system,
                edition=edition,
                category=category,
                confidence=confidence,
                source='knowledge_base_fuzzy'
            )
        
        return None
    
    def _classify_by_path(self, full_path: str) -> Optional[ClassificationResult]:
        """Classify by path components"""
        path_parts = Path(full_path).parts[:-1]  # Exclude filename
        
        for part in reversed(path_parts):
            clean_part = self._normalize_text(part)
            
            # Exact match
            if clean_part in self._path_keywords:
                system = self._path_keywords[clean_part]
                return ClassificationResult(
                    game_system=system,
                    edition="From Folder",
                    category="Heuristic",
                    confidence=0.8,
                    source='knowledge_base_path'
                )
            
            # Fuzzy path matching
            for keyword in self._path_keywords.keys():
                if len(keyword) >= 4 and partial_ratio(clean_part, keyword) >= 90:
                    system = self._path_keywords[keyword]
                    return ClassificationResult(
                        game_system=system,
                        edition="From Folder",
                        category="Heuristic",
                        confidence=0.75,
                        source='knowledge_base_path_fuzzy'
                    )
        
        return None


class MimeTypeStrategy(ClassificationStrategy):
    """MIME type-based classification strategy"""
    
    MIME_MAPPINGS = {
        'video': ('Media', 'Video', None),
        'audio': ('Media', 'Audio', None),
        'image': ('Media', 'Images', None),
        'application/pdf': ('Documents', 'PDF', None),
        'application/zip': ('Archives', None, None),
        'application/x-rar': ('Archives', None, None),
        'application/x-7z-compressed': ('Archives', None, None),
        'text': ('Documents', 'Text', None),
        'application/msword': ('Documents', 'Office', None),
        'application/vnd.openxmlformats': ('Documents', 'Office', None),
    }
    
    def classify(self, filename: str, full_path: str, mime_type: str) -> Optional[ClassificationResult]:
        """Classify by MIME type"""
        if not mime_type:
            return None
        
        major_type = mime_type.split('/')[0]
        
        # Check specific MIME types first
        for mime_pattern, (system, edition, category) in self.MIME_MAPPINGS.items():
            if mime_pattern in mime_type:
                return ClassificationResult(
                    game_system=system,
                    edition=edition,
                    category=category,
                    confidence=0.7,
                    source='mime_type'
                )
        
        # Check major types
        if major_type in self.MIME_MAPPINGS:
            system, edition, category = self.MIME_MAPPINGS[major_type]
            return ClassificationResult(
                game_system=system,
                edition=edition,
                category=category,
                confidence=0.6,
                source='mime_type_major'
            )
        
        return None


class EnterpriseClassifier:
    """Enterprise file classifier with strategy pattern and resource management"""
    
    def __init__(self, knowledge_db_path: Optional[str] = None):
        self.strategies = []
        self.db_manager = None
        
        # Initialize strategies
        self._initialize_strategies(knowledge_db_path)
    
    def _initialize_strategies(self, knowledge_db_path: Optional[str]):
        """Initialize classification strategies in priority order"""
        
        # Knowledge base strategy (highest priority)
        if knowledge_db_path and Path(knowledge_db_path).exists():
            try:
                self.db_manager = DatabaseConnectionManager(knowledge_db_path)
                kb_strategy = KnowledgeBaseStrategy(self.db_manager)
                self.strategies.append(kb_strategy)
                logger.info("Knowledge base strategy initialized")
            except Exception as e:
                logger.warning(f"Knowledge base strategy failed to initialize: {e}")
        else:
            logger.warning("Knowledge base not available - limited classification capability")
        
        # MIME type strategy (fallback)
        self.strategies.append(MimeTypeStrategy())
        logger.info("MIME type strategy initialized")
    
    def classify(self, filename: str, full_path: str, mime_type: str) -> Tuple[str, Optional[str], Optional[str]]:
        """Classify file using enterprise strategy chain"""
        
        # Try each strategy in priority order
        for strategy in self.strategies:
            try:
                result = strategy.classify(filename, full_path, mime_type)
                if result and result.confidence >= 0.6:
                    logger.debug(f"Classified {filename} using {result.source}: {result.game_system}")
                    return result.game_system, result.edition, result.category
            except Exception as e:
                logger.error(f"Strategy {strategy.__class__.__name__} failed for {filename}: {e}")
        
        # Ultimate fallback
        return 'Miscellaneous', None, None
    
    def close(self):
        """Clean up resources"""
        if self.db_manager:
            self.db_manager.close()
        
        # Force garbage collection
        gc.collect()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Legacy compatibility
class Classifier(EnterpriseClassifier):
    """Legacy classifier interface for backward compatibility"""
    pass