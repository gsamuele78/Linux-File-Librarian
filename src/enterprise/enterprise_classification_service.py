#!/usr/bin/env python3
"""
Enterprise Classification Service
Implements professional classification with automatic knowledge base and ISBN enrichment
"""

import logging
from pathlib import Path
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


@dataclass
class ClassificationResult:
    """Professional classification result"""
    game_system: str
    edition: str
    category: str
    confidence: float
    source: str  # 'knowledge_base', 'isbn', 'heuristic'


class ClassificationStrategy(ABC):
    """Strategy pattern for classification methods"""
    
    @abstractmethod
    def classify(self, file_path: Path, filename: str, mime_type: str) -> Optional[ClassificationResult]:
        pass


class KnowledgeBaseStrategy(ClassificationStrategy):
    """Knowledge base classification strategy"""
    
    def __init__(self, classifier):
        self.classifier = classifier
    
    def classify(self, file_path: Path, filename: str, mime_type: str) -> Optional[ClassificationResult]:
        try:
            game_system, edition, category = self.classifier.classify(filename, str(file_path), mime_type)
            
            if game_system not in ['Miscellaneous', 'Unknown', None]:
                return ClassificationResult(
                    game_system=game_system,
                    edition=edition or 'Unknown',
                    category=category or 'Unknown',
                    confidence=0.9,
                    source='knowledge_base'
                )
        except Exception as e:
            logger.debug(f"Knowledge base classification failed for {filename}: {e}")
        
        return None


class ISBNEnrichmentStrategy(ClassificationStrategy):
    """ISBN enrichment classification strategy"""
    
    def __init__(self):
        self._isbn_enricher = None
    
    def _get_isbn_enricher(self):
        if self._isbn_enricher is None:
            try:
                from src.core.isbn_enricher import enrich_file_with_isbn_metadata
                self._isbn_enricher = enrich_file_with_isbn_metadata
            except ImportError:
                logger.warning("ISBN enricher not available")
                self._isbn_enricher = lambda x: []
        return self._isbn_enricher
    
    def classify(self, file_path: Path, filename: str, mime_type: str) -> Optional[ClassificationResult]:
        if not mime_type.startswith('application/pdf'):
            return None
        
        try:
            enricher = self._get_isbn_enricher()
            results = enricher(str(file_path))
            
            if results:
                metadata = results[0]['metadata']
                title = metadata.get('title', '')
                publisher = metadata.get('publisher', 'Unknown Publisher')
                
                if title:
                    return ClassificationResult(
                        game_system=publisher,
                        edition='Unknown',
                        category='Books',
                        confidence=0.7,
                        source='isbn'
                    )
        except Exception as e:
            logger.debug(f"ISBN enrichment failed for {filename}: {e}")
        
        return None


class HeuristicStrategy(ClassificationStrategy):
    """Heuristic classification strategy"""
    
    def classify(self, file_path: Path, filename: str, mime_type: str) -> Optional[ClassificationResult]:
        major_type = mime_type.split('/')[0]
        
        if major_type == 'video':
            return ClassificationResult('Media', 'Unknown', 'Video', 0.5, 'heuristic')
        elif major_type == 'audio':
            return ClassificationResult('Media', 'Unknown', 'Audio', 0.5, 'heuristic')
        elif major_type == 'image':
            return ClassificationResult('Media', 'Unknown', 'Images', 0.5, 'heuristic')
        elif 'pdf' in mime_type:
            return ClassificationResult('Documents', 'Unknown', 'PDF', 0.5, 'heuristic')
        elif 'zip' in mime_type or 'rar' in mime_type:
            return ClassificationResult('Archives', 'Unknown', 'Compressed', 0.5, 'heuristic')
        
        return ClassificationResult('Miscellaneous', 'Unknown', 'Unknown', 0.1, 'heuristic')


class EnterpriseClassificationService:
    """Enterprise classification service with automatic resource management"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.strategies = []
        self._initialize_strategies()
    
    def _initialize_strategies(self):
        """Initialize classification strategies in priority order"""
        
        # 1. Knowledge Base Strategy (highest priority)
        kb_path = self.config.get('knowledge_base_db_url', 'knowledge.sqlite')
        if not Path(kb_path).exists():
            logger.info("Building knowledge base automatically...")
            self._ensure_knowledge_base()
        
        if Path(kb_path).exists():
            try:
                from src.enterprise.enterprise_classifier import EnterpriseClassifier as Classifier
                classifier = Classifier(kb_path)
                self.strategies.append(KnowledgeBaseStrategy(classifier))
                logger.info("Knowledge base strategy initialized")
            except Exception as e:
                logger.warning(f"Knowledge base strategy failed: {e}")
        
        # 2. ISBN Enrichment Strategy (medium priority)
        self.strategies.append(ISBNEnrichmentStrategy())
        logger.info("ISBN enrichment strategy initialized")
        
        # 3. Heuristic Strategy (fallback)
        self.strategies.append(HeuristicStrategy())
        logger.info("Heuristic strategy initialized")
    
    def _ensure_knowledge_base(self):
        """Ensure knowledge base exists, build if needed"""
        import subprocess
        import sys
        
        try:
            logger.info("Building TTRPG knowledge base...")
            result = subprocess.run(
                [sys.executable, '-m', 'src.build_knowledgebase'],
                cwd=Path.cwd(),
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                logger.info("Knowledge base built successfully")
            else:
                logger.warning(f"Knowledge base build completed with warnings: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.warning("Knowledge base build timed out")
        except Exception as e:
            logger.warning(f"Could not build knowledge base: {e}")
    
    def classify_file(self, file_info: Dict) -> Dict:
        """Classify file using enterprise strategy chain"""
        file_path = Path(file_info['path'])
        filename = file_path.name
        
        # Get mime type
        import mimetypes
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if not mime_type:
            mime_type = 'application/octet-stream'
        
        # Try strategies in priority order
        for strategy in self.strategies:
            try:
                result = strategy.classify(file_path, filename, mime_type)
                if result and result.confidence > 0.6:  # Accept high-confidence results
                    logger.debug(f"Classified {filename} using {result.source}: {result.game_system}")
                    return {
                        **file_info,
                        'game_system': result.game_system,
                        'edition': result.edition,
                        'category': result.category,
                        'classification_source': result.source,
                        'classification_confidence': result.confidence
                    }
            except Exception as e:
                logger.debug(f"Strategy {strategy.__class__.__name__} failed for {filename}: {e}")
        
        # Fallback to heuristic with any confidence
        try:
            heuristic = HeuristicStrategy()
            result = heuristic.classify(file_path, filename, mime_type)
            if result:
                return {
                    **file_info,
                    'game_system': result.game_system,
                    'edition': result.edition,
                    'category': result.category,
                    'classification_source': result.source,
                    'classification_confidence': result.confidence
                }
        except Exception as e:
            logger.error(f"All classification strategies failed for {filename}: {e}")
        
        # Ultimate fallback
        return {
            **file_info,
            'game_system': 'Unknown',
            'edition': 'Unknown',
            'category': 'Unknown',
            'classification_source': 'fallback',
            'classification_confidence': 0.0
        }