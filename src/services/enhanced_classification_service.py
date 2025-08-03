#!/usr/bin/env python3
"""
Enhanced Classification Service

Integrates the Enhanced Media Manager with the existing classification system
to provide comprehensive file analysis and organization similar to MediaElch
but for documents and general media files.
"""

import logging
from pathlib import Path
from typing import Dict, Optional, List
from dataclasses import dataclass

from src.services.enhanced_media_manager import EnhancedMediaManager, EnhancedClassificationResult

logger = logging.getLogger(__name__)


@dataclass
class LegacyClassificationResult:
    """Legacy classification result for backward compatibility"""
    game_system: str
    edition: str
    category: str
    confidence: float
    source: str
    metadata: Dict = None


class EnhancedClassificationService:
    """Enhanced classification service with media manager integration"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.media_manager = EnhancedMediaManager()
        self.legacy_service = None
        
        # Initialize legacy service for TTRPG content
        self._initialize_legacy_service()
        
        # Check and report dependencies
        self._check_dependencies()
    
    def _initialize_legacy_service(self):
        """Initialize legacy classification service for TTRPG content"""
        try:
            from src.enterprise.enterprise_classification_service import EnterpriseClassificationService
            self.legacy_service = EnterpriseClassificationService(self.config)
            logger.info("Legacy TTRPG classification service initialized")
        except Exception as e:
            logger.warning(f"Legacy classification service failed to initialize: {e}")
    
    def _check_dependencies(self):
        """Check and report available dependencies"""
        dependencies = self.media_manager.check_dependencies()
        
        available = [name for name, status in dependencies.items() if status]
        missing = [name for name, status in dependencies.items() if not status]
        
        if available:
            logger.info(f"Available media processing libraries: {', '.join(available)}")
        
        if missing:
            logger.warning(f"Missing optional libraries (install for enhanced functionality): {', '.join(missing)}")
            
            # Provide installation hints
            requirements = self.media_manager.get_installation_requirements()
            for category, packages in requirements.items():
                if any(pkg.split()[0] in missing for pkg in packages):
                    logger.info(f"For {category}: pip install {' '.join(pkg for pkg in packages if not '(system' in pkg)}")
    
    def classify_file(self, file_info: Dict) -> Dict:
        """Enhanced file classification with media manager integration"""
        file_path = Path(file_info['path'])
        
        try:
            # First try enhanced media manager
            enhanced_result = self.media_manager.analyze_file(file_path)
            
            # Check if this might be TTRPG content that legacy service handles better
            if self._should_use_legacy_service(file_path, enhanced_result):
                legacy_result = self._try_legacy_classification(file_info)
                if legacy_result and legacy_result.confidence > enhanced_result.confidence:
                    return self._convert_legacy_result(file_info, legacy_result)
            
            # Use enhanced result
            return self._convert_enhanced_result(file_info, enhanced_result)
            
        except Exception as e:
            logger.error(f"Enhanced classification failed for {file_path}: {e}")
            
            # Fallback to legacy service
            if self.legacy_service:
                try:
                    legacy_result = self.legacy_service.classify_file(file_info)
                    return legacy_result
                except Exception as legacy_e:
                    logger.error(f"Legacy classification also failed: {legacy_e}")
            
            # Ultimate fallback
            return self._create_fallback_result(file_info)
    
    def _should_use_legacy_service(self, file_path: Path, enhanced_result: EnhancedClassificationResult) -> bool:
        """Determine if legacy service might provide better classification"""
        
        # Use legacy for potential TTRPG content
        filename_lower = file_path.name.lower()
        ttrpg_keywords = [
            'd&d', 'dungeons', 'dragons', 'pathfinder', 'rpg', 'campaign', 
            'adventure', 'module', 'character', 'sheet', 'dnd', 'pf2e'
        ]
        
        has_ttrpg_keywords = any(keyword in filename_lower for keyword in ttrpg_keywords)
        
        # Use legacy if enhanced confidence is low and might be TTRPG
        return (has_ttrpg_keywords and enhanced_result.confidence < 0.7) or \
               (enhanced_result.category == "Documents" and has_ttrpg_keywords)
    
    def _try_legacy_classification(self, file_info: Dict) -> Optional[LegacyClassificationResult]:
        """Try legacy classification service"""
        if not self.legacy_service:
            return None
        
        try:
            result = self.legacy_service.classify_file(file_info)
            
            # Convert to legacy result format
            return LegacyClassificationResult(
                game_system=result.get('game_system', 'Unknown'),
                edition=result.get('edition', 'Unknown'),
                category=result.get('category', 'Unknown'),
                confidence=result.get('classification_confidence', 0.0),
                source=result.get('classification_source', 'legacy'),
                metadata=result
            )
        except Exception as e:
            logger.debug(f"Legacy classification failed: {e}")
            return None
    
    def _convert_enhanced_result(self, file_info: Dict, result: EnhancedClassificationResult) -> Dict:
        """Convert enhanced result to legacy format"""
        
        # Create 3-level hierarchy path
        hierarchy_path = result.suggested_path
        if len(hierarchy_path) < 3:
            hierarchy_path.extend(['Unknown'] * (3 - len(hierarchy_path)))
        
        # Map to legacy format
        game_system = hierarchy_path[0]  # Top level (Media, Documents, Gaming, etc.)
        edition = hierarchy_path[1]      # Second level (Video, Audio, RPG, etc.)
        category = hierarchy_path[2]     # Third level (Movies, PDF, Documents, etc.)
        
        # Add enhanced metadata
        enhanced_metadata = {
            'media_type': result.media_type,
            'suggested_path': result.suggested_path,
            'enhanced_metadata': {
                'title': result.metadata.title,
                'author': result.metadata.author,
                'creator': result.metadata.creator,
                'publisher': result.metadata.publisher,
                'year': result.metadata.year,
                'genre': result.metadata.genre,
                'duration': result.metadata.duration,
                'resolution': result.metadata.resolution,
                'format_info': result.metadata.format_info,
                'isbn': result.metadata.isbn,
                'language': result.metadata.language,
                'pages': result.metadata.pages,
                'file_size': result.metadata.file_size,
                'creation_date': result.metadata.creation_date,
                'modification_date': result.metadata.modification_date
            }
        }
        
        return {
            **file_info,
            'game_system': game_system,
            'edition': edition,
            'category': category,
            'classification_source': f"enhanced_{result.source}",
            'classification_confidence': result.confidence,
            **enhanced_metadata
        }
    
    def _convert_legacy_result(self, file_info: Dict, result: LegacyClassificationResult) -> Dict:
        """Convert legacy result to standard format"""
        return {
            **file_info,
            'game_system': result.game_system,
            'edition': result.edition,
            'category': result.category,
            'classification_source': f"legacy_{result.source}",
            'classification_confidence': result.confidence,
            'legacy_metadata': result.metadata
        }
    
    def _create_fallback_result(self, file_info: Dict) -> Dict:
        """Create fallback result when all classification fails"""
        file_path = Path(file_info['path'])
        
        # Basic classification based on file extension
        extension = file_path.suffix.lower()
        
        if extension in ['.pdf']:
            game_system, edition, category = "Documents", "PDF", "Files"
        elif extension in ['.mp4', '.avi', '.mkv', '.mov']:
            game_system, edition, category = "Media", "Video", "Files"
        elif extension in ['.mp3', '.flac', '.wav']:
            game_system, edition, category = "Media", "Audio", "Files"
        elif extension in ['.jpg', '.png', '.gif']:
            game_system, edition, category = "Media", "Images", "Files"
        elif extension in ['.doc', '.docx', '.txt']:
            game_system, edition, category = "Documents", "Text", "Files"
        else:
            game_system, edition, category = "Miscellaneous", "Unknown", "Files"
        
        return {
            **file_info,
            'game_system': game_system,
            'edition': edition,
            'category': category,
            'classification_source': 'fallback',
            'classification_confidence': 0.1
        }
    
    def get_classification_stats(self) -> Dict:
        """Get classification statistics and capabilities"""
        dependencies = self.media_manager.check_dependencies()
        
        return {
            'enhanced_manager_available': True,
            'legacy_service_available': self.legacy_service is not None,
            'dependencies': dependencies,
            'supported_formats': {
                'pdf': dependencies.get('PyPDF2', False) or dependencies.get('pdfplumber', False),
                'video': dependencies.get('ffmpeg', False),
                'audio': dependencies.get('mutagen', False),
                'images': dependencies.get('Pillow', False),
                'documents': dependencies.get('python-docx', False),
                'general': True  # Always available
            },
            'detectors': [
                'PDFDetector',
                'VideoDetector', 
                'AudioDetector',
                'ImageDetector',
                'DocumentDetector'
            ]
        }
    
    def install_missing_dependencies(self) -> List[str]:
        """Get list of commands to install missing dependencies"""
        dependencies = self.media_manager.check_dependencies()
        missing = [name for name, status in dependencies.items() if not status]
        
        install_commands = []
        
        if missing:
            # Python packages
            python_packages = []
            if 'PyPDF2' in missing:
                python_packages.append('PyPDF2')
            if 'pdfplumber' in missing:
                python_packages.append('pdfplumber')
            if 'mutagen' in missing:
                python_packages.append('mutagen')
            if 'Pillow' in missing:
                python_packages.append('Pillow')
            if 'python-docx' in missing:
                python_packages.append('python-docx')
            if 'python-magic' in missing:
                python_packages.append('python-magic')
            
            if python_packages:
                install_commands.append(f"pip install {' '.join(python_packages)}")
            
            # System packages
            if 'ffmpeg' in missing:
                install_commands.append("# Install ffmpeg:")
                install_commands.append("# Ubuntu/Debian: sudo apt install ffmpeg")
                install_commands.append("# CentOS/RHEL: sudo yum install ffmpeg")
                install_commands.append("# macOS: brew install ffmpeg")
        
        return install_commands