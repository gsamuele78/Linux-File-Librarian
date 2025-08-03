#!/usr/bin/env python3
"""
Enhanced Media Manager for Linux File Librarian

Integrates with popular open-source Python libraries for comprehensive
media and document detection, similar to MediaElch but for documents and general files.

Key Features:
- PDF metadata extraction and classification
- Video/Audio metadata extraction using ffprobe
- Image metadata extraction using Pillow/ExifRead
- Document classification using python-magic
- ISBN detection and book metadata enrichment
- Media file organization with proper metadata
"""

import logging
import mimetypes
import os
import subprocess
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


@dataclass
class MediaMetadata:
    """Enhanced media metadata container"""
    title: Optional[str] = None
    author: Optional[str] = None
    creator: Optional[str] = None
    publisher: Optional[str] = None
    year: Optional[str] = None
    genre: Optional[str] = None
    duration: Optional[str] = None
    resolution: Optional[str] = None
    format_info: Optional[str] = None
    isbn: Optional[str] = None
    language: Optional[str] = None
    pages: Optional[int] = None
    file_size: Optional[int] = None
    creation_date: Optional[str] = None
    modification_date: Optional[str] = None


@dataclass
class EnhancedClassificationResult:
    """Enhanced classification result with rich metadata"""
    category: str
    subcategory: str
    media_type: str
    confidence: float
    metadata: MediaMetadata
    source: str
    suggested_path: List[str]  # 3-level hierarchy


class MediaDetector(ABC):
    """Abstract base class for media detectors"""
    
    @abstractmethod
    def can_handle(self, file_path: Path, mime_type: str) -> bool:
        """Check if this detector can handle the file"""
        pass
    
    @abstractmethod
    def extract_metadata(self, file_path: Path) -> MediaMetadata:
        """Extract metadata from the file"""
        pass
    
    @abstractmethod
    def classify(self, file_path: Path, metadata: MediaMetadata) -> EnhancedClassificationResult:
        """Classify the file based on metadata"""
        pass


class PDFDetector(MediaDetector):
    """PDF document detector using PyPDF2 and pdfplumber"""
    
    def can_handle(self, file_path: Path, mime_type: str) -> bool:
        return 'pdf' in mime_type.lower() or file_path.suffix.lower() == '.pdf'
    
    def extract_metadata(self, file_path: Path) -> MediaMetadata:
        metadata = MediaMetadata()
        
        try:
            # Try PyPDF2 first for basic metadata
            try:
                import PyPDF2
                with open(file_path, 'rb') as file:
                    reader = PyPDF2.PdfReader(file)
                    if reader.metadata:
                        metadata.title = reader.metadata.get('/Title')
                        metadata.author = reader.metadata.get('/Author')
                        metadata.creator = reader.metadata.get('/Creator')
                        metadata.creation_date = str(reader.metadata.get('/CreationDate', ''))
                    metadata.pages = len(reader.pages)
            except ImportError:
                logger.debug("PyPDF2 not available, trying pdfplumber")
            except Exception as e:
                logger.debug(f"PyPDF2 extraction failed: {e}")
            
            # Try pdfplumber for better text extraction
            try:
                import pdfplumber
                with pdfplumber.open(file_path) as pdf:
                    if not metadata.pages:
                        metadata.pages = len(pdf.pages)
                    
                    # Extract text from first page for ISBN detection
                    if pdf.pages:
                        first_page_text = pdf.pages[0].extract_text()
                        if first_page_text:
                            metadata.isbn = self._extract_isbn(first_page_text)
            except ImportError:
                logger.debug("pdfplumber not available")
            except Exception as e:
                logger.debug(f"pdfplumber extraction failed: {e}")
            
            # Get file stats
            stat = file_path.stat()
            metadata.file_size = stat.st_size
            metadata.modification_date = str(stat.st_mtime)
            
        except Exception as e:
            logger.error(f"PDF metadata extraction failed for {file_path}: {e}")
        
        return metadata
    
    def _extract_isbn(self, text: str) -> Optional[str]:
        """Extract ISBN from text using regex"""
        import re
        
        # ISBN-13 pattern
        isbn13_pattern = r'ISBN[-\s]?(?:13)?[-\s]?:?[-\s]?(\d{3}[-\s]?\d{1}[-\s]?\d{3}[-\s]?\d{5}[-\s]?\d{1})'
        # ISBN-10 pattern
        isbn10_pattern = r'ISBN[-\s]?(?:10)?[-\s]?:?[-\s]?(\d{1}[-\s]?\d{3}[-\s]?\d{5}[-\s]?[\dX])'
        
        for pattern in [isbn13_pattern, isbn10_pattern]:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return re.sub(r'[-\s]', '', match.group(1))
        
        return None
    
    def classify(self, file_path: Path, metadata: MediaMetadata) -> EnhancedClassificationResult:
        """Classify PDF based on content and metadata"""
        
        # Determine category based on content analysis
        filename_lower = file_path.name.lower()
        
        # Check for RPG/Gaming content
        rpg_keywords = ['d&d', 'dungeons', 'dragons', 'pathfinder', 'rpg', 'campaign', 'adventure', 'module']
        if any(keyword in filename_lower for keyword in rpg_keywords):
            category = "Gaming"
            subcategory = "RPG"
            suggested_path = ["Gaming", "RPG", "Documents"]
        
        # Check for technical/programming content
        elif any(keyword in filename_lower for keyword in ['python', 'java', 'programming', 'code', 'api', 'manual', 'guide']):
            category = "Technical"
            subcategory = "Programming"
            suggested_path = ["Technical", "Programming", "Documentation"]
        
        # Check for academic content
        elif any(keyword in filename_lower for keyword in ['thesis', 'paper', 'research', 'journal', 'academic']):
            category = "Academic"
            subcategory = "Research"
            suggested_path = ["Academic", "Research", "Papers"]
        
        # Check for business content
        elif any(keyword in filename_lower for keyword in ['report', 'business', 'financial', 'invoice', 'contract']):
            category = "Business"
            subcategory = "Documents"
            suggested_path = ["Business", "Documents", "Reports"]
        
        # Default to general documents
        else:
            category = "Documents"
            subcategory = "General"
            suggested_path = ["Documents", "General", "PDF"]
        
        return EnhancedClassificationResult(
            category=category,
            subcategory=subcategory,
            media_type="PDF",
            confidence=0.8,
            metadata=metadata,
            source="pdf_detector",
            suggested_path=suggested_path
        )


class VideoDetector(MediaDetector):
    """Video file detector using ffprobe"""
    
    def can_handle(self, file_path: Path, mime_type: str) -> bool:
        video_extensions = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v'}
        return (mime_type.startswith('video/') or 
                file_path.suffix.lower() in video_extensions)
    
    def extract_metadata(self, file_path: Path) -> MediaMetadata:
        metadata = MediaMetadata()
        
        try:
            # Use ffprobe to extract video metadata
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_format', '-show_streams', str(file_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                
                # Extract format information
                if 'format' in data:
                    format_info = data['format']
                    metadata.duration = format_info.get('duration')
                    metadata.format_info = format_info.get('format_name')
                    
                    # Extract tags if available
                    tags = format_info.get('tags', {})
                    metadata.title = tags.get('title')
                    metadata.author = tags.get('artist') or tags.get('author')
                    metadata.year = tags.get('date') or tags.get('year')
                    metadata.genre = tags.get('genre')
                
                # Extract video stream information
                for stream in data.get('streams', []):
                    if stream.get('codec_type') == 'video':
                        width = stream.get('width')
                        height = stream.get('height')
                        if width and height:
                            metadata.resolution = f"{width}x{height}"
                        break
            
            # Get file stats
            stat = file_path.stat()
            metadata.file_size = stat.st_size
            metadata.modification_date = str(stat.st_mtime)
            
        except subprocess.TimeoutExpired:
            logger.warning(f"ffprobe timeout for {file_path}")
        except FileNotFoundError:
            logger.warning("ffprobe not found - install ffmpeg for video metadata extraction")
        except Exception as e:
            logger.error(f"Video metadata extraction failed for {file_path}: {e}")
        
        return metadata
    
    def classify(self, file_path: Path, metadata: MediaMetadata) -> EnhancedClassificationResult:
        """Classify video based on metadata and filename"""
        
        filename_lower = file_path.name.lower()
        
        # Determine subcategory based on content
        if any(keyword in filename_lower for keyword in ['movie', 'film', 'cinema']):
            subcategory = "Movies"
        elif any(keyword in filename_lower for keyword in ['tv', 'series', 'episode', 's01', 's02']):
            subcategory = "TV Shows"
        elif any(keyword in filename_lower for keyword in ['tutorial', 'course', 'lesson', 'training']):
            subcategory = "Educational"
        elif any(keyword in filename_lower for keyword in ['music', 'concert', 'live']):
            subcategory = "Music Videos"
        else:
            subcategory = "General"
        
        # Determine quality tier based on resolution
        quality_tier = "Standard"
        if metadata.resolution:
            if '1920' in metadata.resolution or '1080' in metadata.resolution:
                quality_tier = "HD"
            elif '3840' in metadata.resolution or '4K' in metadata.resolution:
                quality_tier = "4K"
            elif '720' in metadata.resolution:
                quality_tier = "HD"
        
        suggested_path = ["Media", "Video", subcategory]
        
        return EnhancedClassificationResult(
            category="Media",
            subcategory=subcategory,
            media_type="Video",
            confidence=0.9,
            metadata=metadata,
            source="video_detector",
            suggested_path=suggested_path
        )


class AudioDetector(MediaDetector):
    """Audio file detector using mutagen"""
    
    def can_handle(self, file_path: Path, mime_type: str) -> bool:
        audio_extensions = {'.mp3', '.flac', '.wav', '.ogg', '.m4a', '.aac', '.wma'}
        return (mime_type.startswith('audio/') or 
                file_path.suffix.lower() in audio_extensions)
    
    def extract_metadata(self, file_path: Path) -> MediaMetadata:
        metadata = MediaMetadata()
        
        try:
            # Try mutagen for audio metadata
            try:
                from mutagen import File as MutagenFile
                audio_file = MutagenFile(file_path)
                
                if audio_file:
                    metadata.title = self._get_tag(audio_file, ['TIT2', 'TITLE', '\xa9nam'])
                    metadata.author = self._get_tag(audio_file, ['TPE1', 'ARTIST', '\xa9ART'])
                    metadata.genre = self._get_tag(audio_file, ['TCON', 'GENRE', '\xa9gen'])
                    metadata.year = self._get_tag(audio_file, ['TDRC', 'DATE', '\xa9day'])
                    
                    if hasattr(audio_file, 'info') and audio_file.info:
                        metadata.duration = str(audio_file.info.length)
                        metadata.format_info = audio_file.mime[0] if audio_file.mime else None
            
            except ImportError:
                logger.debug("mutagen not available for audio metadata")
            except Exception as e:
                logger.debug(f"mutagen extraction failed: {e}")
            
            # Get file stats
            stat = file_path.stat()
            metadata.file_size = stat.st_size
            metadata.modification_date = str(stat.st_mtime)
            
        except Exception as e:
            logger.error(f"Audio metadata extraction failed for {file_path}: {e}")
        
        return metadata
    
    def _get_tag(self, audio_file, tag_names: List[str]) -> Optional[str]:
        """Get tag value from various possible tag names"""
        for tag_name in tag_names:
            if tag_name in audio_file:
                value = audio_file[tag_name]
                if isinstance(value, list) and value:
                    return str(value[0])
                elif value:
                    return str(value)
        return None
    
    def classify(self, file_path: Path, metadata: MediaMetadata) -> EnhancedClassificationResult:
        """Classify audio based on metadata"""
        
        filename_lower = file_path.name.lower()
        
        # Determine subcategory
        if any(keyword in filename_lower for keyword in ['podcast', 'interview', 'talk']):
            subcategory = "Podcasts"
        elif any(keyword in filename_lower for keyword in ['audiobook', 'book', 'chapter']):
            subcategory = "Audiobooks"
        elif metadata.genre and 'classical' in metadata.genre.lower():
            subcategory = "Classical"
        elif metadata.genre and any(genre in metadata.genre.lower() for genre in ['rock', 'pop', 'jazz', 'blues']):
            subcategory = "Music"
        else:
            subcategory = "General"
        
        suggested_path = ["Media", "Audio", subcategory]
        
        return EnhancedClassificationResult(
            category="Media",
            subcategory=subcategory,
            media_type="Audio",
            confidence=0.8,
            metadata=metadata,
            source="audio_detector",
            suggested_path=suggested_path
        )


class ImageDetector(MediaDetector):
    """Image file detector using Pillow and ExifRead"""
    
    def can_handle(self, file_path: Path, mime_type: str) -> bool:
        image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.svg'}
        return (mime_type.startswith('image/') or 
                file_path.suffix.lower() in image_extensions)
    
    def extract_metadata(self, file_path: Path) -> MediaMetadata:
        metadata = MediaMetadata()
        
        try:
            # Try Pillow for basic image info
            try:
                from PIL import Image
                from PIL.ExifTags import TAGS
                
                with Image.open(file_path) as img:
                    metadata.resolution = f"{img.width}x{img.height}"
                    metadata.format_info = img.format
                    
                    # Extract EXIF data
                    exif_data = img._getexif()
                    if exif_data:
                        for tag_id, value in exif_data.items():
                            tag = TAGS.get(tag_id, tag_id)
                            if tag == 'DateTime':
                                metadata.creation_date = str(value)
                            elif tag == 'Artist':
                                metadata.author = str(value)
                            elif tag == 'ImageDescription':
                                metadata.title = str(value)
            
            except ImportError:
                logger.debug("Pillow not available for image metadata")
            except Exception as e:
                logger.debug(f"Pillow extraction failed: {e}")
            
            # Get file stats
            stat = file_path.stat()
            metadata.file_size = stat.st_size
            metadata.modification_date = str(stat.st_mtime)
            
        except Exception as e:
            logger.error(f"Image metadata extraction failed for {file_path}: {e}")
        
        return metadata
    
    def classify(self, file_path: Path, metadata: MediaMetadata) -> EnhancedClassificationResult:
        """Classify image based on metadata and filename"""
        
        filename_lower = file_path.name.lower()
        
        # Determine subcategory
        if any(keyword in filename_lower for keyword in ['photo', 'img', 'pic']):
            subcategory = "Photos"
        elif any(keyword in filename_lower for keyword in ['screenshot', 'screen', 'capture']):
            subcategory = "Screenshots"
        elif any(keyword in filename_lower for keyword in ['diagram', 'chart', 'graph']):
            subcategory = "Diagrams"
        elif any(keyword in filename_lower for keyword in ['icon', 'logo', 'symbol']):
            subcategory = "Icons"
        else:
            subcategory = "General"
        
        suggested_path = ["Media", "Images", subcategory]
        
        return EnhancedClassificationResult(
            category="Media",
            subcategory=subcategory,
            media_type="Image",
            confidence=0.7,
            metadata=metadata,
            source="image_detector",
            suggested_path=suggested_path
        )


class DocumentDetector(MediaDetector):
    """General document detector using python-magic"""
    
    def can_handle(self, file_path: Path, mime_type: str) -> bool:
        doc_types = ['application/msword', 'application/vnd.openxmlformats-officedocument',
                     'application/vnd.oasis.opendocument', 'text/plain', 'text/rtf']
        doc_extensions = {'.doc', '.docx', '.odt', '.rtf', '.txt', '.md'}
        
        return (any(doc_type in mime_type for doc_type in doc_types) or
                file_path.suffix.lower() in doc_extensions)
    
    def extract_metadata(self, file_path: Path) -> MediaMetadata:
        metadata = MediaMetadata()
        
        try:
            # Try python-docx for Word documents
            if file_path.suffix.lower() in ['.docx']:
                try:
                    from docx import Document
                    doc = Document(file_path)
                    
                    # Extract core properties
                    props = doc.core_properties
                    metadata.title = props.title
                    metadata.author = props.author
                    metadata.creation_date = str(props.created) if props.created else None
                    metadata.modification_date = str(props.modified) if props.modified else None
                    
                except ImportError:
                    logger.debug("python-docx not available")
                except Exception as e:
                    logger.debug(f"python-docx extraction failed: {e}")
            
            # Get file stats
            stat = file_path.stat()
            metadata.file_size = stat.st_size
            if not metadata.modification_date:
                metadata.modification_date = str(stat.st_mtime)
            
        except Exception as e:
            logger.error(f"Document metadata extraction failed for {file_path}: {e}")
        
        return metadata
    
    def classify(self, file_path: Path, metadata: MediaMetadata) -> EnhancedClassificationResult:
        """Classify document based on extension and content"""
        
        extension = file_path.suffix.lower()
        filename_lower = file_path.name.lower()
        
        # Determine subcategory based on file type and content
        if extension in ['.md', '.txt']:
            subcategory = "Text"
        elif extension in ['.doc', '.docx']:
            subcategory = "Word Documents"
        elif extension in ['.odt']:
            subcategory = "OpenDocument"
        elif extension in ['.rtf']:
            subcategory = "Rich Text"
        else:
            subcategory = "General"
        
        # Check for specific document types
        if any(keyword in filename_lower for keyword in ['readme', 'license', 'changelog']):
            subcategory = "Project Documentation"
        elif any(keyword in filename_lower for keyword in ['manual', 'guide', 'help']):
            subcategory = "Manuals"
        
        suggested_path = ["Documents", subcategory, "Files"]
        
        return EnhancedClassificationResult(
            category="Documents",
            subcategory=subcategory,
            media_type="Document",
            confidence=0.6,
            metadata=metadata,
            source="document_detector",
            suggested_path=suggested_path
        )


class EnhancedMediaManager:
    """Enhanced media manager integrating all detectors"""
    
    def __init__(self):
        self.detectors = [
            PDFDetector(),
            VideoDetector(),
            AudioDetector(),
            ImageDetector(),
            DocumentDetector()
        ]
        logger.info(f"Enhanced Media Manager initialized with {len(self.detectors)} detectors")
    
    def analyze_file(self, file_path: Path) -> EnhancedClassificationResult:
        """Analyze file and return enhanced classification"""
        
        # Get MIME type
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if not mime_type:
            mime_type = 'application/octet-stream'
        
        # Find appropriate detector
        for detector in self.detectors:
            if detector.can_handle(file_path, mime_type):
                try:
                    logger.debug(f"Using {detector.__class__.__name__} for {file_path}")
                    metadata = detector.extract_metadata(file_path)
                    result = detector.classify(file_path, metadata)
                    return result
                except Exception as e:
                    logger.error(f"Detector {detector.__class__.__name__} failed for {file_path}: {e}")
                    continue
        
        # Fallback classification
        return self._fallback_classification(file_path, mime_type)
    
    def _fallback_classification(self, file_path: Path, mime_type: str) -> EnhancedClassificationResult:
        """Fallback classification for unhandled files"""
        
        metadata = MediaMetadata()
        stat = file_path.stat()
        metadata.file_size = stat.st_size
        metadata.modification_date = str(stat.st_mtime)
        
        # Basic classification based on MIME type
        major_type = mime_type.split('/')[0]
        
        if major_type == 'application':
            category = "Applications"
            subcategory = "Executables"
        elif major_type == 'text':
            category = "Documents"
            subcategory = "Text"
        else:
            category = "Miscellaneous"
            subcategory = "Unknown"
        
        suggested_path = [category, subcategory, "Files"]
        
        return EnhancedClassificationResult(
            category=category,
            subcategory=subcategory,
            media_type=major_type.title(),
            confidence=0.3,
            metadata=metadata,
            source="fallback",
            suggested_path=suggested_path
        )
    
    def get_installation_requirements(self) -> Dict[str, List[str]]:
        """Get installation requirements for enhanced functionality"""
        return {
            "pdf_processing": ["PyPDF2", "pdfplumber"],
            "video_processing": ["ffmpeg (system package)"],
            "audio_processing": ["mutagen"],
            "image_processing": ["Pillow"],
            "document_processing": ["python-docx"],
            "system_detection": ["python-magic", "python-magic-bin (Windows)"]
        }
    
    def check_dependencies(self) -> Dict[str, bool]:
        """Check which dependencies are available"""
        dependencies = {}
        
        # Check Python packages
        packages = {
            'PyPDF2': 'PyPDF2',
            'pdfplumber': 'pdfplumber',
            'mutagen': 'mutagen',
            'Pillow': 'PIL',
            'python-docx': 'docx',
            'python-magic': 'magic'
        }
        
        for name, import_name in packages.items():
            try:
                __import__(import_name)
                dependencies[name] = True
            except ImportError:
                dependencies[name] = False
        
        # Check system tools
        try:
            subprocess.run(['ffprobe', '-version'], capture_output=True, timeout=5)
            dependencies['ffmpeg'] = True
        except (FileNotFoundError, subprocess.TimeoutExpired):
            dependencies['ffmpeg'] = False
        
        return dependencies