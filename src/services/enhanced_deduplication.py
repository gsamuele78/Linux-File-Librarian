#!/usr/bin/env python3
"""
Enhanced Deduplication System

Provides comprehensive deduplication with content analysis, metadata comparison,
and intelligent duplicate resolution strategies.
"""

import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


@dataclass
class DuplicateGroup:
    """Group of duplicate files"""
    files: List[Dict]
    hash_value: str
    total_size: int
    duplicate_type: str  # 'exact', 'content', 'metadata'
    confidence: float
    recommended_keep: Optional[Dict] = None


@dataclass
class DeduplicationResult:
    """Deduplication operation result"""
    original_count: int
    unique_count: int
    duplicate_groups: List[DuplicateGroup]
    space_saved: int
    processing_time: float


class DuplicateDetector(ABC):
    """Abstract base class for duplicate detectors"""
    
    @abstractmethod
    def detect_duplicates(self, files: List[Dict]) -> List[DuplicateGroup]:
        """Detect duplicates in file list"""
        pass
    
    @abstractmethod
    def get_confidence_threshold(self) -> float:
        """Get minimum confidence threshold"""
        pass


class ExactDuplicateDetector(DuplicateDetector):
    """Detects exact duplicates using file hashes"""
    
    def detect_duplicates(self, files: List[Dict]) -> List[DuplicateGroup]:
        """Detect exact duplicates using SHA256 hashes"""
        hash_groups = {}
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Calculate hashes in parallel
            hash_futures = {executor.submit(self._calculate_hash, f): f for f in files}
            
            for future in hash_futures:
                try:
                    file_info = hash_futures[future]
                    file_hash = future.result(timeout=120)
                    
                    if file_hash not in hash_groups:
                        hash_groups[file_hash] = []
                    hash_groups[file_hash].append(file_info)
                    
                except Exception as e:
                    logger.warning(f"Hash calculation failed for {hash_futures[future].get('path', 'unknown')}: {e}")
        
        # Create duplicate groups
        duplicate_groups = []
        for file_hash, group_files in hash_groups.items():
            if len(group_files) > 1:
                total_size = sum(f.get('size', 0) for f in group_files)
                
                duplicate_group = DuplicateGroup(
                    files=group_files,
                    hash_value=file_hash,
                    total_size=total_size,
                    duplicate_type='exact',
                    confidence=1.0,
                    recommended_keep=self._select_best_file(group_files)
                )
                duplicate_groups.append(duplicate_group)
        
        return duplicate_groups
    
    def _calculate_hash(self, file_info: Dict) -> str:
        """Calculate SHA256 hash of file"""
        file_path = Path(file_info['path'])
        hash_obj = hashlib.sha256()
        
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except Exception as e:
            logger.error(f"Hash calculation error for {file_path}: {e}")
            return f"error_{file_path.name}_{file_info.get('size', 0)}"
    
    def _select_best_file(self, files: List[Dict]) -> Dict:
        """Select the best file to keep from duplicates"""
        # Prefer files with better paths (shorter, more organized)
        def path_score(file_info):
            path = Path(file_info['path'])
            score = 0
            
            # Prefer shorter paths
            score -= len(path.parts) * 10
            
            # Prefer files not in temp/cache directories
            path_str = str(path).lower()
            if any(bad in path_str for bad in ['temp', 'cache', 'tmp', 'download']):
                score -= 100
            
            # Prefer files with metadata
            if file_info.get('enhanced_metadata'):
                score += 50
            
            # Prefer files with better classification
            confidence = file_info.get('classification_confidence', 0)
            score += confidence * 20
            
            return score
        
        return max(files, key=path_score)
    
    def get_confidence_threshold(self) -> float:
        return 1.0


class ContentDuplicateDetector(DuplicateDetector):
    """Detects content duplicates using metadata and partial hashing"""
    
    def detect_duplicates(self, files: List[Dict]) -> List[DuplicateGroup]:
        """Detect content duplicates using metadata comparison"""
        duplicate_groups = []
        
        # Group by size first for efficiency
        size_groups = {}
        for file_info in files:
            size = file_info.get('size', 0)
            if size not in size_groups:
                size_groups[size] = []
            size_groups[size].append(file_info)
        
        # Check each size group for content duplicates
        for size, group_files in size_groups.items():
            if len(group_files) > 1:
                content_groups = self._group_by_content(group_files)
                duplicate_groups.extend(content_groups)
        
        return duplicate_groups
    
    def _group_by_content(self, files: List[Dict]) -> List[DuplicateGroup]:
        """Group files by content similarity"""
        groups = []
        
        # Group by enhanced metadata if available
        metadata_groups = {}
        
        for file_info in files:
            metadata = file_info.get('enhanced_metadata', {})
            
            # Create content signature
            signature_parts = []
            
            # Use title, author, duration for media files
            if metadata.get('title'):
                signature_parts.append(metadata['title'].lower().strip())
            if metadata.get('author'):
                signature_parts.append(metadata['author'].lower().strip())
            if metadata.get('duration'):
                signature_parts.append(str(metadata['duration']))
            
            # Use filename if no metadata
            if not signature_parts:
                path = Path(file_info['path'])
                signature_parts.append(path.stem.lower())
            
            signature = '|'.join(signature_parts)
            
            if signature not in metadata_groups:
                metadata_groups[signature] = []
            metadata_groups[signature].append(file_info)
        
        # Create duplicate groups for content matches
        for signature, group_files in metadata_groups.items():
            if len(group_files) > 1:
                # Verify with partial hash for higher confidence
                verified_groups = self._verify_with_partial_hash(group_files)
                groups.extend(verified_groups)
        
        return groups
    
    def _verify_with_partial_hash(self, files: List[Dict]) -> List[DuplicateGroup]:
        """Verify content duplicates with partial file hashing"""
        hash_groups = {}
        
        for file_info in files:
            try:
                partial_hash = self._calculate_partial_hash(file_info)
                if partial_hash not in hash_groups:
                    hash_groups[partial_hash] = []
                hash_groups[partial_hash].append(file_info)
            except Exception as e:
                logger.debug(f"Partial hash failed for {file_info.get('path')}: {e}")
        
        groups = []
        for partial_hash, group_files in hash_groups.items():
            if len(group_files) > 1:
                total_size = sum(f.get('size', 0) for f in group_files)
                
                duplicate_group = DuplicateGroup(
                    files=group_files,
                    hash_value=partial_hash,
                    total_size=total_size,
                    duplicate_type='content',
                    confidence=0.8,
                    recommended_keep=self._select_best_content_file(group_files)
                )
                groups.append(duplicate_group)
        
        return groups
    
    def _calculate_partial_hash(self, file_info: Dict) -> str:
        """Calculate hash of first and last 8KB of file"""
        file_path = Path(file_info['path'])
        hash_obj = hashlib.md5()
        
        with open(file_path, 'rb') as f:
            # Hash first 8KB
            first_chunk = f.read(8192)
            hash_obj.update(first_chunk)
            
            # Hash last 8KB if file is large enough
            file_size = file_info.get('size', 0)
            if file_size > 16384:
                f.seek(-8192, 2)  # Seek to 8KB from end
                last_chunk = f.read(8192)
                hash_obj.update(last_chunk)
        
        return hash_obj.hexdigest()
    
    def _select_best_content_file(self, files: List[Dict]) -> Dict:
        """Select best file based on content quality"""
        def content_score(file_info):
            score = 0
            
            # Prefer files with more metadata
            metadata = file_info.get('enhanced_metadata', {})
            metadata_count = sum(1 for v in metadata.values() if v and v != 'Unknown')
            score += metadata_count * 10
            
            # Prefer higher quality media
            resolution = metadata.get('resolution', '')
            if '1080' in resolution:
                score += 30
            elif '720' in resolution:
                score += 20
            elif '4K' in resolution or '3840' in resolution:
                score += 50
            
            # Prefer better file formats
            format_info = metadata.get('format_info', '').lower()
            if 'mp4' in format_info or 'pdf' in format_info:
                score += 15
            
            return score
        
        return max(files, key=content_score)
    
    def get_confidence_threshold(self) -> float:
        return 0.7


class MetadataDuplicateDetector(DuplicateDetector):
    """Detects duplicates based on metadata similarity"""
    
    def detect_duplicates(self, files: List[Dict]) -> List[DuplicateGroup]:
        """Detect metadata-based duplicates"""
        duplicate_groups = []
        
        # Group by metadata signatures
        metadata_groups = {}
        
        for file_info in files:
            signature = self._create_metadata_signature(file_info)
            if signature and len(signature) > 10:  # Meaningful signature
                if signature not in metadata_groups:
                    metadata_groups[signature] = []
                metadata_groups[signature].append(file_info)
        
        # Create duplicate groups
        for signature, group_files in metadata_groups.items():
            if len(group_files) > 1:
                # Calculate similarity confidence
                confidence = self._calculate_metadata_confidence(group_files)
                
                if confidence >= self.get_confidence_threshold():
                    total_size = sum(f.get('size', 0) for f in group_files)
                    
                    duplicate_group = DuplicateGroup(
                        files=group_files,
                        hash_value=signature,
                        total_size=total_size,
                        duplicate_type='metadata',
                        confidence=confidence,
                        recommended_keep=self._select_best_metadata_file(group_files)
                    )
                    duplicate_groups.append(duplicate_group)
        
        return duplicate_groups
    
    def _create_metadata_signature(self, file_info: Dict) -> str:
        """Create metadata signature for comparison"""
        metadata = file_info.get('enhanced_metadata', {})
        
        signature_parts = []
        
        # Core metadata fields
        for field in ['title', 'author', 'creator', 'publisher']:
            value = metadata.get(field)
            if value and value != 'Unknown':
                signature_parts.append(f"{field}:{value.lower().strip()}")
        
        # ISBN for books
        isbn = metadata.get('isbn')
        if isbn:
            signature_parts.append(f"isbn:{isbn}")
        
        # Duration for media (rounded to nearest 10 seconds)
        duration = metadata.get('duration')
        if duration:
            try:
                duration_seconds = int(float(duration))
                rounded_duration = (duration_seconds // 10) * 10
                signature_parts.append(f"duration:{rounded_duration}")
            except (ValueError, TypeError):
                pass
        
        return '|'.join(signature_parts)
    
    def _calculate_metadata_confidence(self, files: List[Dict]) -> float:
        """Calculate confidence that files are duplicates based on metadata"""
        if len(files) < 2:
            return 0.0
        
        # Compare metadata fields across all files
        common_fields = 0
        total_fields = 0
        
        metadata_list = [f.get('enhanced_metadata', {}) for f in files]
        
        for field in ['title', 'author', 'creator', 'publisher', 'year', 'isbn']:
            values = [m.get(field) for m in metadata_list if m.get(field) and m.get(field) != 'Unknown']
            
            if values:
                total_fields += 1
                # Check if all values are the same (case-insensitive)
                normalized_values = [v.lower().strip() if isinstance(v, str) else str(v) for v in values]
                if len(set(normalized_values)) == 1:
                    common_fields += 1
        
        if total_fields == 0:
            return 0.0
        
        base_confidence = common_fields / total_fields
        
        # Boost confidence for strong identifiers
        for metadata in metadata_list:
            if metadata.get('isbn'):
                base_confidence = min(1.0, base_confidence + 0.2)
                break
        
        return base_confidence
    
    def _select_best_metadata_file(self, files: List[Dict]) -> Dict:
        """Select file with most complete metadata"""
        def metadata_score(file_info):
            metadata = file_info.get('enhanced_metadata', {})
            
            # Count non-empty metadata fields
            field_count = sum(1 for v in metadata.values() 
                            if v and v != 'Unknown' and str(v).strip())
            
            # Bonus for high-value fields
            bonus = 0
            if metadata.get('isbn'):
                bonus += 20
            if metadata.get('title'):
                bonus += 10
            if metadata.get('author'):
                bonus += 10
            
            return field_count * 5 + bonus
        
        return max(files, key=metadata_score)
    
    def get_confidence_threshold(self) -> float:
        return 0.6


class EnhancedDeduplicationManager:
    """Enhanced deduplication manager with multiple detection strategies"""
    
    def __init__(self):
        self.detectors = [
            ExactDuplicateDetector(),
            ContentDuplicateDetector(),
            MetadataDuplicateDetector()
        ]
        self.dedup_stats = {
            'files_processed': 0,
            'duplicates_found': 0,
            'space_saved': 0,
            'groups_created': 0
        }
    
    def deduplicate_files(self, files: List[Dict]) -> DeduplicationResult:
        """Perform comprehensive deduplication"""
        import time
        start_time = time.time()
        
        logger.info(f"Starting enhanced deduplication of {len(files)} files")
        
        all_duplicate_groups = []
        processed_hashes = set()
        
        # Run each detector
        for detector in self.detectors:
            try:
                logger.debug(f"Running {detector.__class__.__name__}")
                groups = detector.detect_duplicates(files)
                
                # Filter out groups we've already processed
                new_groups = []
                for group in groups:
                    if group.hash_value not in processed_hashes:
                        new_groups.append(group)
                        processed_hashes.add(group.hash_value)
                
                all_duplicate_groups.extend(new_groups)
                logger.debug(f"{detector.__class__.__name__} found {len(new_groups)} new duplicate groups")
                
            except Exception as e:
                logger.error(f"Detector {detector.__class__.__name__} failed: {e}")
        
        # Calculate statistics
        unique_files = self._calculate_unique_files(files, all_duplicate_groups)
        space_saved = sum(group.total_size - max(f.get('size', 0) for f in group.files) 
                         for group in all_duplicate_groups)
        
        processing_time = time.time() - start_time
        
        # Update stats
        self.dedup_stats.update({
            'files_processed': len(files),
            'duplicates_found': sum(len(group.files) - 1 for group in all_duplicate_groups),
            'space_saved': space_saved,
            'groups_created': len(all_duplicate_groups)
        })
        
        result = DeduplicationResult(
            original_count=len(files),
            unique_count=len(unique_files),
            duplicate_groups=all_duplicate_groups,
            space_saved=space_saved,
            processing_time=processing_time
        )
        
        logger.info(f"Deduplication complete: {len(unique_files)} unique files, "
                   f"{len(all_duplicate_groups)} duplicate groups, "
                   f"{space_saved / (1024*1024):.1f}MB potential space savings")
        
        return result
    
    def _calculate_unique_files(self, files: List[Dict], duplicate_groups: List[DuplicateGroup]) -> List[Dict]:
        """Calculate list of unique files after deduplication"""
        duplicate_paths = set()
        
        # Collect all duplicate file paths
        for group in duplicate_groups:
            for file_info in group.files:
                duplicate_paths.add(file_info['path'])
        
        # Keep files not in any duplicate group, plus recommended files from each group
        unique_files = []
        
        # Add non-duplicate files
        for file_info in files:
            if file_info['path'] not in duplicate_paths:
                unique_files.append(file_info)
        
        # Add recommended files from duplicate groups
        for group in duplicate_groups:
            if group.recommended_keep:
                unique_files.append(group.recommended_keep)
        
        return unique_files
    
    def get_deduplication_report(self) -> Dict:
        """Get comprehensive deduplication report"""
        return {
            'statistics': self.dedup_stats,
            'detectors': [d.__class__.__name__ for d in self.detectors],
            'detector_thresholds': {d.__class__.__name__: d.get_confidence_threshold() 
                                  for d in self.detectors}
        }
    
    def resolve_duplicates(self, duplicate_groups: List[DuplicateGroup], 
                          strategy: str = 'keep_recommended') -> List[Dict]:
        """Resolve duplicates using specified strategy"""
        
        if strategy == 'keep_recommended':
            return [group.recommended_keep for group in duplicate_groups 
                   if group.recommended_keep]
        
        elif strategy == 'keep_first':
            return [group.files[0] for group in duplicate_groups]
        
        elif strategy == 'keep_largest':
            return [max(group.files, key=lambda f: f.get('size', 0)) 
                   for group in duplicate_groups]
        
        elif strategy == 'keep_newest':
            return [max(group.files, key=lambda f: f.get('modified', 0)) 
                   for group in duplicate_groups]
        
        else:
            raise ValueError(f"Unknown resolution strategy: {strategy}")
    
    def export_duplicate_report(self, duplicate_groups: List[DuplicateGroup], 
                               output_path: Path) -> bool:
        """Export duplicate groups to JSON report"""
        try:
            import json
            
            report_data = {
                'summary': {
                    'total_groups': len(duplicate_groups),
                    'total_duplicates': sum(len(g.files) for g in duplicate_groups),
                    'potential_space_savings': sum(g.total_size - max(f.get('size', 0) for f in g.files) 
                                                 for g in duplicate_groups)
                },
                'duplicate_groups': []
            }
            
            for i, group in enumerate(duplicate_groups):
                group_data = {
                    'group_id': i + 1,
                    'duplicate_type': group.duplicate_type,
                    'confidence': group.confidence,
                    'total_size': group.total_size,
                    'files': [
                        {
                            'path': f['path'],
                            'size': f.get('size', 0),
                            'modified': f.get('modified', 0),
                            'is_recommended': f == group.recommended_keep
                        }
                        for f in group.files
                    ]
                }
                report_data['duplicate_groups'].append(group_data)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, default=str)
            
            logger.info(f"Duplicate report exported to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export duplicate report: {e}")
            return False