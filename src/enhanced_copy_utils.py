#!/usr/bin/env python3
"""
Enhanced copy utilities with hierarchical directory structure
"""

import os
import shutil
from pathlib import Path
from typing import Optional


def detect_hierarchical_structure(source_path: Path, game_system: str) -> tuple:
    """Detect hierarchical structure from source path for TTRPG files"""
    try:
        path_parts = source_path.parts
        path_str = str(source_path).lower()
        
        # D&D hierarchical detection
        if 'dungeons' in path_str and 'dragons' in path_str:
            for i, part in enumerate(path_parts):
                if 'dungeons' in part.lower() and 'dragons' in part.lower():
                    # Extract hierarchy after D&D folder
                    remaining_parts = path_parts[i+1:-1]  # Exclude filename
                    if len(remaining_parts) >= 2:
                        return remaining_parts[0], remaining_parts[1], remaining_parts[2] if len(remaining_parts) > 2 else None
                    elif len(remaining_parts) == 1:
                        return remaining_parts[0], 'General', None
        
        # Pathfinder hierarchical detection
        if 'pathfinder' in path_str:
            for i, part in enumerate(path_parts):
                if 'pathfinder' in part.lower():
                    # Extract hierarchy after Pathfinder folder
                    remaining_parts = path_parts[i+1:-1]  # Exclude filename
                    if len(remaining_parts) >= 2:
                        return remaining_parts[0], remaining_parts[1], remaining_parts[2] if len(remaining_parts) > 2 else None
                    elif len(remaining_parts) == 1:
                        return remaining_parts[0], 'General', None
        
        return None, None, None
        
    except Exception as e:
        print(f"Error detecting hierarchical structure for {source_path}: {e}")
        return None, None, None


def categorize_non_ttrpg_file(file_info: dict) -> tuple:
    """Intelligent categorization for non-TTRPG files using classification data"""
    category = file_info.get('category', 'Unknown')
    game_system = file_info.get('game_system', 'Unknown')
    edition = file_info.get('edition', 'Unknown')
    source_path = Path(file_info['path'])
    
    # Media files categorization
    if category in ['Media', 'Video', 'Audio', 'Images']:
        media_type = _detect_media_subcategory(source_path, category)
        return 'Media', media_type, _detect_content_type(source_path)
    
    # Document files categorization
    elif category in ['Documents', 'PDF', 'Office', 'Text']:
        doc_type = _detect_document_subcategory(source_path, category)
        return 'Documents', doc_type, _detect_document_content(source_path)
    
    # Archive files categorization
    elif category in ['Archives', 'Software & Data']:
        archive_type = _detect_archive_subcategory(source_path)
        return 'Archives', archive_type, 'General'
    
    # Unknown/Miscellaneous
    else:
        return 'Miscellaneous', 'General', 'Uncategorized'


def _detect_media_subcategory(source_path: Path, category: str) -> str:
    """Detect media subcategory from path and filename"""
    path_str = str(source_path).lower()
    filename = source_path.name.lower()
    
    if 'video' in path_str or filename.endswith(('.mp4', '.avi', '.mkv', '.mov')):
        return 'Video'
    elif 'audio' in path_str or filename.endswith(('.mp3', '.wav', '.flac', '.ogg')):
        return 'Audio'
    elif 'image' in path_str or filename.endswith(('.jpg', '.png', '.gif', '.bmp')):
        return 'Images'
    else:
        return category or 'General'


def _detect_document_subcategory(source_path: Path, category: str) -> str:
    """Detect document subcategory from path and filename"""
    path_str = str(source_path).lower()
    filename = source_path.name.lower()
    
    if filename.endswith('.pdf'):
        return 'PDF'
    elif filename.endswith(('.doc', '.docx', '.odt')):
        return 'Text Documents'
    elif filename.endswith(('.xls', '.xlsx', '.ods')):
        return 'Spreadsheets'
    elif filename.endswith(('.ppt', '.pptx', '.odp')):
        return 'Presentations'
    else:
        return 'General'


def _detect_archive_subcategory(source_path: Path) -> str:
    """Detect archive subcategory from filename"""
    filename = source_path.name.lower()
    
    if filename.endswith(('.zip', '.rar', '.7z')):
        return 'Compressed'
    elif filename.endswith(('.iso', '.img')):
        return 'Disk Images'
    else:
        return 'General'


def _detect_content_type(source_path: Path) -> str:
    """Detect content type from path structure"""
    path_parts = [part.lower() for part in source_path.parts]
    
    if any('tutorial' in part for part in path_parts):
        return 'Tutorials'
    elif any('music' in part for part in path_parts):
        return 'Music'
    elif any('sound' in part for part in path_parts):
        return 'Sound Effects'
    else:
        return 'General'


def _detect_document_content(source_path: Path) -> str:
    """Detect document content type from path structure"""
    path_parts = [part.lower() for part in source_path.parts]
    
    if any('manual' in part for part in path_parts):
        return 'Manuals'
    elif any('guide' in part for part in path_parts):
        return 'Guides'
    elif any('reference' in part for part in path_parts):
        return 'Reference'
    else:
        return 'General'


def create_enhanced_destination_path(destination_root: Path, file_info: dict) -> Path:
    """Create hierarchical path using detected structure for TTRPG or intelligent categorization for others"""
    
    source_path = Path(file_info['path'])
    game_system = file_info.get('game_system', 'Unknown')
    
    # For D&D and Pathfinder, use detected hierarchical structure
    if game_system in ['Dungeons & Dragons', 'D&D', 'Pathfinder']:
        level1, level2, level3 = detect_hierarchical_structure(source_path, game_system)
        
        if level1:
            dest_dir = destination_root / game_system / level1
            if level2:
                dest_dir = dest_dir / level2
                if level3:
                    dest_dir = dest_dir / level3
        else:
            # Fallback to classification data
            edition = file_info.get('edition', 'General')
            category = file_info.get('category', 'General')
            dest_dir = destination_root / game_system / edition / category
    
    # For other files, use intelligent categorization
    else:
        big_group, middle_cat, sub_cat = categorize_non_ttrpg_file(file_info)
        dest_dir = destination_root / big_group / middle_cat
        if sub_cat:
            dest_dir = dest_dir / sub_cat
    
    return dest_dir



def copy_file_enhanced(source_path: Path, dest_dir: Path) -> Optional[Path]:
    """Copy file to destination with conflict resolution"""
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / source_path.name
        
        # Handle name conflicts
        counter = 1
        while dest_path.exists():
            stem = source_path.stem
            suffix = source_path.suffix
            dest_path = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1
        
        # Copy file
        shutil.copy2(source_path, dest_path)
        return dest_path
        
    except Exception as e:
        print(f"Copy error for {source_path}: {e}")
        return None