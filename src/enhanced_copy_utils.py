#!/usr/bin/env python3
"""
Enhanced Copy Utilities

Provides enhanced file copying with intelligent directory structure creation
based on enhanced media classification results.
"""

import logging
import shutil
import os
from pathlib import Path
from typing import Dict, Optional, List
import unicodedata
import re

logger = logging.getLogger(__name__)


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for cross-platform compatibility"""
    
    # Normalize unicode characters
    filename = unicodedata.normalize('NFKD', filename)
    
    # Remove or replace problematic characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    filename = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', filename)  # Remove control characters
    
    # Replace multiple spaces/underscores with single ones
    filename = re.sub(r'[_\s]+', '_', filename)
    
    # Remove leading/trailing dots and spaces
    filename = filename.strip('. ')
    
    # Ensure filename isn't empty
    if not filename:
        filename = 'unnamed_file'
    
    # Limit length (keeping extension)
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200-len(ext)] + ext
    
    return filename


def create_enhanced_destination_path(library_root: Path, file_info: Dict) -> Path:
    """Create enhanced destination path based on classification results"""
    
    # Get classification information
    game_system = file_info.get('game_system', 'Unknown')
    edition = file_info.get('edition', 'Unknown')
    category = file_info.get('category', 'Unknown')
    
    # Check for enhanced metadata path
    suggested_path = file_info.get('suggested_path')
    if suggested_path and len(suggested_path) >= 3:
        level1, level2, level3 = suggested_path[0], suggested_path[1], suggested_path[2]
    else:
        level1, level2, level3 = game_system, edition, category
    
    # Sanitize path components
    level1 = sanitize_filename(level1)
    level2 = sanitize_filename(level2)
    level3 = sanitize_filename(level3)
    
    # Create enhanced directory structure
    dest_dir = library_root / level1 / level2 / level3
    
    # Add metadata-based subdirectories for better organization
    enhanced_metadata = file_info.get('enhanced_metadata', {})
    
    # For media files, add year/genre subdirectories if available
    if level1 == "Media" and enhanced_metadata:
        year = enhanced_metadata.get('year')
        genre = enhanced_metadata.get('genre')
        
        if year and year != 'Unknown':
            year_clean = sanitize_filename(str(year)[:4])  # Just the year part
            dest_dir = dest_dir / year_clean
        
        if genre and genre != 'Unknown' and level2 in ['Audio', 'Video']:
            genre_clean = sanitize_filename(genre)
            dest_dir = dest_dir / genre_clean
    
    # For documents, add author/publisher subdirectories if available
    elif level1 == "Documents" and enhanced_metadata:
        author = enhanced_metadata.get('author') or enhanced_metadata.get('creator')
        publisher = enhanced_metadata.get('publisher')
        
        if author and author != 'Unknown':
            author_clean = sanitize_filename(author)
            dest_dir = dest_dir / author_clean
        elif publisher and publisher != 'Unknown':
            publisher_clean = sanitize_filename(publisher)
            dest_dir = dest_dir / publisher_clean
    
    # Ensure directory exists
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    logger.debug(f"Created destination directory: {dest_dir}")
    return dest_dir


def generate_unique_filename(dest_dir: Path, original_filename: str) -> str:
    """Generate unique filename to avoid conflicts"""
    
    sanitized_name = sanitize_filename(original_filename)
    dest_path = dest_dir / sanitized_name
    
    if not dest_path.exists():
        return sanitized_name
    
    # File exists, generate unique name
    name, ext = os.path.splitext(sanitized_name)
    counter = 1
    
    while True:
        new_name = f"{name}_{counter:03d}{ext}"
        new_path = dest_dir / new_name
        
        if not new_path.exists():
            return new_name
        
        counter += 1
        
        # Safety limit
        if counter > 999:
            import time
            timestamp = int(time.time())
            return f"{name}_{timestamp}{ext}"


def copy_file_enhanced(source_path: Path, dest_dir: Path) -> Optional[Path]:
    """Enhanced file copying with metadata preservation and error handling"""
    
    try:
        # Generate unique destination filename
        dest_filename = generate_unique_filename(dest_dir, source_path.name)
        dest_path = dest_dir / dest_filename
        
        # Check available space
        available_space = shutil.disk_usage(dest_dir).free
        file_size = source_path.stat().st_size
        
        if available_space < file_size * 1.5:  # Need 1.5x space for safety
            logger.error(f"Insufficient disk space for {source_path}")
            return None
        
        # Perform the copy
        logger.debug(f"Copying {source_path} -> {dest_path}")
        
        # Use copy2 to preserve metadata
        shutil.copy2(source_path, dest_path)
        
        # Verify copy was successful
        if not dest_path.exists():
            logger.error(f"Copy verification failed: {dest_path} does not exist")
            return None
        
        # Verify file size matches
        if dest_path.stat().st_size != source_path.stat().st_size:
            logger.error(f"Copy verification failed: size mismatch for {dest_path}")
            dest_path.unlink()  # Remove corrupted copy
            return None
        
        logger.debug(f"Successfully copied to {dest_path}")
        return dest_path
        
    except PermissionError as e:
        logger.error(f"Permission denied copying {source_path}: {e}")
        return None
    except OSError as e:
        logger.error(f"OS error copying {source_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error copying {source_path}: {e}")
        return None


def create_media_nfo_file(dest_path: Path, file_info: Dict) -> bool:
    """Create NFO file with metadata (similar to MediaElch)"""
    
    enhanced_metadata = file_info.get('enhanced_metadata', {})
    if not enhanced_metadata or not any(enhanced_metadata.values()):
        return False
    
    try:
        nfo_path = dest_path.with_suffix('.nfo')
        
        # Create NFO content based on media type
        media_type = file_info.get('media_type', 'Unknown')
        
        if media_type == 'Video':
            nfo_content = create_video_nfo(enhanced_metadata, file_info)
        elif media_type == 'Audio':
            nfo_content = create_audio_nfo(enhanced_metadata, file_info)
        elif media_type == 'PDF':
            nfo_content = create_document_nfo(enhanced_metadata, file_info)
        else:
            nfo_content = create_generic_nfo(enhanced_metadata, file_info)
        
        if nfo_content:
            with open(nfo_path, 'w', encoding='utf-8') as f:
                f.write(nfo_content)
            
            logger.debug(f"Created NFO file: {nfo_path}")
            return True
            
    except Exception as e:
        logger.error(f"Failed to create NFO file for {dest_path}: {e}")
    
    return False


def create_video_nfo(metadata: Dict, file_info: Dict) -> str:
    """Create video NFO content"""
    
    title = metadata.get('title') or Path(file_info['path']).stem
    
    nfo_content = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<movie>
    <title>{escape_xml(title)}</title>
    <originaltitle>{escape_xml(title)}</originaltitle>
    <year>{metadata.get('year', 'Unknown')}</year>
    <genre>{metadata.get('genre', 'Unknown')}</genre>
    <director>{metadata.get('creator', 'Unknown')}</director>
    <plot>Automatically classified video file</plot>
    <runtime>{metadata.get('duration', 'Unknown')}</runtime>
    <fileinfo>
        <streamdetails>
            <video>
                <codec>{metadata.get('format_info', 'Unknown')}</codec>
                <width>{metadata.get('resolution', '').split('x')[0] if 'x' in str(metadata.get('resolution', '')) else 'Unknown'}</width>
                <height>{metadata.get('resolution', '').split('x')[1] if 'x' in str(metadata.get('resolution', '')) else 'Unknown'}</height>
            </video>
        </streamdetails>
    </fileinfo>
    <classification_info>
        <source>{file_info.get('classification_source', 'Unknown')}</source>
        <confidence>{file_info.get('classification_confidence', 0.0)}</confidence>
        <category>{file_info.get('category', 'Unknown')}</category>
    </classification_info>
</movie>"""
    
    return nfo_content


def create_audio_nfo(metadata: Dict, file_info: Dict) -> str:
    """Create audio NFO content"""
    
    title = metadata.get('title') or Path(file_info['path']).stem
    
    nfo_content = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<album>
    <title>{escape_xml(title)}</title>
    <artist>{escape_xml(metadata.get('author', 'Unknown'))}</artist>
    <year>{metadata.get('year', 'Unknown')}</year>
    <genre>{metadata.get('genre', 'Unknown')}</genre>
    <duration>{metadata.get('duration', 'Unknown')}</duration>
    <format>{metadata.get('format_info', 'Unknown')}</format>
    <classification_info>
        <source>{file_info.get('classification_source', 'Unknown')}</source>
        <confidence>{file_info.get('classification_confidence', 0.0)}</confidence>
        <category>{file_info.get('category', 'Unknown')}</category>
    </classification_info>
</album>"""
    
    return nfo_content


def create_document_nfo(metadata: Dict, file_info: Dict) -> str:
    """Create document NFO content"""
    
    title = metadata.get('title') or Path(file_info['path']).stem
    
    nfo_content = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<document>
    <title>{escape_xml(title)}</title>
    <author>{escape_xml(metadata.get('author', 'Unknown'))}</author>
    <creator>{escape_xml(metadata.get('creator', 'Unknown'))}</creator>
    <publisher>{escape_xml(metadata.get('publisher', 'Unknown'))}</publisher>
    <year>{metadata.get('year', 'Unknown')}</year>
    <language>{metadata.get('language', 'Unknown')}</language>
    <pages>{metadata.get('pages', 'Unknown')}</pages>
    <isbn>{metadata.get('isbn', 'Unknown')}</isbn>
    <format>{metadata.get('format_info', 'Unknown')}</format>
    <classification_info>
        <source>{file_info.get('classification_source', 'Unknown')}</source>
        <confidence>{file_info.get('classification_confidence', 0.0)}</confidence>
        <category>{file_info.get('category', 'Unknown')}</category>
    </classification_info>
</document>"""
    
    return nfo_content


def create_generic_nfo(metadata: Dict, file_info: Dict) -> str:
    """Create generic NFO content"""
    
    title = metadata.get('title') or Path(file_info['path']).stem
    
    nfo_content = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<file>
    <title>{escape_xml(title)}</title>
    <creator>{escape_xml(metadata.get('creator', 'Unknown'))}</creator>
    <format>{metadata.get('format_info', 'Unknown')}</format>
    <size>{metadata.get('file_size', 'Unknown')}</size>
    <created>{metadata.get('creation_date', 'Unknown')}</created>
    <modified>{metadata.get('modification_date', 'Unknown')}</modified>
    <classification_info>
        <source>{file_info.get('classification_source', 'Unknown')}</source>
        <confidence>{file_info.get('classification_confidence', 0.0)}</confidence>
        <category>{file_info.get('category', 'Unknown')}</category>
        <game_system>{file_info.get('game_system', 'Unknown')}</game_system>
        <edition>{file_info.get('edition', 'Unknown')}</edition>
    </classification_info>
</file>"""
    
    return nfo_content


def escape_xml(text: str) -> str:
    """Escape XML special characters"""
    if not text or text == 'Unknown':
        return text
    
    text = str(text)
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    text = text.replace('"', '&quot;')
    text = text.replace("'", '&apos;')
    
    return text


def create_directory_index(dest_dir: Path) -> bool:
    """Create directory index file with metadata summary"""
    
    try:
        index_path = dest_dir / '_directory_index.json'
        
        # Collect information about files in directory
        files_info = []
        nfo_files = list(dest_dir.glob('*.nfo'))
        
        for nfo_file in nfo_files:
            try:
                # Parse basic info from NFO
                with open(nfo_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Extract title (simple regex)
                import re
                title_match = re.search(r'<title>(.*?)</title>', content)
                title = title_match.group(1) if title_match else nfo_file.stem
                
                files_info.append({
                    'filename': nfo_file.stem,
                    'title': title,
                    'nfo_file': nfo_file.name
                })
                
            except Exception as e:
                logger.debug(f"Error processing NFO {nfo_file}: {e}")
        
        if files_info:
            import json
            index_data = {
                'directory': str(dest_dir.relative_to(dest_dir.parents[2])),  # Relative to library root
                'file_count': len(files_info),
                'files': files_info,
                'created': str(Path().cwd())  # Timestamp would be better
            }
            
            with open(index_path, 'w', encoding='utf-8') as f:
                json.dump(index_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Created directory index: {index_path}")
            return True
            
    except Exception as e:
        logger.error(f"Failed to create directory index for {dest_dir}: {e}")
    
    return False


def copy_file_with_enhancements(source_path: Path, dest_dir: Path, file_info: Dict) -> Optional[Path]:
    """Copy file with all enhancements: metadata preservation, NFO creation, indexing"""
    
    # Perform the basic copy
    dest_path = copy_file_enhanced(source_path, dest_dir)
    
    if not dest_path:
        return None
    
    # Create NFO file if we have metadata
    try:
        create_media_nfo_file(dest_path, file_info)
    except Exception as e:
        logger.debug(f"NFO creation failed for {dest_path}: {e}")
    
    # Update directory index
    try:
        create_directory_index(dest_dir)
    except Exception as e:
        logger.debug(f"Directory index update failed for {dest_dir}: {e}")
    
    return dest_path