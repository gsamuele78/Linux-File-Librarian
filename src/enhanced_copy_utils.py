#!/usr/bin/env python3
"""
Enterprise File Organization System
Implements professional hierarchical structure using classification data
"""

import os
import shutil
from pathlib import Path
from typing import Optional, Tuple, Dict
from dataclasses import dataclass


@dataclass
class OrganizationHierarchy:
    """Professional file organization structure"""
    primary: str
    secondary: str
    tertiary: Optional[str] = None
    
    def to_path(self, root: Path) -> Path:
        """Convert hierarchy to filesystem path"""
        path = root / self.primary / self.secondary
        return path / self.tertiary if self.tertiary else path


class EnterpriseFileOrganizer:
    """Enterprise-grade file organization using classification data"""
    
    @classmethod
    def organize_file(cls, file_info: Dict) -> OrganizationHierarchy:
        """Create professional organization hierarchy"""
        game_system = file_info.get('game_system', 'Unknown')
        category = file_info.get('category', 'Unknown')
        source_path = Path(file_info['path'])
        
        # TTRPG files
        if game_system not in ['Unknown', 'Miscellaneous']:
            return cls._organize_ttrpg_file(file_info, source_path)
        
        # Content-based organization
        return cls._organize_content_file(file_info, source_path)
    
    @classmethod
    def _organize_ttrpg_file(cls, file_info: Dict, source_path: Path) -> OrganizationHierarchy:
        """Organize TTRPG files"""
        game_system = file_info.get('game_system', 'Unknown')
        edition = file_info.get('edition', 'General')
        content_type = cls._detect_ttrpg_content(source_path)
        
        return OrganizationHierarchy(
            primary=f"TTRPG/{game_system}",
            secondary=edition if edition != 'Unknown' else 'General',
            tertiary=content_type
        )
    
    @classmethod
    def _organize_content_file(cls, file_info: Dict, source_path: Path) -> OrganizationHierarchy:
        """Organize by content type"""
        category = file_info.get('category', 'Unknown')
        ext = source_path.suffix.lower()
        
        if ext in ['.mp4', '.avi', '.mkv', '.mov']:
            return OrganizationHierarchy('Media', 'Video', 'General')
        elif ext in ['.mp3', '.wav', '.flac', '.ogg']:
            return OrganizationHierarchy('Media', 'Audio', 'General')
        elif ext in ['.jpg', '.png', '.gif', '.bmp']:
            return OrganizationHierarchy('Media', 'Images', 'General')
        elif ext == '.pdf':
            return OrganizationHierarchy('Documents', 'PDF', 'General')
        elif ext in ['.zip', '.rar', '.7z']:
            return OrganizationHierarchy('Archives', 'Compressed', None)
        else:
            return OrganizationHierarchy('General', 'Uncategorized', None)
    
    @staticmethod
    def _detect_ttrpg_content(source_path: Path) -> str:
        """Detect TTRPG content type"""
        filename = source_path.name.lower()
        
        if any(term in filename for term in ['core', 'player', 'handbook']):
            return 'Core Rules'
        elif any(term in filename for term in ['adventure', 'module']):
            return 'Adventures'
        elif any(term in filename for term in ['supplement', 'guide']):
            return 'Supplements'
        else:
            return 'General'


def detect_hierarchical_structure(source_path: Path, game_system: str) -> tuple:
    """Legacy function - use EnterpriseFileOrganizer instead"""
    return None, None, None


# Legacy functions - replaced by EnterpriseFileOrganizer
def categorize_non_ttrpg_file(file_info: dict) -> tuple:
    """Legacy function"""
    return 'General', 'Uncategorized', None


def create_enhanced_destination_path(destination_root: Path, file_info: dict) -> Path:
    """Create professional hierarchical path using enterprise organization"""
    hierarchy = EnterpriseFileOrganizer.organize_file(file_info)
    return hierarchy.to_path(destination_root)



def copy_file_enhanced(source_path: Path, dest_dir: Path) -> Optional[Path]:
    """Copy file to destination with conflict resolution"""
    try:
        # Create destination directory
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / source_path.name
        
        # Handle name conflicts
        counter = 1
        while dest_path.exists():
            stem = source_path.stem
            suffix = source_path.suffix
            dest_path = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1
            
            # Prevent infinite loops
            if counter > 1000:
                print(f"Too many conflicts for {source_path.name}, skipping")
                return None
        
        # Verify source exists and is readable
        if not source_path.exists():
            print(f"Source file does not exist: {source_path}")
            return None
            
        if not os.access(source_path, os.R_OK):
            print(f"Source file not readable: {source_path}")
            return None
        
        # Check destination directory is writable
        if not os.access(dest_dir, os.W_OK):
            print(f"Destination directory not writable: {dest_dir}")
            return None
        
        # Copy file with metadata preservation
        shutil.copy2(source_path, dest_path)
        
        # Verify copy was successful
        if not dest_path.exists():
            print(f"Copy verification failed: {dest_path} was not created")
            return None
            
        # Verify file sizes match
        if source_path.stat().st_size != dest_path.stat().st_size:
            print(f"Copy verification failed: size mismatch for {dest_path}")
            dest_path.unlink()  # Remove incomplete copy
            return None
        
        return dest_path
        
    except PermissionError as e:
        print(f"Permission error copying {source_path}: {e}")
        return None
    except OSError as e:
        print(f"OS error copying {source_path}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error copying {source_path}: {type(e).__name__}: {e}")
        return None