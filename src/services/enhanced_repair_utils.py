#!/usr/bin/env python3
"""
Enhanced Document Repair Utilities

Provides comprehensive document repair capabilities for various file types
with intelligent error detection and recovery strategies.
"""

import logging
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


@dataclass
class RepairResult:
    """Document repair result"""
    success: bool
    original_path: Path
    repaired_path: Optional[Path]
    issues_found: List[str]
    repairs_applied: List[str]
    confidence: float


class DocumentRepairer(ABC):
    """Abstract base class for document repairers"""
    
    @abstractmethod
    def can_repair(self, file_path: Path, mime_type: str) -> bool:
        """Check if this repairer can handle the file"""
        pass
    
    @abstractmethod
    def diagnose(self, file_path: Path) -> List[str]:
        """Diagnose issues with the file"""
        pass
    
    @abstractmethod
    def repair(self, file_path: Path, output_dir: Path) -> RepairResult:
        """Repair the file"""
        pass


class PDFRepairer(DocumentRepairer):
    """PDF document repairer"""
    
    def can_repair(self, file_path: Path, mime_type: str) -> bool:
        return 'pdf' in mime_type.lower() or file_path.suffix.lower() == '.pdf'
    
    def diagnose(self, file_path: Path) -> List[str]:
        """Diagnose PDF issues"""
        issues = []
        
        try:
            # Check file size
            if file_path.stat().st_size == 0:
                issues.append("Empty file")
                return issues
            
            # Try PyPDF2 diagnosis
            try:
                import PyPDF2
                with open(file_path, 'rb') as f:
                    reader = PyPDF2.PdfReader(f)
                    
                    if reader.is_encrypted:
                        issues.append("Password protected")
                    
                    if len(reader.pages) == 0:
                        issues.append("No pages found")
                    
                    # Check for corrupted pages
                    corrupted_pages = 0
                    for i, page in enumerate(reader.pages[:5]):  # Check first 5 pages
                        try:
                            page.extract_text()
                        except Exception:
                            corrupted_pages += 1
                    
                    if corrupted_pages > 0:
                        issues.append(f"Corrupted pages detected ({corrupted_pages})")
                        
            except ImportError:
                issues.append("PyPDF2 not available for diagnosis")
            except Exception as e:
                issues.append(f"PDF structure issues: {str(e)[:100]}")
            
            # Check with qpdf if available
            try:
                result = subprocess.run(['qpdf', '--check', str(file_path)], 
                                      capture_output=True, text=True, timeout=30)
                if result.returncode != 0:
                    issues.append("PDF structure validation failed")
            except (FileNotFoundError, subprocess.TimeoutExpired):
                pass
                
        except Exception as e:
            issues.append(f"Diagnosis failed: {str(e)[:100]}")
        
        return issues
    
    def repair(self, file_path: Path, output_dir: Path) -> RepairResult:
        """Repair PDF file"""
        issues_found = self.diagnose(file_path)
        repairs_applied = []
        repaired_path = None
        
        if not issues_found:
            return RepairResult(True, file_path, file_path, [], [], 1.0)
        
        try:
            output_path = output_dir / f"repaired_{file_path.name}"
            
            # Try qpdf repair first
            if self._try_qpdf_repair(file_path, output_path):
                repairs_applied.append("qpdf structure repair")
                repaired_path = output_path
            
            # Try PyPDF2 repair
            elif self._try_pypdf2_repair(file_path, output_path):
                repairs_applied.append("PyPDF2 repair")
                repaired_path = output_path
            
            # Try ghostscript repair
            elif self._try_ghostscript_repair(file_path, output_path):
                repairs_applied.append("Ghostscript repair")
                repaired_path = output_path
            
            success = repaired_path is not None
            confidence = 0.8 if success else 0.0
            
            return RepairResult(success, file_path, repaired_path, issues_found, repairs_applied, confidence)
            
        except Exception as e:
            logger.error(f"PDF repair failed for {file_path}: {e}")
            return RepairResult(False, file_path, None, issues_found, [], 0.0)
    
    def _try_qpdf_repair(self, input_path: Path, output_path: Path) -> bool:
        """Try repairing with qpdf"""
        try:
            result = subprocess.run([
                'qpdf', '--qdf', '--replace-input', str(input_path), str(output_path)
            ], capture_output=True, timeout=60)
            return result.returncode == 0 and output_path.exists()
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False
    
    def _try_pypdf2_repair(self, input_path: Path, output_path: Path) -> bool:
        """Try repairing with PyPDF2"""
        try:
            import PyPDF2
            with open(input_path, 'rb') as input_file:
                reader = PyPDF2.PdfReader(input_file, strict=False)
                writer = PyPDF2.PdfWriter()
                
                for page in reader.pages:
                    try:
                        writer.add_page(page)
                    except Exception:
                        continue  # Skip corrupted pages
                
                with open(output_path, 'wb') as output_file:
                    writer.write(output_file)
                
                return output_path.exists() and output_path.stat().st_size > 0
        except ImportError:
            return False
        except Exception:
            return False
    
    def _try_ghostscript_repair(self, input_path: Path, output_path: Path) -> bool:
        """Try repairing with Ghostscript"""
        try:
            result = subprocess.run([
                'gs', '-dNOPAUSE', '-dBATCH', '-sDEVICE=pdfwrite',
                f'-sOutputFile={output_path}', str(input_path)
            ], capture_output=True, timeout=120)
            return result.returncode == 0 and output_path.exists()
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False


class VideoRepairer(DocumentRepairer):
    """Video file repairer using ffmpeg"""
    
    def can_repair(self, file_path: Path, mime_type: str) -> bool:
        video_extensions = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm'}
        return mime_type.startswith('video/') or file_path.suffix.lower() in video_extensions
    
    def diagnose(self, file_path: Path) -> List[str]:
        """Diagnose video issues"""
        issues = []
        
        try:
            # Use ffprobe to check video integrity
            cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', 
                   '-of', 'csv=p=0', str(file_path)]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                issues.append("Video format corruption detected")
            
            # Check for missing duration
            if result.stdout.strip() == 'N/A':
                issues.append("Missing or corrupted duration metadata")
            
            # Check for audio/video sync issues
            cmd_streams = ['ffprobe', '-v', 'error', '-show_streams', 
                          '-select_streams', 'v:0', str(file_path)]
            
            result_streams = subprocess.run(cmd_streams, capture_output=True, text=True, timeout=30)
            if 'codec_name=unknown' in result_streams.stdout:
                issues.append("Unknown video codec")
                
        except (FileNotFoundError, subprocess.TimeoutExpired):
            issues.append("ffprobe not available for diagnosis")
        except Exception as e:
            issues.append(f"Video diagnosis failed: {str(e)[:100]}")
        
        return issues
    
    def repair(self, file_path: Path, output_dir: Path) -> RepairResult:
        """Repair video file"""
        issues_found = self.diagnose(file_path)
        repairs_applied = []
        repaired_path = None
        
        if not issues_found:
            return RepairResult(True, file_path, file_path, [], [], 1.0)
        
        try:
            output_path = output_dir / f"repaired_{file_path.name}"
            
            # Try ffmpeg repair
            if self._try_ffmpeg_repair(file_path, output_path):
                repairs_applied.append("ffmpeg container repair")
                repaired_path = output_path
            
            success = repaired_path is not None
            confidence = 0.7 if success else 0.0
            
            return RepairResult(success, file_path, repaired_path, issues_found, repairs_applied, confidence)
            
        except Exception as e:
            logger.error(f"Video repair failed for {file_path}: {e}")
            return RepairResult(False, file_path, None, issues_found, [], 0.0)
    
    def _try_ffmpeg_repair(self, input_path: Path, output_path: Path) -> bool:
        """Try repairing with ffmpeg"""
        try:
            # Basic container repair
            result = subprocess.run([
                'ffmpeg', '-i', str(input_path), '-c', 'copy', 
                '-avoid_negative_ts', 'make_zero', str(output_path)
            ], capture_output=True, timeout=300)
            
            return result.returncode == 0 and output_path.exists()
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False


class ArchiveRepairer(DocumentRepairer):
    """Archive file repairer"""
    
    def can_repair(self, file_path: Path, mime_type: str) -> bool:
        archive_extensions = {'.zip', '.rar', '.7z', '.tar', '.gz'}
        return any(ext in mime_type for ext in ['zip', 'rar', '7z', 'tar', 'gzip']) or \
               file_path.suffix.lower() in archive_extensions
    
    def diagnose(self, file_path: Path) -> List[str]:
        """Diagnose archive issues"""
        issues = []
        
        try:
            extension = file_path.suffix.lower()
            
            if extension == '.zip':
                issues.extend(self._diagnose_zip(file_path))
            elif extension == '.rar':
                issues.extend(self._diagnose_rar(file_path))
            elif extension in ['.7z']:
                issues.extend(self._diagnose_7z(file_path))
                
        except Exception as e:
            issues.append(f"Archive diagnosis failed: {str(e)[:100]}")
        
        return issues
    
    def _diagnose_zip(self, file_path: Path) -> List[str]:
        """Diagnose ZIP file"""
        issues = []
        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                bad_files = zf.testzip()
                if bad_files:
                    issues.append(f"Corrupted files in archive: {bad_files}")
        except zipfile.BadZipFile:
            issues.append("Corrupted ZIP structure")
        except ImportError:
            issues.append("zipfile module not available")
        return issues
    
    def _diagnose_rar(self, file_path: Path) -> List[str]:
        """Diagnose RAR file"""
        issues = []
        try:
            result = subprocess.run(['unrar', 't', str(file_path)], 
                                  capture_output=True, text=True, timeout=60)
            if result.returncode != 0:
                issues.append("RAR integrity test failed")
        except (FileNotFoundError, subprocess.TimeoutExpired):
            issues.append("unrar not available for testing")
        return issues
    
    def _diagnose_7z(self, file_path: Path) -> List[str]:
        """Diagnose 7z file"""
        issues = []
        try:
            result = subprocess.run(['7z', 't', str(file_path)], 
                                  capture_output=True, text=True, timeout=60)
            if result.returncode != 0:
                issues.append("7z integrity test failed")
        except (FileNotFoundError, subprocess.TimeoutExpired):
            issues.append("7z not available for testing")
        return issues
    
    def repair(self, file_path: Path, output_dir: Path) -> RepairResult:
        """Repair archive file"""
        issues_found = self.diagnose(file_path)
        repairs_applied = []
        repaired_path = None
        
        if not issues_found:
            return RepairResult(True, file_path, file_path, [], [], 1.0)
        
        try:
            # For archives, repair usually means extracting and re-compressing
            if self._try_extract_recompress(file_path, output_dir):
                repairs_applied.append("extract and recompress")
                repaired_path = output_dir / f"repaired_{file_path.name}"
            
            success = repaired_path is not None
            confidence = 0.6 if success else 0.0
            
            return RepairResult(success, file_path, repaired_path, issues_found, repairs_applied, confidence)
            
        except Exception as e:
            logger.error(f"Archive repair failed for {file_path}: {e}")
            return RepairResult(False, file_path, None, issues_found, [], 0.0)
    
    def _try_extract_recompress(self, file_path: Path, output_dir: Path) -> bool:
        """Try extracting and recompressing archive"""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Extract what we can
                if file_path.suffix.lower() == '.zip':
                    with zipfile.ZipFile(file_path, 'r') as zf:
                        zf.extractall(temp_path)
                    
                    # Recompress
                    output_path = output_dir / f"repaired_{file_path.name}"
                    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                        for file in temp_path.rglob('*'):
                            if file.is_file():
                                zf.write(file, file.relative_to(temp_path))
                    
                    return output_path.exists()
                    
        except Exception:
            return False
        
        return False


class EnhancedRepairManager:
    """Enhanced repair manager coordinating all repairers"""
    
    def __init__(self):
        self.repairers = [
            PDFRepairer(),
            VideoRepairer(),
            ArchiveRepairer()
        ]
        self.repair_stats = {'attempted': 0, 'successful': 0, 'failed': 0}
    
    def repair_file(self, file_path: Path, output_dir: Path) -> RepairResult:
        """Repair file using appropriate repairer"""
        
        # Get MIME type
        import mimetypes
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if not mime_type:
            mime_type = 'application/octet-stream'
        
        # Find appropriate repairer
        for repairer in self.repairers:
            if repairer.can_repair(file_path, mime_type):
                try:
                    self.repair_stats['attempted'] += 1
                    result = repairer.repair(file_path, output_dir)
                    
                    if result.success:
                        self.repair_stats['successful'] += 1
                        logger.info(f"Repaired {file_path}: {', '.join(result.repairs_applied)}")
                    else:
                        self.repair_stats['failed'] += 1
                        logger.warning(f"Repair failed for {file_path}: {', '.join(result.issues_found)}")
                    
                    return result
                    
                except Exception as e:
                    logger.error(f"Repairer {repairer.__class__.__name__} failed for {file_path}: {e}")
                    continue
        
        # No suitable repairer found
        return RepairResult(False, file_path, None, ["No suitable repairer available"], [], 0.0)
    
    def get_repair_stats(self) -> Dict:
        """Get repair statistics"""
        total = self.repair_stats['attempted']
        success_rate = (self.repair_stats['successful'] / total * 100) if total > 0 else 0
        
        return {
            **self.repair_stats,
            'success_rate': success_rate,
            'available_repairers': [r.__class__.__name__ for r in self.repairers]
        }
    
    def check_repair_dependencies(self) -> Dict[str, bool]:
        """Check which repair tools are available"""
        dependencies = {}
        
        # Check Python packages
        packages = ['PyPDF2', 'zipfile']
        for package in packages:
            try:
                __import__(package)
                dependencies[package] = True
            except ImportError:
                dependencies[package] = False
        
        # Check system tools
        tools = ['qpdf', 'gs', 'ffmpeg', 'ffprobe', 'unrar', '7z']
        for tool in tools:
            try:
                subprocess.run([tool, '--version'], capture_output=True, timeout=5)
                dependencies[tool] = True
            except (FileNotFoundError, subprocess.TimeoutExpired):
                dependencies[tool] = False
        
        return dependencies