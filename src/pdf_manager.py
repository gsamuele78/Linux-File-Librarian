import os
import shutil
from subprocess import run, PIPE, TimeoutExpired, CalledProcessError
import fitz
import warnings
import sys
from contextlib import contextmanager, suppress
import io
import signal
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum
import tempfile
import logging
import gc

class RepairStrategy(Enum):
    """PDF repair strategy enumeration"""
    QPDF_REPAIR = "qpdf_repair"
    QPDF_LINEARIZE = "qpdf_linearize"
    GHOSTSCRIPT_REPAIR = "ghostscript_repair"
    PYMUPDF_REBUILD = "pymupdf_rebuild"
    PDFTK_REPAIR = "pdftk_repair"

@dataclass
class PDFRepairResult:
    """PDF repair operation result"""
    success: bool
    strategy_used: Optional[RepairStrategy]
    output_path: Optional[str]
    error_message: Optional[str]
    file_size_before: int
    file_size_after: int
    repair_time_seconds: float

class EnterprisePDFManager:
    """Enterprise-grade PDF management with multiple repair strategies"""
    
    def __init__(self, log_error, timeout_seconds: int = 30):
        self.log_error = log_error
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(__name__)
        self._repair_strategies = {
            RepairStrategy.QPDF_REPAIR: self._qpdf_repair,
            RepairStrategy.QPDF_LINEARIZE: self._qpdf_linearize,
            RepairStrategy.GHOSTSCRIPT_REPAIR: self._ghostscript_repair,
            RepairStrategy.PYMUPDF_REBUILD: self._pymupdf_rebuild,
            RepairStrategy.PDFTK_REPAIR: self._pdftk_repair
        }
    
    @contextmanager
    def suppress_mupdf_errors(self):
        """Suppress non-critical MuPDF stderr output"""
        old_stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            yield
        finally:
            captured = sys.stderr.getvalue()
            sys.stderr = old_stderr
            # Only show critical errors, suppress common format issues
            if captured and not any(x in captured.lower() for x in [
                'cmsopenprofilefrommem failed',
                'non-page object in page tree', 
                'too many kids in page tree',
                'expected object number',
                'zlib error: (null)',
                'malloc (',
                'bytes) failed'
            ]):
                print(captured, file=sys.stderr, end='')
    
    @contextmanager
    def timeout_handler(self, timeout_seconds: Optional[int] = None):
        """Enterprise timeout handler with configurable timeout"""
        timeout = timeout_seconds or self.timeout_seconds
        """Handle timeouts for PDF operations"""
        def timeout_signal(signum, frame):
            raise TimeoutError(f"PDF operation timed out after {timeout} seconds")
        
        old_handler = signal.signal(signal.SIGALRM, timeout_signal)
        signal.alarm(timeout)
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
            self._cleanup_temp_objects()  # Force cleanup after timeout
            
    def _cleanup_temp_objects(self):
        """Clean up temporary objects"""
        # Removed unnecessary gc.collect() for better performance
        pass

    def validate_pdf_integrity(self, file_path: str) -> Tuple[bool, str]:
        """Enterprise PDF validation using multiple tools"""
        try:
            result = run(['/usr/bin/qpdf', '--check', file_path], 
                        capture_output=True, text=True, timeout=self.timeout_seconds)
            output = result.stdout + result.stderr
            if 'no syntax or stream errors' in output.lower():
                return True, output
            return False, output
        except (TimeoutExpired, CalledProcessError, OSError) as e:
            self.logger.warning(f"PDF validation failed for {file_path}: {e}")
            return False, str(e)

    def get_pdf_details(self, file_path):
        """
        Robustly validate a PDF, extract metadata, and check for text content.
        Advanced error handling and logging. Returns (is_valid, has_text, pdf_version, pdf_creator, pdf_producer).
        """
        is_valid, has_text = False, False
        pdf_version = None
        pdf_creator = None
        pdf_producer = None
        qpdf_checked = False
        qpdf_check_output = None
        warnings.filterwarnings("ignore", category=UserWarning)
        mupdf_error_seen = set()
        

        if os.path.basename(file_path).startswith("._"):
            msg = f"Skipping AppleDouble resource fork: {file_path}"
            print(f"  [Info] {msg}", file=sys.stderr)
            self.log_error("SKIPPED_APPLEDOUBLE", file_path, msg)
            return False, False, pdf_version, pdf_creator, pdf_producer
        try:
            with open(file_path, "rb") as f:
                header = f.read(1024)
                if not header.startswith(b'%PDF-'):
                    msg = f"File does not have a valid PDF header. Skipping."
                    print(f"  [Warning] {msg} {file_path}", file=sys.stderr)
                    self.log_error("SKIPPED_INVALID_PDF_HEADER", file_path, msg)
                    return False, False, pdf_version, pdf_creator, pdf_producer
                try:
                    pdf_version = header[5:8].decode(errors='replace')
                except Exception:
                    pdf_version = None
        except Exception as e:
            msg = f"Could not read file header: {e}"
            print(f"  [Warning] {msg} {file_path}", file=sys.stderr)
            self.log_error("HEADER_READ_ERROR", file_path, msg)
            return False, False, pdf_version, pdf_creator, pdf_producer
        try:
            with self.timeout_handler(30):
                with self.suppress_mupdf_errors():
                    with fitz.open(file_path) as doc:
                        if doc.page_count > 0:
                            is_valid = True
                            meta = doc.metadata or {}
                            pdf_creator = meta.get('creator')
                            pdf_producer = meta.get('producer')
                            # Limit text extraction for very large PDFs to prevent memory issues
                            max_pages_to_check = min(doc.page_count, 10)
                            from tqdm import tqdm
                            for i in tqdm(range(max_pages_to_check), desc=f"Extracting text: {os.path.basename(file_path)}", leave=False):
                                page = None
                                try:
                                    page = doc.load_page(i)
                                    text = ''
                                    get_text_fn = getattr(page, 'get_text', None)
                                    getText_fn = getattr(page, 'getText', None)
                                    try:
                                        if callable(get_text_fn):
                                            text = get_text_fn("text")
                                        elif callable(getText_fn):
                                            text = getText_fn("text")
                                    except Exception as e:
                                        self.log_error("TEXT_EXTRACTION_ERROR", file_path, str(e))
                                        continue
                                    if text and str(text).strip():
                                        has_text = True
                                        break
                                except (MemoryError, RuntimeError) as e:
                                    # Skip corrupted pages that cause memory issues
                                    continue
                                finally:
                                    # Critical: Release page object immediately
                                    if page is not None:
                                        try:
                                            del page
                                        except (AttributeError, NameError):
                                            # Expected when page object is already cleaned up
                                            pass
                                    gc.collect()
        except TimeoutError as e:
            msg = f"PDF processing timeout: {e}"
            self.log_error("PDF_TIMEOUT_ERROR", file_path, msg)
            # Try validation as fallback for timeout PDFs
            is_valid, qpdf_check_output = self.validate_pdf_integrity(file_path)
            qpdf_checked = True
        except (MemoryError, RuntimeError) as e:
            # Handle memory allocation failures from corrupted PDFs
            msg = f"Memory/Runtime error with PDF: {e}"
            self.log_error("PDF_MEMORY_ERROR", file_path, msg)
            # Try validation as fallback for corrupted PDFs
            is_valid, qpdf_check_output = self.validate_pdf_integrity(file_path)
            qpdf_checked = True
        except Exception as e:
            msg = f"Could not open PDF with PyMuPDF: {e}"
            if msg not in mupdf_error_seen:
                print(f"  [Warning] {msg} {file_path}", file=sys.stderr)
                self.log_error("MUPDF_OPEN_ERROR", file_path, msg)
                mupdf_error_seen.add(msg)
            # Fallback: try validation
            is_valid, qpdf_check_output = self.validate_pdf_integrity(file_path)
            qpdf_checked = True
        return is_valid, has_text, pdf_version, pdf_creator, pdf_producer

    def repair_pdf_enterprise(self, input_path: str, output_path: str) -> PDFRepairResult:
        """Enterprise PDF repair using multiple strategies with comprehensive reporting"""
        import time
        start_time = time.perf_counter()
        input_size = Path(input_path).stat().st_size if Path(input_path).exists() else 0
        
        # Clean output path
        if Path(output_path).exists():
            try:
                Path(output_path).unlink()
            except (OSError, IOError) as e:
                self.logger.error(f"Error removing existing output file {output_path}: {e}")
        
        # Try repair strategies in order of effectiveness
        strategies = [
            RepairStrategy.QPDF_REPAIR,
            RepairStrategy.GHOSTSCRIPT_REPAIR,
            RepairStrategy.QPDF_LINEARIZE,
            RepairStrategy.PDFTK_REPAIR,
            RepairStrategy.PYMUPDF_REBUILD
        ]
        
        for strategy in strategies:
            try:
                if self._repair_strategies[strategy](input_path, output_path):
                    output_size = Path(output_path).stat().st_size if Path(output_path).exists() else 0
                    repair_time = time.perf_counter() - start_time
                    
                    return PDFRepairResult(
                        success=True,
                        strategy_used=strategy,
                        output_path=output_path,
                        error_message=None,
                        file_size_before=input_size,
                        file_size_after=output_size,
                        repair_time_seconds=repair_time
                    )
            except Exception as e:
                self.logger.warning(f"Repair strategy {strategy.value} failed: {e}")
                continue
        
        # All strategies failed
        repair_time = time.perf_counter() - start_time
        return PDFRepairResult(
            success=False,
            strategy_used=None,
            output_path=None,
            error_message="All repair strategies failed",
            file_size_before=input_size,
            file_size_after=0,
            repair_time_seconds=repair_time
        )
    def _run_repair_command(self, cmd: list, input_path: str, timeout: int = 60) -> bool:
        """Execute repair command with enterprise error handling"""
        try:
            result = run(cmd, capture_output=True, text=True, timeout=timeout, check=True)
            self.logger.info(f"Repair command succeeded: {' '.join(cmd)}")
            return True
        except TimeoutExpired:
            self.logger.warning(f"Repair command timed out: {' '.join(cmd)}")
        except CalledProcessError as e:
            self.logger.warning(f"Repair command failed: {' '.join(cmd)}, error: {e}")
        except OSError as e:
            self.logger.error(f"Repair tool not found: {cmd[0]}, error: {e}")
        return False
    
    def _qpdf_repair(self, input_path: str, output_path: str) -> bool:
        """QPDF repair strategy - most reliable"""
        cmd = ['/usr/bin/qpdf', '--repair', input_path, output_path]
        return self._run_repair_command(cmd, input_path) and Path(output_path).exists()
    
    def _qpdf_linearize(self, input_path: str, output_path: str) -> bool:
        """QPDF linearize strategy - good for structure issues"""
        cmd = ['/usr/bin/qpdf', '--linearize', input_path, output_path]
        return self._run_repair_command(cmd, input_path) and Path(output_path).exists()
    
    def _ghostscript_repair(self, input_path: str, output_path: str) -> bool:
        """Ghostscript repair strategy - excellent for complex PDFs"""
        cmd = ['/usr/bin/gs', '-dNOPAUSE', '-dBATCH', '-sDEVICE=pdfwrite',
               '-dPDFSETTINGS=/prepress', '-dCompatibilityLevel=1.4',
               f'-sOutputFile={output_path}', input_path]
        return self._run_repair_command(cmd, input_path) and Path(output_path).exists()
    
    def _pdftk_repair(self, input_path: str, output_path: str) -> bool:
        """PDFtk repair strategy - good for form and metadata issues"""
        cmd = ['/usr/bin/pdftk', input_path, 'output', output_path, 'compress']
        return self._run_repair_command(cmd, input_path) and Path(output_path).exists()
    
    def _pymupdf_rebuild(self, input_path: str, output_path: str) -> bool:
        """PyMuPDF rebuild strategy - last resort with page extraction"""
        try:
            with self.suppress_mupdf_errors():
                with fitz.open(input_path) as doc:
                    if doc.page_count > 0:
                        doc.save(output_path, garbage=4, deflate=True, clean=True)
                        self.logger.info(f"PDF rebuilt with PyMuPDF: {input_path}")
                        return Path(output_path).exists()
        except (MemoryError, RuntimeError) as e:
            self.logger.error(f"PyMuPDF rebuild failed due to memory/runtime error: {e}")
        except Exception as e:
            self.logger.warning(f"PyMuPDF rebuild failed: {e}")
        return False
    
    # Legacy method for backward compatibility
    def repair_pdf(self, input_path: str, output_path: str) -> bool:
        """Legacy repair method - delegates to enterprise method"""
        result = self.repair_pdf_enterprise(input_path, output_path)
        return result.success

# Backward compatibility alias
PDFManager = EnterprisePDFManager

# Enterprise PDF analysis utilities
class PDFAnalyzer:
    """Enterprise PDF analysis and metrics collection"""
    
    @staticmethod
    def analyze_pdf_health(file_path: str) -> Dict[str, Any]:
        """Comprehensive PDF health analysis"""
        analysis = {
            'file_size_mb': Path(file_path).stat().st_size / (1024 * 1024),
            'is_encrypted': False,
            'page_count': 0,
            'has_forms': False,
            'has_annotations': False,
            'pdf_version': None,
            'creation_date': None,
            'modification_date': None,
            'producer': None,
            'creator': None,
            'health_score': 0.0,
            'issues': []
        }
        
        try:
            with fitz.open(file_path) as doc:
                analysis['is_encrypted'] = doc.needs_pass
                analysis['page_count'] = doc.page_count
                
                # Check metadata
                metadata = doc.metadata or {}
                analysis['pdf_version'] = metadata.get('format', 'Unknown')
                analysis['creation_date'] = metadata.get('creationDate')
                analysis['modification_date'] = metadata.get('modDate')
                analysis['producer'] = metadata.get('producer')
                analysis['creator'] = metadata.get('creator')
                
                # Analyze first few pages for issues
                issues = []
                for i in range(min(3, doc.page_count)):
                    try:
                        page = doc.load_page(i)
                        with suppress(AttributeError):
                            annotations = getattr(page, 'get_annotations', lambda: [])() or []
                            if annotations:
                                analysis['has_annotations'] = True
                        with suppress(AttributeError):
                            widgets = getattr(page, 'get_widgets', lambda: [])() or []
                            if widgets:
                                analysis['has_forms'] = True
                    except Exception as e:
                        issues.append(f"Page {i+1} analysis failed: {str(e)[:50]}")
                
                analysis['issues'] = issues
                
                # Calculate health score (0-100)
                score = 100.0
                if analysis['is_encrypted']:
                    score -= 20
                if len(issues) > 0:
                    score -= min(30, len(issues) * 10)
                if analysis['file_size_mb'] > 100:
                    score -= 10
                
                analysis['health_score'] = max(0.0, score)
                
        except Exception as e:
            analysis['issues'].append(f"Analysis failed: {str(e)[:100]}")
            analysis['health_score'] = 0.0
        
        return analysis