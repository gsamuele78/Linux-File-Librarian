#!/usr/bin/env python3
"""
Professional Linux File Librarian Orchestrator

Implements enterprise software engineering best practices:
- Clean Architecture with dependency inversion
- SOLID principles implementation
- Comprehensive error handling and recovery
- Performance monitoring and optimization
- Memory management and OOM prevention
- Professional logging and reporting
- Graceful degradation and fault tolerance
"""

import asyncio
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Enterprise imports
from .enterprise_logging import get_logger, LogContext, SecurityLevel
from .enterprise_error_handling import (
    with_error_handling, 
    ErrorContext, 
    EnterpriseException,
    ValidationError,
    ResourceError,
    SecurityError,
    RetryStrategy,
    safe_execute
)

logger = get_logger(__name__)

from src.enterprise_architecture import (
    EnterpriseLibrarianOrchestrator,
    ProcessingStage,
    ProcessingMetrics,
    PerformanceProfiler,
    ProcessingStatus
)


class FileDiscoveryStage(ProcessingStage):
    """Professional file discovery with optimization"""
    
    @with_error_handling(
        operation="file_discovery",
        component="FileDiscoveryStage",
        recovery_strategies=[RetryStrategy(max_attempts=2)]
    )
    async def process(self, source_paths: List[str]) -> List[Dict]:
        """Discover files with enterprise patterns"""
        if not source_paths:
            raise ValidationError("No source paths provided", field="source_paths")
        
        logger.info(f"Starting file discovery for {len(source_paths)} paths")
        
        discovered_files = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            for path in source_paths:
                if not Path(path).exists():
                    logger.warning(f"Source path does not exist: {path}")
                    continue
                    
                future = executor.submit(self._discover_path, path)
                futures.append(future)
            
            for future in futures:
                path_files = safe_execute(
                    future.result,
                    timeout=300,
                    operation="discover_path",
                    component="FileDiscoveryStage",
                    default_return=[]
                )
                discovered_files.extend(path_files)
        
        safe_count = str(len(discovered_files))[:10]
        logger.info(f"Discovered {safe_count} files")
        return discovered_files
    
    def _discover_path(self, path: str) -> List[Dict]:
        """Discover files in single path with enterprise error handling"""
        files = []
        path_obj = Path(path)
        
        if not path_obj.exists():
            raise ValidationError(f"Path does not exist: {path}", field="path", value=path)
        
        if not path_obj.is_dir():
            raise ValidationError(f"Path is not a directory: {path}", field="path", value=path)
        
        try:
            file_count = 0
            for file_path in path_obj.rglob('*'):
                if file_path.is_file():
                    try:
                        stat_info = file_path.stat()
                        files.append({
                            'path': str(file_path),
                            'size': stat_info.st_size,
                            'modified': stat_info.st_mtime,
                            'status': ProcessingStatus.PENDING
                        })
                        file_count += 1
                        
                        # Memory management for large directories
                        if file_count % 10000 == 0:
                            logger.info(f"Discovered {file_count} files in {path}")
                            
                    except (OSError, PermissionError) as e:
                        logger.warning(f"Cannot access file {file_path}: {e}")
                        continue
                        
        except PermissionError as e:
            raise ResourceError(f"Permission denied accessing {path}: {e}", resource_type="filesystem")
        except OSError as e:
            raise ResourceError(f"OS error accessing {path}: {e}", resource_type="filesystem")
        
        logger.debug(f"Discovered {len(files)} files in {path}")
        return files


class FileValidationStage(ProcessingStage):
    """Professional file validation"""
    
    @with_error_handling(
        operation="file_validation",
        component="FileValidationStage"
    )
    async def process(self, files: List[Dict]) -> List[Dict]:
        """Validate files with comprehensive checks"""
        if not files:
            logger.warning("No files to validate")
            return []
            
        logger.info(f"Validating {len(files)} files")
        
        valid_files = []
        failed_count = 0
        
        # Adaptive batch size based on available memory
        import psutil
        available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)
        batch_size = min(1000, max(100, int(available_memory_mb / 5)))
        
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._validate_file, file_info) 
                          for file_info in batch]
                
                for future in futures:
                    validated_file = safe_execute(
                        future.result,
                        timeout=30,
                        operation="validate_file",
                        component="FileValidationStage",
                        default_return=None
                    )
                    
                    if validated_file:
                        valid_files.append(validated_file)
                    else:
                        failed_count += 1
            
            # Progress reporting
            if i % (batch_size * 10) == 0:
                logger.info(f"Validated {i + len(batch)}/{len(files)} files")
        
        logger.info(f"Validated {len(valid_files)} files successfully, {failed_count} failed")
        return valid_files
    
    def _validate_file(self, file_info: Dict) -> Optional[Dict]:
        """Validate single file with comprehensive checks"""
        file_path_str = file_info.get('path')
        if not file_path_str:
            raise ValidationError("File info missing path", field="path")
        
        file_path = Path(file_path_str)
        
        # Existence check
        if not file_path.exists():
            logger.debug(f"File no longer exists: {file_path}")
            return None
        
        # File type check
        if not file_path.is_file():
            logger.debug(f"Path is not a file: {file_path}")
            return None
        
        try:
            stat_info = file_path.stat()
            
            # Size validation
            if stat_info.st_size == 0:
                logger.debug(f"Empty file skipped: {file_path}")
                return None
            
            # Size limit check (2GB max)
            if stat_info.st_size > 2 * 1024 * 1024 * 1024:
                logger.warning(f"File too large (>2GB): {file_path}")
                return None
            
            # Accessibility check using os.access
            if not os.access(file_path, os.R_OK):
                logger.warning(f"File not readable: {file_path}")
                return None
            
            # Update file info with validated data
            file_info.update({
                'size': stat_info.st_size,
                'modified': stat_info.st_mtime,
                'status': ProcessingStatus.COMPLETED
            })
            
            return file_info
            
        except PermissionError as e:
            logger.warning(f"Permission denied for {file_path}: {e}")
            return None
        except OSError as e:
            logger.warning(f"OS error accessing {file_path}: {e}")
            return None


class FileClassificationStage(ProcessingStage):
    """Professional file classification"""
    
    def __init__(self, name: str, classification_service, max_workers: int = 4):
        super().__init__(name, max_workers)
        self.classification_service = classification_service
    
    @with_error_handling(
        operation="file_classification",
        component="FileClassificationStage"
    )
    async def process(self, files: List[Dict]) -> List[Dict]:
        """Classify files professionally with enterprise patterns"""
        if not files:
            logger.warning("No files to classify")
            return []
            
        logger.info(f"Classifying {len(files)} files")
        
        classified_files = []
        failed_count = 0
        
        # Adaptive batch processing based on memory
        import psutil
        import gc
        
        available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)
        batch_size = min(500, max(50, int(available_memory_mb / 10)))
        
        logger.debug(f"Using batch size: {batch_size} (available memory: {available_memory_mb:.1f}MB)")
        
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._classify_file, file_info) 
                          for file_info in batch]
                
                for future in futures:
                    classified_file = safe_execute(
                        future.result,
                        timeout=60,
                        operation="classify_file",
                        component="FileClassificationStage",
                        default_return=None
                    )
                    
                    if classified_file:
                        classified_files.append(classified_file)
                    else:
                        failed_count += 1
            
            # Memory management between batches
            gc.collect()
            
            # Progress reporting
            if i % (batch_size * 5) == 0:
                logger.info(f"Classified {i + len(batch)}/{len(files)} files")
        
        logger.info(f"Classified {len(classified_files)} files successfully, {failed_count} failed")
        return classified_files
    
    def _classify_file(self, file_info: Dict) -> Optional[Dict]:
        """Classify single file using enterprise service with validation"""
        file_path = file_info.get('path')
        if not file_path:
            raise ValidationError("File info missing path for classification", field="path")
        
        try:
            # Validate classification service is available
            if not self.classification_service:
                raise EnterpriseException(
                    "Classification service not initialized",
                    component="FileClassificationStage"
                )
            
            # Perform classification
            classified_info = self.classification_service.classify_file(file_info)
            
            # Validate classification result
            if not classified_info:
                logger.warning(f"Classification service returned None for {file_path}")
                file_info['status'] = ProcessingStatus.FAILED
                return file_info
            
            # Ensure required fields are present
            required_fields = ['game_system', 'edition', 'category']
            for field in required_fields:
                if field not in classified_info:
                    classified_info[field] = 'Unknown'
            
            classified_info['status'] = ProcessingStatus.COMPLETED
            return classified_info
            
        except Exception as e:
            safe_path = str(file_path)[:100]
            safe_error = str(e)[:100]
            logger.error(f"Classification error for {safe_path}: {safe_error}")
            file_info.update({
                'status': ProcessingStatus.FAILED,
                'game_system': 'Unknown',
                'edition': 'Unknown',
                'category': 'Unknown',
                'classification_error': str(e)
            })
            return file_info


class FileDeduplicationStage(ProcessingStage):
    """Professional file deduplication"""
    
    async def process(self, files: List[Dict]) -> List[Dict]:
        """Deduplicate files efficiently"""
        logger.info(f"Deduplicating {len(files)} files")
        
        # Group files by size for efficient deduplication
        size_groups = {}
        for file_info in files:
            size = file_info.get('size', 0)
            if size not in size_groups:
                size_groups[size] = []
            size_groups[size].append(file_info)
        
        unique_files = []
        
        for size, file_group in size_groups.items():
            if len(file_group) == 1:
                # No duplicates possible
                unique_files.extend(file_group)
            else:
                # Check for actual duplicates using hash
                unique_files.extend(await self._deduplicate_group(file_group))
        
        logger.info(f"Deduplicated to {len(unique_files)} unique files")
        return unique_files
    
    async def _deduplicate_group(self, file_group: List[Dict]) -> List[Dict]:
        """Deduplicate group of same-size files"""
        hash_map = {}
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._calculate_hash, file_info) 
                      for file_info in file_group]
            
            for future in futures:
                try:
                    file_info, file_hash = future.result(timeout=120)
                    if file_hash not in hash_map:
                        hash_map[file_hash] = file_info
                except Exception as e:
                    logger.error(f"Hash calculation failed: {e}")
        
        return list(hash_map.values())
    
    def _calculate_hash(self, file_info: Dict) -> Tuple[Dict, str]:
        """Calculate file hash efficiently"""
        import hashlib
        
        file_path = Path(file_info['path'])
        hash_obj = hashlib.sha256()
        
        try:
            with open(file_path, 'rb') as f:
                # Read in chunks to handle large files
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_obj.update(chunk)
            
            return file_info, hash_obj.hexdigest()
        except Exception as e:
            logger.error(f"Hash calculation error for {file_path}: {e}")
            return file_info, f"error_{time.time()}"


class FileCopyStage(ProcessingStage):
    """Professional file copying with optimization"""
    
    def __init__(self, name: str, destination_root: str, max_workers: int = 4):
        super().__init__(name, max_workers)
        self.destination_root = Path(destination_root)
        self.destination_root.mkdir(parents=True, exist_ok=True)
    
    async def process(self, files: List[Dict]) -> List[Dict]:
        """Copy files to organized structure"""
        logger.info(f"Copying {len(files)} files to library")
        
        copied_files = []
        
        # Adaptive batch size based on available disk space
        import shutil
        available_space = shutil.disk_usage(self.destination_root).free
        batch_size = min(100, max(10, int(available_space / (100 * 1024 * 1024))))  # Based on 100MB per file estimate
        
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._copy_file, file_info) 
                          for file_info in batch]
                
                for future in futures:
                    try:
                        copied_file = future.result(timeout=60)  # Reduced timeout
                        if copied_file:
                            copied_files.append(copied_file)
                    except TimeoutError:
                        logger.error(f"File copy timeout after 60 seconds")
                        future.cancel()
                    except Exception as e:
                        logger.error(f"File copy failed: {type(e).__name__}: {e}")
                        if hasattr(future, 'cancel'):
                            future.cancel()
        
        logger.info(f"Copied {len(copied_files)} files successfully")
        return copied_files
    
    def _copy_file(self, file_info: Dict) -> Optional[Dict]:
        """Copy single file to organized location"""
        import os
        import shutil
        from .enhanced_copy_utils import create_enhanced_destination_path, copy_file_enhanced
        
        try:
            source_path = Path(file_info['path'])
            
            # Check if source file still exists
            if not source_path.exists():
                logger.error(f"Source file no longer exists: {source_path}")
                file_info['status'] = ProcessingStatus.FAILED
                return file_info
            
            # Check if source is readable
            if not os.access(source_path, os.R_OK):
                logger.error(f"Source file not readable: {source_path}")
                file_info['status'] = ProcessingStatus.FAILED
                return file_info
            
            # Use enhanced copy utilities for consistent 3-level hierarchy
            dest_dir = create_enhanced_destination_path(self.destination_root, file_info)
            
            # Check available disk space
            import shutil as shutil_disk
            available_space = shutil_disk.disk_usage(self.destination_root).free
            file_size = source_path.stat().st_size
            
            if available_space < file_size * 2:  # Need 2x space for safety
                logger.error(f"Insufficient disk space for {source_path} (need {file_size}, have {available_space})")
                file_info['status'] = ProcessingStatus.FAILED
                return file_info
            
            dest_path = copy_file_enhanced(source_path, dest_dir)
            
            if dest_path:
                file_info.update({
                    'destination_path': str(dest_path),
                    'status': ProcessingStatus.COMPLETED
                })
            else:
                logger.error(f"Copy operation failed for {source_path} - copy_file_enhanced returned None")
                file_info['status'] = ProcessingStatus.FAILED
            
            return file_info
            
        except PermissionError as e:
            logger.error(f"Permission denied copying {file_info.get('path', 'unknown')}: {e}")
            file_info['status'] = ProcessingStatus.FAILED
            return file_info
        except OSError as e:
            logger.error(f"OS error copying {file_info.get('path', 'unknown')}: {e}")
            file_info['status'] = ProcessingStatus.FAILED
            return file_info
        except Exception as e:
            logger.error(f"Unexpected copy error for {file_info.get('path', 'unknown')}: {type(e).__name__}: {e}")
            file_info['status'] = ProcessingStatus.FAILED
            return file_info
    



class ProfessionalLibrarianOrchestrator:
    """Professional orchestrator with enterprise patterns"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.orchestrator = EnterpriseLibrarianOrchestrator()
        self.profiler = PerformanceProfiler()
        self.metrics = ProcessingMetrics()
        
        # Initialize components
        self._initialize_components()
    
    @with_error_handling(
        operation="component_initialization",
        component="ProfessionalLibrarianOrchestrator"
    )
    def _initialize_components(self):
        """Initialize system components with enterprise patterns"""
        logger.info("Initializing professional librarian components")
        
        # Initialize performance monitoring
        from .enterprise_performance_monitor import get_performance_monitor
        self.performance_monitor = get_performance_monitor()
        self.performance_monitor.start_monitoring()
        
        # Initialize classification service
        from .enterprise_classification_service import EnterpriseClassificationService
        self.classification_service = EnterpriseClassificationService(self.config)
        
        # Initialize enterprise logging context
        from .enterprise_logging import LogContext, SecurityLevel
        self.log_context = LogContext(
            operation="library_processing",
            component="ProfessionalLibrarianOrchestrator",
            security_level=SecurityLevel.INTERNAL
        )
        
        logger.info("All enterprise components initialized successfully")
    
    @with_error_handling(
        operation="library_processing",
        component="ProfessionalLibrarianOrchestrator"
    )
    async def process_library(self) -> ProcessingMetrics:
        """Process library with professional pipeline and enterprise monitoring"""
        
        with logger.context(self.log_context):
            logger.info("Starting professional library processing")
            
            with self.performance_monitor.profile_operation('full_pipeline') as perf_metrics:
                try:
                    # Validate configuration
                    source_paths = self.config.get('source_paths', [])
                    if isinstance(source_paths, str):
                        source_paths = [p.strip() for p in source_paths.split(',')]
                    
                    if not source_paths:
                        raise ValidationError("No source paths configured", field="source_paths")
                    
                    library_root = self.config.get('library_root')
                    if not library_root:
                        raise ValidationError("No library root configured", field="library_root")
                    
                    # Create processing pipeline
                    pipeline = self.orchestrator.create_pipeline('main_processing')
                    
                    # Add stages with enterprise configuration
                    max_workers = self.config.get('max_workers', 4)
                    pipeline.add_stage(FileDiscoveryStage('file_discovery', max_workers))
                    pipeline.add_stage(FileValidationStage('file_validation', max_workers))
                    pipeline.add_stage(FileClassificationStage('file_classification', self.classification_service, max_workers))
                    pipeline.add_stage(FileDeduplicationStage('file_deduplication', max_workers))
                    pipeline.add_stage(FileCopyStage('file_copy', library_root, max_workers))
                    
                    # Execute pipeline with monitoring
                    logger.info(f"Processing {len(source_paths)} source paths with {max_workers} workers")
                    metrics = await pipeline.process(source_paths)
                    
                    # Update performance metrics
                    perf_metrics.custom_metrics.update({
                        'files_processed': metrics.files_processed,
                        'files_failed': metrics.files_failed,
                        'throughput_files_per_second': metrics.throughput_files_per_second
                    })
                    
                    # Generate professional report
                    await self._generate_report(metrics)
                    
                    logger.info(f"Library processing completed: {metrics.files_processed} files processed")
                    return metrics
                    
                except Exception as e:
                    perf_metrics.custom_metrics['error'] = str(e)
                    logger.error(f"Pipeline processing failed: {e}", exception=e)
                    raise
    
    async def _generate_report(self, metrics: ProcessingMetrics):
        """Generate comprehensive professional report with enterprise analytics"""
        logger.info("Generating professional processing report")
        
        try:
            # Generate performance report
            performance_report = self.performance_monitor.generate_report()
            
            # Get system status
            system_status = self.orchestrator.get_system_status()
            
            # Create comprehensive report
            report = {
                'report_metadata': {
                    'report_id': f"librarian_report_{int(time.time())}",
                    'generated_at': time.time(),
                    'version': '2.0.0-enterprise'
                },
                'processing_summary': {
                    'files_processed': metrics.files_processed,
                    'files_failed': metrics.files_failed,
                    'duration_seconds': metrics.duration,
                    'throughput_files_per_second': metrics.throughput_files_per_second,
                    'throughput_mb_per_second': metrics.throughput_mb_per_second,
                    'memory_peak_mb': metrics.memory_peak_mb,
                    'success_rate': (metrics.files_processed / max(metrics.files_processed + metrics.files_failed, 1)) * 100
                },
                'system_health': system_status.get('health', {}),
                'performance_analysis': performance_report.to_dict(),
                'recommendations': self._generate_recommendations(metrics, performance_report),
                'configuration_summary': {
                    'source_paths_count': len(self.config.get('source_paths', [])),
                    'library_root': self.config.get('library_root', 'unknown'),
                    'max_workers': self.config.get('max_workers', 4)
                }
            }
            
            # Save main report
            import json
            report_path = Path('librarian_professional_report.json')
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            # Save detailed performance report
            perf_report_path = Path('librarian_performance_report.json')
            self.performance_monitor.save_report(performance_report, perf_report_path)
            
            logger.info(f"Professional reports saved: {report_path}, {perf_report_path}")
            
            # Print summary
            self._print_summary(report)
            
        except Exception as e:
            logger.error(f"Report generation failed: {e}", exception=e)
            # Generate minimal fallback report
            self._generate_fallback_report(metrics)
    
    def _generate_recommendations(self, metrics: ProcessingMetrics, performance_report) -> List[str]:
        """Generate comprehensive optimization recommendations"""
        recommendations = []
        
        # Performance-based recommendations
        if metrics.throughput_files_per_second < 5:
            recommendations.append(
                f"Low throughput detected ({metrics.throughput_files_per_second:.2f} files/sec). "
                "Consider increasing worker threads, optimizing storage, or reducing file validation complexity."
            )
        
        if metrics.memory_peak_mb > 2000:
            recommendations.append(
                f"High memory usage detected ({metrics.memory_peak_mb:.1f}MB peak). "
                "Consider reducing batch sizes, enabling memory optimization, or processing in smaller chunks."
            )
        
        # Success rate recommendations
        success_rate = (metrics.files_processed / max(metrics.files_processed + metrics.files_failed, 1)) * 100
        if success_rate < 95:
            recommendations.append(
                f"Low success rate ({success_rate:.1f}%). "
                "Review error logs and consider improving error handling or file validation."
            )
        
        # Duration-based recommendations
        if metrics.duration > 3600:  # More than 1 hour
            recommendations.append(
                f"Long processing time ({metrics.duration/60:.1f} minutes). "
                "Consider parallel processing, faster storage, or incremental processing."
            )
        
        # Add performance monitor recommendations
        if hasattr(performance_report, 'recommendations'):
            recommendations.extend(performance_report.recommendations)
        
        # Configuration recommendations
        max_workers = self.config.get('max_workers', 4)
        import psutil
        cpu_count = psutil.cpu_count()
        
        if max_workers < cpu_count // 2:
            recommendations.append(
                f"Consider increasing max_workers from {max_workers} to {cpu_count//2} "
                f"to better utilize available CPU cores ({cpu_count})."
            )
        
        return recommendations
    

    
    def _print_summary(self, report: Dict):
        """Print comprehensive professional summary"""
        summary = report['processing_summary']
        
        print("\n" + "="*80)
        print("ENTERPRISE LIBRARY PROCESSING SUMMARY")
        print("="*80)
        print(f"Report ID: {report['report_metadata']['report_id']}")
        print(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(report['report_metadata']['generated_at']))}")
        print("-"*80)
        print(f"Files Processed: {summary['files_processed']:,}")
        print(f"Files Failed: {summary['files_failed']:,}")
        print(f"Success Rate: {summary['success_rate']:.1f}%")
        print(f"Duration: {summary['duration_seconds']:.2f} seconds ({summary['duration_seconds']/60:.1f} minutes)")
        print(f"Throughput: {summary['throughput_files_per_second']:.2f} files/sec")
        print(f"Data Throughput: {summary['throughput_mb_per_second']:.2f} MB/sec")
        print(f"Memory Peak: {summary['memory_peak_mb']:.1f} MB")
        
        # Performance summary
        perf_summary = report['performance_analysis'].get('summary', {})
        if perf_summary:
            print("-"*80)
            print("PERFORMANCE ANALYSIS:")
            print(f"Average CPU: {perf_summary.get('avg_cpu_percent', 0):.1f}%")
            print(f"Peak CPU: {perf_summary.get('peak_cpu_percent', 0):.1f}%")
            print(f"Average Memory: {perf_summary.get('avg_memory_percent', 0):.1f}%")
            print(f"Peak Memory: {perf_summary.get('peak_memory_percent', 0):.1f}%")
        
        # Recommendations
        if report['recommendations']:
            print("-"*80)
            print("OPTIMIZATION RECOMMENDATIONS:")
            for i, rec in enumerate(report['recommendations'], 1):
                print(f"{i}. {rec}")
        
        print("="*80)
        print(f"Detailed reports saved: librarian_professional_report.json, librarian_performance_report.json")
        print("="*80)
    
    def _generate_fallback_report(self, metrics: ProcessingMetrics):
        """Generate minimal fallback report if main report generation fails"""
        try:
            fallback_report = {
                'processing_summary': {
                    'files_processed': metrics.files_processed,
                    'files_failed': metrics.files_failed,
                    'duration_seconds': metrics.duration,
                    'status': 'completed_with_errors'
                },
                'error': 'Full report generation failed - this is a minimal fallback report'
            }
            
            import json
            with open('librarian_fallback_report.json', 'w') as f:
                json.dump(fallback_report, f, indent=2)
            
            logger.warning("Fallback report generated: librarian_fallback_report.json")
            
        except Exception as e:
            logger.error(f"Even fallback report generation failed: {e}")


@with_error_handling(
    operation="main_processing",
    component="ProfessionalOrchestrator"
)
async def main():
    """Professional main entry point with enterprise error handling"""
    # Load enterprise configuration
    from .enterprise_config_manager import load_config
    
    try:
        config_dict = load_config()
        
        # Convert enterprise config to legacy format for compatibility
        legacy_config = {
            'source_paths': config_dict.library.source_paths,
            'library_root': config_dict.library.library_root,
            'knowledge_base_db_url': config_dict.database.url,
            'max_workers': config_dict.processing.max_workers,
            'debug': config_dict.debug
        }
        
        # Create professional orchestrator
        orchestrator = ProfessionalLibrarianOrchestrator(legacy_config)
        
        # Process library with performance monitoring
        with logger.performance_monitor("full_library_processing"):
            metrics = await orchestrator.process_library()
        
        logger.info("Professional library processing completed successfully")
        logger.audit(
            action="library_processing_completed",
            resource="file_library",
            result="success",
            files_processed=metrics.files_processed,
            duration_seconds=metrics.duration
        )
        
        return 0
        
    except ValidationError as e:
        logger.error(f"Configuration validation failed: {e}")
        return 2
    except SecurityError as e:
        logger.critical(f"Security error: {e}")
        return 3
    except EnterpriseException as e:
        logger.error(f"Enterprise processing failed: {e}")
        return 1
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exception=e)
        return 1


if __name__ == "__main__":
    import asyncio
    
    # Configure enterprise logging
    from .enterprise_logging import configure_root_logging
    configure_root_logging()
    
    # Run main with proper error handling
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exception=e)
        sys.exit(1)