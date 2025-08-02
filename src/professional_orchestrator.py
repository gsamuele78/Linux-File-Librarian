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
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configure professional logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('librarian_professional.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

from src.enterprise_architecture import (
    EnterpriseLibrarianOrchestrator,
    ProcessingStage,
    ProcessingMetrics,
    PerformanceProfiler,
    ProcessingStatus
)


class FileDiscoveryStage(ProcessingStage):
    """Professional file discovery with optimization"""
    
    async def process(self, source_paths: List[str]) -> List[Dict]:
        """Discover files with enterprise patterns"""
        logger.info(f"Starting file discovery for {len(source_paths)} paths")
        
        discovered_files = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            for path in source_paths:
                future = executor.submit(self._discover_path, path)
                futures.append(future)
            
            for future in futures:
                try:
                    path_files = future.result(timeout=300)  # 5 minute timeout
                    discovered_files.extend(path_files)
                except Exception as e:
                    logger.error(f"File discovery failed for path: {e}")
        
        logger.info(f"Discovered {len(discovered_files)} files")
        return discovered_files
    
    def _discover_path(self, path: str) -> List[Dict]:
        """Discover files in single path"""
        files = []
        path_obj = Path(path)
        
        if not path_obj.exists():
            logger.warning(f"Path does not exist: {path}")
            return files
        
        try:
            for file_path in path_obj.rglob('*'):
                if file_path.is_file():
                    files.append({
                        'path': str(file_path),
                        'size': file_path.stat().st_size,
                        'modified': file_path.stat().st_mtime,
                        'status': ProcessingStatus.PENDING
                    })
        except Exception as e:
            logger.error(f"Error discovering files in {path}: {e}")
        
        return files


class FileValidationStage(ProcessingStage):
    """Professional file validation"""
    
    async def process(self, files: List[Dict]) -> List[Dict]:
        """Validate files with comprehensive checks"""
        logger.info(f"Validating {len(files)} files")
        
        valid_files = []
        
        # Process in batches to prevent memory issues
        batch_size = 1000
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._validate_file, file_info) 
                          for file_info in batch]
                
                for future in futures:
                    try:
                        validated_file = future.result(timeout=30)
                        if validated_file:
                            valid_files.append(validated_file)
                    except Exception as e:
                        logger.error(f"File validation failed: {e}")
        
        logger.info(f"Validated {len(valid_files)} files successfully")
        return valid_files
    
    def _validate_file(self, file_info: Dict) -> Optional[Dict]:
        """Validate single file"""
        try:
            file_path = Path(file_info['path'])
            
            # Basic validation
            if not file_path.exists():
                return None
            
            if file_path.stat().st_size == 0:
                return None
            
            # Update status
            file_info['status'] = ProcessingStatus.COMPLETED
            return file_info
            
        except Exception as e:
            logger.error(f"Validation error for {file_info.get('path', 'unknown')}: {e}")
            return None


class FileClassificationStage(ProcessingStage):
    """Professional file classification"""
    
    def __init__(self, name: str, classifier, max_workers: int = 4):
        super().__init__(name, max_workers)
        self.classifier = classifier
    
    async def process(self, files: List[Dict]) -> List[Dict]:
        """Classify files professionally"""
        logger.info(f"Classifying {len(files)} files")
        
        classified_files = []
        
        # Adaptive batch processing based on memory
        import psutil
        available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)
        batch_size = min(500, max(50, int(available_memory_mb / 10)))
        
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._classify_file, file_info) 
                          for file_info in batch]
                
                for future in futures:
                    try:
                        classified_file = future.result(timeout=60)
                        if classified_file:
                            classified_files.append(classified_file)
                    except Exception as e:
                        logger.error(f"Classification failed: {e}")
            
            # Memory management between batches
            import gc
            gc.collect()
        
        logger.info(f"Classified {len(classified_files)} files")
        return classified_files
    
    def _classify_file(self, file_info: Dict) -> Optional[Dict]:
        """Classify single file"""
        try:
            file_path = Path(file_info['path'])
            
            # Get mime type
            import mimetypes
            mime_type, _ = mimetypes.guess_type(str(file_path))
            if not mime_type:
                mime_type = 'application/octet-stream'
            
            # Use classifier to determine category
            game_system, edition, category = self.classifier.classify(
                file_path.name, str(file_path), mime_type
            )
            
            # Use ISBN enricher as additional resource if primary classification is generic
            if (game_system in ['Miscellaneous', None] or category in ['Miscellaneous', None]) and \
               mime_type.startswith('application/pdf'):
                try:
                    from src.isbn_enricher import enrich_file_with_isbn_metadata
                    
                    logger.debug(f"Attempting ISBN enrichment for: {file_path.name}")
                    isbn_results = enrich_file_with_isbn_metadata(str(file_path))
                    if isbn_results:
                        meta = isbn_results[0]['metadata']
                        title = meta.get('title', '')
                        
                        if title:
                            logger.debug(f"ISBN enrichment found title: {title}")
                            ntitle = self.classifier.normalize_text(title)
                            if ntitle in self.classifier.product_cache:
                                game_system, edition, category = self.classifier.product_cache[ntitle]
                                logger.debug(f"ISBN enrichment matched knowledge base: {game_system}")
                            else:
                                # Use ISBN metadata for generic book classification
                                category = 'Books'
                                game_system = meta.get('publisher', 'Unknown Publisher')
                                logger.debug(f"ISBN enrichment classified as book: {game_system}")
                                
                except Exception as e:
                    logger.debug(f"ISBN enrichment failed for {file_path.name}: {e}")
                    pass  # Continue with original classification if ISBN enrichment fails
            
            file_info.update({
                'category': category or 'Unknown',
                'game_system': game_system or 'Unknown',
                'edition': edition or 'Unknown',
                'status': ProcessingStatus.COMPLETED
            })
            
            return file_info
            
        except Exception as e:
            logger.error(f"Classification error for {file_info.get('path', 'unknown')}: {e}")
            file_info['status'] = ProcessingStatus.FAILED
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
                        copied_file = future.result(timeout=300)
                        if copied_file:
                            copied_files.append(copied_file)
                    except Exception as e:
                        logger.error(f"File copy failed: {e}")
        
        logger.info(f"Copied {len(copied_files)} files successfully")
        return copied_files
    
    def _copy_file(self, file_info: Dict) -> Optional[Dict]:
        """Copy single file to organized location"""
        import shutil
        from src.enhanced_copy_utils import create_enhanced_destination_path, copy_file_enhanced
        
        try:
            source_path = Path(file_info['path'])
            
            # Use enhanced copy utilities for consistent 3-level hierarchy
            dest_dir = create_enhanced_destination_path(self.destination_root, file_info)
            dest_path = copy_file_enhanced(source_path, dest_dir)
            
            if dest_path:
                file_info.update({
                    'destination_path': str(dest_path),
                    'status': ProcessingStatus.COMPLETED
                })
            else:
                file_info['status'] = ProcessingStatus.FAILED
            
            return file_info
            
        except Exception as e:
            logger.error(f"Copy error for {file_info.get('path', 'unknown')}: {e}")
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
    
    def _initialize_components(self):
        """Initialize system components"""
        logger.info("Initializing professional librarian components")
        
        # Lazy load heavy dependencies
        try:
            from src.classifier import Classifier
            from src.config_loader import load_config
            
            # Check if knowledge base exists, build if needed
            kb_path = self.config.get('knowledge_base_db_url', 'knowledge.sqlite')
            if not Path(kb_path).exists():
                logger.info("Knowledge base not found, building automatically...")
                self._build_knowledge_base()
            
            self.classifier = Classifier(kb_path)
            logger.info("Components initialized successfully")
            
        except Exception as e:
            logger.error(f"Component initialization failed: {e}")
            raise
    
    async def process_library(self) -> ProcessingMetrics:
        """Process library with professional pipeline"""
        logger.info("Starting professional library processing")
        
        with self.profiler.profile('full_pipeline'):
            try:
                # Create processing pipeline
                pipeline = self.orchestrator.create_pipeline('main_processing')
                
                # Add stages
                pipeline.add_stage(FileDiscoveryStage('file_discovery'))
                pipeline.add_stage(FileValidationStage('file_validation'))
                pipeline.add_stage(FileClassificationStage('file_classification', self.classifier))
                pipeline.add_stage(FileDeduplicationStage('file_deduplication'))
                pipeline.add_stage(FileCopyStage('file_copy', self.config['library_root']))
                
                # Process source paths
                source_paths = self.config.get('source_paths', [])
                if isinstance(source_paths, str):
                    source_paths = [p.strip() for p in source_paths.split(',')]
                
                # Execute pipeline
                metrics = await pipeline.process(source_paths)
                
                # Generate professional report
                await self._generate_report(metrics)
                
                return metrics
                
            except Exception as e:
                logger.error(f"Pipeline processing failed: {e}")
                raise
    
    async def _generate_report(self, metrics: ProcessingMetrics):
        """Generate comprehensive professional report"""
        logger.info("Generating professional processing report")
        
        system_status = self.orchestrator.get_system_status()
        performance_report = self.profiler.get_report()
        
        report = {
            'processing_summary': {
                'files_processed': metrics.files_processed,
                'files_failed': metrics.files_failed,
                'duration_seconds': metrics.duration,
                'throughput_files_per_second': metrics.throughput_files_per_second,
                'throughput_mb_per_second': metrics.throughput_mb_per_second,
                'memory_peak_mb': metrics.memory_peak_mb
            },
            'system_health': system_status['health'],
            'performance_analysis': performance_report,
            'recommendations': self._generate_recommendations(metrics, performance_report)
        }
        
        # Save report
        import json
        report_path = Path('librarian_professional_report.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Professional report saved to {report_path}")
        
        # Print summary
        self._print_summary(report)
    
    def _generate_recommendations(self, metrics: ProcessingMetrics, performance_report: Dict) -> List[str]:
        """Generate optimization recommendations"""
        recommendations = []
        
        if metrics.throughput_files_per_second < 10:
            recommendations.append("Consider increasing worker threads for better throughput")
        
        if metrics.memory_peak_mb > 2000:
            recommendations.append("High memory usage detected - consider reducing batch sizes")
        
        if len(performance_report.get('recommendations', [])) > 0:
            recommendations.extend(performance_report['recommendations'])
        
        return recommendations
    
    def _build_knowledge_base(self):
        """Build knowledge base automatically"""
        try:
            import subprocess
            import sys
            
            logger.info("Building TTRPG knowledge base for enhanced classification...")
            logger.info("This may take a few minutes to scrape web sources...")
            
            # Run the knowledge base builder with real-time output
            process = subprocess.Popen(
                [sys.executable, '-m', 'src.build_knowledgebase'],
                cwd=Path.cwd(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Stream output in real-time
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    logger.info(f"KB Build: {output.strip()}")
            
            return_code = process.poll()
            
            if return_code == 0:
                logger.info("Knowledge base built successfully")
            else:
                logger.warning(f"Knowledge base build completed with warnings (exit code: {return_code})")
                
        except Exception as e:
            logger.warning(f"Could not build knowledge base: {e}")
            logger.info("Continuing with basic classification without knowledge base")
    
    def _print_summary(self, report: Dict):
        """Print professional summary"""
        summary = report['processing_summary']
        
        print("\n" + "="*60)
        print("PROFESSIONAL LIBRARY PROCESSING SUMMARY")
        print("="*60)
        print(f"Files Processed: {summary['files_processed']:,}")
        print(f"Files Failed: {summary['files_failed']:,}")
        print(f"Duration: {summary['duration_seconds']:.2f} seconds")
        print(f"Throughput: {summary['throughput_files_per_second']:.2f} files/sec")
        print(f"Memory Peak: {summary['memory_peak_mb']:.1f} MB")
        
        if report['recommendations']:
            print("\nRECOMMENDATIONS:")
            for i, rec in enumerate(report['recommendations'], 1):
                print(f"{i}. {rec}")
        
        print("="*60)


async def main():
    """Professional main entry point"""
    try:
        # Load configuration
        from src.config_loader import load_config
        config = load_config()
        
        # Create professional orchestrator
        orchestrator = ProfessionalLibrarianOrchestrator(config)
        
        # Process library
        metrics = await orchestrator.process_library()
        
        logger.info("Professional library processing completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Professional processing failed: {e}")
        return 1


if __name__ == "__main__":
    import asyncio
    sys.exit(asyncio.run(main()))