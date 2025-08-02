#!/usr/bin/env python3
"""
Integration Adapter for Enterprise Architecture

Seamlessly integrates enterprise architecture with existing scripts:
- Backward compatibility with existing librarian.py
- Transparent enterprise features activation
- Legacy script enhancement
- Gradual migration support
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnterpriseIntegrationAdapter:
    """Adapter to integrate enterprise features with existing scripts"""
    
    def __init__(self):
        self.enterprise_mode = self._detect_enterprise_mode()
        self.legacy_components = {}
        self.enterprise_components = {}
        
    def _detect_enterprise_mode(self) -> bool:
        """Detect if enterprise mode should be enabled"""
        # Check environment variable
        if os.getenv('LIBRARIAN_ENTERPRISE_MODE', '').lower() == 'true':
            return True
        
        # Check for enterprise flag file
        enterprise_flag = Path(__file__).parent.parent / '.enterprise_mode'
        if enterprise_flag.exists():
            return True
        
        # Check system resources (enable enterprise mode on powerful systems)
        try:
            import psutil
            memory_gb = psutil.virtual_memory().total / (1024**3)
            cpu_count = psutil.cpu_count()
            
            # Enable enterprise mode on systems with 8GB+ RAM and 4+ cores
            if memory_gb >= 8 and cpu_count >= 4:
                return True
        except ImportError:
            pass
        
        return False
    
    def wrap_legacy_librarian(self):
        """Wrap existing librarian.py with enterprise features"""
        if not self.enterprise_mode:
            # Run legacy version as-is
            return self._run_legacy_librarian()
        
        logger.info("Enterprise mode enabled - enhancing legacy librarian")
        
        try:
            # Import enterprise components
            from src.enterprise_architecture_fixed import EnterpriseLibrarianOrchestrator
            from src.system_optimization import OptimizedFileProcessor
            from src.performance_monitor import PerformanceMonitor
            
            # Create enterprise wrapper
            return self._run_enterprise_enhanced_librarian()
            
        except ImportError as e:
            logger.warning(f"Enterprise components not available: {e}")
            logger.info("Falling back to legacy mode")
            return self._run_legacy_librarian()
    
    def _run_legacy_librarian(self):
        """Run original librarian.py"""
        try:
            # Import and run original librarian
            from src.librarian import main as legacy_main
            return legacy_main()
        except Exception as e:
            logger.error(f"Legacy librarian failed: {e}")
            return 1
    
    def _run_enterprise_enhanced_librarian(self):
        """Run librarian with enterprise enhancements"""
        try:
            # Import enterprise and legacy components
            from src.enterprise_architecture_fixed import EnterpriseLibrarianOrchestrator
            from src.system_optimization import OptimizedFileProcessor
            from src.librarian import LibraryBuilder
            from src.config_loader import load_config
            
            # Load configuration
            config = load_config()
            
            # Create enterprise orchestrator
            orchestrator = EnterpriseLibrarianOrchestrator()
            
            # Create optimized processor
            processor = OptimizedFileProcessor()
            
            # Enhance legacy LibraryBuilder with enterprise features
            enhanced_builder = self._create_enhanced_builder(config, orchestrator, processor)
            
            # Run enhanced processing
            return enhanced_builder.run_enhanced_processing()
            
        except Exception as e:
            logger.error(f"Enterprise enhanced librarian failed: {e}")
            logger.info("Falling back to legacy mode")
            return self._run_legacy_librarian()
    
    def _create_enhanced_builder(self, config, orchestrator, processor):
        """Create enhanced library builder"""
        from src.library_builder import LibraryBuilder
        
        class EnhancedLibraryBuilder(LibraryBuilder):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.orchestrator = orchestrator
                self.processor = processor
                self.enterprise_metrics = {}
            
            def run_enhanced_processing(self):
                """Run processing with enterprise enhancements"""
                logger.info("Starting enterprise-enhanced processing")
                
                try:
                    # Setup enterprise monitoring
                    self.orchestrator.health_check.register_check(
                        'memory', lambda: psutil.virtual_memory().percent < 90
                    )
                    
                    # Run original processing with enhancements
                    with self.processor.profiler.profile('full_processing'):
                        # Call original scan_files with optimization
                        files = self._enhanced_scan_files()
                        
                        # Call original processing methods with enhancements
                        if files:
                            files = self._enhanced_validate_and_repair_pdfs(files)
                            files = self._enhanced_classify_and_analyze(files)
                            files = self._enhanced_deduplicate_files(files)
                            files = self._enhanced_copy_and_index(files)
                    
                    # Generate enterprise report
                    self._generate_enterprise_report()
                    
                    logger.info("Enterprise-enhanced processing completed successfully")
                    return 0
                    
                except Exception as e:
                    logger.error(f"Enhanced processing failed: {e}")
                    # Fallback to original processing
                    return super().run_processing()
            
            def _enhanced_scan_files(self):
                """Enhanced file scanning with optimization"""
                logger.info("Enhanced file scanning with enterprise optimization")
                
                # Use optimized file discovery if available
                try:
                    import asyncio
                    from src.professional_orchestrator import FileDiscoveryStage
                    
                    # Create discovery stage
                    discovery = FileDiscoveryStage('enhanced_discovery')
                    
                    # Run async discovery
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    source_paths = self.config.get('source_paths', [])
                    if isinstance(source_paths, str):
                        source_paths = [p.strip() for p in source_paths.split(',')]
                    
                    discovered_files = loop.run_until_complete(discovery.process(source_paths))
                    loop.close()
                    
                    # Convert to legacy format
                    return [f['path'] for f in discovered_files]
                    
                except Exception as e:
                    logger.warning(f"Enhanced scanning failed, using legacy: {e}")
                    return super().scan_files()
            
            def _enhanced_validate_and_repair_pdfs(self, files):
                """Enhanced PDF validation with optimization"""
                logger.info("Enhanced PDF validation with enterprise features")
                
                try:
                    # Use batch processing for better performance
                    batch_processor = self.processor.batch_processor
                    
                    def validate_batch(file_batch):
                        return [f for f in file_batch if self._validate_single_file(f)]
                    
                    return batch_processor.process_batches(files, validate_batch)
                    
                except Exception as e:
                    logger.warning(f"Enhanced validation failed, using legacy: {e}")
                    return super().validate_and_repair_pdfs(files)
            
            def _enhanced_classify_and_analyze(self, files):
                """Enhanced classification with caching"""
                logger.info("Enhanced classification with enterprise caching")
                
                try:
                    cache = self.processor.cache_optimizer
                    
                    def classify_with_cache(file_path):
                        cache_key = f"classify_{hash(file_path)}"
                        return cache.get_cached(cache_key, super().classify_single_file, file_path)
                    
                    # Process with caching
                    classified_files = []
                    for file_path in files:
                        try:
                            result = classify_with_cache(file_path)
                            if result:
                                classified_files.append(result)
                        except Exception as e:
                            logger.warning(f"Classification failed for {file_path}: {e}")
                    
                    return classified_files
                    
                except Exception as e:
                    logger.warning(f"Enhanced classification failed, using legacy: {e}")
                    return super().classify_and_analyze(files)
            
            def _enhanced_deduplicate_files(self, files):
                """Enhanced deduplication with optimization"""
                logger.info("Enhanced deduplication with enterprise optimization")
                
                try:
                    # Use memory-mapped processing for large files
                    mmap_processor = self.processor.mmap_processor
                    
                    def calculate_hash_optimized(file_path):
                        return mmap_processor.process_large_file(
                            Path(file_path), 
                            lambda f: self._calculate_file_hash(f)
                        )
                    
                    # Group by size first (optimization)
                    size_groups = {}
                    for file_info in files:
                        size = file_info.get('size', 0)
                        if size not in size_groups:
                            size_groups[size] = []
                        size_groups[size].append(file_info)
                    
                    # Deduplicate each group
                    unique_files = []
                    for size, group in size_groups.items():
                        if len(group) == 1:
                            unique_files.extend(group)
                        else:
                            # Hash-based deduplication for same-size files
                            hash_map = {}
                            for file_info in group:
                                try:
                                    file_hash = calculate_hash_optimized(file_info['path'])
                                    if file_hash not in hash_map:
                                        hash_map[file_hash] = file_info
                                except Exception as e:
                                    logger.warning(f"Hash calculation failed: {e}")
                                    # Keep file if hash fails
                                    unique_files.append(file_info)
                            
                            unique_files.extend(hash_map.values())
                    
                    return unique_files
                    
                except Exception as e:
                    logger.warning(f"Enhanced deduplication failed, using legacy: {e}")
                    return super().deduplicate_files(files)
            
            def _enhanced_copy_and_index(self, files):
                """Enhanced copying with progress tracking"""
                logger.info("Enhanced copying with enterprise progress tracking")
                
                try:
                    # Use async copying for better performance
                    import asyncio
                    from src.professional_orchestrator import FileCopyStage
                    
                    copy_stage = FileCopyStage('enhanced_copy', self.config['library_root'])
                    
                    # Convert to expected format
                    file_dicts = []
                    for file_info in files:
                        if isinstance(file_info, str):
                            file_dicts.append({'path': file_info})
                        else:
                            file_dicts.append(file_info)
                    
                    # Run async copying
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    copied_files = loop.run_until_complete(copy_stage.process(file_dicts))
                    loop.close()
                    
                    return copied_files
                    
                except Exception as e:
                    logger.warning(f"Enhanced copying failed, using legacy: {e}")
                    return super().copy_and_index(files)
            
            def _generate_enterprise_report(self):
                """Generate comprehensive enterprise report"""
                try:
                    system_status = self.orchestrator.get_system_status()
                    optimization_report = self.processor.get_optimization_report()
                    
                    report = {
                        'mode': 'enterprise_enhanced',
                        'system_status': system_status,
                        'optimization_report': optimization_report,
                        'legacy_compatibility': True
                    }
                    
                    # Save report
                    import json
                    with open('enterprise_enhanced_report.json', 'w') as f:
                        json.dump(report, f, indent=2, default=str)
                    
                    logger.info("Enterprise report generated: enterprise_enhanced_report.json")
                    
                except Exception as e:
                    logger.warning(f"Report generation failed: {e}")
        
        return EnhancedLibraryBuilder(
            config['source_paths'],
            config['library_root'],
            config
        )


def integrate_with_existing_scripts():
    """Integration function for existing scripts"""
    
    # Create adapter
    adapter = EnterpriseIntegrationAdapter()
    
    # Check which script is being called
    script_name = Path(sys.argv[0]).name
    
    if script_name == 'librarian.py' or 'librarian' in script_name:
        return adapter.wrap_legacy_librarian()
    
    elif script_name == 'run_librarian.sh':
        # Enhance shell script execution
        return enhance_shell_script_execution()
    
    else:
        # Default behavior
        return adapter.wrap_legacy_librarian()


def enhance_shell_script_execution():
    """Enhance shell script execution with enterprise features"""
    logger.info("Enhancing shell script execution with enterprise features")
    
    try:
        # Set enterprise mode environment variable
        os.environ['LIBRARIAN_ENTERPRISE_MODE'] = 'true'
        
        # Run enhanced librarian
        adapter = EnterpriseIntegrationAdapter()
        return adapter.wrap_legacy_librarian()
        
    except Exception as e:
        logger.error(f"Shell script enhancement failed: {e}")
        return 1


# Monkey patch for seamless integration
def patch_existing_imports():
    """Monkey patch existing imports for seamless integration"""
    try:
        import src.librarian as librarian_module
        
        # Store original main function
        original_main = librarian_module.main
        
        # Create enhanced main function
        def enhanced_main():
            adapter = EnterpriseIntegrationAdapter()
            return adapter.wrap_legacy_librarian()
        
        # Replace main function
        librarian_module.main = enhanced_main
        
        logger.info("Successfully patched librarian.main with enterprise features")
        
    except Exception as e:
        logger.warning(f"Could not patch existing imports: {e}")


# Auto-patch on import
if __name__ != "__main__":
    patch_existing_imports()


if __name__ == "__main__":
    sys.exit(integrate_with_existing_scripts())