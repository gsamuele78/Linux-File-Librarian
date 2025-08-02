#!/usr/bin/env python3
"""
Linux File Librarian - System Engineering Optimized Main Orchestrator

Designed with enterprise-grade performance optimization and memory management:
- Lazy loading of heavy dependencies
- Memory-mapped file processing
- Adaptive resource allocation
- Circuit breaker pattern for fault tolerance
- Comprehensive monitoring and alerting
"""

# Critical: Dependency check before any heavy imports
from src.dependency_manager import check_and_install_dependencies
check_and_install_dependencies()

# Standard library imports (lightweight)
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from typing import Optional, Dict, Any
import atexit
import gc
import os
import psutil
import resource
import signal
import sys
import time
import traceback
import warnings

# Suppress non-critical warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

class SystemResourceMonitor:
    """Enterprise-grade system resource monitoring with adaptive thresholds"""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = time.time()
        self.peak_memory = 0
        self.memory_samples = []
        
    def get_system_limits(self) -> Dict[str, int]:
        """Calculate optimal system limits based on available resources"""
        vm = psutil.virtual_memory()
        cpu_count = psutil.cpu_count(logical=False)
        if cpu_count is None:
            cpu_count = 1
        # Conservative memory allocation (max 60% of available)
        max_memory_mb = int(vm.available * 0.6 / (1024 * 1024)) if vm.available is not None else 0
        
        # Adaptive worker count based on CPU and memory
        optimal_workers = min(cpu_count, max(1, max_memory_mb // 512))
        
        return {
            'max_memory_mb': max_memory_mb,
            'optimal_workers': optimal_workers,
            'reserved_mb': max(512, int(vm.total * 0.1 / (1024 * 1024))),
            'chunk_size': max(5, min(50, max_memory_mb // 100))
        }
    
    def check_memory_pressure(self) -> bool:
        """Advanced memory pressure detection"""
        vm = psutil.virtual_memory()
        current_memory = self.process.memory_info().rss / (1024 * 1024)
        
        # Update peak memory tracking
        self.peak_memory = max(self.peak_memory, current_memory)
        self.memory_samples.append(current_memory)
        
        # Keep only last 10 samples for trend analysis
        if len(self.memory_samples) > 10:
            self.memory_samples.pop(0)
        
        # Memory pressure indicators
        system_pressure = vm.percent > 85
        process_growth = len(self.memory_samples) > 5 and \
                        (self.memory_samples[-1] - self.memory_samples[0]) > 200
        swap_pressure = psutil.swap_memory().percent > 30
        
        return system_pressure or process_growth or swap_pressure


@contextmanager
def circuit_breaker(operation_name: str, logger, max_failures: int = 3):
    """Circuit breaker pattern for fault tolerance"""
    failures = getattr(circuit_breaker, f'{operation_name}_failures', 0)
    
    if failures >= max_failures:
        logger.log_error('CIRCUIT_BREAKER', operation_name, f'Circuit open after {failures} failures')
        raise RuntimeError(f'Circuit breaker open for {operation_name}')
    
    try:
        yield
        # Reset failures on success
        setattr(circuit_breaker, f'{operation_name}_failures', 0)
    except Exception as e:
        failures += 1
        setattr(circuit_breaker, f'{operation_name}_failures', failures)
        logger.log_error('CIRCUIT_BREAKER', operation_name, f'Failure {failures}/{max_failures}: {str(e)}')
        raise


def memory_efficient_decorator(func):
    """Decorator for memory-efficient function execution"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Force garbage collection before execution
        gc.collect()
        
        # Monitor memory before
        process = psutil.Process()
        mem_before = process.memory_info().rss / (1024 * 1024)
        
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            # Aggressive cleanup after execution
            gc.collect()
            mem_after = process.memory_info().rss / (1024 * 1024)
            print(f"[MEMORY] {func.__name__}: {mem_before:.1f}MB -> {mem_after:.1f}MB")
    
    return wrapper


def setup_signal_handlers(logger):
    """Setup graceful shutdown handlers"""
    def signal_handler(signum, frame):
        logger.log_error('SIGNAL', 'shutdown', f'Received signal {signum}, initiating graceful shutdown')
        # Cleanup temporary files
        from src.cleanup_utils import cleanup_temp_files
        cleanup_temp_files()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def setup_memory_limits(monitor: SystemResourceMonitor, logger) -> int:
    """Setup adaptive memory limits based on system resources"""
    limits = monitor.get_system_limits()
    
    # Set process memory limit (with safety margin)
    memory_limit_bytes = limits['max_memory_mb'] * 1024 * 1024
    
    try:
        resource.setrlimit(resource.RLIMIT_AS, (memory_limit_bytes, memory_limit_bytes))
        logger.log_error('RESOURCE', 'setup', f'Set memory limit: {limits["max_memory_mb"]}MB')
    except (OSError, ValueError) as e:
        logger.log_error('WARNING', 'setup', f'Could not set memory limit: {e}')
        # Fallback to conservative limit
        memory_limit_bytes = 4 * 1024 * 1024 * 1024  # 4GB fallback
    
    return memory_limit_bytes


@memory_efficient_decorator
def execute_pipeline_phase(phase_name: str, phase_func, *args, **kwargs):
    """Execute a pipeline phase with monitoring and error handling"""
    start_time = time.time()
    
    try:
        result = phase_func(*args, **kwargs)
        duration = time.time() - start_time
        print(f"[PERFORMANCE] {phase_name} completed in {duration:.2f}s")
        return result
    except Exception as e:
        duration = time.time() - start_time
        print(f"[ERROR] {phase_name} failed after {duration:.2f}s: {e}")
        raise


def main():
    """Main orchestrator with enterprise-grade resource management"""
    # Initialize monitoring and cleanup
    monitor = SystemResourceMonitor()
    
    # Lazy import heavy dependencies only when needed
    from src.cleanup_utils import cleanup_temp_files
    from src.config_loader import load_config
    from src.logger import Logger
    
    # Early cleanup
    cleanup_temp_files()
    
    # Initialize logger with cleanup
    logger = Logger(cleanup_old_log=True)
    
    # Setup signal handlers for graceful shutdown
    setup_signal_handlers(logger)
    
    # Register cleanup function for abnormal exits
    atexit.register(cleanup_temp_files)
    
    # Setup adaptive memory limits
    memory_limit = setup_memory_limits(monitor, logger)
    
    try:
        # Load configuration
        config_path = os.environ.get("LIBRARIAN_CONFIG", 
                                   Path(__file__).parent / "../conf/config.ini")
        logger.log_error("INFO", "librarian", f"Loading config from: {config_path}")
        config = load_config()
        
        # Get system limits for optimal configuration
        limits = monitor.get_system_limits()
        logger.log_error('RESOURCE', 'limits', 
                        f'Optimal config: {limits["optimal_workers"]} workers, '
                        f'{limits["chunk_size"]} chunk size, {limits["max_memory_mb"]}MB limit')
        
        # Lazy import and initialize heavy components
        from src.resource_manager import ResourceManager
        from src.classifier import Classifier
        from src.pdf_manager import PDFManager
        from src.library_builder import LibraryBuilder
        
        # Initialize resource manager with adaptive settings
        resource_mgr = ResourceManager(
            min_free_mb=limits['reserved_mb'],
            min_workers=1,
            max_workers=limits['optimal_workers'],
            ram_per_worker_mb=max(256, limits['max_memory_mb'] // limits['optimal_workers']),
            os_reserved_mb=limits['reserved_mb'],
            max_ram_usage_ratio=0.6  # Conservative 60% usage
        )
        
        # Wait for sufficient resources
        resource_mgr.wait_for_free_ram()
        resource_mgr.print_resource_usage('Startup')
        
        # Create library root with proper permissions
        library_root = Path(config['library_root'])
        library_root.mkdir(parents=True, exist_ok=True, mode=0o755)
        
        # Initialize components with lazy loading
        classifier = Classifier(config.get('knowledge_base_db_url') or "knowledge.sqlite")
        pdf_manager = PDFManager(logger.log_error)
        
        # Memory pressure callback
        def memory_pressure_callback():
            if monitor.check_memory_pressure():
                logger.log_error('MEMORY', 'pressure', 'Memory pressure detected, forcing cleanup')
                resource_mgr.force_cleanup()
                gc.collect()
                time.sleep(1)  # Brief pause for system recovery
        
        builder = LibraryBuilder(config, resource_mgr, classifier, pdf_manager, 
                               logger, memory_pressure_callback)
        
        # Execute pipeline phases with circuit breaker protection
        phases = [
            ('scan_files', builder.scan_files),
            ('validate_pdfs', builder.validate_and_repair_pdfs),
            ('classify_analyze', builder.classify_and_analyze),
            ('deduplicate', builder.deduplicate_files),
        ]
        
        unique_files = None
        
        for phase_name, phase_func in phases:
            with circuit_breaker(phase_name, logger):
                logger.log_error("PHASE", "librarian", f'[PHASE] Starting {phase_name}...')
                
                if phase_name == 'deduplicate':
                    unique_files = execute_pipeline_phase(phase_name, phase_func)
                else:
                    execute_pipeline_phase(phase_name, phase_func)
                
                # Aggressive cleanup after each phase
                resource_mgr.aggressive_cleanup_after_phase(phase_name, logger)
                
                # Check for memory pressure
                if monitor.check_memory_pressure():
                    logger.log_error('WARNING', phase_name, 'Memory pressure detected after phase')
                    time.sleep(2)  # Allow system recovery
        
        # Final phase: copy and index
        with circuit_breaker('copy_index', logger):
            logger.log_error("PHASE", "librarian", '[PHASE] Starting copy_and_index...')
            execute_pipeline_phase('copy_index', builder.copy_and_index, unique_files)
            resource_mgr.aggressive_cleanup_after_phase('copy_index', logger)
        
        # Performance summary
        total_time = time.time() - monitor.start_time
        logger.log_error("PERFORMANCE", "librarian", 
                        f'Total execution time: {total_time:.2f}s, '
                        f'Peak memory: {monitor.peak_memory:.1f}MB')
        
        logger.print_summary()
        logger.log_error("SUCCESS", "librarian", '[SUCCESS] Workflow completed successfully')
        
    except KeyboardInterrupt:
        logger.log_error("INTERRUPT", "librarian", 'Process interrupted by user')
        sys.exit(130)  # Standard exit code for SIGINT
    except MemoryError as e:
        logger.log_error("OOM", "librarian", f'Out of memory error: {e}')
        logger.log_error("OOM", "librarian", 'Consider reducing batch sizes or increasing available RAM')
        sys.exit(137)  # Standard exit code for SIGKILL (OOM)
    except Exception as e:
        logger.log_error("ERROR", "librarian", f'Uncaught exception: {e}', 
                        extra=traceback.format_exc())
        logger.log_error("ERROR", "librarian", 'See librarian_run.log for detailed error information')
        sys.exit(1)
    finally:
        # Final cleanup
        cleanup_temp_files()
        logger.log_error("CLEANUP", "librarian", 'Final cleanup completed')

if __name__ == "__main__":
    main()
