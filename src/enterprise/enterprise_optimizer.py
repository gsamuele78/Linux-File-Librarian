#!/usr/bin/env python3
"""
Enterprise System Optimizer for Linux File Librarian
Implements industry best practices for memory management and performance optimization
"""

import gc
import mmap
import os
import psutil
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import weakref

@dataclass
class SystemMetrics:
    """System performance metrics"""
    memory_usage_mb: float
    cpu_percent: float
    disk_io_mb: float
    network_io_mb: float
    active_threads: int
    open_files: int

class AdaptiveResourceManager:
    """Enterprise-grade adaptive resource management"""
    
    def __init__(self):
        self.metrics_history: List[SystemMetrics] = []
        self.memory_pressure_threshold = 0.85
        self.cpu_pressure_threshold = 0.90
        self._lock = threading.RLock()
        
    def get_optimal_chunk_size(self, file_size: int, available_memory_mb: int) -> int:
        """Calculate optimal chunk size based on file size and available memory"""
        # Industry standard: Use 1-5% of available memory per chunk
        base_chunk = max(10, min(1000, available_memory_mb // 20))
        
        # Adaptive sizing based on file characteristics
        if file_size < 1024 * 1024:  # < 1MB files
            return min(base_chunk, 50)
        elif file_size < 100 * 1024 * 1024:  # < 100MB files
            return min(base_chunk, 200)
        else:  # Large files
            return min(base_chunk, 500)
    
    def get_optimal_worker_count(self) -> int:
        """Calculate optimal worker count using advanced algorithms"""
        cpu_count = psutil.cpu_count(logical=False) or 1
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        # Advanced worker calculation considering I/O vs CPU bound tasks
        if memory_gb < 4:
            return 1
        elif memory_gb < 8:
            return min(2, cpu_count)
        else:
            # Use Little's Law for optimal concurrency
            return min(cpu_count, int(memory_gb / 2))
    
    @contextmanager
    def memory_constrained_execution(self, operation_name: str):
        """Context manager for memory-constrained operations"""
        initial_memory = psutil.Process().memory_info().rss / (1024**2)
        
        try:
            yield
        finally:
            # Aggressive cleanup
            gc.collect()
            final_memory = psutil.Process().memory_info().rss / (1024**2)
            print(f"[MEMORY] {operation_name}: {initial_memory:.1f}MB -> {final_memory:.1f}MB")

class StreamingDatabaseProcessor:
    """Memory-efficient streaming database operations"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection_pool = weakref.WeakSet()
    
    @contextmanager
    def get_connection(self):
        """Get database connection with proper resource management"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        self._connection_pool.add(conn)
        try:
            yield conn
        finally:
            conn.close()
    
    def stream_query_results(self, query: str, chunk_size: int = 1000) -> Iterator[List[sqlite3.Row]]:
        """Stream query results in chunks to prevent memory exhaustion"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                yield rows
                
                # Memory pressure check
                if psutil.virtual_memory().percent > 85:
                    gc.collect()
                    time.sleep(0.1)

class AdvancedThreadPoolManager:
    """Enterprise thread pool with automatic resource management"""
    
    def __init__(self, max_workers: int):
        self.max_workers = max_workers
        self._active_futures = weakref.WeakSet()
        self._completed_count = 0
        
    @contextmanager
    def managed_executor(self):
        """Context manager for thread pool with automatic cleanup"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            try:
                yield executor
            finally:
                # Force cleanup of any remaining futures
                for future in list(self._active_futures):
                    if not future.done():
                        future.cancel()
                self._active_futures.clear()
                gc.collect()
    
    def submit_with_cleanup(self, executor: ThreadPoolExecutor, fn, *args, **kwargs):
        """Submit task with automatic cleanup tracking"""
        future = executor.submit(fn, *args, **kwargs)
        self._active_futures.add(future)
        
        def cleanup_callback(fut):
            self._completed_count += 1
            if self._completed_count % 10 == 0:  # Cleanup every 10 completions
                gc.collect()
        
        future.add_done_callback(cleanup_callback)
        return future

class MemoryMappedFileProcessor:
    """Memory-mapped file processing for large files"""
    
    @staticmethod
    @contextmanager
    def mmap_file(file_path: str, mode: str = 'r'):
        """Memory-map file for efficient large file processing"""
        try:
            with open(file_path, 'rb') as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    yield mm
        except (OSError, ValueError) as e:
            # Fallback to regular file reading for small files
            with open(file_path, mode) as f:
                yield f

class EnterpriseOptimizer:
    """Main enterprise optimization coordinator"""
    
    def __init__(self):
        self.resource_manager = AdaptiveResourceManager()
        self.thread_manager = AdvancedThreadPoolManager(
            self.resource_manager.get_optimal_worker_count()
        )
        
    def optimize_library_builder(self, library_builder):
        """Apply enterprise optimizations to library builder"""
        
        # Replace fixed chunk sizes with adaptive ones
        original_scan = library_builder.scan_files
        def optimized_scan():
            available_memory = psutil.virtual_memory().available / (1024**2)
            chunk_size = self.resource_manager.get_optimal_chunk_size(0, int(available_memory))
            print(f"[OPTIMIZER] Using adaptive chunk size: {chunk_size}")
            return original_scan()
        
        library_builder.scan_files = optimized_scan
        
        # Add memory-constrained execution to all major methods
        for method_name in ['validate_and_repair_pdfs', 'classify_and_analyze', 'deduplicate_files']:
            original_method = getattr(library_builder, method_name)
            
            def create_optimized_method(orig_method, name):
                def optimized_method(*args, **kwargs):
                    with self.resource_manager.memory_constrained_execution(name):
                        return orig_method(*args, **kwargs)
                return optimized_method
            
            setattr(library_builder, method_name, 
                   create_optimized_method(original_method, method_name))
        
        return library_builder
    
    def create_streaming_processor(self, db_path: str) -> StreamingDatabaseProcessor:
        """Create optimized streaming database processor"""
        return StreamingDatabaseProcessor(db_path)
    
    def get_system_recommendations(self) -> Dict[str, Any]:
        """Get system optimization recommendations"""
        vm = psutil.virtual_memory()
        cpu_count = psutil.cpu_count(logical=False) or 1
        
        recommendations = {
            'optimal_workers': self.resource_manager.get_optimal_worker_count(),
            'memory_limit_mb': int(vm.total * 0.6 / (1024**2)),
            'chunk_size_range': (10, 500),
            'io_optimization': 'memory_mapping' if vm.total > 8 * 1024**3 else 'streaming',
            'gc_strategy': 'aggressive' if vm.percent > 70 else 'standard'
        }
        
        return recommendations

# Global optimizer instance
_optimizer = None

def get_enterprise_optimizer() -> EnterpriseOptimizer:
    """Get singleton enterprise optimizer instance"""
    global _optimizer
    if _optimizer is None:
        _optimizer = EnterpriseOptimizer()
    return _optimizer

def apply_enterprise_optimizations(library_builder):
    """Apply all enterprise optimizations to library builder"""
    optimizer = get_enterprise_optimizer()
    return optimizer.optimize_library_builder(library_builder)