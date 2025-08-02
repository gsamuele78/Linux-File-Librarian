#!/usr/bin/env python3
"""
System Optimization Module

Implements advanced optimization techniques based on industry best practices:
- Memory-mapped file processing
- CPU cache optimization
- I/O optimization with async patterns
- Garbage collection tuning
- System resource monitoring
- Predictive scaling
"""

import asyncio
import gc
import mmap
import os
import psutil
import resource
import threading
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
import logging

logger = logging.getLogger(__name__)


@dataclass
class SystemMetrics:
    """Comprehensive system metrics"""
    cpu_percent: float
    memory_percent: float
    memory_available_mb: float
    disk_io_read_mb: float
    disk_io_write_mb: float
    network_io_mb: float
    load_average: Tuple[float, float, float]
    process_count: int
    thread_count: int
    file_descriptors: int
    timestamp: float


class SystemOptimizer:
    """Advanced system optimization"""
    
    def __init__(self):
        self.metrics_history = []
        self.optimization_settings = self._calculate_optimal_settings()
        self._setup_gc_optimization()
    
    def _calculate_optimal_settings(self) -> Dict:
        """Calculate optimal system settings"""
        cpu_count = psutil.cpu_count(logical=False) or 1
        memory = psutil.virtual_memory()
        
        # Calculate optimal thread pools
        io_threads = min(32, max(4, cpu_count * 2))  # I/O bound operations
        cpu_threads = max(1, cpu_count - 1)  # CPU bound operations
        
        # Memory settings
        max_memory_per_process = int(memory.total * 0.4)  # 40% of total memory
        chunk_size = min(64 * 1024 * 1024, max_memory_per_process // 10)  # 64MB or 10% of process limit
        
        return {
            'io_thread_pool_size': io_threads,
            'cpu_thread_pool_size': cpu_threads,
            'max_memory_per_process': max_memory_per_process,
            'optimal_chunk_size': chunk_size,
            'gc_threshold': (700, 10, 10),  # Tuned GC thresholds
            'max_file_descriptors': min(65536, resource.getrlimit(resource.RLIMIT_NOFILE)[1])
        }
    
    def _setup_gc_optimization(self):
        """Setup optimized garbage collection"""
        settings = self.optimization_settings
        gc.set_threshold(*settings['gc_threshold'])
        
        # Disable automatic GC for critical sections
        self.gc_disabled = False
    
    @contextmanager
    def disable_gc(self):
        """Temporarily disable garbage collection for performance"""
        if not self.gc_disabled:
            gc.disable()
            self.gc_disabled = True
        try:
            yield
        finally:
            if self.gc_disabled:
                gc.enable()
                self.gc_disabled = False
                gc.collect()  # Force collection after re-enabling
    
    def get_current_metrics(self) -> SystemMetrics:
        """Get current system metrics"""
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk_io = psutil.disk_io_counters()
        net_io = psutil.net_io_counters()
        
        return SystemMetrics(
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            memory_available_mb=memory.available / (1024 * 1024),
            disk_io_read_mb=(disk_io.read_bytes if disk_io else 0) / (1024 * 1024),
            disk_io_write_mb=(disk_io.write_bytes if disk_io else 0) / (1024 * 1024),
            network_io_mb=((net_io.bytes_sent + net_io.bytes_recv) if net_io else 0) / (1024 * 1024),
            load_average=os.getloadavg(),
            process_count=len(psutil.pids()),
            thread_count=threading.active_count(),
            file_descriptors=len(psutil.Process().open_files()),
            timestamp=time.time()
        )
    
    def should_throttle(self) -> bool:
        """Determine if processing should be throttled"""
        metrics = self.get_current_metrics()
        
        # Throttle conditions
        high_cpu = metrics.cpu_percent > 90
        high_memory = metrics.memory_percent > 85
        high_load = metrics.load_average[0] > psutil.cpu_count() * 1.5
        low_memory = metrics.memory_available_mb < 500
        
        return high_cpu or high_memory or high_load or low_memory
    
    def get_optimal_batch_size(self, base_size: int = 100) -> int:
        """Calculate optimal batch size based on current system state"""
        metrics = self.get_current_metrics()
        
        # Adjust based on available memory
        memory_factor = min(2.0, metrics.memory_available_mb / 1000)  # Scale with available GB
        cpu_factor = max(0.5, (100 - metrics.cpu_percent) / 100)  # Scale with CPU availability
        
        optimal_size = int(base_size * memory_factor * cpu_factor)
        return max(10, min(1000, optimal_size))  # Clamp between 10 and 1000


class MemoryMappedProcessor:
    """Memory-mapped file processing for large files"""
    
    def __init__(self, optimizer: SystemOptimizer):
        self.optimizer = optimizer
    
    @contextmanager
    def mmap_file(self, file_path: Path, mode: str = 'r'):
        """Memory map a file for efficient processing"""
        try:
            with open(file_path, 'rb' if 'b' in mode else 'r') as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    yield mm
        except Exception as e:
            logger.error(f"Memory mapping failed for {file_path}: {e}")
            # Fallback to regular file reading
            with open(file_path, mode) as f:
                yield f
    
    def process_large_file(self, file_path: Path, processor_func) -> any:
        """Process large file using memory mapping"""
        file_size = file_path.stat().st_size
        
        # Use memory mapping for files larger than 10MB
        if file_size > 10 * 1024 * 1024:
            with self.mmap_file(file_path) as mm:
                return processor_func(mm)
        else:
            # Regular processing for smaller files
            with open(file_path, 'rb') as f:
                return processor_func(f)


class AsyncIOOptimizer:
    """Asynchronous I/O optimization"""
    
    def __init__(self, optimizer: SystemOptimizer):
        self.optimizer = optimizer
        self.semaphore = asyncio.Semaphore(optimizer.optimization_settings['io_thread_pool_size'])
    
    async def process_files_async(self, file_paths: List[Path], processor_func) -> List:
        """Process files asynchronously with optimal concurrency"""
        results = []
        
        async def process_single_file(file_path: Path):
            async with self.semaphore:
                # Check if we should throttle
                if self.optimizer.should_throttle():
                    await asyncio.sleep(0.1)  # Brief pause
                
                try:
                    # Run CPU-bound processing in thread pool
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(None, processor_func, file_path)
                    return result
                except Exception as e:
                    logger.error(f"Async processing failed for {file_path}: {e}")
                    return None
        
        # Process files in batches to prevent overwhelming the system
        batch_size = self.optimizer.get_optimal_batch_size(50)
        
        for i in range(0, len(file_paths), batch_size):
            batch = file_paths[i:i + batch_size]
            
            # Process batch concurrently
            tasks = [process_single_file(fp) for fp in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out exceptions and None results
            valid_results = [r for r in batch_results if r is not None and not isinstance(r, Exception)]
            results.extend(valid_results)
            
            # Brief pause between batches for system stability
            await asyncio.sleep(0.01)
        
        return results


class CacheOptimizer:
    """CPU cache optimization techniques"""
    
    def __init__(self):
        self.cache = {}
        self.cache_stats = {'hits': 0, 'misses': 0}
        self.max_cache_size = 10000
    
    def get_cached(self, key: str, compute_func, *args, **kwargs):
        """Get value from cache or compute and cache"""
        if key in self.cache:
            self.cache_stats['hits'] += 1
            return self.cache[key]
        
        # Cache miss - compute value
        self.cache_stats['misses'] += 1
        value = compute_func(*args, **kwargs)
        
        # Add to cache with size limit
        if len(self.cache) >= self.max_cache_size:
            # Remove oldest entry (simple FIFO)
            oldest_key = next(iter(self.cache))
            del self.cache[oldest_key]
        
        self.cache[key] = value
        return value
    
    def clear_cache(self):
        """Clear cache and reset stats"""
        self.cache.clear()
        self.cache_stats = {'hits': 0, 'misses': 0}
    
    def get_cache_efficiency(self) -> float:
        """Get cache hit ratio"""
        total = self.cache_stats['hits'] + self.cache_stats['misses']
        return self.cache_stats['hits'] / max(total, 1)


class PredictiveScaler:
    """Predictive scaling based on workload patterns"""
    
    def __init__(self, optimizer: SystemOptimizer):
        self.optimizer = optimizer
        self.workload_history = []
        self.scaling_decisions = []
    
    def record_workload(self, files_processed: int, duration: float, memory_used: float):
        """Record workload metrics for prediction"""
        workload = {
            'files_processed': files_processed,
            'duration': duration,
            'memory_used': memory_used,
            'throughput': files_processed / max(duration, 0.001),
            'timestamp': time.time()
        }
        
        self.workload_history.append(workload)
        
        # Keep only recent history (last 100 entries)
        if len(self.workload_history) > 100:
            self.workload_history.pop(0)
    
    def predict_optimal_workers(self, estimated_files: int) -> int:
        """Predict optimal number of workers based on history"""
        if not self.workload_history:
            return self.optimizer.optimization_settings['io_thread_pool_size']
        
        # Simple prediction based on recent performance
        recent_workloads = self.workload_history[-10:]  # Last 10 workloads
        avg_throughput = sum(w['throughput'] for w in recent_workloads) / len(recent_workloads)
        
        # Estimate processing time with current settings
        current_workers = self.optimizer.optimization_settings['io_thread_pool_size']
        estimated_time = estimated_files / (avg_throughput * current_workers)
        
        # Adjust workers based on estimated time and system capacity
        if estimated_time > 300:  # More than 5 minutes
            # Increase workers if system can handle it
            max_workers = min(32, psutil.cpu_count() * 2)
            return min(max_workers, current_workers * 2)
        elif estimated_time < 60:  # Less than 1 minute
            # Reduce workers to save resources
            return max(2, current_workers // 2)
        
        return current_workers


class OptimizedFileProcessor:
    """Optimized file processor combining all optimization techniques"""
    
    def __init__(self):
        self.system_optimizer = SystemOptimizer()
        self.mmap_processor = MemoryMappedProcessor(self.system_optimizer)
        self.async_optimizer = AsyncIOOptimizer(self.system_optimizer)
        self.cache_optimizer = CacheOptimizer()
        self.predictive_scaler = PredictiveScaler(self.system_optimizer)
    
    async def process_files_optimized(self, file_paths: List[Path], processor_func) -> List:
        """Process files with all optimizations applied"""
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / (1024 * 1024)
        
        logger.info(f"Starting optimized processing of {len(file_paths)} files")
        
        try:
            # Predict optimal workers
            optimal_workers = self.predictive_scaler.predict_optimal_workers(len(file_paths))
            logger.info(f"Using {optimal_workers} workers based on prediction")
            
            # Update semaphore for optimal concurrency
            self.async_optimizer.semaphore = asyncio.Semaphore(optimal_workers)
            
            # Process files with optimizations
            with self.system_optimizer.disable_gc():  # Disable GC during processing
                results = await self.async_optimizer.process_files_async(file_paths, processor_func)
            
            # Record workload for future predictions
            duration = time.time() - start_time
            end_memory = psutil.Process().memory_info().rss / (1024 * 1024)
            memory_used = end_memory - start_memory
            
            self.predictive_scaler.record_workload(len(results), duration, memory_used)
            
            # Log performance metrics
            cache_efficiency = self.cache_optimizer.get_cache_efficiency()
            logger.info(f"Processing completed: {len(results)} files in {duration:.2f}s")
            logger.info(f"Cache efficiency: {cache_efficiency:.2%}")
            logger.info(f"Memory used: {memory_used:.1f}MB")
            
            return results
            
        except Exception as e:
            logger.error(f"Optimized processing failed: {e}")
            raise
    
    def get_optimization_report(self) -> Dict:
        """Get comprehensive optimization report"""
        metrics = self.system_optimizer.get_current_metrics()
        settings = self.system_optimizer.optimization_settings
        
        return {
            'system_metrics': {
                'cpu_percent': metrics.cpu_percent,
                'memory_percent': metrics.memory_percent,
                'memory_available_mb': metrics.memory_available_mb,
                'load_average': metrics.load_average
            },
            'optimization_settings': settings,
            'cache_stats': {
                'efficiency': self.cache_optimizer.get_cache_efficiency(),
                'hits': self.cache_optimizer.cache_stats['hits'],
                'misses': self.cache_optimizer.cache_stats['misses']
            },
            'workload_history_count': len(self.predictive_scaler.workload_history),
            'recommendations': self._generate_optimization_recommendations(metrics, settings)
        }
    
    def _generate_optimization_recommendations(self, metrics: SystemMetrics, settings: Dict) -> List[str]:
        """Generate optimization recommendations"""
        recommendations = []
        
        if metrics.cpu_percent > 80:
            recommendations.append("High CPU usage - consider reducing concurrent workers")
        
        if metrics.memory_percent > 80:
            recommendations.append("High memory usage - consider reducing batch sizes")
        
        if metrics.load_average[0] > psutil.cpu_count() * 1.5:
            recommendations.append("High system load - consider throttling processing")
        
        cache_efficiency = self.cache_optimizer.get_cache_efficiency()
        if cache_efficiency < 0.5:
            recommendations.append("Low cache efficiency - consider increasing cache size")
        
        if metrics.file_descriptors > settings['max_file_descriptors'] * 0.8:
            recommendations.append("High file descriptor usage - consider reducing concurrent file operations")
        
        return recommendations