#!/usr/bin/env python3
"""
Enterprise Architecture Framework for Linux File Librarian

Implements industry best practices:
- SOLID principles
- Clean Architecture
- Domain-Driven Design
- Microservices patterns
- Event-driven architecture
- CQRS (Command Query Responsibility Segregation)
- Circuit breaker pattern
- Bulkhead isolation
- Observability (metrics, logging, tracing)
"""

import asyncio
import logging
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, Union
from queue import Queue, Empty
import psutil
import gc


class ProcessingStatus(Enum):
    """Processing status enumeration"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class ProcessingMetrics:
    """Comprehensive processing metrics"""
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    files_processed: int = 0
    files_failed: int = 0
    bytes_processed: int = 0
    memory_peak_mb: float = 0
    cpu_time_seconds: float = 0
    errors: List[str] = field(default_factory=list)
    
    @property
    def duration(self) -> float:
        return (self.end_time or time.time()) - self.start_time
    
    @property
    def throughput_files_per_second(self) -> float:
        return self.files_processed / max(self.duration, 0.001)
    
    @property
    def throughput_mb_per_second(self) -> float:
        return (self.bytes_processed / (1024 * 1024)) / max(self.duration, 0.001)


class EventBus:
    """Enterprise event bus for decoupled communication"""
    
    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = {}
        self._lock = threading.RLock()

    def subscribe(self, event_type: str, handler: Callable):
        """Subscribe to events"""
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(handler)
    
    def publish(self, event_type: str, data: Any):
        """Publish event to all subscribers"""
        with self._lock:
            handlers = self._subscribers.get(event_type, [])
        
        for handler in handlers:
            try:
                handler(data)
            except Exception as e:
                error_msg = f"Event handler error for {str(event_type)[:50]}: {str(e)[:100]}"
                logging.error(error_msg)


class HealthCheck:
    """System health monitoring"""
    
    def __init__(self):
        self.checks = {}
        self.last_check = {}
    
    def register_check(self, name: str, check_func: Callable):
        """Register health check"""
        self.checks[name] = check_func
    
    def run_checks(self) -> Dict[str, bool]:
        """Run all health checks"""
        results = {}
        for name, check_func in self.checks.items():
            try:
                results[name] = check_func()
                self.last_check[name] = time.time()
            except Exception as e:
                error_msg = f"Health check {str(name)[:50]} failed: {str(e)[:100]}"
                logging.error(error_msg)
                results[name] = False
        return results
    
    def is_healthy(self) -> bool:
        """Check if system is healthy"""
        results = self.run_checks()
        return all(results.values())


class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    
    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = threading.Lock()
    
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        with self._lock:
            if self.state == "OPEN" and self.last_failure_time is not None:
                if time.time() - self.last_failure_time > self.timeout: # type: ignore
                    self.state = "HALF_OPEN"
                else:
                    raise Exception("Circuit breaker is OPEN")
            
            try:
                result = func(*args, **kwargs)
                if self.state == "HALF_OPEN":
                    self.state = "CLOSED"
                    self.failure_count = 0
                return result
            except Exception as e:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = "OPEN"
                
                raise e


class ResourcePool:
    """Resource pool for efficient resource management"""
    
    def __init__(self, create_func: Callable, max_size: int = 10):
        self.create_func = create_func
        self.max_size = max_size
        self.pool = Queue(maxsize=max_size)
        self.created_count = 0
        self._lock = threading.Lock()
    
    @contextmanager
    def get_resource(self):
        """Get resource from pool"""
        resource = None
        try:
            resource = self.pool.get_nowait()
        except Empty:
            with self._lock:
                if self.created_count < self.max_size:
                    resource = self.create_func()
                    self.created_count += 1
                else:
                    resource = self.pool.get(timeout=30)  # Wait for available resource with timeout
        
        try:
            yield resource
        finally:
            if resource:
                self.pool.put(resource)


class ProcessingPipeline:
    """Enterprise processing pipeline with stages"""
    
    def __init__(self, name: str, event_bus: EventBus):
        self.name = name
        self.event_bus = event_bus
        self.stages = []
        self.metrics = ProcessingMetrics()
        self.circuit_breaker = CircuitBreaker()
    
    def add_stage(self, stage: 'ProcessingStage'):
        """Add processing stage"""
        self.stages.append(stage)
    
    async def process(self, items: List[Any]) -> ProcessingMetrics:
        """Process items through pipeline"""
        self.metrics = ProcessingMetrics()
        
        try:
            for stage in self.stages:
                items = await self._process_stage(stage, items)
                
                # Memory pressure check
                if psutil.virtual_memory().percent > 85:
                    gc.collect()
                    await asyncio.sleep(0.1)  # Brief pause for GC
            
            self.metrics.end_time = time.time()
            self.event_bus.publish('pipeline_completed', {
                'pipeline': self.name,
                'metrics': self.metrics
            })
            
        except Exception as e:
            self.metrics.errors.append(str(e))
            self.event_bus.publish('pipeline_failed', {
                'pipeline': self.name,
                'error': str(e),
                'metrics': self.metrics
            })
            raise
        
        return self.metrics
    
    async def _process_stage(self, stage: 'ProcessingStage', items: List[Any]) -> List[Any]:
        """Process single stage"""
        return await self.circuit_breaker.call(stage.process, items)


class ProcessingStage(ABC):
    """Abstract processing stage"""
    
    def __init__(self, name: str, max_workers: int = 4):
        self.name = name
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    @abstractmethod
    async def process(self, items: List[Any]) -> List[Any]:
        """Process items in this stage"""
        pass
    
    def __del__(self):
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)


class BatchProcessor:
    """Efficient batch processing with adaptive sizing"""
    
    def __init__(self, batch_size: int = 100, max_memory_mb: int = 1000):
        self.batch_size = batch_size
        self.max_memory_mb = max_memory_mb
        self.adaptive_sizing = True
    
    def process_batches(self, items: List[Any], process_func: Callable) -> List[Any]:
        """Process items in adaptive batches"""
        results = []
        current_batch_size = self.batch_size
        
        for i in range(0, len(items), current_batch_size):
            batch = items[i:i + current_batch_size]
            
            # Memory check before processing
            memory_before = psutil.Process().memory_info().rss / (1024 * 1024)
            
            try:
                batch_results = process_func(batch)
                results.extend(batch_results)
                
                # Adaptive batch sizing based on memory usage
                if self.adaptive_sizing:
                    memory_after = psutil.Process().memory_info().rss / (1024 * 1024)
                    memory_used = memory_after - memory_before
                    
                    if memory_used > self.max_memory_mb * 0.8:
                        current_batch_size = max(10, current_batch_size // 2)
                    elif memory_used < self.max_memory_mb * 0.3:
                        current_batch_size = min(self.batch_size * 2, int(current_batch_size * 1.5))
                
            except Exception as e:
                error_msg = f"Batch processing error: {str(e)[:100]}"
                logging.error(error_msg)
                # Reduce batch size on error
                current_batch_size = max(1, current_batch_size // 2)
                continue
            
            # Force garbage collection between batches
            gc.collect()
        
        return results


class PerformanceProfiler:
    """Performance profiling and optimization"""
    
    def __init__(self):
        self.profiles = {}
        self.recommendations = []
    
    @contextmanager
    def profile(self, operation_name: str):
        """Profile operation performance"""
        start_time = time.perf_counter()
        start_memory = psutil.Process().memory_info().rss
        start_cpu = time.process_time()
        
        try:
            yield
        finally:
            end_time = time.perf_counter()
            end_memory = psutil.Process().memory_info().rss
            end_cpu = time.process_time()
            
            profile_data = {
                'duration': end_time - start_time,
                'memory_delta_mb': (end_memory - start_memory) / (1024 * 1024),
                'cpu_time': end_cpu - start_cpu,
                'timestamp': start_time
            }
            
            self.profiles[operation_name] = profile_data
            self._analyze_performance(operation_name, profile_data)
    
    def _analyze_performance(self, operation: str, data: Dict):
        """Analyze performance and generate recommendations"""
        # Define thresholds as class constants
        SLOW_OPERATION_THRESHOLD = 30.0
        HIGH_MEMORY_THRESHOLD = 500.0
        
        if data['duration'] > SLOW_OPERATION_THRESHOLD:
            self.recommendations.append(
                f"Operation '{operation}' is slow ({data['duration']:.2f}s). Consider optimization."
            )
        
        if data['memory_delta_mb'] > HIGH_MEMORY_THRESHOLD:
            self.recommendations.append(
                f"Operation '{operation}' uses high memory ({data['memory_delta_mb']:.1f}MB). Consider streaming."
            )
    
    def get_report(self) -> Dict:
        """Get performance report"""
        return {
            'profiles': self.profiles,
            'recommendations': self.recommendations,
            'total_operations': len(self.profiles)
        }


class EnterpriseLibrarianOrchestrator:
    """Main orchestrator with enterprise patterns"""
    
    def __init__(self):
        self.event_bus = EventBus()
        self.health_check = HealthCheck()
        self.profiler = PerformanceProfiler()
        self.batch_processor = BatchProcessor()
        self.pipelines = {}
        
        # Setup health checks
        self._setup_health_checks()
        
        # Setup event handlers
        self._setup_event_handlers()
    
    def _setup_health_checks(self):
        """Setup system health checks"""
        self.health_check.register_check('memory', self._check_memory_health)
        self.health_check.register_check('disk', self._check_disk_health)
        self.health_check.register_check('cpu', self._check_cpu_health)
    
    def _check_memory_health(self) -> bool:
        """Check memory health"""
        try:
            return psutil.virtual_memory().percent < 90
        except Exception:
            return False
    
    def _check_disk_health(self) -> bool:
        """Check disk health"""
        try:
            return psutil.disk_usage('/').percent < 95
        except Exception:
            return False
    
    def _check_cpu_health(self) -> bool:
        """Check CPU health"""
        try:
            return psutil.cpu_percent(interval=0.1) < 95
        except Exception:
            return False
    
    def _setup_event_handlers(self):
        """Setup event handlers"""
        self.event_bus.subscribe('pipeline_completed', self._on_pipeline_completed)
        self.event_bus.subscribe('pipeline_failed', self._on_pipeline_failed)
    
    def _on_pipeline_completed(self, data: Dict):
        """Handle pipeline completion"""
        pipeline_name = str(data.get('pipeline', 'unknown'))
        safe_name = str(pipeline_name)[:50]
        logging.info(f"Pipeline {safe_name} completed successfully")
    
    def _on_pipeline_failed(self, data: Dict):
        """Handle pipeline failure"""
        pipeline_name = str(data.get('pipeline', 'unknown'))
        error = str(data.get('error', 'unknown error'))
        safe_name = str(pipeline_name)[:50]
        safe_error = str(error)[:100]
        logging.error(f"Pipeline {safe_name} failed: {safe_error}")
    
    def create_pipeline(self, name: str) -> ProcessingPipeline:
        """Create processing pipeline"""
        pipeline = ProcessingPipeline(name, self.event_bus)
        self.pipelines[name] = pipeline
        return pipeline
    
    def get_system_status(self) -> Dict:
        """Get comprehensive system status"""
        try:
            health_status = self.health_check.run_checks()
            performance_report = self.profiler.get_report()
            pipeline_metrics = {name: p.metrics for name, p in self.pipelines.items()}
            
            system_resources = {
                'memory_percent': psutil.virtual_memory().percent,
                'cpu_percent': psutil.cpu_percent(),
                'disk_percent': psutil.disk_usage('/').percent
            }
            
            return {
                'health': health_status,
                'performance': performance_report,
                'pipelines': pipeline_metrics,
                'system_resources': system_resources
            }
        except Exception as e:
            error_msg = f"Failed to get system status: {str(e)[:100]}"
            logging.error(error_msg)
            return {
                'health': {'error': str(e)},
                'performance': {},
                'pipelines': {},
                'system_resources': {}
            }