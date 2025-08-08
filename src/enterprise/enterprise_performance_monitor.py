#!/usr/bin/env python3
"""
Enterprise Performance Monitoring System
Implements comprehensive performance monitoring, metrics collection, and optimization recommendations
"""

import asyncio
import json
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
import psutil
import gc

from src.enterprise.enterprise_logging import get_logger

logger = get_logger(__name__)


@dataclass
class SystemMetrics:
    """System resource metrics"""
    timestamp: float
    cpu_percent: float
    memory_percent: float
    memory_available_mb: float
    disk_io_read_mb: float
    disk_io_write_mb: float
    network_bytes_sent: int = 0
    network_bytes_recv: int = 0
    load_average: float = 0.0
    
    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = time.time()


@dataclass
class OperationMetrics:
    """Individual operation performance metrics"""
    operation_name: str
    start_time: float
    end_time: Optional[float] = None
    duration_ms: Optional[float] = None
    memory_start_mb: float = 0
    memory_end_mb: float = 0
    memory_peak_mb: float = 0
    cpu_time_start: float = 0
    cpu_time_end: float = 0
    success: bool = True
    error_message: Optional[str] = None
    custom_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def complete(self, success: bool = True, error_message: Optional[str] = None):
        """Mark operation as complete"""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000
        self.success = success
        self.error_message = error_message
        
        # Update memory metrics
        process = psutil.Process()
        self.memory_end_mb = process.memory_info().rss / (1024 * 1024)
        self.cpu_time_end = process.cpu_times().user + process.cpu_times().system


@dataclass
class PerformanceReport:
    """Comprehensive performance report"""
    report_id: str
    start_time: float
    end_time: float
    total_duration_seconds: float
    system_metrics: List[SystemMetrics]
    operation_metrics: List[OperationMetrics]
    recommendations: List[str]
    summary: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'report_id': self.report_id,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'total_duration_seconds': self.total_duration_seconds,
            'system_metrics_count': len(self.system_metrics),
            'operation_metrics_count': len(self.operation_metrics),
            'recommendations': self.recommendations,
            'summary': self.summary,
            'system_metrics': [asdict(m) for m in self.system_metrics[-10:]],  # Last 10 samples
            'operation_metrics': [asdict(m) for m in self.operation_metrics]
        }


class SystemMonitor:
    """System resource monitoring with adaptive sampling"""
    
    def __init__(self, sample_interval: float = 10.0):
        self.sample_interval = sample_interval
        self.metrics: List[SystemMetrics] = []
        self.monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._initial_disk_io = None
        self._initial_network = None
    
    def start_monitoring(self):
        """Start system monitoring in background thread"""
        if self.monitoring:
            return
        
        self.monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info(f"System monitoring started (interval: {self.sample_interval}s)")
    
    def stop_monitoring(self):
        """Stop system monitoring"""
        self.monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
        logger.info("System monitoring stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        self._initialize_counters()
        
        while self.monitoring:
            try:
                metrics = self._collect_system_metrics()
                
                with self._lock:
                    self.metrics.append(metrics)
                    
                    # Keep only last 1000 samples to prevent memory growth
                    if len(self.metrics) > 1000:
                        self.metrics = self.metrics[-1000:]
                
                time.sleep(self.sample_interval)
                
            except Exception as e:
                logger.error(f"System monitoring error: {e}")
                time.sleep(self.sample_interval)
    
    def _initialize_counters(self):
        """Initialize disk and network counters"""
        try:
            self._initial_disk_io = psutil.disk_io_counters()
            self._initial_network = psutil.net_io_counters()
        except Exception as e:
            logger.warning(f"Could not initialize performance counters: {e}")
    
    def _collect_system_metrics(self) -> SystemMetrics:
        """Collect current system metrics"""
        try:
            # CPU and memory
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            disk_read_mb = 0
            disk_write_mb = 0
            
            if disk_io and self._initial_disk_io:
                disk_read_mb = (disk_io.read_bytes - self._initial_disk_io.read_bytes) / (1024 * 1024)
                disk_write_mb = (disk_io.write_bytes - self._initial_disk_io.write_bytes) / (1024 * 1024)
            
            # Network I/O
            network = psutil.net_io_counters()
            network_sent = 0
            network_recv = 0
            
            if network and self._initial_network:
                network_sent = network.bytes_sent - self._initial_network.bytes_sent
                network_recv = network.bytes_recv - self._initial_network.bytes_recv
            
            # Load average (Unix-like systems)
            load_avg = 0.0
            try:
                if hasattr(psutil, 'getloadavg'):
                    load_avg = psutil.getloadavg()[0]
            except (AttributeError, OSError):
                pass
            
            return SystemMetrics(
                timestamp=time.time(),
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                memory_available_mb=memory.available / (1024 * 1024),
                disk_io_read_mb=disk_read_mb,
                disk_io_write_mb=disk_write_mb,
                network_bytes_sent=network_sent,
                network_bytes_recv=network_recv,
                load_average=load_avg
            )
            
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
            return SystemMetrics(
                timestamp=time.time(),
                cpu_percent=0,
                memory_percent=0,
                memory_available_mb=0,
                disk_io_read_mb=0,
                disk_io_write_mb=0
            )
    
    def get_metrics(self) -> List[SystemMetrics]:
        """Get collected system metrics"""
        with self._lock:
            return self.metrics.copy()
    
    def get_current_metrics(self) -> SystemMetrics:
        """Get current system metrics"""
        return self._collect_system_metrics()


class OperationProfiler:
    """Operation-level performance profiling"""
    
    def __init__(self):
        self.operations: List[OperationMetrics] = []
        self._active_operations: Dict[str, OperationMetrics] = {}
        self._lock = threading.Lock()
    
    @contextmanager
    def profile_operation(self, operation_name: str, **custom_metrics):
        """Context manager for profiling operations"""
        operation_id = f"{operation_name}_{int(time.time() * 1000)}"
        
        # Start profiling
        process = psutil.Process()
        start_metrics = OperationMetrics(
            operation_name=operation_name,
            start_time=time.time(),
            memory_start_mb=process.memory_info().rss / (1024 * 1024),
            cpu_time_start=process.cpu_times().user + process.cpu_times().system,
            custom_metrics=custom_metrics
        )
        
        with self._lock:
            self._active_operations[operation_id] = start_metrics
        
        success = True
        error_message = None
        
        try:
            yield start_metrics
        except Exception as e:
            success = False
            error_message = str(e)
            raise
        finally:
            # Complete profiling
            start_metrics.complete(success, error_message)
            
            with self._lock:
                self.operations.append(start_metrics)
                self._active_operations.pop(operation_id, None)
                
                # Keep only last 1000 operations
                if len(self.operations) > 1000:
                    self.operations = self.operations[-1000:]
    
    def get_operations(self) -> List[OperationMetrics]:
        """Get all completed operations"""
        with self._lock:
            return self.operations.copy()
    
    def get_active_operations(self) -> List[OperationMetrics]:
        """Get currently active operations"""
        with self._lock:
            return list(self._active_operations.values())


class PerformanceAnalyzer:
    """Analyzes performance data and generates recommendations"""
    
    def __init__(self):
        self.thresholds = {
            'cpu_high': 80.0,
            'memory_high': 85.0,
            'memory_critical': 95.0,
            'disk_io_high': 100.0,  # MB/s
            'operation_slow': 30.0,  # seconds
            'operation_memory_high': 500.0,  # MB
        }
    
    def analyze_system_performance(self, metrics: List[SystemMetrics]) -> List[str]:
        """Analyze system metrics and generate recommendations"""
        recommendations = []
        
        if not metrics:
            return recommendations
        
        # CPU analysis
        avg_cpu = sum(m.cpu_percent for m in metrics) / len(metrics)
        max_cpu = max(m.cpu_percent for m in metrics)
        
        if max_cpu > self.thresholds['cpu_high']:
            recommendations.append(
                f"High CPU usage detected (peak: {max_cpu:.1f}%, avg: {avg_cpu:.1f}%). "
                "Consider reducing concurrent operations or optimizing CPU-intensive tasks."
            )
        
        # Memory analysis
        avg_memory = sum(m.memory_percent for m in metrics) / len(metrics)
        max_memory = max(m.memory_percent for m in metrics)
        
        if max_memory > self.thresholds['memory_critical']:
            recommendations.append(
                f"Critical memory usage detected (peak: {max_memory:.1f}%). "
                "System may become unstable. Reduce batch sizes and enable memory optimization."
            )
        elif max_memory > self.thresholds['memory_high']:
            recommendations.append(
                f"High memory usage detected (peak: {max_memory:.1f}%, avg: {avg_memory:.1f}%). "
                "Consider reducing batch sizes or increasing available RAM."
            )
        
        # Disk I/O analysis
        max_disk_read = max(m.disk_io_read_mb for m in metrics)
        max_disk_write = max(m.disk_io_write_mb for m in metrics)
        
        if max_disk_read > self.thresholds['disk_io_high'] or max_disk_write > self.thresholds['disk_io_high']:
            recommendations.append(
                f"High disk I/O detected (read: {max_disk_read:.1f}MB/s, write: {max_disk_write:.1f}MB/s). "
                "Consider using SSD storage or optimizing file operations."
            )
        
        return recommendations
    
    def analyze_operation_performance(self, operations: List[OperationMetrics]) -> List[str]:
        """Analyze operation metrics and generate recommendations"""
        recommendations = []
        
        if not operations:
            return recommendations
        
        # Find slow operations
        slow_operations = [op for op in operations if op.duration_ms and op.duration_ms > self.thresholds['operation_slow'] * 1000]
        
        if slow_operations:
            slowest = max(slow_operations, key=lambda x: x.duration_ms or 0)
            if slowest.duration_ms is not None:
                duration_s = slowest.duration_ms / 1000
                recommendations.append(
                    f"Slow operations detected. Slowest: '{slowest.operation_name}' "
                    f"took {duration_s:.2f}s. Consider optimization or parallelization."
                )
        
        # Find memory-intensive operations
        memory_intensive = [op for op in operations if (op.memory_end_mb - op.memory_start_mb) > self.thresholds['operation_memory_high']]
        
        if memory_intensive:
            most_intensive = max(memory_intensive, key=lambda x: x.memory_end_mb - x.memory_start_mb)
            memory_used = most_intensive.memory_end_mb - most_intensive.memory_start_mb
            recommendations.append(
                f"Memory-intensive operations detected. '{most_intensive.operation_name}' "
                f"used {memory_used:.1f}MB. Consider streaming or batch processing."
            )
        
        # Success rate analysis
        failed_operations = [op for op in operations if not op.success]
        if failed_operations:
            failure_rate = len(failed_operations) / len(operations) * 100
            recommendations.append(
                f"Operation failure rate: {failure_rate:.1f}% ({len(failed_operations)}/{len(operations)}). "
                "Review error handling and retry strategies."
            )
        
        return recommendations


class EnterprisePerformanceMonitor:
    """Main enterprise performance monitoring system"""
    
    def __init__(self, sample_interval: float = 10.0):
        self.system_monitor = SystemMonitor(sample_interval)
        self.operation_profiler = OperationProfiler()
        self.analyzer = PerformanceAnalyzer()
        self.start_time = time.time()
        self.monitoring_active = False
    
    def start_monitoring(self):
        """Start comprehensive performance monitoring"""
        if self.monitoring_active:
            return
        
        self.monitoring_active = True
        self.start_time = time.time()
        self.system_monitor.start_monitoring()
        logger.info("Enterprise performance monitoring started")
    
    def stop_monitoring(self):
        """Stop performance monitoring"""
        if not self.monitoring_active:
            return
        
        self.monitoring_active = False
        self.system_monitor.stop_monitoring()
        logger.info("Enterprise performance monitoring stopped")
    
    @contextmanager
    def profile_operation(self, operation_name: str, **custom_metrics):
        """Profile an operation"""
        with self.operation_profiler.profile_operation(operation_name, **custom_metrics) as metrics:
            yield metrics
    
    def generate_report(self) -> PerformanceReport:
        """Generate comprehensive performance report"""
        end_time = time.time()
        
        # Collect metrics
        system_metrics = self.system_monitor.get_metrics()
        operation_metrics = self.operation_profiler.get_operations()
        
        # Generate recommendations
        system_recommendations = self.analyzer.analyze_system_performance(system_metrics)
        operation_recommendations = self.analyzer.analyze_operation_performance(operation_metrics)
        all_recommendations = system_recommendations + operation_recommendations
        
        # Generate summary
        summary = self._generate_summary(system_metrics, operation_metrics)
        
        report = PerformanceReport(
            report_id=f"perf_report_{int(end_time)}",
            start_time=self.start_time,
            end_time=end_time,
            total_duration_seconds=end_time - self.start_time,
            system_metrics=system_metrics,
            operation_metrics=operation_metrics,
            recommendations=all_recommendations,
            summary=summary
        )
        
        return report
    
    def _generate_summary(self, system_metrics: List[SystemMetrics], operation_metrics: List[OperationMetrics]) -> Dict[str, Any]:
        """Generate performance summary statistics"""
        summary = {
            'monitoring_duration_seconds': time.time() - self.start_time,
            'system_samples': len(system_metrics),
            'operations_completed': len(operation_metrics),
            'operations_failed': len([op for op in operation_metrics if not op.success])
        }
        
        if system_metrics:
            summary.update({
                'avg_cpu_percent': sum(m.cpu_percent for m in system_metrics) / len(system_metrics),
                'peak_cpu_percent': max(m.cpu_percent for m in system_metrics),
                'avg_memory_percent': sum(m.memory_percent for m in system_metrics) / len(system_metrics),
                'peak_memory_percent': max(m.memory_percent for m in system_metrics),
                'min_available_memory_mb': min(m.memory_available_mb for m in system_metrics)
            })
        
        if operation_metrics:
            completed_ops = [op for op in operation_metrics if op.duration_ms is not None]
            if completed_ops:
                durations = [op.duration_ms for op in completed_ops if op.duration_ms is not None]
                if durations:
                    summary.update({
                        'avg_operation_duration_ms': sum(durations) / len(durations),
                        'slowest_operation_ms': max(durations),
                        'total_operations_time_seconds': sum(durations) / 1000
                    })
        
        return summary
    
    def save_report(self, report: PerformanceReport, output_file: Path):
        """Save performance report to file"""
        try:
            with open(output_file, 'w') as f:
                json.dump(report.to_dict(), f, indent=2, default=str)
            
            logger.info(f"Performance report saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to save performance report: {e}")
    
    def get_current_status(self) -> Dict[str, Any]:
        """Get current monitoring status"""
        current_metrics = self.system_monitor.get_current_metrics()
        active_operations = self.operation_profiler.get_active_operations()
        
        return {
            'monitoring_active': self.monitoring_active,
            'monitoring_duration_seconds': time.time() - self.start_time,
            'current_cpu_percent': current_metrics.cpu_percent,
            'current_memory_percent': current_metrics.memory_percent,
            'current_memory_available_mb': current_metrics.memory_available_mb,
            'active_operations': len(active_operations),
            'active_operation_names': [op.operation_name for op in active_operations]
        }


# Global performance monitor instance
_performance_monitor: Optional[EnterprisePerformanceMonitor] = None


def get_performance_monitor(sample_interval: float = 10.0) -> EnterprisePerformanceMonitor:
    """Get global performance monitor instance"""
    global _performance_monitor
    
    if _performance_monitor is None:
        _performance_monitor = EnterprisePerformanceMonitor(sample_interval)
    
    return _performance_monitor


@contextmanager
def monitor_performance(operation_name: str, **custom_metrics):
    """Context manager for performance monitoring"""
    monitor = get_performance_monitor()
    with monitor.profile_operation(operation_name, **custom_metrics) as metrics:
        yield metrics