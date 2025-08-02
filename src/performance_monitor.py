#!/usr/bin/env python3
"""
Real-time Performance Monitoring System
Enterprise-grade monitoring with predictive OOM detection
"""

import gc
import os
import psutil
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
import json

@dataclass
class PerformanceMetrics:
    """Performance metrics snapshot"""
    timestamp: float
    memory_rss_mb: float
    memory_vms_mb: float
    memory_percent: float
    cpu_percent: float
    io_read_mb: float
    io_write_mb: float
    thread_count: int
    file_descriptors: int
    gc_collections: Dict[int, int] = field(default_factory=dict)

class OOMPredictor:
    """Predictive Out-of-Memory detection using trend analysis"""
    
    def __init__(self, window_size: int = 20):
        self.window_size = window_size
        self.memory_history = deque(maxlen=window_size)
        self.prediction_threshold = 0.90  # 90% memory usage
        
    def add_sample(self, memory_percent: float):
        """Add memory usage sample"""
        self.memory_history.append((time.time(), memory_percent))
    
    def predict_oom_risk(self) -> tuple[float, str]:
        """Predict OOM risk level and recommendation"""
        if len(self.memory_history) < 5:
            return 0.0, "Insufficient data"
        
        # Calculate memory growth trend
        recent_samples = list(self.memory_history)[-10:]
        if len(recent_samples) < 2:
            return 0.0, "Insufficient samples"
        
        # Linear regression for trend analysis
        x_vals = [i for i in range(len(recent_samples))]
        y_vals = [sample[1] for sample in recent_samples]
        
        n = len(recent_samples)
        sum_x = sum(x_vals)
        sum_y = sum(y_vals)
        sum_xy = sum(x * y for x, y in zip(x_vals, y_vals))
        sum_x2 = sum(x * x for x in x_vals)
        
        # Calculate slope (memory growth rate)
        if n * sum_x2 - sum_x * sum_x == 0:
            slope = 0
        else:
            slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        
        current_memory = y_vals[-1]
        
        # Risk assessment
        if current_memory > 95:
            return 1.0, "CRITICAL: Immediate OOM risk"
        elif current_memory > 85 and slope > 1:
            return 0.8, "HIGH: Rapid memory growth detected"
        elif current_memory > 75 and slope > 0.5:
            return 0.6, "MEDIUM: Steady memory growth"
        elif current_memory > 60:
            return 0.3, "LOW: Monitor memory usage"
        else:
            return 0.1, "NORMAL: Memory usage stable"

class PerformanceMonitor:
    """Real-time system performance monitoring"""
    
    def __init__(self, monitoring_interval: float = 1.0):
        self.monitoring_interval = monitoring_interval
        self.metrics_history = deque(maxlen=1000)  # Keep last 1000 samples
        self.oom_predictor = OOMPredictor()
        self.alert_callbacks: List[Callable] = []
        self.monitoring_thread: Optional[threading.Thread] = None
        self.stop_monitoring = threading.Event()
        self.process = psutil.Process()
        
        # Performance thresholds
        self.thresholds = {
            'memory_critical': 90.0,
            'memory_warning': 75.0,
            'cpu_critical': 95.0,
            'cpu_warning': 80.0,
            'io_warning_mb': 100.0
        }
    
    def add_alert_callback(self, callback: Callable[[str, PerformanceMetrics], None]):
        """Add callback for performance alerts"""
        self.alert_callbacks.append(callback)
    
    def collect_metrics(self) -> PerformanceMetrics:
        """Collect current performance metrics"""
        try:
            # Memory metrics
            memory_info = self.process.memory_info()
            system_memory = psutil.virtual_memory()
            
            # CPU metrics
            cpu_percent = self.process.cpu_percent()
            
            # I/O metrics
            try:
                io_counters = self.process.io_counters()
                io_read_mb = io_counters.read_bytes / (1024 * 1024)
                io_write_mb = io_counters.write_bytes / (1024 * 1024)
            except (AttributeError, psutil.AccessDenied):
                io_read_mb = io_write_mb = 0.0
            
            # Thread and file descriptor counts
            try:
                thread_count = self.process.num_threads()
                file_descriptors = self.process.num_fds()
            except (AttributeError, psutil.AccessDenied):
                thread_count = file_descriptors = 0
            
            # Garbage collection stats
            gc_stats = {i: gc.get_count()[i] for i in range(3)}
            
            metrics = PerformanceMetrics(
                timestamp=time.time(),
                memory_rss_mb=memory_info.rss / (1024 * 1024),
                memory_vms_mb=memory_info.vms / (1024 * 1024),
                memory_percent=system_memory.percent,
                cpu_percent=cpu_percent,
                io_read_mb=io_read_mb,
                io_write_mb=io_write_mb,
                thread_count=thread_count,
                file_descriptors=file_descriptors,
                gc_collections=gc_stats
            )
            
            return metrics
            
        except Exception as e:
            print(f"[MONITOR] Error collecting metrics: {e}")
            return PerformanceMetrics(
                timestamp=time.time(),
                memory_rss_mb=0, memory_vms_mb=0, memory_percent=0,
                cpu_percent=0, io_read_mb=0, io_write_mb=0,
                thread_count=0, file_descriptors=0
            )
    
    def check_thresholds(self, metrics: PerformanceMetrics):
        """Check performance thresholds and trigger alerts"""
        alerts = []
        
        # Memory alerts
        if metrics.memory_percent >= self.thresholds['memory_critical']:
            alerts.append(f"CRITICAL: Memory usage {metrics.memory_percent:.1f}%")
        elif metrics.memory_percent >= self.thresholds['memory_warning']:
            alerts.append(f"WARNING: Memory usage {metrics.memory_percent:.1f}%")
        
        # CPU alerts
        if metrics.cpu_percent >= self.thresholds['cpu_critical']:
            alerts.append(f"CRITICAL: CPU usage {metrics.cpu_percent:.1f}%")
        elif metrics.cpu_percent >= self.thresholds['cpu_warning']:
            alerts.append(f"WARNING: CPU usage {metrics.cpu_percent:.1f}%")
        
        # I/O alerts
        total_io = metrics.io_read_mb + metrics.io_write_mb
        if total_io >= self.thresholds['io_warning_mb']:
            alerts.append(f"WARNING: High I/O activity {total_io:.1f}MB")
        
        # File descriptor alerts
        if metrics.file_descriptors > 1000:
            alerts.append(f"WARNING: High file descriptor count {metrics.file_descriptors}")
        
        # Trigger alert callbacks
        for alert in alerts:
            for callback in self.alert_callbacks:
                try:
                    callback(alert, metrics)
                except Exception as e:
                    print(f"[MONITOR] Alert callback error: {e}")
    
    def monitoring_loop(self):
        """Main monitoring loop"""
        while not self.stop_monitoring.is_set():
            try:
                metrics = self.collect_metrics()
                self.metrics_history.append(metrics)
                
                # Update OOM predictor
                self.oom_predictor.add_sample(metrics.memory_percent)
                
                # Check thresholds
                self.check_thresholds(metrics)
                
                # OOM prediction
                risk_level, recommendation = self.oom_predictor.predict_oom_risk()
                if risk_level > 0.7:  # High risk
                    for callback in self.alert_callbacks:
                        try:
                            callback(f"OOM RISK: {recommendation}", metrics)
                        except Exception as e:
                            print(f"[MONITOR] OOM alert callback error: {e}")
                
                time.sleep(self.monitoring_interval)
                
            except Exception as e:
                print(f"[MONITOR] Monitoring loop error: {e}")
                time.sleep(self.monitoring_interval)
    
    def start_monitoring(self):
        """Start background monitoring"""
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            return
        
        self.stop_monitoring.clear()
        self.monitoring_thread = threading.Thread(target=self.monitoring_loop, daemon=True)
        self.monitoring_thread.start()
        print("[MONITOR] Performance monitoring started")
    
    def stop_monitoring_thread(self):
        """Stop background monitoring"""
        self.stop_monitoring.set()
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=2.0)
        print("[MONITOR] Performance monitoring stopped")
    
    def get_current_metrics(self) -> Optional[PerformanceMetrics]:
        """Get most recent metrics"""
        return self.metrics_history[-1] if self.metrics_history else None
    
    def get_metrics_summary(self, last_n_minutes: int = 5) -> Dict:
        """Get performance summary for last N minutes"""
        cutoff_time = time.time() - (last_n_minutes * 60)
        recent_metrics = [m for m in self.metrics_history if m.timestamp >= cutoff_time]
        
        if not recent_metrics:
            return {}
        
        memory_values = [m.memory_percent for m in recent_metrics]
        cpu_values = [m.cpu_percent for m in recent_metrics]
        
        return {
            'time_window_minutes': last_n_minutes,
            'sample_count': len(recent_metrics),
            'memory': {
                'current': memory_values[-1],
                'average': sum(memory_values) / len(memory_values),
                'peak': max(memory_values),
                'trend': 'increasing' if memory_values[-1] > memory_values[0] else 'stable'
            },
            'cpu': {
                'current': cpu_values[-1],
                'average': sum(cpu_values) / len(cpu_values),
                'peak': max(cpu_values)
            },
            'oom_risk': self.oom_predictor.predict_oom_risk()
        }
    
    def export_metrics(self, filepath: str, last_n_minutes: int = 60):
        """Export metrics to JSON file"""
        cutoff_time = time.time() - (last_n_minutes * 60)
        recent_metrics = [m for m in self.metrics_history if m.timestamp >= cutoff_time]
        
        export_data = {
            'export_timestamp': time.time(),
            'time_window_minutes': last_n_minutes,
            'metrics': [
                {
                    'timestamp': m.timestamp,
                    'memory_rss_mb': m.memory_rss_mb,
                    'memory_percent': m.memory_percent,
                    'cpu_percent': m.cpu_percent,
                    'io_read_mb': m.io_read_mb,
                    'io_write_mb': m.io_write_mb,
                    'thread_count': m.thread_count,
                    'file_descriptors': m.file_descriptors
                }
                for m in recent_metrics
            ]
        }
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        print(f"[MONITOR] Exported {len(recent_metrics)} metrics to {filepath}")

# Global monitor instance
_monitor = None

def get_performance_monitor() -> PerformanceMonitor:
    """Get singleton performance monitor instance"""
    global _monitor
    if _monitor is None:
        _monitor = PerformanceMonitor()
    return _monitor

def setup_monitoring_alerts():
    """Setup default monitoring alerts"""
    monitor = get_performance_monitor()
    
    def default_alert_handler(alert: str, metrics: PerformanceMetrics):
        print(f"[ALERT] {alert} - RSS: {metrics.memory_rss_mb:.1f}MB, "
              f"CPU: {metrics.cpu_percent:.1f}%, Threads: {metrics.thread_count}")
        
        # Force garbage collection on high memory usage
        if metrics.memory_percent > 85:
            gc.collect()
    
    monitor.add_alert_callback(default_alert_handler)
    return monitor