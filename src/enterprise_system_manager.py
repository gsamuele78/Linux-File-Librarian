#!/usr/bin/env python3
"""
Enterprise System Management Module

Provides comprehensive system management, monitoring, and maintenance
capabilities for enterprise deployments.
"""

import os
import sys
import subprocess
import shutil
import psutil
import logging
import json
import time
import threading
import signal
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import tempfile

logger = logging.getLogger(__name__)


class ServiceStatus(Enum):
    """Service status enumeration"""
    RUNNING = "running"
    STOPPED = "stopped"
    FAILED = "failed"
    STARTING = "starting"
    STOPPING = "stopping"
    UNKNOWN = "unknown"


class MaintenanceMode(Enum):
    """Maintenance mode types"""
    NONE = "none"
    SOFT = "soft"
    HARD = "hard"
    EMERGENCY = "emergency"


@dataclass
class SystemHealth:
    """System health metrics"""
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    load_average: List[float]
    process_count: int
    file_descriptors: int
    network_connections: int
    uptime_seconds: float
    timestamp: float
    health_score: float
    issues: List[str]


@dataclass
class ServiceInfo:
    """Service information"""
    name: str
    status: ServiceStatus
    pid: Optional[int]
    memory_mb: float
    cpu_percent: float
    uptime_seconds: float
    restart_count: int
    last_restart: Optional[float]


class SystemMonitor:
    """Advanced system monitoring with alerting"""
    
    def __init__(self, alert_thresholds: Optional[Dict[str, float]] = None):
        self.alert_thresholds = alert_thresholds or {
            'cpu_usage': 85.0,
            'memory_usage': 90.0,
            'disk_usage': 95.0,
            'load_average': 10.0,
            'file_descriptors': 80.0
        }
        self.monitoring_active = False
        self.monitor_thread = None
        self.alert_callbacks = []
    
    def get_system_health(self) -> SystemHealth:
        """Get comprehensive system health metrics"""
        try:
            # CPU metrics
            cpu_usage = psutil.cpu_percent(interval=1.0)
            load_avg = os.getloadavg()
            
            # Memory metrics
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            
            # Disk metrics
            disk = psutil.disk_usage('/')
            disk_usage = (disk.used / disk.total) * 100
            
            # Process metrics
            process_count = len(psutil.pids())
            
            # File descriptor metrics
            try:
                current_process = psutil.Process()
                file_descriptors = len(current_process.open_files())
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                file_descriptors = 0
            
            # Network metrics
            try:
                network_connections = len(psutil.net_connections())
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                network_connections = 0
            
            # System uptime
            uptime_seconds = time.time() - psutil.boot_time()
            
            # Calculate health score and identify issues
            health_score, issues = self._calculate_health_score(
                cpu_usage, memory_usage, disk_usage, load_avg[0], file_descriptors
            )
            
            return SystemHealth(
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                disk_usage=disk_usage,
                load_average=list(load_avg),
                process_count=process_count,
                file_descriptors=file_descriptors,
                network_connections=network_connections,
                uptime_seconds=uptime_seconds,
                timestamp=time.time(),
                health_score=health_score,
                issues=issues
            )
            
        except Exception as e:
            logger.error(f"Failed to get system health: {e}")
            return SystemHealth(
                cpu_usage=0.0, memory_usage=0.0, disk_usage=0.0,
                load_average=[0.0, 0.0, 0.0], process_count=0,
                file_descriptors=0, network_connections=0,
                uptime_seconds=0.0, timestamp=time.time(),
                health_score=0.0, issues=[f"Health check failed: {e}"]
            )
    
    def _calculate_health_score(self, cpu: float, memory: float, disk: float, 
                              load: float, fd_count: int) -> tuple[float, List[str]]:
        """Calculate system health score (0-100) and identify issues"""
        score = 100.0
        issues = []
        
        # CPU usage impact
        if cpu > self.alert_thresholds['cpu_usage']:
            penalty = min(30, (cpu - self.alert_thresholds['cpu_usage']) * 2)
            score -= penalty
            issues.append(f"High CPU usage: {cpu:.1f}%")
        
        # Memory usage impact
        if memory > self.alert_thresholds['memory_usage']:
            penalty = min(40, (memory - self.alert_thresholds['memory_usage']) * 4)
            score -= penalty
            issues.append(f"High memory usage: {memory:.1f}%")
        
        # Disk usage impact
        if disk > self.alert_thresholds['disk_usage']:
            penalty = min(50, (disk - self.alert_thresholds['disk_usage']) * 10)
            score -= penalty
            issues.append(f"High disk usage: {disk:.1f}%")
        
        # Load average impact
        cpu_count = psutil.cpu_count()
        if load > cpu_count * 2:
            penalty = min(25, (load - cpu_count) * 5)
            score -= penalty
            issues.append(f"High load average: {load:.2f}")
        
        # File descriptor impact (rough estimate)
        max_fd = 1024  # Conservative estimate
        fd_usage = (fd_count / max_fd) * 100
        if fd_usage > self.alert_thresholds['file_descriptors']:
            penalty = min(20, (fd_usage - self.alert_thresholds['file_descriptors']) * 2)
            score -= penalty
            issues.append(f"High file descriptor usage: {fd_count}")
        
        return max(0.0, score), issues
    
    def add_alert_callback(self, callback: Callable[[SystemHealth], None]):
        """Add callback for health alerts"""
        self.alert_callbacks.append(callback)
    
    def start_monitoring(self, interval_seconds: int = 60):
        """Start continuous system monitoring"""
        if self.monitoring_active:
            logger.warning("Monitoring already active")
            return
        
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(interval_seconds,),
            daemon=True
        )
        self.monitor_thread.start()
        logger.info(f"System monitoring started with {interval_seconds}s interval")
    
    def stop_monitoring(self):
        """Stop system monitoring"""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("System monitoring stopped")
    
    def _monitoring_loop(self, interval_seconds: int):
        """Main monitoring loop"""
        while self.monitoring_active:
            try:
                health = self.get_system_health()
                
                # Trigger alerts if health score is low or there are issues
                if health.health_score < 70 or health.issues:
                    for callback in self.alert_callbacks:
                        try:
                            callback(health)
                        except Exception as e:
                            logger.error(f"Alert callback failed: {e}")
                
                time.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                time.sleep(interval_seconds)


class ProcessManager:
    """Enterprise process management"""
    
    def __init__(self):
        self.managed_processes = {}
        self.process_configs = {}
    
    def register_process(self, name: str, command: List[str], 
                        working_dir: Optional[Path] = None,
                        env_vars: Optional[Dict[str, str]] = None,
                        auto_restart: bool = True,
                        max_restarts: int = 5):
        """Register a process for management"""
        self.process_configs[name] = {
            'command': command,
            'working_dir': working_dir,
            'env_vars': env_vars or {},
            'auto_restart': auto_restart,
            'max_restarts': max_restarts,
            'restart_count': 0,
            'last_restart': None
        }
        logger.info(f"Registered process: {name}")
    
    def start_process(self, name: str) -> bool:
        """Start a managed process"""
        if name not in self.process_configs:
            logger.error(f"Process not registered: {name}")
            return False
        
        if name in self.managed_processes:
            if self.is_process_running(name):
                logger.warning(f"Process already running: {name}")
                return True
            else:
                # Clean up dead process
                del self.managed_processes[name]
        
        try:
            config = self.process_configs[name]
            
            # Prepare environment
            env = os.environ.copy()
            env.update(config['env_vars'])
            
            # Start process
            process = subprocess.Popen(
                config['command'],
                cwd=config['working_dir'],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.managed_processes[name] = {
                'process': process,
                'start_time': time.time(),
                'config': config
            }
            
            logger.info(f"Started process: {name} (PID: {process.pid})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start process {name}: {e}")
            return False
    
    def stop_process(self, name: str, timeout: int = 10) -> bool:
        """Stop a managed process gracefully"""
        if name not in self.managed_processes:
            logger.warning(f"Process not managed: {name}")
            return True
        
        try:
            process_info = self.managed_processes[name]
            process = process_info['process']
            
            if not self.is_process_running(name):
                del self.managed_processes[name]
                return True
            
            # Try graceful shutdown first
            process.terminate()
            
            try:
                process.wait(timeout=timeout)
                logger.info(f"Process terminated gracefully: {name}")
            except subprocess.TimeoutExpired:
                # Force kill if graceful shutdown fails
                process.kill()
                process.wait()
                logger.warning(f"Process force killed: {name}")
            
            del self.managed_processes[name]
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop process {name}: {e}")
            return False
    
    def restart_process(self, name: str) -> bool:
        """Restart a managed process"""
        if name not in self.process_configs:
            logger.error(f"Process not registered: {name}")
            return False
        
        # Update restart count
        config = self.process_configs[name]
        config['restart_count'] += 1
        config['last_restart'] = time.time()
        
        # Check restart limits
        if config['restart_count'] > config['max_restarts']:
            logger.error(f"Process {name} exceeded max restarts ({config['max_restarts']})")
            return False
        
        # Stop and start
        self.stop_process(name)
        return self.start_process(name)
    
    def is_process_running(self, name: str) -> bool:
        """Check if a managed process is running"""
        if name not in self.managed_processes:
            return False
        
        try:
            process = self.managed_processes[name]['process']
            return process.poll() is None
        except Exception:
            return False
    
    def get_process_info(self, name: str) -> Optional[ServiceInfo]:
        """Get information about a managed process"""
        if name not in self.managed_processes:
            return None
        
        try:
            process_info = self.managed_processes[name]
            process = process_info['process']
            config = self.process_configs[name]
            
            if not self.is_process_running(name):
                return ServiceInfo(
                    name=name,
                    status=ServiceStatus.STOPPED,
                    pid=None,
                    memory_mb=0.0,
                    cpu_percent=0.0,
                    uptime_seconds=0.0,
                    restart_count=config['restart_count'],
                    last_restart=config['last_restart']
                )
            
            # Get process metrics
            try:
                ps_process = psutil.Process(process.pid)
                memory_mb = ps_process.memory_info().rss / (1024 * 1024)
                cpu_percent = ps_process.cpu_percent()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                memory_mb = 0.0
                cpu_percent = 0.0
            
            uptime = time.time() - process_info['start_time']
            
            return ServiceInfo(
                name=name,
                status=ServiceStatus.RUNNING,
                pid=process.pid,
                memory_mb=memory_mb,
                cpu_percent=cpu_percent,
                uptime_seconds=uptime,
                restart_count=config['restart_count'],
                last_restart=config['last_restart']
            )
            
        except Exception as e:
            logger.error(f"Failed to get process info for {name}: {e}")
            return None
    
    def get_all_processes(self) -> Dict[str, ServiceInfo]:
        """Get information about all managed processes"""
        result = {}
        for name in self.process_configs:
            info = self.get_process_info(name)
            if info:
                result[name] = info
        return result


class MaintenanceManager:
    """System maintenance and cleanup management"""
    
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.maintenance_mode = MaintenanceMode.NONE
        self.maintenance_start_time = None
        self.cleanup_tasks = []
    
    def enter_maintenance_mode(self, mode: MaintenanceMode, 
                             reason: str = "Scheduled maintenance") -> bool:
        """Enter maintenance mode"""
        try:
            if self.maintenance_mode != MaintenanceMode.NONE:
                logger.warning(f"Already in maintenance mode: {self.maintenance_mode}")
                return False
            
            self.maintenance_mode = mode
            self.maintenance_start_time = time.time()
            
            logger.info(f"Entered maintenance mode: {mode.value} - {reason}")
            
            # Create maintenance marker file
            marker_file = self.project_root / '.maintenance'
            with open(marker_file, 'w') as f:
                json.dump({
                    'mode': mode.value,
                    'reason': reason,
                    'start_time': self.maintenance_start_time
                }, f, indent=2)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to enter maintenance mode: {e}")
            return False
    
    def exit_maintenance_mode(self) -> bool:
        """Exit maintenance mode"""
        try:
            if self.maintenance_mode == MaintenanceMode.NONE:
                logger.warning("Not in maintenance mode")
                return True
            
            duration = time.time() - (self.maintenance_start_time or 0)
            logger.info(f"Exiting maintenance mode after {duration:.1f} seconds")
            
            self.maintenance_mode = MaintenanceMode.NONE
            self.maintenance_start_time = None
            
            # Remove maintenance marker file
            marker_file = self.project_root / '.maintenance'
            if marker_file.exists():
                marker_file.unlink()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to exit maintenance mode: {e}")
            return False
    
    def is_in_maintenance(self) -> bool:
        """Check if system is in maintenance mode"""
        return self.maintenance_mode != MaintenanceMode.NONE
    
    def cleanup_logs(self, max_age_days: int = 30, max_size_mb: int = 100) -> Dict[str, Any]:
        """Clean up old log files"""
        try:
            logs_dir = self.project_root / 'logs'
            if not logs_dir.exists():
                return {'status': 'success', 'message': 'No logs directory found'}
            
            cleaned_files = []
            total_size_freed = 0
            cutoff_time = time.time() - (max_age_days * 24 * 3600)
            
            for log_file in logs_dir.glob('*.log*'):
                try:
                    stat = log_file.stat()
                    file_size = stat.st_size
                    
                    # Check age and size criteria
                    should_clean = (
                        stat.st_mtime < cutoff_time or
                        file_size > max_size_mb * 1024 * 1024
                    )
                    
                    if should_clean:
                        log_file.unlink()
                        cleaned_files.append(str(log_file))
                        total_size_freed += file_size
                        
                except Exception as e:
                    logger.warning(f"Failed to clean log file {log_file}: {e}")
            
            return {
                'status': 'success',
                'cleaned_files': cleaned_files,
                'files_count': len(cleaned_files),
                'size_freed_mb': total_size_freed / (1024 * 1024)
            }
            
        except Exception as e:
            logger.error(f"Log cleanup failed: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def cleanup_temp_files(self) -> Dict[str, Any]:
        """Clean up temporary files"""
        try:
            temp_dirs = [
                self.project_root / 'temp',
                self.project_root / 'tmp',
                Path(tempfile.gettempdir()) / 'librarian_*'
            ]
            
            cleaned_files = []
            total_size_freed = 0
            
            for temp_pattern in temp_dirs:
                if '*' in str(temp_pattern):
                    # Handle glob patterns
                    parent = temp_pattern.parent
                    pattern = temp_pattern.name
                    if parent.exists():
                        for temp_path in parent.glob(pattern):
                            if temp_path.is_dir():
                                size = sum(f.stat().st_size for f in temp_path.rglob('*') if f.is_file())
                                shutil.rmtree(temp_path)
                                cleaned_files.append(str(temp_path))
                                total_size_freed += size
                else:
                    # Handle direct paths
                    if temp_pattern.exists():
                        if temp_pattern.is_dir():
                            size = sum(f.stat().st_size for f in temp_pattern.rglob('*') if f.is_file())
                            shutil.rmtree(temp_pattern)
                        else:
                            size = temp_pattern.stat().st_size
                            temp_pattern.unlink()
                        
                        cleaned_files.append(str(temp_pattern))
                        total_size_freed += size
            
            return {
                'status': 'success',
                'cleaned_files': cleaned_files,
                'files_count': len(cleaned_files),
                'size_freed_mb': total_size_freed / (1024 * 1024)
            }
            
        except Exception as e:
            logger.error(f"Temp cleanup failed: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def optimize_database(self) -> Dict[str, Any]:
        """Optimize SQLite databases"""
        try:
            db_files = list(self.project_root.glob('*.sqlite')) + list(self.project_root.glob('*.db'))
            optimized_dbs = []
            
            for db_file in db_files:
                try:
                    import sqlite3
                    
                    # Get size before optimization
                    size_before = db_file.stat().st_size
                    
                    # Optimize database
                    with sqlite3.connect(str(db_file)) as conn:
                        conn.execute('VACUUM')
                        conn.execute('ANALYZE')
                    
                    # Get size after optimization
                    size_after = db_file.stat().st_size
                    size_saved = size_before - size_after
                    
                    optimized_dbs.append({
                        'file': str(db_file),
                        'size_before_mb': size_before / (1024 * 1024),
                        'size_after_mb': size_after / (1024 * 1024),
                        'size_saved_mb': size_saved / (1024 * 1024)
                    })
                    
                except Exception as e:
                    logger.warning(f"Failed to optimize database {db_file}: {e}")
            
            return {
                'status': 'success',
                'optimized_databases': optimized_dbs,
                'total_saved_mb': sum(db['size_saved_mb'] for db in optimized_dbs)
            }
            
        except Exception as e:
            logger.error(f"Database optimization failed: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def run_full_maintenance(self) -> Dict[str, Any]:
        """Run comprehensive system maintenance"""
        try:
            if not self.enter_maintenance_mode(MaintenanceMode.SOFT, "Automated maintenance"):
                return {'status': 'error', 'message': 'Failed to enter maintenance mode'}
            
            results = {}
            
            # Log cleanup
            results['log_cleanup'] = self.cleanup_logs()
            
            # Temp file cleanup
            results['temp_cleanup'] = self.cleanup_temp_files()
            
            # Database optimization
            results['database_optimization'] = self.optimize_database()
            
            # Calculate total space freed
            total_freed = 0
            for task_result in results.values():
                if task_result.get('status') == 'success':
                    total_freed += task_result.get('size_freed_mb', 0)
                    total_freed += task_result.get('total_saved_mb', 0)
            
            self.exit_maintenance_mode()
            
            return {
                'status': 'success',
                'maintenance_results': results,
                'total_space_freed_mb': total_freed,
                'maintenance_duration': time.time() - (self.maintenance_start_time or time.time())
            }
            
        except Exception as e:
            self.exit_maintenance_mode()
            logger.error(f"Full maintenance failed: {e}")
            return {'status': 'error', 'message': str(e)}


class EnterpriseSystemManager:
    """Comprehensive enterprise system management"""
    
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.monitor = SystemMonitor()
        self.process_manager = ProcessManager()
        self.maintenance_manager = MaintenanceManager(project_root)
        self.shutdown_handlers = []
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.shutdown()
    
    def initialize(self) -> bool:
        """Initialize system management"""
        try:
            logger.info("Initializing enterprise system manager")
            
            # Setup monitoring alerts
            self.monitor.add_alert_callback(self._health_alert_handler)
            
            # Register core processes
            self._register_core_processes()
            
            # Start monitoring
            self.monitor.start_monitoring(interval_seconds=60)
            
            logger.info("Enterprise system manager initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize system manager: {e}")
            return False
    
    def _register_core_processes(self):
        """Register core system processes"""
        # Register main librarian process
        librarian_cmd = [
            str(self.project_root / 'venv' / 'bin' / 'python'),
            str(self.project_root / 'src' / 'librarian.py')
        ]
        self.process_manager.register_process(
            'librarian',
            librarian_cmd,
            working_dir=self.project_root,
            auto_restart=True,
            max_restarts=3
        )
        
        # Register knowledge base builder
        kb_cmd = [
            str(self.project_root / 'venv' / 'bin' / 'python'),
            str(self.project_root / 'src' / 'build_knowledgebase.py')
        ]
        self.process_manager.register_process(
            'knowledge_base_builder',
            kb_cmd,
            working_dir=self.project_root,
            auto_restart=False
        )
    
    def _health_alert_handler(self, health: SystemHealth):
        """Handle system health alerts"""
        if health.health_score < 50:
            logger.critical(f"Critical system health: {health.health_score:.1f}%")
            logger.critical(f"Issues: {', '.join(health.issues)}")
            
            # Consider entering emergency maintenance mode
            if health.health_score < 25:
                self.maintenance_manager.enter_maintenance_mode(
                    MaintenanceMode.EMERGENCY,
                    f"Critical health score: {health.health_score:.1f}%"
                )
        elif health.health_score < 70:
            logger.warning(f"Poor system health: {health.health_score:.1f}%")
            logger.warning(f"Issues: {', '.join(health.issues)}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        try:
            health = self.monitor.get_system_health()
            processes = self.process_manager.get_all_processes()
            
            return {
                'timestamp': time.time(),
                'system_health': asdict(health),
                'processes': {name: asdict(info) for name, info in processes.items()},
                'maintenance_mode': self.maintenance_manager.maintenance_mode.value,
                'project_root': str(self.project_root),
                'monitoring_active': self.monitor.monitoring_active
            }
            
        except Exception as e:
            logger.error(f"Failed to get system status: {e}")
            return {'error': str(e), 'timestamp': time.time()}
    
    def perform_maintenance(self, maintenance_type: str = 'full') -> Dict[str, Any]:
        """Perform system maintenance"""
        try:
            if maintenance_type == 'full':
                return self.maintenance_manager.run_full_maintenance()
            elif maintenance_type == 'logs':
                return self.maintenance_manager.cleanup_logs()
            elif maintenance_type == 'temp':
                return self.maintenance_manager.cleanup_temp_files()
            elif maintenance_type == 'database':
                return self.maintenance_manager.optimize_database()
            else:
                return {'status': 'error', 'message': f'Unknown maintenance type: {maintenance_type}'}
                
        except Exception as e:
            logger.error(f"Maintenance failed: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def add_shutdown_handler(self, handler: Callable[[], None]):
        """Add shutdown handler"""
        self.shutdown_handlers.append(handler)
    
    def shutdown(self):
        """Graceful system shutdown"""
        try:
            logger.info("Starting graceful system shutdown")
            
            # Stop monitoring
            self.monitor.stop_monitoring()
            
            # Stop all managed processes
            for name in list(self.process_manager.process_configs.keys()):
                self.process_manager.stop_process(name)
            
            # Exit maintenance mode if active
            if self.maintenance_manager.is_in_maintenance():
                self.maintenance_manager.exit_maintenance_mode()
            
            # Run shutdown handlers
            for handler in self.shutdown_handlers:
                try:
                    handler()
                except Exception as e:
                    logger.error(f"Shutdown handler failed: {e}")
            
            logger.info("System shutdown completed")
            
        except Exception as e:
            logger.error(f"Shutdown failed: {e}")


def main():
    """Main system manager entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enterprise System Manager')
    parser.add_argument('--project-root', type=Path, default=Path.cwd(), help='Project root directory')
    parser.add_argument('--status', action='store_true', help='Show system status')
    parser.add_argument('--maintenance', choices=['full', 'logs', 'temp', 'database'], help='Run maintenance')
    parser.add_argument('--monitor', action='store_true', help='Start monitoring mode')
    
    args = parser.parse_args()
    
    manager = EnterpriseSystemManager(args.project_root)
    
    if not manager.initialize():
        logger.error("Failed to initialize system manager")
        sys.exit(1)
    
    if args.status:
        status = manager.get_system_status()
        print(json.dumps(status, indent=2, default=str))
        return
    
    if args.maintenance:
        result = manager.perform_maintenance(args.maintenance)
        print(json.dumps(result, indent=2, default=str))
        return
    
    if args.monitor:
        logger.info("Starting monitoring mode (Ctrl+C to stop)")
        try:
            while True:
                time.sleep(60)
                status = manager.get_system_status()
                health = status.get('system_health', {})
                print(f"Health Score: {health.get('health_score', 0):.1f}% | "
                      f"CPU: {health.get('cpu_usage', 0):.1f}% | "
                      f"Memory: {health.get('memory_usage', 0):.1f}% | "
                      f"Disk: {health.get('disk_usage', 0):.1f}%")
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        finally:
            manager.shutdown()


if __name__ == '__main__':
    main()