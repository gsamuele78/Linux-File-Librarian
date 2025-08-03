#!/usr/bin/env python3
"""
Enterprise Logging Framework
Implements structured logging with security, performance monitoring, and compliance
"""

import json
import logging
import logging.handlers
import os
import re
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Union
import threading


class LogLevel(Enum):
    """Enterprise log levels"""
    TRACE = 5
    DEBUG = 10
    INFO = 20
    WARN = 30
    ERROR = 40
    CRITICAL = 50


class SecurityLevel(Enum):
    """Security classification levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


@dataclass
class LogContext:
    """Structured log context"""
    operation: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    component: Optional[str] = None
    security_level: SecurityLevel = SecurityLevel.INTERNAL


@dataclass
class PerformanceMetrics:
    """Performance metrics for operations"""
    operation: str
    duration_ms: float
    memory_mb: float
    cpu_percent: float
    success: bool
    error_type: Optional[str] = None


class SecuritySanitizer:
    """Sanitizes log messages to prevent injection attacks"""
    
    # Patterns that could indicate log injection attempts
    DANGEROUS_PATTERNS = [
        r'[\r\n]',  # Newline injection
        r'%[0-9a-fA-F]{2}',  # URL encoding
        r'\\[rnt]',  # Escape sequences
        r'<script',  # XSS attempts
        r'javascript:',  # JavaScript injection
        r'data:',  # Data URLs
    ]
    
    # PII patterns to redact
    PII_PATTERNS = [
        (r'\b\d{3}-\d{2}-\d{4}\b', '[SSN-REDACTED]'),  # SSN
        (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL-REDACTED]'),  # Email
        (r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b', '[CARD-REDACTED]'),  # Credit card
        (r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '[IP-REDACTED]'),  # IP address
    ]
    
    @classmethod
    def sanitize_message(cls, message: str, security_level: SecurityLevel = SecurityLevel.INTERNAL) -> str:
        """Sanitize log message for security"""
        if not isinstance(message, str):
            message = str(message)
        
        # Remove dangerous patterns
        for pattern in cls.DANGEROUS_PATTERNS:
            message = re.sub(pattern, '[SANITIZED]', message, flags=re.IGNORECASE)
        
        # Redact PII based on security level
        if security_level in [SecurityLevel.PUBLIC, SecurityLevel.INTERNAL]:
            for pattern, replacement in cls.PII_PATTERNS:
                message = re.sub(pattern, replacement, message)
        
        # Limit message length to prevent DoS
        if len(message) > 10000:
            message = message[:9950] + '[TRUNCATED]'
        
        return message


class StructuredFormatter(logging.Formatter):
    """JSON structured log formatter"""
    
    def __init__(self):
        super().__init__()
        self.hostname = os.uname().nodename
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON"""
        
        # Base log structure
        log_entry = {
            'timestamp': time.time(),
            'level': record.levelname,
            'logger': record.name,
            'message': SecuritySanitizer.sanitize_message(record.getMessage()),
            'hostname': self.hostname,
            'process_id': os.getpid(),
            'thread_id': threading.get_ident(),
        }
        
        # Add context if available
        if hasattr(record, 'context'):
            log_entry['context'] = asdict(record.context)
        
        # Add performance metrics if available
        if hasattr(record, 'metrics'):
            log_entry['metrics'] = asdict(record.metrics)
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': self.formatException(record.exc_info)
            }
        
        # Add file location for debugging
        if record.levelno >= logging.ERROR:
            log_entry['location'] = {
                'file': record.pathname,
                'line': record.lineno,
                'function': record.funcName
            }
        
        return json.dumps(log_entry, default=str)


class EnterpriseLogger:
    """Enterprise logging system with security and performance monitoring"""
    
    def __init__(self, name: str, log_dir: Path = Path("logs")):
        self.name = name
        self.log_dir = log_dir
        self.log_dir.mkdir(exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()
        
        # Thread-local context storage
        self._context = threading.local()
    
    def _setup_handlers(self):
        """Setup log handlers with rotation and formatting"""
        
        # Console handler with simple format
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler with structured format and rotation
        file_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / f"{self.name}.log",
            maxBytes=50 * 1024 * 1024,  # 50MB
            backupCount=10
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(StructuredFormatter())
        self.logger.addHandler(file_handler)
        
        # Error file handler
        error_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / f"{self.name}_errors.log",
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(StructuredFormatter())
        self.logger.addHandler(error_handler)
        
        # Security audit handler
        audit_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / f"{self.name}_audit.log",
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=20
        )
        audit_handler.setLevel(logging.INFO)
        audit_handler.setFormatter(StructuredFormatter())
        self.logger.addHandler(audit_handler)
    
    def set_context(self, context: LogContext):
        """Set logging context for current thread"""
        self._context.current = context
    
    def get_context(self) -> Optional[LogContext]:
        """Get current logging context"""
        return getattr(self._context, 'current', None)
    
    @contextmanager
    def context(self, context: LogContext):
        """Context manager for scoped logging context"""
        old_context = self.get_context()
        self.set_context(context)
        try:
            yield
        finally:
            if old_context:
                self.set_context(old_context)
            else:
                self._context.current = None
    
    def _log_with_context(self, level: int, message: str, **kwargs):
        """Log message with context and security sanitization"""
        
        # Create log record
        record = self.logger.makeRecord(
            self.logger.name,
            level,
            "",  # pathname
            0,   # lineno
            SecuritySanitizer.sanitize_message(message),
            (),  # args
            None  # exc_info
        )
        
        # Add context if available
        context = self.get_context()
        if context:
            record.context = context
        
        # Add custom attributes
        for key, value in kwargs.items():
            setattr(record, key, value)
        
        # Handle the record
        self.logger.handle(record)
    
    def trace(self, message: str, **kwargs):
        """Log trace message"""
        self._log_with_context(LogLevel.TRACE.value, message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self._log_with_context(LogLevel.DEBUG.value, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self._log_with_context(LogLevel.INFO.value, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self._log_with_context(LogLevel.WARN.value, message, **kwargs)
    
    def error(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """Log error message with optional exception"""
        if exception:
            kwargs['exc_info'] = (type(exception), exception, exception.__traceback__)
        self._log_with_context(LogLevel.ERROR.value, message, **kwargs)
    
    def critical(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """Log critical message"""
        if exception:
            kwargs['exc_info'] = (type(exception), exception, exception.__traceback__)
        self._log_with_context(LogLevel.CRITICAL.value, message, **kwargs)
    
    def audit(self, action: str, resource: str, result: str, **kwargs):
        """Log security audit event"""
        audit_message = f"AUDIT: {action} on {resource} - {result}"
        context = LogContext(
            operation="security_audit",
            security_level=SecurityLevel.RESTRICTED
        )
        
        with self.context(context):
            self.info(audit_message, **kwargs)
    
    @contextmanager
    def performance_monitor(self, operation: str):
        """Monitor operation performance"""
        import psutil
        
        start_time = time.time()
        process = psutil.Process()
        start_memory = process.memory_info().rss / (1024 * 1024)  # MB
        start_cpu = process.cpu_percent()
        
        success = True
        error_type = None
        
        try:
            yield
        except Exception as e:
            success = False
            error_type = type(e).__name__
            raise
        finally:
            end_time = time.time()
            end_memory = process.memory_info().rss / (1024 * 1024)  # MB
            end_cpu = process.cpu_percent()
            
            metrics = PerformanceMetrics(
                operation=operation,
                duration_ms=(end_time - start_time) * 1000,
                memory_mb=end_memory - start_memory,
                cpu_percent=max(start_cpu, end_cpu),
                success=success,
                error_type=error_type
            )
            
            # Log performance metrics
            self._log_with_context(
                LogLevel.INFO.value,
                f"Performance: {operation} completed in {metrics.duration_ms:.2f}ms",
                metrics=metrics
            )


# Global logger registry
_loggers: Dict[str, EnterpriseLogger] = {}
_lock = threading.Lock()


def get_logger(name: str, log_dir: Path = Path("logs")) -> EnterpriseLogger:
    """Get or create enterprise logger instance"""
    with _lock:
        if name not in _loggers:
            _loggers[name] = EnterpriseLogger(name, log_dir)
        return _loggers[name]


def configure_root_logging(log_dir: Path = Path("logs"), level: str = "INFO"):
    """Configure root logging for the application"""
    
    # Ensure log directory exists
    log_dir.mkdir(exist_ok=True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add enterprise handlers
    enterprise_logger = get_logger("root", log_dir)
    for handler in enterprise_logger.logger.handlers:
        root_logger.addHandler(handler)


# Convenience functions for backward compatibility
def log_info(message: str, **kwargs):
    """Log info message using default logger"""
    logger = get_logger("default")
    logger.info(message, **kwargs)


def log_error(message: str, exception: Optional[Exception] = None, **kwargs):
    """Log error message using default logger"""
    logger = get_logger("default")
    logger.error(message, exception=exception, **kwargs)


def log_warning(message: str, **kwargs):
    """Log warning message using default logger"""
    logger = get_logger("default")
    logger.warning(message, **kwargs)