#!/usr/bin/env python3
"""
Enterprise Error Handling Framework
Implements comprehensive error handling, recovery strategies, and resilience patterns
"""

import functools
import time
import traceback
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type, Union
import threading

from .enterprise_logging import get_logger, LogContext, SecurityLevel

logger = get_logger(__name__)


class ErrorSeverity(Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error categories for classification"""
    SYSTEM = "system"
    NETWORK = "network"
    SECURITY = "security"
    VALIDATION = "validation"
    BUSINESS_LOGIC = "business_logic"
    EXTERNAL_SERVICE = "external_service"
    RESOURCE = "resource"


@dataclass
class ErrorContext:
    """Comprehensive error context"""
    operation: str
    component: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None


@dataclass
class ErrorMetrics:
    """Error metrics for monitoring"""
    error_type: str
    category: ErrorCategory
    severity: ErrorSeverity
    count: int = 1
    first_occurrence: float = 0
    last_occurrence: float = 0
    
    def __post_init__(self):
        if self.first_occurrence == 0:
            self.first_occurrence = time.time()
        self.last_occurrence = time.time()


class EnterpriseException(Exception):
    """Base enterprise exception with enhanced context"""
    
    def __init__(
        self,
        message: str,
        category: ErrorCategory = ErrorCategory.SYSTEM,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        context: Optional[ErrorContext] = None,
        cause: Optional[Exception] = None,
        recoverable: bool = True
    ):
        super().__init__(message)
        self.category = category
        self.severity = severity
        self.context = context
        self.cause = cause
        self.recoverable = recoverable
        self.timestamp = time.time()
        self.error_id = f"{int(self.timestamp)}_{id(self)}"


class ValidationError(EnterpriseException):
    """Validation error with field-specific context"""
    
    def __init__(self, message: str, field: str = None, value: Any = None, **kwargs):
        super().__init__(message, category=ErrorCategory.VALIDATION, **kwargs)
        self.field = field
        self.value = value


class SecurityError(EnterpriseException):
    """Security-related error"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.SECURITY,
            severity=ErrorSeverity.HIGH,
            recoverable=False,
            **kwargs
        )


class ResourceError(EnterpriseException):
    """Resource-related error (memory, disk, network)"""
    
    def __init__(self, message: str, resource_type: str = None, **kwargs):
        super().__init__(message, category=ErrorCategory.RESOURCE, **kwargs)
        self.resource_type = resource_type


class ExternalServiceError(EnterpriseException):
    """External service error with retry capability"""
    
    def __init__(self, message: str, service_name: str = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.EXTERNAL_SERVICE,
            **kwargs
        )
        self.service_name = service_name


class RecoveryStrategy(ABC):
    """Abstract recovery strategy"""
    
    @abstractmethod
    def can_recover(self, error: Exception, context: ErrorContext) -> bool:
        """Check if this strategy can recover from the error"""
        pass
    
    @abstractmethod
    def recover(self, error: Exception, context: ErrorContext) -> Any:
        """Attempt to recover from the error"""
        pass


class RetryStrategy(RecoveryStrategy):
    """Retry strategy with exponential backoff"""
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        retryable_exceptions: List[Type[Exception]] = None
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.retryable_exceptions = retryable_exceptions or [
            ConnectionError,
            TimeoutError,
            ExternalServiceError,
            ResourceError
        ]
    
    def can_recover(self, error: Exception, context: ErrorContext) -> bool:
        """Check if error is retryable"""
        return any(isinstance(error, exc_type) for exc_type in self.retryable_exceptions)
    
    def recover(self, error: Exception, context: ErrorContext) -> Any:
        """Implement retry with exponential backoff"""
        for attempt in range(self.max_attempts):
            if attempt > 0:
                delay = min(
                    self.base_delay * (self.backoff_factor ** (attempt - 1)),
                    self.max_delay
                )
                logger.info(f"Retrying operation after {delay:.2f}s (attempt {attempt + 1}/{self.max_attempts})")
                time.sleep(delay)
            
            try:
                # This would be implemented by the calling code
                # The strategy just provides the retry logic
                return None
            except Exception as retry_error:
                if attempt == self.max_attempts - 1:
                    raise retry_error
                
                logger.warning(f"Retry attempt {attempt + 1} failed: {retry_error}")


class FallbackStrategy(RecoveryStrategy):
    """Fallback strategy with alternative implementations"""
    
    def __init__(self, fallback_func: Callable):
        self.fallback_func = fallback_func
    
    def can_recover(self, error: Exception, context: ErrorContext) -> bool:
        """Always can provide fallback"""
        return True
    
    def recover(self, error: Exception, context: ErrorContext) -> Any:
        """Execute fallback function"""
        logger.info(f"Executing fallback strategy for {context.operation}")
        return self.fallback_func(error, context)


class CircuitBreakerStrategy(RecoveryStrategy):
    """Circuit breaker pattern for external services"""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        timeout: float = 60.0,
        expected_exception: Type[Exception] = Exception
    ):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = threading.Lock()
    
    def can_recover(self, error: Exception, context: ErrorContext) -> bool:
        """Check if circuit breaker should handle this error"""
        return isinstance(error, self.expected_exception)
    
    def recover(self, error: Exception, context: ErrorContext) -> Any:
        """Implement circuit breaker logic"""
        with self._lock:
            if self.state == "OPEN":
                if self.last_failure_time and time.time() - self.last_failure_time > self.timeout:
                    self.state = "HALF_OPEN"
                    logger.info("Circuit breaker transitioning to HALF_OPEN")
                else:
                    raise EnterpriseException(
                        "Circuit breaker is OPEN - service unavailable",
                        category=ErrorCategory.EXTERNAL_SERVICE,
                        severity=ErrorSeverity.HIGH
                    )
            
            # Record failure
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.error(f"Circuit breaker OPEN after {self.failure_count} failures")
            
            raise error


class ErrorHandler:
    """Enterprise error handler with recovery strategies"""
    
    def __init__(self):
        self.recovery_strategies: List[RecoveryStrategy] = []
        self.error_metrics: Dict[str, ErrorMetrics] = {}
        self._lock = threading.Lock()
    
    def add_recovery_strategy(self, strategy: RecoveryStrategy):
        """Add recovery strategy"""
        self.recovery_strategies.append(strategy)
    
    def handle_error(
        self,
        error: Exception,
        context: ErrorContext,
        raise_on_failure: bool = True
    ) -> Optional[Any]:
        """Handle error with recovery strategies"""
        
        # Log error with context
        self._log_error(error, context)
        
        # Update error metrics
        self._update_error_metrics(error)
        
        # Try recovery strategies
        for strategy in self.recovery_strategies:
            try:
                if strategy.can_recover(error, context):
                    logger.info(f"Attempting recovery with {strategy.__class__.__name__}")
                    result = strategy.recover(error, context)
                    logger.info(f"Recovery successful with {strategy.__class__.__name__}")
                    return result
            except Exception as recovery_error:
                safe_strategy = str(strategy.__class__.__name__)[:50]
                safe_error = str(recovery_error)[:100]
                logger.error(f"Recovery strategy {safe_strategy} failed: {safe_error}")
        
        # No recovery possible
        logger.error(f"No recovery strategy succeeded for error: {error}")
        
        if raise_on_failure:
            if isinstance(error, EnterpriseException):
                raise error
            else:
                raise EnterpriseException(
                    f"Unhandled error: {error}",
                    cause=error,
                    context=context
                )
        
        return None
    
    def _log_error(self, error: Exception, context: ErrorContext):
        """Log error with appropriate security level"""
        
        security_level = SecurityLevel.INTERNAL
        if isinstance(error, SecurityError):
            security_level = SecurityLevel.RESTRICTED
        
        log_context = LogContext(
            operation=context.operation,
            component=context.component,
            user_id=context.user_id,
            session_id=context.session_id,
            request_id=context.request_id,
            security_level=security_level
        )
        
        with logger.context(log_context):
            if isinstance(error, EnterpriseException):
                logger.error(
                    f"Enterprise error in {context.operation}: {error}",
                    exception=error,
                    error_id=error.error_id,
                    category=error.category.value,
                    severity=error.severity.value,
                    recoverable=error.recoverable
                )
            else:
                logger.error(
                    f"System error in {context.operation}: {error}",
                    exception=error,
                    error_type=type(error).__name__
                )
    
    def _update_error_metrics(self, error: Exception):
        """Update error metrics for monitoring"""
        error_type = type(error).__name__
        
        with self._lock:
            if error_type not in self.error_metrics:
                category = ErrorCategory.SYSTEM
                severity = ErrorSeverity.MEDIUM
                
                if isinstance(error, EnterpriseException):
                    category = error.category
                    severity = error.severity
                
                self.error_metrics[error_type] = ErrorMetrics(
                    error_type=error_type,
                    category=category,
                    severity=severity
                )
            else:
                metrics = self.error_metrics[error_type]
                metrics.count += 1
                metrics.last_occurrence = time.time()
    
    def get_error_metrics(self) -> Dict[str, ErrorMetrics]:
        """Get error metrics for monitoring"""
        with self._lock:
            return self.error_metrics.copy()


# Global error handler instance
_error_handler = ErrorHandler()


def get_error_handler() -> ErrorHandler:
    """Get global error handler instance"""
    return _error_handler


def with_error_handling(
    operation: str,
    component: str,
    recovery_strategies: List[RecoveryStrategy] = None,
    raise_on_failure: bool = True
):
    """Decorator for automatic error handling"""
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            context = ErrorContext(
                operation=operation,
                component=component,
                request_id=kwargs.pop('request_id', None),
                user_id=kwargs.pop('user_id', None),
                session_id=kwargs.pop('session_id', None),
                additional_data=kwargs.pop('error_context', None)
            )
            
            # Add temporary recovery strategies
            handler = get_error_handler()
            original_strategies = handler.recovery_strategies.copy()
            
            if recovery_strategies:
                for strategy in recovery_strategies:
                    handler.add_recovery_strategy(strategy)
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                result = handler.handle_error(e, context, raise_on_failure)
                return result
            finally:
                # Restore original strategies
                handler.recovery_strategies = original_strategies
        
        return wrapper
    return decorator


@contextmanager
def error_context(
    operation: str,
    component: str,
    user_id: str = None,
    session_id: str = None,
    request_id: str = None
):
    """Context manager for error handling"""
    context = ErrorContext(
        operation=operation,
        component=component,
        user_id=user_id,
        session_id=session_id,
        request_id=request_id
    )
    
    try:
        yield context
    except Exception as e:
        handler = get_error_handler()
        handler.handle_error(e, context)


def safe_execute(
    func: Callable,
    *args,
    operation: str = "unknown",
    component: str = "unknown",
    default_return: Any = None,
    **kwargs
) -> Any:
    """Safely execute function with error handling"""
    
    context = ErrorContext(operation=operation, component=component)
    
    try:
        return func(*args, **kwargs)
    except Exception as e:
        handler = get_error_handler()
        result = handler.handle_error(e, context, raise_on_failure=False)
        return result if result is not None else default_return


# Initialize default recovery strategies
def initialize_default_strategies():
    """Initialize default recovery strategies"""
    handler = get_error_handler()
    
    # Add retry strategy for common recoverable errors
    retry_strategy = RetryStrategy(
        max_attempts=3,
        base_delay=1.0,
        retryable_exceptions=[
            ConnectionError,
            TimeoutError,
            ExternalServiceError,
            ResourceError
        ]
    )
    handler.add_recovery_strategy(retry_strategy)
    
    # Add circuit breaker for external services
    circuit_breaker = CircuitBreakerStrategy(
        failure_threshold=5,
        timeout=60.0,
        expected_exception=ExternalServiceError
    )
    handler.add_recovery_strategy(circuit_breaker)


# Initialize on module import
initialize_default_strategies()