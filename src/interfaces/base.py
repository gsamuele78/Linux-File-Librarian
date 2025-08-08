#!/usr/bin/env python3
"""
Base Interfaces for Enterprise Architecture

Defines core interfaces and abstract base classes for the Linux File Librarian
enterprise system following SOLID principles and clean architecture patterns.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Protocol
from pathlib import Path
from dataclasses import dataclass
from enum import Enum


class ProcessingStatus(Enum):
    """Processing status enumeration"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ProcessingResult:
    """Standard processing result"""
    status: ProcessingStatus
    message: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[Exception] = None


class IConfigurationManager(ABC):
    """Configuration management interface"""
    
    @abstractmethod
    def load_config(self, config_path: Path) -> Dict[str, Any]:
        """Load configuration from file"""
        pass
    
    @abstractmethod
    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get configuration setting"""
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        """Validate configuration"""
        pass


class IFileScanner(ABC):
    """File scanning interface"""
    
    @abstractmethod
    def scan_directory(self, path: Path) -> List[Dict[str, Any]]:
        """Scan directory for files"""
        pass
    
    @abstractmethod
    def filter_files(self, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter files based on criteria"""
        pass


class IClassificationService(ABC):
    """File classification interface"""
    
    @abstractmethod
    def classify_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Classify a single file"""
        pass
    
    @abstractmethod
    def get_classification_confidence(self, file_info: Dict[str, Any]) -> float:
        """Get classification confidence score"""
        pass


class IMetadataProvider(ABC):
    """Metadata provider interface"""
    
    @abstractmethod
    def search(self, query: str, media_type: str) -> List[Dict[str, Any]]:
        """Search for metadata"""
        pass
    
    @abstractmethod
    def get_details(self, provider_id: str, media_type: str) -> Optional[Dict[str, Any]]:
        """Get detailed metadata"""
        pass
    
    @abstractmethod
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get artwork URLs"""
        pass


class IDeduplicationService(ABC):
    """Deduplication service interface"""
    
    @abstractmethod
    def find_duplicates(self, files: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Find duplicate files"""
        pass
    
    @abstractmethod
    def select_best_file(self, duplicates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Select best file from duplicates"""
        pass


class IRepairService(ABC):
    """File repair service interface"""
    
    @abstractmethod
    def can_repair(self, file_path: Path) -> bool:
        """Check if file can be repaired"""
        pass
    
    @abstractmethod
    def repair_file(self, file_path: Path, output_dir: Path) -> ProcessingResult:
        """Repair corrupted file"""
        pass


class ICopyService(ABC):
    """File copying service interface"""
    
    @abstractmethod
    def copy_file(self, source: Path, destination: Path, metadata: Dict[str, Any]) -> ProcessingResult:
        """Copy file with metadata"""
        pass
    
    @abstractmethod
    def generate_nfo(self, file_path: Path, metadata: Dict[str, Any]) -> bool:
        """Generate NFO file"""
        pass


class IEnterpriseProcessor(ABC):
    """Enterprise processing pipeline interface"""
    
    @abstractmethod
    async def process_files(self, files: List[Dict[str, Any]], destination: Path) -> ProcessingResult:
        """Process files through enterprise pipeline"""
        pass
    
    @abstractmethod
    def get_processing_status(self) -> Dict[str, Any]:
        """Get current processing status"""
        pass
    
    @abstractmethod
    def generate_report(self) -> Dict[str, Any]:
        """Generate processing report"""
        pass


class ILogger(Protocol):
    """Logging interface"""
    
    def info(self, message: str, **kwargs) -> None: ...
    def warning(self, message: str, **kwargs) -> None: ...
    def error(self, message: str, **kwargs) -> None: ...
    def debug(self, message: str, **kwargs) -> None: ...


class IMetricsCollector(ABC):
    """Metrics collection interface"""
    
    @abstractmethod
    def record_metric(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Record a metric"""
        pass
    
    @abstractmethod
    def increment_counter(self, name: str, tags: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter"""
        pass
    
    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """Get collected metrics"""
        pass


class IHealthCheck(ABC):
    """Health check interface"""
    
    @abstractmethod
    def check_health(self) -> Dict[str, Any]:
        """Perform health check"""
        pass
    
    @abstractmethod
    def get_dependencies_status(self) -> Dict[str, bool]:
        """Get status of dependencies"""
        pass