# API Reference

## Core Interfaces

### IConfigurationManager
Configuration management interface for enterprise settings.

```python
from src.interfaces.base import IConfigurationManager

class ConfigurationManager(IConfigurationManager):
    def load_config(self, config_path: Path) -> Dict[str, Any]:
        """Load configuration from file"""
        
    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get configuration setting"""
        
    def validate_config(self) -> bool:
        """Validate configuration"""
```

### IClassificationService
File classification and categorization interface.

```python
from src.interfaces.base import IClassificationService

class ClassificationService(IClassificationService):
    def classify_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Classify a single file"""
        
    def get_classification_confidence(self, file_info: Dict[str, Any]) -> float:
        """Get classification confidence score"""
```

### IMetadataProvider
External metadata provider interface.

```python
from src.interfaces.base import IMetadataProvider

class MetadataProvider(IMetadataProvider):
    def search(self, query: str, media_type: str) -> List[Dict[str, Any]]:
        """Search for metadata"""
        
    def get_details(self, provider_id: str, media_type: str) -> Optional[Dict[str, Any]]:
        """Get detailed metadata"""
        
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        """Get artwork URLs"""
```

## Enterprise Services

### EnterpriseProcessor
Main processing pipeline orchestrator.

```python
from src.enterprise.enterprise_integration import EnterpriseProcessor

processor = EnterpriseProcessor(config)
result = await processor.process_files(files, destination_path)
```

### ProfessionalOrchestrator
High-level file processing orchestration.

```python
from src.enterprise.professional_orchestrator import ProfessionalOrchestrator

orchestrator = ProfessionalOrchestrator()
orchestrator.run_professional_processing()
```

## Provider System

### Gaming Providers
Specialized TTRPG content providers.

```python
from src.providers.gaming_providers import (
    PaizoProvider, WizardsProvider, DriveThruRPGProvider,
    GiochiUnitiProvider, AcheronGamesProvider
)

# Initialize providers
paizo = PaizoProvider()
results = paizo.search("Pathfinder Core Rulebook", "gaming")
```

### Internet Providers
Free internet metadata providers.

```python
from src.providers.additional_providers import (
    CrossRefProvider, ArxivProvider, GoogleBooksProvider
)

# Academic paper search
crossref = CrossRefProvider()
papers = crossref.search("machine learning", "academic")
```

## Service Layer

### Enhanced Services
Advanced file processing services.

```python
from src.services.enhanced_media_manager import EnhancedMediaManager
from src.services.enhanced_deduplication import EnhancedDeduplicationManager
from src.services.enhanced_repair_utils import EnhancedRepairManager

# Media processing
media_manager = EnhancedMediaManager()
media_type = media_manager.detect_media_type(file_path)

# Deduplication
dedup_manager = EnhancedDeduplicationManager()
result = dedup_manager.deduplicate_files(file_list)

# File repair
repair_manager = EnhancedRepairManager()
repair_result = repair_manager.repair_file(file_path, output_dir)
```

## Configuration API

### Configuration Structure
```python
config = {
    'paths': {
        'source_paths': ['/path1', '/path2'],
        'library_root': '/organized/library'
    },
    'processing': {
        'max_workers': 4,
        'enable_internet_enrichment': True,
        'download_artwork': True
    },
    'providers': {
        'enable_paizo': True,
        'enable_giochi_uniti': True,
        'tmdb_api_key': 'your_key'
    }
}
```

### Loading Configuration
```python
from src.core.config_loader import ConfigLoader

loader = ConfigLoader()
config = loader.load_config('config/config.ini')
```

## Error Handling

### Enterprise Error Handling
```python
from src.enterprise.enterprise_error_handling import (
    with_error_handling, EnterpriseException
)

@with_error_handling(operation="file_processing", component="MyService")
def process_file(file_path):
    # Processing logic
    pass
```

### Custom Exceptions
```python
from src.enterprise.enterprise_error_handling import EnterpriseException

class CustomProcessingError(EnterpriseException):
    def __init__(self, message: str, file_path: str):
        super().__init__(message)
        self.file_path = file_path
```

## Logging API

### Enterprise Logging
```python
from src.enterprise.enterprise_logging import get_logger, LogContext

logger = get_logger(__name__)

with logger.context(LogContext(operation="file_processing")):
    logger.info("Processing started")
    logger.error("Processing failed", error=exception)
```

### Structured Logging
```python
logger.info("File processed", 
           file_path="/path/to/file",
           processing_time=1.23,
           success=True)
```

## Metrics and Monitoring

### Metrics Collection
```python
from src.interfaces.base import IMetricsCollector

class MetricsCollector(IMetricsCollector):
    def record_metric(self, name: str, value: float, tags: Dict[str, str] = None):
        """Record a metric value"""
        
    def increment_counter(self, name: str, tags: Dict[str, str] = None):
        """Increment a counter"""
```

### Health Checks
```python
from src.interfaces.base import IHealthCheck

class HealthCheck(IHealthCheck):
    def check_health(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        
    def get_dependencies_status(self) -> Dict[str, bool]:
        """Check status of external dependencies"""
```

## Testing API

### Unit Testing
```python
import pytest
from src.services.enhanced_classification_service import EnhancedClassificationService

def test_classification_service():
    service = EnhancedClassificationService({})
    result = service.classify_file({'path': '/test/file.pdf'})
    assert result['category'] is not None
```

### Integration Testing
```python
from tests.integration.test_enterprise_pipeline import EnterprisePipelineTest

class TestFullPipeline(EnterprisePipelineTest):
    def test_complete_processing(self):
        result = self.run_full_pipeline(test_files)
        assert result.success_rate > 0.9
```

## Extension Points

### Custom Providers
```python
from src.interfaces.base import IMetadataProvider

class CustomProvider(IMetadataProvider):
    def search(self, query: str, media_type: str) -> List[Dict[str, Any]]:
        # Custom search implementation
        pass
        
    def get_details(self, provider_id: str, media_type: str) -> Optional[Dict[str, Any]]:
        # Custom details implementation
        pass
```

### Custom Services
```python
from src.interfaces.base import IClassificationService

class CustomClassificationService(IClassificationService):
    def classify_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        # Custom classification logic
        pass
```

## Performance Optimization

### Caching
```python
from functools import lru_cache

@lru_cache(maxsize=1000)
def expensive_operation(file_path: str) -> Dict[str, Any]:
    # Cached expensive operation
    pass
```

### Async Processing
```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

async def process_files_async(files: List[Dict[str, Any]]):
    with ThreadPoolExecutor(max_workers=4) as executor:
        tasks = [executor.submit(process_file, file) for file in files]
        results = await asyncio.gather(*tasks)
    return results
```

This API reference provides the essential interfaces and usage patterns for extending and integrating with the Linux File Librarian enterprise system.