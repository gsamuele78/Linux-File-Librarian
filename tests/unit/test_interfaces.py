#!/usr/bin/env python3
"""
Unit Tests for Enterprise Interfaces

Tests the core interface definitions and contract compliance.
"""

import pytest
from pathlib import Path
from typing import Dict, List, Optional, Any
from unittest.mock import Mock, patch

from src.interfaces.base import (
    IConfigurationManager,
    IClassificationService,
    IMetadataProvider,
    IDeduplicationService,
    IRepairService,
    ICopyService,
    IEnterpriseProcessor,
    ProcessingStatus,
    ProcessingResult
)


class TestProcessingResult:
    """Test ProcessingResult dataclass"""
    
    def test_processing_result_creation(self):
        """Test ProcessingResult creation"""
        result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            message="Processing completed successfully"
        )
        
        assert result.status == ProcessingStatus.COMPLETED
        assert result.message == "Processing completed successfully"
        assert result.data is None
        assert result.error is None
    
    def test_processing_result_with_data(self):
        """Test ProcessingResult with data"""
        test_data = {"files_processed": 10, "success_rate": 0.9}
        result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            message="Processing completed",
            data=test_data
        )
        
        assert result.data == test_data
    
    def test_processing_result_with_error(self):
        """Test ProcessingResult with error"""
        test_error = Exception("Test error")
        result = ProcessingResult(
            status=ProcessingStatus.FAILED,
            message="Processing failed",
            error=test_error
        )
        
        assert result.error == test_error


class MockConfigurationManager(IConfigurationManager):
    """Mock implementation for testing"""
    
    def __init__(self):
        self.config = {}
    
    def load_config(self, config_path: Path) -> Dict[str, Any]:
        return {"test": "value"}
    
    def get_setting(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)
    
    def validate_config(self) -> bool:
        return True


class MockClassificationService(IClassificationService):
    """Mock implementation for testing"""
    
    def classify_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "category": "test",
            "confidence": 0.9,
            "metadata": {}
        }
    
    def get_classification_confidence(self, file_info: Dict[str, Any]) -> float:
        return 0.9


class MockMetadataProvider(IMetadataProvider):
    """Mock implementation for testing"""
    
    def search(self, query: str, media_type: str) -> List[Dict[str, Any]]:
        return [{"id": "1", "title": "Test Result"}]
    
    def get_details(self, provider_id: str, media_type: str) -> Optional[Dict[str, Any]]:
        return {"title": "Test Details", "description": "Test description"}
    
    def get_artwork(self, provider_id: str, media_type: str) -> Dict[str, str]:
        return {"poster": "http://example.com/poster.jpg"}


class TestInterfaceImplementations:
    """Test interface implementations"""
    
    def test_configuration_manager_interface(self):
        """Test IConfigurationManager implementation"""
        manager = MockConfigurationManager()
        
        # Test interface methods
        config = manager.load_config(Path("test.ini"))
        assert isinstance(config, dict)
        
        setting = manager.get_setting("test_key", "default")
        assert setting == "default"
        
        is_valid = manager.validate_config()
        assert isinstance(is_valid, bool)
    
    def test_classification_service_interface(self):
        """Test IClassificationService implementation"""
        service = MockClassificationService()
        
        # Test classification
        file_info = {"path": "/test/file.pdf"}
        result = service.classify_file(file_info)
        
        assert isinstance(result, dict)
        assert "category" in result
        assert "confidence" in result
        
        # Test confidence
        confidence = service.get_classification_confidence(file_info)
        assert isinstance(confidence, float)
        assert 0.0 <= confidence <= 1.0
    
    def test_metadata_provider_interface(self):
        """Test IMetadataProvider implementation"""
        provider = MockMetadataProvider()
        
        # Test search
        results = provider.search("test query", "book")
        assert isinstance(results, list)
        
        # Test details
        details = provider.get_details("1", "book")
        assert isinstance(details, dict) or details is None
        
        # Test artwork
        artwork = provider.get_artwork("1", "book")
        assert isinstance(artwork, dict)


class TestInterfaceContracts:
    """Test interface contract compliance"""
    
    def test_configuration_manager_contract(self):
        """Test IConfigurationManager contract"""
        # Verify abstract methods exist
        assert hasattr(IConfigurationManager, 'load_config')
        assert hasattr(IConfigurationManager, 'get_setting')
        assert hasattr(IConfigurationManager, 'validate_config')
        
        # Verify methods are abstract
        with pytest.raises(TypeError):
            IConfigurationManager()
    
    def test_classification_service_contract(self):
        """Test IClassificationService contract"""
        # Verify abstract methods exist
        assert hasattr(IClassificationService, 'classify_file')
        assert hasattr(IClassificationService, 'get_classification_confidence')
        
        # Verify methods are abstract
        with pytest.raises(TypeError):
            IClassificationService()
    
    def test_metadata_provider_contract(self):
        """Test IMetadataProvider contract"""
        # Verify abstract methods exist
        assert hasattr(IMetadataProvider, 'search')
        assert hasattr(IMetadataProvider, 'get_details')
        assert hasattr(IMetadataProvider, 'get_artwork')
        
        # Verify methods are abstract
        with pytest.raises(TypeError):
            IMetadataProvider()


class TestProcessingStatus:
    """Test ProcessingStatus enum"""
    
    def test_processing_status_values(self):
        """Test ProcessingStatus enum values"""
        assert ProcessingStatus.PENDING.value == "pending"
        assert ProcessingStatus.PROCESSING.value == "processing"
        assert ProcessingStatus.COMPLETED.value == "completed"
        assert ProcessingStatus.FAILED.value == "failed"
        assert ProcessingStatus.SKIPPED.value == "skipped"
    
    def test_processing_status_comparison(self):
        """Test ProcessingStatus comparison"""
        assert ProcessingStatus.PENDING != ProcessingStatus.COMPLETED
        assert ProcessingStatus.FAILED == ProcessingStatus.FAILED


if __name__ == "__main__":
    pytest.main([__file__])