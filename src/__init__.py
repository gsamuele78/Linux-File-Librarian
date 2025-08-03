#!/usr/bin/env python3
"""
Linux File Librarian - Enterprise Edition

An enterprise-grade file management system designed with system engineer principles.
Transforms chaotic file collections into clean, searchable, and intelligently 
organized libraries with minimal user intervention.

Version: 2.0.0-enterprise
License: MIT
"""

__version__ = "2.0.0-enterprise"
__author__ = "Linux File Librarian Team"
__license__ = "MIT"
__description__ = "Enterprise-grade file management and organization system"

# Core modules
from src.core.config_loader import ConfigLoader
from src.core.classifier import Classifier
from src.core.library_builder import LibraryBuilder

# Enterprise services
from src.enterprise.enterprise_integration import EnterpriseProcessor
from src.enterprise.professional_orchestrator import ProfessionalOrchestrator
from src.enterprise.enterprise_config_manager import EnterpriseConfigManager

# Interfaces
from src.interfaces.base import (
    IConfigurationManager,
    IClassificationService,
    IMetadataProvider,
    IEnterpriseProcessor,
    ProcessingStatus,
    ProcessingResult
)

__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__license__",
    "__description__",
    
    # Core classes
    "ConfigLoader",
    "Classifier", 
    "LibraryBuilder",
    
    # Enterprise classes
    "EnterpriseProcessor",
    "ProfessionalOrchestrator",
    "EnterpriseConfigManager",
    
    # Interfaces
    "IConfigurationManager",
    "IClassificationService", 
    "IMetadataProvider",
    "IEnterpriseProcessor",
    "ProcessingStatus",
    "ProcessingResult"
]