#!/usr/bin/env python3
"""
Enterprise Configuration Management
Implements secure, validated, and environment-aware configuration management
"""

import os
import json
from configparser import ConfigParser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

from .enterprise_logging import get_logger
from .enterprise_error_handling import ValidationError, SecurityError, EnterpriseException

logger = get_logger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration with validation"""
    url: str
    timeout: int = 30
    pool_size: int = 5
    
    def __post_init__(self):
        if not self.url:
            raise ValidationError("Database URL is required", field="database.url")
        
        # Validate URL format for security
        if self.url.startswith(('http://', 'https://')):
            parsed = urlparse(self.url)
            if not parsed.netloc:
                raise ValidationError("Invalid database URL format", field="database.url")


@dataclass
class ProcessingConfig:
    """Processing configuration with performance tuning"""
    max_workers: int = 4
    batch_size: int = 100
    memory_limit_mb: int = 1000
    timeout_seconds: int = 300
    
    def __post_init__(self):
        if self.max_workers < 1 or self.max_workers > 32:
            raise ValidationError("max_workers must be between 1 and 32", field="processing.max_workers")
        
        if self.batch_size < 1 or self.batch_size > 10000:
            raise ValidationError("batch_size must be between 1 and 10000", field="processing.batch_size")
        
        if self.memory_limit_mb < 100:
            raise ValidationError("memory_limit_mb must be at least 100", field="processing.memory_limit_mb")


@dataclass
class SecurityConfig:
    """Security configuration"""
    enable_audit_logging: bool = True
    max_file_size_mb: int = 2048
    allowed_extensions: List[str] = field(default_factory=lambda: [
        '.pdf', '.txt', '.doc', '.docx', '.jpg', '.png', '.mp4', '.mp3'
    ])
    blocked_paths: List[str] = field(default_factory=lambda: [
        '/etc', '/var', '/usr', '/sys', '/proc'
    ])
    
    def __post_init__(self):
        if self.max_file_size_mb < 1:
            raise ValidationError("max_file_size_mb must be at least 1", field="security.max_file_size_mb")


@dataclass
class LibraryConfig:
    """Library configuration with path validation"""
    source_paths: List[str]
    library_root: str
    create_backups: bool = True
    preserve_structure: bool = False
    
    def __post_init__(self):
        if not self.source_paths:
            raise ValidationError("At least one source path is required", field="library.source_paths")
        
        if not self.library_root:
            raise ValidationError("Library root path is required", field="library.library_root")
        
        # Validate paths exist and are accessible
        for path in self.source_paths:
            path_obj = Path(path)
            if not path_obj.exists():
                raise ValidationError(f"Source path does not exist: {path}", field="library.source_paths")
            if not path_obj.is_dir():
                raise ValidationError(f"Source path is not a directory: {path}", field="library.source_paths")
        
        # Validate library root is writable
        library_path = Path(self.library_root)
        if library_path.exists() and not os.access(library_path, os.W_OK):
            raise ValidationError(f"Library root is not writable: {self.library_root}", field="library.library_root")


@dataclass
class EnterpriseConfig:
    """Complete enterprise configuration"""
    database: DatabaseConfig
    processing: ProcessingConfig
    security: SecurityConfig
    library: LibraryConfig
    environment: str = "production"
    debug: bool = False
    
    def __post_init__(self):
        if self.environment not in ["development", "testing", "staging", "production"]:
            raise ValidationError("Invalid environment", field="environment")


class ConfigurationValidator:
    """Validates configuration for security and correctness"""
    
    @staticmethod
    def validate_paths(paths: List[str]) -> List[str]:
        """Validate and sanitize file paths"""
        validated_paths = []
        
        for path in paths:
            # Resolve path to prevent directory traversal
            try:
                resolved_path = Path(path).resolve()
                
                # Security check: prevent access to system directories
                path_str = str(resolved_path)
                dangerous_paths = ['/etc', '/var', '/usr', '/sys', '/proc', '/root']
                
                if any(path_str.startswith(dangerous) for dangerous in dangerous_paths):
                    raise SecurityError(f"Access to system directory not allowed: {path}")
                
                validated_paths.append(str(resolved_path))
                
            except (OSError, ValueError) as e:
                raise ValidationError(f"Invalid path: {path} - {e}", field="paths")
        
        return validated_paths
    
    @staticmethod
    def validate_extensions(extensions: List[str]) -> List[str]:
        """Validate file extensions for security"""
        dangerous_extensions = ['.exe', '.bat', '.cmd', '.scr', '.com', '.pif', '.vbs', '.js']
        
        validated_extensions = []
        for ext in extensions:
            ext = ext.lower()
            if not ext.startswith('.'):
                ext = '.' + ext
            
            if ext in dangerous_extensions:
                logger.warning(f"Potentially dangerous extension allowed: {ext}")
            
            validated_extensions.append(ext)
        
        return validated_extensions


class EnterpriseConfigManager:
    """Enterprise configuration manager with environment support"""
    
    def __init__(self, config_file: Optional[Path] = None, environment: str = None):
        self.config_file = config_file or Path("conf/config.ini")
        self.environment = environment or os.getenv("LIBRARIAN_ENV", "production")
        self._config: Optional[EnterpriseConfig] = None
        self._validator = ConfigurationValidator()
    
    def load_config(self) -> EnterpriseConfig:
        """Load and validate configuration"""
        if self._config:
            return self._config
        
        logger.info(f"Loading configuration from {self.config_file} (environment: {self.environment})")
        
        if not self.config_file.exists():
            raise ValidationError(f"Configuration file not found: {self.config_file}")
        
        try:
            # Load base configuration
            config_parser = ConfigParser()
            config_parser.read(self.config_file)
            
            # Load environment-specific overrides
            env_config_file = self.config_file.parent / f"config.{self.environment}.ini"
            if env_config_file.exists():
                logger.info(f"Loading environment overrides from {env_config_file}")
                config_parser.read(env_config_file)
            
            # Parse configuration sections
            self._config = self._parse_configuration(config_parser)
            
            logger.info("Configuration loaded and validated successfully")
            return self._config
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise EnterpriseException(f"Configuration loading failed: {e}")
    
    def _parse_configuration(self, config_parser: ConfigParser) -> EnterpriseConfig:
        """Parse configuration from ConfigParser"""
        
        # Database configuration
        db_section = config_parser['Database'] if 'Database' in config_parser else {}
        database_config = DatabaseConfig(
            url=db_section.get('knowledge_base_db_url', 'knowledge.sqlite'),
            timeout=int(db_section.get('timeout', '30')),
            pool_size=int(db_section.get('pool_size', '5'))
        )
        
        # Processing configuration
        proc_section = config_parser['Processing'] if 'Processing' in config_parser else {}
        processing_config = ProcessingConfig(
            max_workers=int(proc_section.get('max_workers', '4')),
            batch_size=int(proc_section.get('batch_size', '100')),
            memory_limit_mb=int(proc_section.get('memory_limit_mb', '1000')),
            timeout_seconds=int(proc_section.get('timeout_seconds', '300'))
        )
        
        # Security configuration
        sec_section = config_parser['Security'] if 'Security' in config_parser else {}
        security_config = SecurityConfig(
            enable_audit_logging=sec_section.getboolean('enable_audit_logging', True),
            max_file_size_mb=int(sec_section.get('max_file_size_mb', '2048')),
            allowed_extensions=self._parse_list(sec_section.get('allowed_extensions', '')),
            blocked_paths=self._parse_list(sec_section.get('blocked_paths', ''))
        )
        
        # Library configuration
        lib_section = config_parser['Library'] if 'Library' in config_parser else {}
        if 'Library' not in config_parser:
            raise ValidationError("[Library] section is required in configuration")
        
        source_paths = self._parse_list(lib_section.get('source_paths', ''))
        if not source_paths:
            raise ValidationError("source_paths is required in [Library] section")
        
        library_root = lib_section.get('library_root', '')
        if not library_root:
            raise ValidationError("library_root is required in [Library] section")
        
        # Validate paths
        validated_source_paths = self._validator.validate_paths(source_paths)
        validated_library_root = self._validator.validate_paths([library_root])[0]
        
        library_config = LibraryConfig(
            source_paths=validated_source_paths,
            library_root=validated_library_root,
            create_backups=lib_section.getboolean('create_backups', True),
            preserve_structure=lib_section.getboolean('preserve_structure', False)
        )
        
        # Main configuration
        main_section = config_parser['DEFAULT'] if 'DEFAULT' in config_parser else {}
        
        return EnterpriseConfig(
            database=database_config,
            processing=processing_config,
            security=security_config,
            library=library_config,
            environment=self.environment,
            debug=main_section.getboolean('debug', False)
        )\n    \n    def _parse_list(self, value: str) -> List[str]:\n        \"\"\"Parse comma-separated list from configuration\"\"\"\n        if not value:\n            return []\n        return [item.strip() for item in value.split(',') if item.strip()]\n    \n    def get_config(self) -> EnterpriseConfig:\n        \"\"\"Get current configuration (load if not already loaded)\"\"\"\n        if not self._config:\n            return self.load_config()\n        return self._config\n    \n    def reload_config(self) -> EnterpriseConfig:\n        \"\"\"Reload configuration from file\"\"\"\n        self._config = None\n        return self.load_config()\n    \n    def validate_runtime_config(self) -> bool:\n        \"\"\"Validate configuration at runtime\"\"\"\n        try:\n            config = self.get_config()\n            \n            # Check source paths still exist\n            for path in config.library.source_paths:\n                if not Path(path).exists():\n                    logger.error(f\"Source path no longer exists: {path}\")\n                    return False\n            \n            # Check library root is writable\n            library_path = Path(config.library.library_root)\n            if library_path.exists() and not os.access(library_path, os.W_OK):\n                logger.error(f\"Library root is not writable: {library_path}\")\n                return False\n            \n            # Check database accessibility\n            if config.database.url != 'knowledge.sqlite':\n                db_path = Path(config.database.url)\n                if not db_path.exists():\n                    logger.warning(f\"Database file does not exist: {db_path}\")\n            \n            return True\n            \n        except Exception as e:\n            logger.error(f\"Runtime configuration validation failed: {e}\")\n            return False\n    \n    def export_config(self, output_file: Path, include_sensitive: bool = False) -> None:\n        \"\"\"Export configuration to JSON for debugging\"\"\"\n        config = self.get_config()\n        \n        # Convert to dictionary\n        config_dict = {\n            'database': {\n                'url': config.database.url if include_sensitive else '[REDACTED]',\n                'timeout': config.database.timeout,\n                'pool_size': config.database.pool_size\n            },\n            'processing': {\n                'max_workers': config.processing.max_workers,\n                'batch_size': config.processing.batch_size,\n                'memory_limit_mb': config.processing.memory_limit_mb,\n                'timeout_seconds': config.processing.timeout_seconds\n            },\n            'security': {\n                'enable_audit_logging': config.security.enable_audit_logging,\n                'max_file_size_mb': config.security.max_file_size_mb,\n                'allowed_extensions': config.security.allowed_extensions,\n                'blocked_paths': config.security.blocked_paths if include_sensitive else '[REDACTED]'\n            },\n            'library': {\n                'source_paths': config.library.source_paths if include_sensitive else '[REDACTED]',\n                'library_root': config.library.library_root if include_sensitive else '[REDACTED]',\n                'create_backups': config.library.create_backups,\n                'preserve_structure': config.library.preserve_structure\n            },\n            'environment': config.environment,\n            'debug': config.debug\n        }\n        \n        with open(output_file, 'w') as f:\n            json.dump(config_dict, f, indent=2)\n        \n        logger.info(f\"Configuration exported to {output_file}\")\n\n\n# Global configuration manager instance\n_config_manager: Optional[EnterpriseConfigManager] = None\n\n\ndef get_config_manager(config_file: Optional[Path] = None, environment: str = None) -> EnterpriseConfigManager:\n    \"\"\"Get global configuration manager instance\"\"\"\n    global _config_manager\n    \n    if _config_manager is None:\n        _config_manager = EnterpriseConfigManager(config_file, environment)\n    \n    return _config_manager\n\n\ndef load_config(config_file: Optional[Path] = None, environment: str = None) -> EnterpriseConfig:\n    \"\"\"Load configuration using global manager\"\"\"\n    manager = get_config_manager(config_file, environment)\n    return manager.load_config()\n\n\n# Legacy compatibility function\ndef load_config_legacy() -> Dict[str, Any]:\n    \"\"\"Legacy configuration loader for backward compatibility\"\"\"\n    try:\n        config = load_config()\n        \n        # Convert to legacy format
        return {
            'source_paths': config.library.source_paths,
            'library_root': config.library.library_root,
            'knowledge_base_db_url': config.database.url,
            'max_workers': config.processing.max_workers,
            'debug': config.debug
        }
        
    except Exception as e:
        logger.error(f"Legacy config loading failed: {e}")
        # Return minimal fallback configuration
        return {
            'source_paths': [],
            'library_root': '',
            'knowledge_base_db_url': 'knowledge.sqlite',
            'max_workers': 4,
            'debug': False
        }