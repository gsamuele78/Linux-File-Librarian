#!/usr/bin/env python3
"""
Enterprise Installation and System Management Module

Provides enterprise-grade installation, configuration, and system management
capabilities with comprehensive error handling, validation, and monitoring.
"""

import os
import sys
import subprocess
import shutil
import platform
import logging
import json
import hashlib
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict, field
from enum import Enum
from contextlib import contextmanager
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class InstallationStatus(Enum):
    """Installation status enumeration"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLBACK = "rollback"


class SystemType(Enum):
    """Supported system types"""
    DEBIAN_UBUNTU = "debian_ubuntu"
    REDHAT_CENTOS = "redhat_centos"
    FEDORA = "fedora"
    ARCH = "arch"
    UNKNOWN = "unknown"


def _default_system_packages():
    return {
        'debian_ubuntu': ['python3-pip', 'python3-tk', 'python3-venv', 'python3-dev',
                        'git', 'libmagic1', 'qpdf', 'ghostscript', 'pdftk'],
        'redhat_centos': ['python3-pip', 'python3-tkinter', 'python3-venv', 'python3-devel',
                        'git', 'file-libs', 'qpdf', 'ghostscript', 'pdftk'],
        'fedora': ['python3-pip', 'python3-tkinter', 'python3-venv', 'python3-devel',
                  'git', 'file-libs', 'qpdf', 'ghostscript', 'pdftk']
    }

def _default_python_packages():
    return [
        'pandas>=1.3.0', 'beautifulsoup4>=4.9.0', 'requests>=2.25.0',
        'tqdm>=4.60.0', 'psutil>=5.8.0', 'pymupdf>=1.18.0',
        'python-magic>=0.4.0', 'rapidfuzz>=1.4.0', 'lxml>=4.6.0'
    ]

@dataclass
class SystemRequirements:
    """System requirements specification"""
    min_python_version: Tuple[int, int] = (3, 8)
    min_memory_gb: float = 2.0
    min_disk_space_gb: float = 5.0
    required_commands: List[str] = field(default_factory=lambda: ['python3', 'pip3', 'git'])
    system_packages: Dict[str, List[str]] = field(default_factory=_default_system_packages)
    python_packages: List[str] = field(default_factory=_default_python_packages)


@dataclass
class InstallationResult:
    """Installation result with comprehensive details"""
    status: InstallationStatus
    success: bool
    message: str
    details: Dict[str, Any]
    duration_seconds: float
    timestamp: float
    rollback_info: Optional[Dict[str, Any]] = None


class SystemDetector:
    """Advanced system detection and validation"""
    
    @staticmethod
    def detect_system_type() -> SystemType:
        """Detect the operating system type"""
        try:
            # Check for specific distribution files
            if Path('/etc/debian_version').exists():
                return SystemType.DEBIAN_UBUNTU
            elif Path('/etc/redhat-release').exists():
                return SystemType.REDHAT_CENTOS
            elif Path('/etc/fedora-release').exists():
                return SystemType.FEDORA
            elif Path('/etc/arch-release').exists():
                return SystemType.ARCH
            else:
                # Fallback to platform detection
                system = platform.system().lower()
                if 'linux' in system:
                    # Try to detect from /etc/os-release
                    if Path('/etc/os-release').exists():
                        with open('/etc/os-release', 'r') as f:
                            content = f.read().lower()
                            if 'ubuntu' in content or 'debian' in content:
                                return SystemType.DEBIAN_UBUNTU
                            elif 'fedora' in content:
                                return SystemType.FEDORA
                            elif 'centos' in content or 'rhel' in content:
                                return SystemType.REDHAT_CENTOS
                
                return SystemType.UNKNOWN
        except Exception as e:
            logger.warning(f"System detection failed: {e}")
            return SystemType.UNKNOWN
    
    @staticmethod
    def get_system_info() -> Dict[str, Any]:
        """Get comprehensive system information"""
        try:
            import psutil
            
            # Basic system info
            info = {
                'platform': platform.platform(),
                'system': platform.system(),
                'release': platform.release(),
                'version': platform.version(),
                'machine': platform.machine(),
                'processor': platform.processor(),
                'python_version': platform.python_version(),
                'cpu_count': os.cpu_count(),
                'memory_gb': psutil.virtual_memory().total / (1024**3),
                'disk_space_gb': psutil.disk_usage('/').total / (1024**3),
                'available_disk_gb': psutil.disk_usage('/').free / (1024**3)
            }
            
            # Add distribution info if available
            try:
                if Path('/etc/os-release').exists():
                    with open('/etc/os-release', 'r') as f:
                        os_release = {}
                        for line in f:
                            if '=' in line:
                                key, value = line.strip().split('=', 1)
                                os_release[key] = value.strip('"')
                        info['os_release'] = os_release
            except Exception:
                pass
            
            return info
        except Exception as e:
            logger.error(f"Failed to get system info: {e}")
            return {'error': str(e)}


class PackageManager:
    """Enterprise package management with multiple backend support"""
    
    def __init__(self, system_type: SystemType):
        self.system_type = system_type
        self.package_managers = {
            SystemType.DEBIAN_UBUNTU: ('apt-get', ['update'], ['install', '-y']),
            SystemType.REDHAT_CENTOS: ('yum', [], ['install', '-y']),
            SystemType.FEDORA: ('dnf', [], ['install', '-y']),
            SystemType.ARCH: ('pacman', ['-Sy'], ['-S', '--noconfirm'])
        }
    
    def _run_command(self, cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """Run system command with proper error handling"""
        safe_cmd = ""
        try:
            safe_cmd = ' '.join(str(c)[:50] for c in cmd[:5])
            logger.info(f"Executing: {safe_cmd}")
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                check=check,
                timeout=300  # 5 minute timeout
            )
            return result
        except subprocess.TimeoutExpired as e:
            logger.error(f"Command timed out: {safe_cmd}")
            raise
        except subprocess.CalledProcessError as e:
            safe_error = str(e.stderr)[:200] if e.stderr else 'No error output'
            logger.error(f"Command failed: {safe_cmd}, error: {safe_error}")
            raise
    
    def update_package_cache(self) -> bool:
        """Update package manager cache"""
        if self.system_type not in self.package_managers:
            logger.error(f"Unsupported system type: {self.system_type}")
            return False
        
        try:
            pm, update_cmd, _ = self.package_managers[self.system_type]
            if update_cmd:
                cmd = ['sudo', pm] + update_cmd
                self._run_command(cmd)
            return True
        except Exception as e:
            logger.error(f"Failed to update package cache: {e}")
            return False
    
    def install_packages(self, packages: List[str]) -> bool:
        """Install system packages"""
        if not packages:
            return True
        
        if self.system_type not in self.package_managers:
            logger.error(f"Unsupported system type: {self.system_type}")
            return False
        
        try:
            pm, _, install_cmd = self.package_managers[self.system_type]
            cmd = ['sudo', pm] + install_cmd + packages
            self._run_command(cmd)
            logger.info(f"Successfully installed packages: {packages}")
            return True
        except Exception as e:
            logger.error(f"Failed to install packages {packages}: {e}")
            return False
    
    def check_package_installed(self, package: str) -> bool:
        """Check if a package is installed"""
        try:
            if self.system_type == SystemType.DEBIAN_UBUNTU:
                result = self._run_command(['dpkg', '-l', package], check=False)
                return result.returncode == 0 and 'ii' in result.stdout
            elif self.system_type in [SystemType.REDHAT_CENTOS, SystemType.FEDORA]:
                result = self._run_command(['rpm', '-q', package], check=False)
                return result.returncode == 0
            elif self.system_type == SystemType.ARCH:
                result = self._run_command(['pacman', '-Q', package], check=False)
                return result.returncode == 0
            return False
        except Exception as e:
            logger.warning(f"Failed to check package {package}: {e}")
            return False


class VirtualEnvironmentManager:
    """Enterprise virtual environment management"""
    
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.venv_path = self.project_root / 'venv'
        self.requirements_file = self.project_root / 'requirements.txt'
        self.enterprise_requirements = self.project_root / 'requirements_enterprise.txt'
    
    def create_virtual_environment(self, force_recreate: bool = False) -> bool:
        """Create virtual environment with validation"""
        try:
            if self.venv_path.exists():
                if force_recreate:
                    logger.info("Removing existing virtual environment")
                    shutil.rmtree(self.venv_path)
                else:
                    logger.info("Virtual environment already exists")
                    return self.validate_virtual_environment()
            
            logger.info(f"Creating virtual environment at {self.venv_path}")
            result = subprocess.run([
                sys.executable, '-m', 'venv', str(self.venv_path)
            ], capture_output=True, text=True, check=True)
            
            # Validate creation
            if not self.validate_virtual_environment():
                raise RuntimeError("Virtual environment validation failed")
            
            logger.info("Virtual environment created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create virtual environment: {e}")
            return False
    
    def validate_virtual_environment(self) -> bool:
        """Validate virtual environment integrity"""
        try:
            # Check essential files exist
            essential_files = [
                self.venv_path / 'bin' / 'python',
                self.venv_path / 'bin' / 'pip',
                self.venv_path / 'pyvenv.cfg'
            ]
            
            for file_path in essential_files:
                if not file_path.exists():
                    logger.error(f"Missing essential file: {file_path}")
                    return False
            
            # Test Python execution
            python_exe = self.venv_path / 'bin' / 'python'
            result = subprocess.run([
                str(python_exe), '-c', 'import sys; print(sys.version)'
            ], capture_output=True, text=True, check=True, timeout=10)
            
            logger.info(f"Virtual environment Python: {result.stdout.strip()}")
            return True
            
        except Exception as e:
            logger.error(f"Virtual environment validation failed: {e}")
            return False
    
    def install_python_packages(self, upgrade_pip: bool = True) -> bool:
        """Install Python packages with comprehensive error handling"""
        try:
            pip_exe = self.venv_path / 'bin' / 'pip'
            
            if not pip_exe.exists():
                logger.error("pip not found in virtual environment")
                return False
            
            # Upgrade pip first
            if upgrade_pip:
                logger.info("Upgrading pip, setuptools, and wheel")
                result = subprocess.run([
                    str(pip_exe), 'install', '--upgrade', 'pip', 'setuptools', 'wheel'
                ], capture_output=True, text=True, check=True, timeout=300)
            
            # Install from requirements files
            requirements_files = []
            if self.enterprise_requirements.exists():
                requirements_files.append(self.enterprise_requirements)
            elif self.requirements_file.exists():
                requirements_files.append(self.requirements_file)
            else:
                logger.warning("No requirements file found, installing basic packages")
                return self._install_basic_packages()
            
            for req_file in requirements_files:
                logger.info(f"Installing packages from {req_file}")
                result = subprocess.run([
                    str(pip_exe), 'install', '-r', str(req_file)
                ], capture_output=True, text=True, check=True, timeout=600)
                
                if result.returncode != 0:
                    logger.error(f"Failed to install from {req_file}: {result.stderr}")
                    return False
            
            # Validate installation
            return self._validate_package_installation()
            
        except Exception as e:
            logger.error(f"Failed to install Python packages: {e}")
            return False
    
    def _install_basic_packages(self) -> bool:
        """Install basic required packages"""
        basic_packages = [
            'pandas>=1.3.0', 'beautifulsoup4>=4.9.0', 'requests>=2.25.0',
            'tqdm>=4.60.0', 'psutil>=5.8.0', 'pymupdf>=1.18.0',
            'python-magic>=0.4.0', 'rapidfuzz>=1.4.0', 'lxml>=4.6.0'
        ]
        
        try:
            pip_exe = self.venv_path / 'bin' / 'pip'
            logger.info("Installing basic packages")
            
            result = subprocess.run([
                str(pip_exe), 'install'
            ] + basic_packages, capture_output=True, text=True, check=True, timeout=600)
            
            return result.returncode == 0
            
        except Exception as e:
            logger.error(f"Failed to install basic packages: {e}")
            return False
    
    def _validate_package_installation(self) -> bool:
        """Validate that critical packages are installed"""
        critical_packages = ['pandas', 'requests', 'tqdm', 'psutil']
        python_exe = self.venv_path / 'bin' / 'python'
        
        try:
            for package in critical_packages:
                result = subprocess.run([
                    str(python_exe), '-c', f'import {package}; print("{package} OK")'
                ], capture_output=True, text=True, check=True, timeout=10)
                
                if result.returncode != 0:
                    logger.error(f"Package validation failed for {package}")
                    return False
            
            logger.info("All critical packages validated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Package validation failed: {e}")
            return False


class ConfigurationManager:
    """Enterprise configuration management"""
    
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.config_dir = self.project_root / 'conf'
        self.config_file = self.config_dir / 'config.ini'
        self.backup_dir = self.project_root / 'conf_backup'
    
    def ensure_configuration_structure(self) -> bool:
        """Ensure configuration directory structure exists"""
        try:
            self.config_dir.mkdir(exist_ok=True)
            self.backup_dir.mkdir(exist_ok=True)
            
            # Create default config if it doesn't exist
            if not self.config_file.exists():
                return self.create_default_configuration()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to ensure configuration structure: {e}")
            return False
    
    def create_default_configuration(self) -> bool:
        """Create default configuration file"""
        try:
            default_config = """[Paths]
# Source directories to scan (comma-separated)
source_paths = /path/to/your/source/directory

# Root directory for the organized library
library_root = /path/to/your/library/destination

# Temporary directory for processing
temp_dir = /tmp/librarian_temp

[Processing]
# Maximum number of worker threads
max_workers = 4

# Batch size for file processing
batch_size = 100

# Enable memory optimization
memory_optimization = true

# Maximum memory usage per process (MB)
max_memory_mb = 2048

[Logging]
# Log level (DEBUG, INFO, WARNING, ERROR)
log_level = INFO

# Log file location
log_file = logs/librarian.log

# Enable audit logging
audit_logging = true

[KnowledgeBaseURLs]
# TTRPG knowledge base URLs (comment out with # to disable)
drivethrurpg = https://www.drivethrurpg.com
dmsguild = https://www.dmsguild.com

[Security]
# Enable path validation
validate_paths = true

# Maximum file size to process (MB)
max_file_size_mb = 500

# Allowed file extensions (comma-separated)
allowed_extensions = .pdf,.txt,.doc,.docx,.epub,.mobi

[Performance]
# Enable performance monitoring
performance_monitoring = true

# Performance report interval (seconds)
report_interval = 300

# Enable caching
enable_caching = true
"""
            
            with open(self.config_file, 'w') as f:
                f.write(default_config)
            
            logger.info(f"Default configuration created at {self.config_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create default configuration: {e}")
            return False
    
    def backup_configuration(self) -> Optional[Path]:
        """Create backup of current configuration"""
        try:
            if not self.config_file.exists():
                return None
            
            timestamp = int(time.time())
            backup_file = self.backup_dir / f"config_{timestamp}.ini"
            
            shutil.copy2(self.config_file, backup_file)
            logger.info(f"Configuration backed up to {backup_file}")
            return backup_file
            
        except Exception as e:
            logger.error(f"Failed to backup configuration: {e}")
            return None


class EnterpriseInstaller:
    """Enterprise-grade installation orchestrator"""
    
    def __init__(self, project_root: Optional[Path] = None):
        self.project_root = Path(project_root) if project_root else Path.cwd()
        self.system_type = SystemDetector.detect_system_type()
        self.package_manager = PackageManager(self.system_type)
        self.venv_manager = VirtualEnvironmentManager(self.project_root)
        self.config_manager = ConfigurationManager(self.project_root)
        self.requirements = SystemRequirements()
        
        # Installation state
        self.installation_log = []
        self.rollback_actions = []
    
    def validate_system_requirements(self) -> Tuple[bool, List[str]]:
        """Comprehensive system requirements validation"""
        issues = []
        
        try:
            # Check Python version
            python_version = sys.version_info[:2]
            if python_version < self.requirements.min_python_version:
                issues.append(f"Python {self.requirements.min_python_version} required, found {python_version}")
            
            # Check system resources
            try:
                import psutil
                
                # Memory check
                memory_gb = psutil.virtual_memory().total / (1024**3)
                if memory_gb < self.requirements.min_memory_gb:
                    issues.append(f"Minimum {self.requirements.min_memory_gb}GB RAM required, found {memory_gb:.1f}GB")
                
                # Disk space check
                disk_gb = psutil.disk_usage(str(self.project_root)).free / (1024**3)
                if disk_gb < self.requirements.min_disk_space_gb:
                    issues.append(f"Minimum {self.requirements.min_disk_space_gb}GB free space required, found {disk_gb:.1f}GB")
                
            except ImportError:
                issues.append("psutil not available for system resource checking")
            
            # Check required commands
            for cmd in self.requirements.required_commands:
                if not shutil.which(cmd):
                    issues.append(f"Required command not found: {cmd}")
            
            # Check system type support
            if self.system_type == SystemType.UNKNOWN:
                issues.append("Unsupported or unrecognized operating system")
            
            return len(issues) == 0, issues
            
        except Exception as e:
            issues.append(f"System validation failed: {e}")
            return False, issues
    
    def install_system_dependencies(self) -> bool:
        """Install system-level dependencies"""
        try:
            if self.system_type == SystemType.UNKNOWN:
                logger.error("Cannot install system dependencies on unknown system type")
                return False
            
            # Update package cache
            if not self.package_manager.update_package_cache():
                logger.warning("Failed to update package cache, continuing anyway")
            
            # Get packages for this system type
            packages = self.requirements.system_packages.get(self.system_type.value, [])
            if not packages:
                logger.warning(f"No system packages defined for {self.system_type}")
                return True
            
            # Check which packages are already installed
            missing_packages = []
            for package in packages:
                if not self.package_manager.check_package_installed(package):
                    missing_packages.append(package)
            
            if not missing_packages:
                logger.info("All system packages already installed")
                return True
            
            # Install missing packages
            logger.info(f"Installing missing packages: {missing_packages}")
            success = self.package_manager.install_packages(missing_packages)
            
            if success:
                self.rollback_actions.append(('remove_packages', missing_packages))
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to install system dependencies: {e}")
            return False
    
    def setup_python_environment(self) -> bool:
        """Setup Python virtual environment and packages"""
        try:
            # Create virtual environment
            if not self.venv_manager.create_virtual_environment():
                return False
            
            self.rollback_actions.append(('remove_venv', str(self.venv_manager.venv_path)))
            
            # Install Python packages
            if not self.venv_manager.install_python_packages():
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Python environment: {e}")
            return False
    
    def setup_configuration(self) -> bool:
        """Setup configuration files and directories"""
        try:
            # Backup existing configuration
            backup_file = self.config_manager.backup_configuration()
            if backup_file:
                self.rollback_actions.append(('restore_config', str(backup_file)))
            
            # Ensure configuration structure
            if not self.config_manager.ensure_configuration_structure():
                return False
            
            # Create necessary directories
            directories = ['logs', 'reports', 'temp']
            for dir_name in directories:
                dir_path = self.project_root / dir_name
                dir_path.mkdir(exist_ok=True)
                self.rollback_actions.append(('remove_directory', str(dir_path)))
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup configuration: {e}")
            return False
    
    def set_permissions(self) -> bool:
        """Set appropriate file permissions"""
        try:
            # Set execute permissions on scripts
            scripts_dir = self.project_root / 'scripts'
            if scripts_dir.exists():
                for script_file in scripts_dir.glob('*.sh'):
                    script_file.chmod(0o755)
                    logger.info(f"Set execute permission on {script_file}")
            
            # Set permissions on virtual environment
            if self.venv_manager.venv_path.exists():
                python_exe = self.venv_manager.venv_path / 'bin' / 'python'
                if python_exe.exists():
                    python_exe.chmod(0o755)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to set permissions: {e}")
            return False
    
    def perform_installation(self, force_reinstall: bool = False) -> InstallationResult:
        """Perform complete enterprise installation"""
        start_time = time.time()
        
        try:
            logger.info("Starting enterprise installation")
            
            # System validation
            logger.info("Validating system requirements")
            valid, issues = self.validate_system_requirements()
            if not valid:
                return InstallationResult(
                    status=InstallationStatus.FAILED,
                    success=False,
                    message="System requirements validation failed",
                    details={'validation_issues': issues},
                    duration_seconds=time.time() - start_time,
                    timestamp=time.time()
                )
            
            # System dependencies
            logger.info("Installing system dependencies")
            if not self.install_system_dependencies():
                return self._create_failure_result(start_time, "System dependencies installation failed")
            
            # Python environment
            logger.info("Setting up Python environment")
            if not self.setup_python_environment():
                return self._create_failure_result(start_time, "Python environment setup failed")
            
            # Configuration
            logger.info("Setting up configuration")
            if not self.setup_configuration():
                return self._create_failure_result(start_time, "Configuration setup failed")
            
            # Permissions
            logger.info("Setting file permissions")
            if not self.set_permissions():
                return self._create_failure_result(start_time, "Permission setup failed")
            
            # Final validation
            logger.info("Performing final validation")
            if not self._validate_installation():
                return self._create_failure_result(start_time, "Installation validation failed")
            
            duration = time.time() - start_time
            logger.info(f"Enterprise installation completed successfully in {duration:.2f} seconds")
            
            return InstallationResult(
                status=InstallationStatus.COMPLETED,
                success=True,
                message="Enterprise installation completed successfully",
                details={
                    'system_type': self.system_type.value,
                    'python_version': sys.version,
                    'project_root': str(self.project_root),
                    'venv_path': str(self.venv_manager.venv_path)
                },
                duration_seconds=duration,
                timestamp=time.time()
            )
            
        except Exception as e:
            logger.error(f"Installation failed with exception: {e}")
            return self._create_failure_result(start_time, f"Installation failed: {e}")
    
    def _create_failure_result(self, start_time: float, message: str) -> InstallationResult:
        """Create failure result with rollback information"""
        return InstallationResult(
            status=InstallationStatus.FAILED,
            success=False,
            message=message,
            details={'rollback_actions': self.rollback_actions},
            duration_seconds=time.time() - start_time,
            timestamp=time.time(),
            rollback_info={'actions': self.rollback_actions}
        )
    
    def _validate_installation(self) -> bool:
        """Validate complete installation"""
        try:
            # Check virtual environment
            if not self.venv_manager.validate_virtual_environment():
                return False
            
            # Check configuration
            if not self.config_manager.config_file.exists():
                return False
            
            # Check essential directories
            essential_dirs = ['logs', 'conf']
            for dir_name in essential_dirs:
                if not (self.project_root / dir_name).exists():
                    return False
            
            # Test Python imports in virtual environment
            python_exe = self.venv_manager.venv_path / 'bin' / 'python'
            test_imports = ['pandas', 'requests', 'tqdm']
            
            for module in test_imports:
                result = subprocess.run([
                    str(python_exe), '-c', f'import {module}'
                ], capture_output=True, text=True, timeout=10)
                
                if result.returncode != 0:
                    logger.error(f"Failed to import {module}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Installation validation failed: {e}")
            return False
    
    def rollback_installation(self, rollback_info: Dict[str, Any]) -> bool:
        """Rollback installation changes"""
        try:
            logger.info("Starting installation rollback")
            
            actions = rollback_info.get('actions', [])
            for action_type, action_data in reversed(actions):
                try:
                    if action_type == 'remove_venv':
                        if Path(action_data).exists():
                            shutil.rmtree(action_data)
                            logger.info(f"Removed virtual environment: {action_data}")
                    
                    elif action_type == 'remove_directory':
                        if Path(action_data).exists():
                            shutil.rmtree(action_data)
                            logger.info(f"Removed directory: {action_data}")
                    
                    elif action_type == 'restore_config':
                        if Path(action_data).exists():
                            shutil.copy2(action_data, self.config_manager.config_file)
                            logger.info(f"Restored configuration from: {action_data}")
                    
                    elif action_type == 'remove_packages':
                        # Note: Package removal is complex and risky, so we skip it
                        logger.info(f"Skipping package removal: {action_data}")
                    
                except Exception as e:
                    logger.error(f"Rollback action failed {action_type}: {e}")
            
            logger.info("Installation rollback completed")
            return True
            
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            return False
    
    def get_installation_status(self) -> Dict[str, Any]:
        """Get current installation status"""
        return {
            'system_info': SystemDetector.get_system_info(),
            'system_type': self.system_type.value,
            'project_root': str(self.project_root),
            'venv_exists': self.venv_manager.venv_path.exists(),
            'config_exists': self.config_manager.config_file.exists(),
            'requirements_validation': self.validate_system_requirements()
        }


def main():
    """Main installation entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enterprise Linux File Librarian Installer')
    parser.add_argument('--project-root', type=Path, help='Project root directory')
    parser.add_argument('--force-reinstall', action='store_true', help='Force reinstallation')
    parser.add_argument('--validate-only', action='store_true', help='Only validate requirements')
    parser.add_argument('--status', action='store_true', help='Show installation status')
    
    args = parser.parse_args()
    
    installer = EnterpriseInstaller(args.project_root)
    
    if args.status:
        status = installer.get_installation_status()
        print(json.dumps(status, indent=2, default=str))
        return
    
    if args.validate_only:
        valid, issues = installer.validate_system_requirements()
        if valid:
            print("✓ System requirements validation passed")
            sys.exit(0)
        else:
            print("✗ System requirements validation failed:")
            for issue in issues:
                print(f"  - {issue}")
            sys.exit(1)
    
    # Perform installation
    result = installer.perform_installation(args.force_reinstall)
    
    print(f"Installation Status: {result.status.value}")
    print(f"Success: {result.success}")
    print(f"Message: {result.message}")
    print(f"Duration: {result.duration_seconds:.2f} seconds")
    
    if result.details:
        print("Details:")
        for key, value in result.details.items():
            print(f"  {key}: {value}")
    
    sys.exit(0 if result.success else 1)


if __name__ == '__main__':
    main()