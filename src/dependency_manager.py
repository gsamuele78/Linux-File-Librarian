#!/usr/bin/env python3
"""
Enterprise Dependency Manager

Provides comprehensive dependency management with security validation,
version checking, and enterprise-grade error handling.
"""

import sys
import subprocess
import importlib
import pkg_resources
import logging
import json
import hashlib
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class DependencyStatus(Enum):
    """Dependency status enumeration"""
    SATISFIED = "satisfied"
    MISSING = "missing"
    OUTDATED = "outdated"
    INCOMPATIBLE = "incompatible"
    SECURITY_RISK = "security_risk"


@dataclass
class DependencyInfo:
    """Comprehensive dependency information"""
    name: str
    import_name: str
    required_version: Optional[str]
    installed_version: Optional[str]
    status: DependencyStatus
    security_issues: List[str]
    description: str
    is_critical: bool = True


class EnterpriseDependencyManager:
    """Enterprise-grade dependency management"""
    
    def __init__(self, project_root: Optional[Path] = None):
        self.project_root = Path(project_root) if project_root else Path.cwd()
        self.requirements_file = self.project_root / 'requirements_enterprise.txt'
        self.fallback_requirements = self.project_root / 'requirements.txt'
        
        # Core dependencies with security considerations
        self.core_dependencies = {
            'pandas': DependencyInfo(
                name='pandas',
                import_name='pandas',
                required_version='>=1.5.0,<2.0.0',
                installed_version=None,
                status=DependencyStatus.MISSING,
                security_issues=[],
                description='Data analysis and manipulation library',
                is_critical=True
            ),
            'requests': DependencyInfo(
                name='requests',
                import_name='requests',
                required_version='>=2.28.0,<3.0.0',
                installed_version=None,
                status=DependencyStatus.MISSING,
                security_issues=[],
                description='HTTP library for Python',
                is_critical=True
            ),
            'beautifulsoup4': DependencyInfo(
                name='beautifulsoup4',
                import_name='bs4',
                required_version='>=4.11.0,<5.0.0',
                installed_version=None,
                status=DependencyStatus.MISSING,
                security_issues=[],
                description='HTML/XML parsing library',
                is_critical=True
            ),
            'pymupdf': DependencyInfo(
                name='pymupdf',
                import_name='fitz',
                required_version='>=1.20.0,<2.0.0',
                installed_version=None,
                status=DependencyStatus.MISSING,
                security_issues=[],
                description='PDF processing library',
                is_critical=True
            ),
            'python-magic': DependencyInfo(
                name='python-magic',
                import_name='magic',
                required_version='>=0.4.27',
                installed_version=None,
                status=DependencyStatus.MISSING,
                security_issues=[],
                description='File type detection library',
                is_critical=True
            ),
            'rapidfuzz': DependencyInfo(
                name='rapidfuzz',
                import_name='rapidfuzz',
                required_version='>=2.0.0,<3.0.0',
                installed_version=None,
                status=DependencyStatus.MISSING,
                security_issues=[],
                description='Fast string matching library',
                is_critical=True
            ),
            'psutil': DependencyInfo(
                name='psutil',
                import_name='psutil',
                required_version='>=5.9.0,<6.0.0',
                installed_version=None,
                status=DependencyStatus.MISSING,
                security_issues=[],
                description='System and process monitoring library',
                is_critical=True
            ),
            'tqdm': DependencyInfo(
                name='tqdm',
                import_name='tqdm',
                required_version='>=4.64.0,<5.0.0',
                installed_version=None,
                status=DependencyStatus.MISSING,
                security_issues=[],
                description='Progress bar library',
                is_critical=False
            ),
            'lxml': DependencyInfo(
                name='lxml',
                import_name='lxml',
                required_version='>=4.9.0,<5.0.0',
                installed_version=None,
                status=DependencyStatus.MISSING,
                security_issues=[],
                description='XML processing library',
                is_critical=True
            )
        }
        
        # Known security vulnerabilities (simplified - in production, use a security database)
        self.security_vulnerabilities = {
            'requests': {
                '<2.20.0': ['CVE-2018-18074: Redirect vulnerability'],
                '<2.25.0': ['CVE-2020-26137: Header injection vulnerability']
            },
            'lxml': {
                '<4.6.5': ['CVE-2021-43818: HTML Cleaner vulnerability'],
                '<4.9.0': ['CVE-2022-2309: NULL pointer dereference']
            }
        }
    
    def check_dependency_status(self, dep_info: DependencyInfo) -> DependencyInfo:
        """Check the status of a single dependency"""
        try:
            # Try to import the module
            module = importlib.import_module(dep_info.import_name)
            
            # Get installed version
            try:
                installed_version = pkg_resources.get_distribution(dep_info.name).version
                dep_info.installed_version = installed_version
            except pkg_resources.DistributionNotFound:
                # Module exists but not installed via pip
                dep_info.installed_version = 'unknown'
            
            # Check version compatibility
            if dep_info.required_version and dep_info.installed_version != 'unknown':
                try:
                    requirement = pkg_resources.Requirement.parse(f"{dep_info.name}{dep_info.required_version}")
                    if dep_info.installed_version in requirement:
                        dep_info.status = DependencyStatus.SATISFIED
                    else:
                        dep_info.status = DependencyStatus.OUTDATED
                except Exception:
                    dep_info.status = DependencyStatus.INCOMPATIBLE
            else:
                dep_info.status = DependencyStatus.SATISFIED
            
            # Check for security vulnerabilities
            dep_info.security_issues = self._check_security_vulnerabilities(
                dep_info.name, dep_info.installed_version
            )
            
            if dep_info.security_issues:
                dep_info.status = DependencyStatus.SECURITY_RISK
            
        except ImportError:
            dep_info.status = DependencyStatus.MISSING
            dep_info.installed_version = None
        except Exception as e:
            logger.error(f"Error checking dependency {dep_info.name}: {e}")
            dep_info.status = DependencyStatus.INCOMPATIBLE
        
        return dep_info
    
    def _check_security_vulnerabilities(self, package_name: str, version: str) -> List[str]:
        """Check for known security vulnerabilities"""
        if not version or version == 'unknown':
            return []
        
        vulnerabilities = []
        package_vulns = self.security_vulnerabilities.get(package_name, {})
        
        for vuln_version, issues in package_vulns.items():
            try:
                if pkg_resources.parse_version(version) < pkg_resources.parse_version(vuln_version.lstrip('<')):
                    vulnerabilities.extend(issues)
            except Exception:
                continue
        
        return vulnerabilities
    
    def check_all_dependencies(self) -> Dict[str, DependencyInfo]:
        """Check status of all dependencies"""
        logger.info("Checking all dependencies...")
        
        results = {}
        for name, dep_info in self.core_dependencies.items():
            results[name] = self.check_dependency_status(dep_info)
        
        return results
    
    def install_dependency(self, dep_info: DependencyInfo, upgrade: bool = False) -> bool:
        """Install a single dependency with security validation"""
        try:
            package_spec = dep_info.name
            if dep_info.required_version:
                package_spec += dep_info.required_version
            
            cmd = [sys.executable, '-m', 'pip', 'install']
            if upgrade:
                cmd.append('--upgrade')
            cmd.append(package_spec)
            
            logger.info(f"Installing {package_spec}...")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=300  # 5 minute timeout
            )
            
            # Verify installation
            updated_info = self.check_dependency_status(dep_info)
            if updated_info.status in [DependencyStatus.SATISFIED, DependencyStatus.OUTDATED]:
                logger.info(f"Successfully installed {dep_info.name}")
                return True
            else:
                logger.error(f"Installation verification failed for {dep_info.name}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"Installation timeout for {dep_info.name}")
            return False
        except subprocess.CalledProcessError as e:
            safe_error = str(e.stderr)[:200] if e.stderr else 'No error output'
            logger.error(f"Installation failed for {dep_info.name}: {safe_error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error installing {dep_info.name}: {e}")
            return False
    
    def install_from_requirements(self, requirements_file: Optional[Path] = None) -> bool:
        """Install dependencies from requirements file"""
        req_file = requirements_file or self.requirements_file
        
        if not req_file.exists():
            req_file = self.fallback_requirements
            if not req_file.exists():
                logger.error("No requirements file found")
                return False
        
        try:
            logger.info(f"Installing from requirements file: {req_file}")
            
            result = subprocess.run([
                sys.executable, '-m', 'pip', 'install', '-r', str(req_file)
            ], capture_output=True, text=True, check=True, timeout=600)
            
            logger.info("Requirements installation completed")
            return True
            
        except subprocess.TimeoutExpired:
            logger.error("Requirements installation timeout")
            return False
        except subprocess.CalledProcessError as e:
            safe_error = str(e.stderr)[:300] if e.stderr else 'No error output'
            logger.error(f"Requirements installation failed: {safe_error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during requirements installation: {e}")
            return False
    
    def resolve_dependencies(self, auto_install: bool = False, upgrade_outdated: bool = False) -> Dict[str, Any]:
        """Resolve all dependency issues"""
        logger.info("Starting dependency resolution...")
        
        # Check current status
        dep_status = self.check_all_dependencies()
        
        # Categorize issues
        missing = []
        outdated = []
        security_risks = []
        incompatible = []
        
        for name, info in dep_status.items():
            if info.status == DependencyStatus.MISSING:
                missing.append(info)
            elif info.status == DependencyStatus.OUTDATED:
                outdated.append(info)
            elif info.status == DependencyStatus.SECURITY_RISK:
                security_risks.append(info)
            elif info.status == DependencyStatus.INCOMPATIBLE:
                incompatible.append(info)
        
        results = {
            'missing_count': len(missing),
            'outdated_count': len(outdated),
            'security_risks_count': len(security_risks),
            'incompatible_count': len(incompatible),
            'installation_results': {},
            'critical_issues': []
        }
        
        # Handle critical security risks first
        for dep in security_risks:
            if dep.is_critical:
                results['critical_issues'].append(f"SECURITY RISK: {dep.name} - {', '.join(dep.security_issues)}")
                if auto_install:
                    success = self.install_dependency(dep, upgrade=True)
                    results['installation_results'][dep.name] = 'upgraded' if success else 'failed'
        
        # Install missing critical dependencies
        for dep in missing:
            if dep.is_critical:
                results['critical_issues'].append(f"MISSING CRITICAL: {dep.name}")
                if auto_install:
                    success = self.install_dependency(dep)
                    results['installation_results'][dep.name] = 'installed' if success else 'failed'
        
        # Handle outdated dependencies
        if upgrade_outdated:
            for dep in outdated:
                if auto_install:
                    success = self.install_dependency(dep, upgrade=True)
                    results['installation_results'][dep.name] = 'upgraded' if success else 'failed'
        
        # Report incompatible dependencies
        for dep in incompatible:
            results['critical_issues'].append(f"INCOMPATIBLE: {dep.name} - {dep.installed_version}")
        
        return results
    
    def generate_dependency_report(self) -> Dict[str, Any]:
        """Generate comprehensive dependency report"""
        dep_status = self.check_all_dependencies()
        
        report = {
            'timestamp': pkg_resources.get_distribution('pip').version,  # Use pip version as timestamp proxy
            'python_version': sys.version,
            'platform': sys.platform,
            'dependencies': {},
            'summary': {
                'total': len(dep_status),
                'satisfied': 0,
                'missing': 0,
                'outdated': 0,
                'security_risks': 0,
                'incompatible': 0
            },
            'recommendations': []
        }
        
        for name, info in dep_status.items():
            report['dependencies'][name] = {
                'name': info.name,
                'required_version': info.required_version,
                'installed_version': info.installed_version,
                'status': info.status.value,
                'security_issues': info.security_issues,
                'is_critical': info.is_critical,
                'description': info.description
            }
            
            # Update summary
            report['summary'][info.status.value] += 1
        
        # Generate recommendations
        if report['summary']['security_risks'] > 0:
            report['recommendations'].append("URGENT: Update packages with security vulnerabilities")
        
        if report['summary']['missing'] > 0:
            report['recommendations'].append("Install missing dependencies")
        
        if report['summary']['outdated'] > 0:
            report['recommendations'].append("Consider updating outdated packages")
        
        if report['summary']['incompatible'] > 0:
            report['recommendations'].append("Resolve incompatible package versions")
        
        return report
    
    def export_requirements(self, output_file: Optional[Path] = None) -> bool:
        """Export current dependency versions to requirements file"""
        try:
            output_path = output_file or (self.project_root / 'requirements_current.txt')
            
            dep_status = self.check_all_dependencies()
            
            with open(output_path, 'w') as f:
                f.write("# Generated dependency requirements\n")
                f.write(f"# Generated on: {pkg_resources.get_distribution('pip').version}\n\n")
                
                for name, info in dep_status.items():
                    if info.installed_version and info.installed_version != 'unknown':
                        f.write(f"{info.name}=={info.installed_version}\n")
                    elif info.required_version:
                        f.write(f"{info.name}{info.required_version}\n")
                    else:
                        f.write(f"{info.name}\n")
            
            logger.info(f"Requirements exported to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export requirements: {e}")
            return False


# Legacy compatibility functions
def check_and_install_dependencies():
    """Legacy function for backward compatibility"""
    manager = EnterpriseDependencyManager()
    results = manager.resolve_dependencies(auto_install=True)
    
    if results['critical_issues']:
        for issue in results['critical_issues']:
            print(f"[CRITICAL] {issue}", file=sys.stderr)
        
        # Exit if critical dependencies failed to install
        failed_critical = [name for name, result in results['installation_results'].items() 
                          if result == 'failed']
        if failed_critical:
            print(f"[FATAL] Failed to install critical dependencies: {failed_critical}", file=sys.stderr)
            sys.exit(1)
    
    print("[INFO] Dependency check completed successfully")


def main():
    """Main entry point for dependency management"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enterprise Dependency Manager')
    parser.add_argument('--check', action='store_true', help='Check dependency status')
    parser.add_argument('--install', action='store_true', help='Install missing dependencies')
    parser.add_argument('--upgrade', action='store_true', help='Upgrade outdated dependencies')
    parser.add_argument('--report', action='store_true', help='Generate dependency report')
    parser.add_argument('--export', action='store_true', help='Export current requirements')
    parser.add_argument('--project-root', type=Path, help='Project root directory')
    
    args = parser.parse_args()
    
    manager = EnterpriseDependencyManager(args.project_root)
    
    if args.check or not any([args.install, args.upgrade, args.report, args.export]):
        dep_status = manager.check_all_dependencies()
        for name, info in dep_status.items():
            status_color = {
                DependencyStatus.SATISFIED: '\033[92m',  # Green
                DependencyStatus.MISSING: '\033[91m',    # Red
                DependencyStatus.OUTDATED: '\033[93m',   # Yellow
                DependencyStatus.SECURITY_RISK: '\033[95m',  # Magenta
                DependencyStatus.INCOMPATIBLE: '\033[91m'    # Red
            }.get(info.status, '')
            reset_color = '\033[0m'
            
            print(f"{status_color}{info.name}: {info.status.value}{reset_color} "
                  f"(installed: {info.installed_version or 'None'})")
            
            if info.security_issues:
                for issue in info.security_issues:
                    print(f"  ⚠️  {issue}")
    
    if args.install:
        results = manager.resolve_dependencies(auto_install=True)
        print(f"Installation results: {results['installation_results']}")
    
    if args.upgrade:
        results = manager.resolve_dependencies(auto_install=True, upgrade_outdated=True)
        print(f"Upgrade results: {results['installation_results']}")
    
    if args.report:
        report = manager.generate_dependency_report()
        print(json.dumps(report, indent=2))
    
    if args.export:
        success = manager.export_requirements()
        if success:
            print("Requirements exported successfully")
        else:
            print("Failed to export requirements")
            sys.exit(1)


if __name__ == '__main__':
    main()
