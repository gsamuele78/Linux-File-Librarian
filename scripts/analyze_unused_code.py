#!/usr/bin/env python3
"""
Analyze workspace for unused code that can be safely cleaned up
"""

import ast
import os
import re
from pathlib import Path
from typing import Set, Dict, List


class CodeAnalyzer:
    def __init__(self, workspace_root: str):
        self.workspace_root = Path(workspace_root)
        self.used_modules = set()
        self.used_functions = set()
        self.used_classes = set()
        self.all_modules = set()
        self.all_functions = set()
        self.all_classes = set()
        
    def analyze_professional_dependencies(self):
        """Analyze dependencies starting from run_professional.sh"""
        print("=== ANALYZING PROFESSIONAL WORKFLOW DEPENDENCIES ===\n")
        
        # Start with professional orchestrator
        prof_orchestrator = self.workspace_root / "src" / "professional_orchestrator.py"
        if prof_orchestrator.exists():
            self._analyze_python_file(prof_orchestrator)
        
        # Analyze enterprise architecture
        enterprise_arch = self.workspace_root / "src" / "enterprise_architecture.py"
        if enterprise_arch.exists():
            self._analyze_python_file(enterprise_arch)
            
        # Analyze enhanced copy utils
        enhanced_copy = self.workspace_root / "src" / "enhanced_copy_utils.py"
        if enhanced_copy.exists():
            self._analyze_python_file(enhanced_copy)
    
    def scan_all_python_files(self):
        """Scan all Python files to build complete inventory"""
        print("=== SCANNING ALL PYTHON FILES ===\n")
        
        src_dir = self.workspace_root / "src"
        if src_dir.exists():
            for py_file in src_dir.glob("*.py"):
                self._inventory_python_file(py_file)
    
    def _analyze_python_file(self, file_path: Path):
        """Analyze a Python file for imports and usage"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parse AST
            tree = ast.parse(content)
            
            # Find imports
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith('src.'):
                            self.used_modules.add(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.module and node.module.startswith('src.'):
                        self.used_modules.add(node.module)
                        for alias in node.names:
                            self.used_functions.add(f"{node.module}.{alias.name}")
                            self.used_classes.add(f"{node.module}.{alias.name}")
            
            print(f"Analyzed: {file_path.name}")
            
        except Exception as e:
            print(f"Error analyzing {file_path}: {e}")
    
    def _inventory_python_file(self, file_path: Path):
        """Build inventory of all functions and classes in a file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            module_name = f"src.{file_path.stem}"
            self.all_modules.add(module_name)
            
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    self.all_functions.add(f"{module_name}.{node.name}")
                elif isinstance(node, ast.ClassDef):
                    self.all_classes.add(f"{module_name}.{node.name}")
            
        except Exception as e:
            print(f"Error inventorying {file_path}: {e}")
    
    def find_unused_code(self):
        """Identify unused modules, functions, and classes"""
        print("\n=== UNUSED CODE ANALYSIS ===\n")
        
        unused_modules = self.all_modules - self.used_modules
        unused_functions = self.all_functions - self.used_functions
        unused_classes = self.all_classes - self.used_classes
        
        # Filter out main entry points and special methods
        unused_modules = {m for m in unused_modules if not self._is_entry_point(m)}
        unused_functions = {f for f in unused_functions if not self._is_special_function(f)}
        unused_classes = {c for c in unused_classes if not self._is_special_class(c)}
        
        return unused_modules, unused_functions, unused_classes
    
    def _is_entry_point(self, module_name: str) -> bool:
        """Check if module is an entry point"""
        entry_points = [
            'src.librarian',
            'src.professional_orchestrator',
            'src.build_knowledgebase',
            'src.search_gui'
        ]
        return module_name in entry_points
    
    def _is_special_function(self, func_name: str) -> bool:
        """Check if function is special (main, __init__, etc.)"""
        return (func_name.endswith('.main') or 
                func_name.endswith('.__init__') or
                func_name.endswith('.__del__') or
                '.__' in func_name)
    
    def _is_special_class(self, class_name: str) -> bool:
        """Check if class is special or commonly used"""
        return False  # Most classes should be checked
    
    def generate_cleanup_report(self):
        """Generate comprehensive cleanup report"""
        print("=== GENERATING CLEANUP REPORT ===\n")
        
        self.analyze_professional_dependencies()
        self.scan_all_python_files()
        unused_modules, unused_functions, unused_classes = self.find_unused_code()
        
        report = []
        report.append("LINUX FILE LIBRARIAN - UNUSED CODE ANALYSIS")
        report.append("=" * 50)
        report.append("")
        
        if unused_modules:
            report.append("UNUSED MODULES (can be safely removed):")
            for module in sorted(unused_modules):
                file_path = module.replace('src.', 'src/') + '.py'
                report.append(f"  - {file_path}")
            report.append("")
        
        if unused_functions:
            report.append("UNUSED FUNCTIONS (can be removed from files):")
            by_module = {}
            for func in unused_functions:
                module, func_name = func.rsplit('.', 1)
                if module not in by_module:
                    by_module[module] = []
                by_module[module].append(func_name)
            
            for module in sorted(by_module.keys()):
                file_path = module.replace('src.', 'src/') + '.py'
                report.append(f"  {file_path}:")
                for func in sorted(by_module[module]):
                    report.append(f"    - {func}()")
            report.append("")
        
        if unused_classes:
            report.append("UNUSED CLASSES (can be removed from files):")
            by_module = {}
            for cls in unused_classes:
                module, cls_name = cls.rsplit('.', 1)
                if module not in by_module:
                    by_module[module] = []
                by_module[module].append(cls_name)
            
            for module in sorted(by_module.keys()):
                file_path = module.replace('src.', 'src/') + '.py'
                report.append(f"  {file_path}:")
                for cls in sorted(by_module[module]):
                    report.append(f"    - class {cls}")
            report.append("")
        
        # Additional cleanup suggestions
        report.append("ADDITIONAL CLEANUP SUGGESTIONS:")
        report.append("  - Remove __pycache__ directories")
        report.append("  - Remove .pyc files")
        report.append("  - Remove temporary .tmp files")
        report.append("  - Remove old log files")
        report.append("  - Remove intermediate_results.sqlite if exists")
        report.append("")
        
        report.append("USED MODULES (keep these):")
        for module in sorted(self.used_modules):
            file_path = module.replace('src.', 'src/') + '.py'
            report.append(f"  - {file_path}")
        
        return "\n".join(report)


def main():
    workspace_root = Path(__file__).parent.parent
    analyzer = CodeAnalyzer(str(workspace_root))
    
    report = analyzer.generate_cleanup_report()
    
    # Save report
    report_file = workspace_root / "cleanup_analysis.txt"
    with open(report_file, 'w') as f:
        f.write(report)
    
    print(report)
    print(f"\nReport saved to: {report_file}")


if __name__ == "__main__":
    main()