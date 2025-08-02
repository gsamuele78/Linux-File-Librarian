#!/usr/bin/env python3
"""
Cleanup unused code from the workspace
"""

import os
import shutil
from pathlib import Path


def cleanup_unused_modules():
    """Remove unused modules identified by analysis"""
    workspace_root = Path(__file__).parent.parent
    
    unused_modules = [
        "src/__init__.py",
        "src/cleanup_utils.py", 
        "src/dependency_manager.py",
        "src/enterprise_optimizer.py",
        "src/integration_adapter.py",
        "src/library_builder.py",
        "src/logger.py",
        "src/memory_constrained_worker.py",
        "src/pdf_manager.py",
        "src/performance_monitor.py",
        "src/resource_manager.py",
        "src/system_optimization.py"
    ]
    
    print("=== REMOVING UNUSED MODULES ===")
    for module in unused_modules:
        file_path = workspace_root / module
        if file_path.exists():
            file_path.unlink()
            print(f"Removed: {module}")
        else:
            print(f"Not found: {module}")


def cleanup_temp_files():
    """Remove temporary files and caches"""
    workspace_root = Path(__file__).parent.parent
    
    print("\n=== CLEANING TEMPORARY FILES ===")
    
    # Remove __pycache__ directories
    for pycache in workspace_root.rglob("__pycache__"):
        if pycache.is_dir():
            shutil.rmtree(pycache)
            print(f"Removed: {pycache}")
    
    # Remove .pyc files
    for pyc_file in workspace_root.rglob("*.pyc"):
        pyc_file.unlink()
        print(f"Removed: {pyc_file}")
    
    # Remove .tmp files
    for tmp_file in workspace_root.rglob("*.tmp"):
        tmp_file.unlink()
        print(f"Removed: {tmp_file}")
    
    # Remove intermediate database if exists
    intermediate_db = workspace_root / "intermediate_results.sqlite"
    if intermediate_db.exists():
        intermediate_db.unlink()
        print(f"Removed: {intermediate_db}")
    
    # Remove old log files (keep recent ones)
    logs_dir = workspace_root / "logs"
    if logs_dir.exists():
        for log_file in logs_dir.glob("*.log"):
            # Keep files modified in last 7 days
            if (log_file.stat().st_mtime < (os.path.getmtime('.') - 7*24*3600)):
                log_file.unlink()
                print(f"Removed old log: {log_file}")


def main():
    print("LINUX FILE LIBRARIAN - CODE CLEANUP")
    print("=" * 40)
    
    response = input("This will remove unused modules and temporary files. Continue? (y/N): ")
    if response.lower() != 'y':
        print("Cleanup cancelled.")
        return
    
    cleanup_unused_modules()
    cleanup_temp_files()
    
    print("\n=== CLEANUP COMPLETED ===")
    print("Remaining core modules for professional workflow:")
    print("  - src/professional_orchestrator.py")
    print("  - src/enterprise_architecture.py") 
    print("  - src/enhanced_copy_utils.py")
    print("  - src/classifier.py")
    print("  - src/config_loader.py")
    print("  - src/isbn_enricher.py")
    print("  - src/build_knowledgebase.py")
    print("  - src/search_gui.py")


if __name__ == "__main__":
    main()