#!/usr/bin/env python3
"""
Python utility to clean all log files
"""

import os
import glob
from pathlib import Path

def cleanup_logs():
    """Clean all log files in the project"""
    project_root = Path(__file__).parent.parent
    
    print("[CLEANUP] Cleaning all log files...")
    
    # Log directories and patterns to clean
    log_patterns = [
        "logs/*.log*",
        "*.log*",
        "analysis_results.csv",
        "file_scan_batches.csv*",
        "pdf_validation_results.csv",
        "cleanup_analysis.txt",
        "librarian_*_report.json",
        "bad_pdfs.log",
        "hashing_analysis_errors.log"
    ]
    
    files_removed = 0
    
    for pattern in log_patterns:
        full_pattern = project_root / pattern
        for file_path in glob.glob(str(full_pattern)):
            try:
                os.remove(file_path)
                files_removed += 1
            except (OSError, PermissionError):
                pass  # Ignore files that can't be removed
    
    print(f"[CLEANUP] Removed {files_removed} log files")

if __name__ == "__main__":
    cleanup_logs()