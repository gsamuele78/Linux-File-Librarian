#!/bin/bash
# Clean all log files before starting operations

echo "[CLEANUP] Cleaning all log files..."

# Remove all log files in logs directory
if [ -d "logs" ]; then
    rm -f logs/*.log logs/*.log.* 2>/dev/null
    echo "[CLEANUP] Removed log files from logs/ directory"
fi

# Remove root level log files
rm -f *.log *.log.* 2>/dev/null

# Remove specific log files
rm -f librarian_run.log* librarian_professional.log* librarian_install.log* 2>/dev/null
rm -f bad_pdfs.log hashing_analysis_errors.log 2>/dev/null

# Remove CSV and analysis files
rm -f analysis_results.csv file_scan_batches.csv* pdf_validation_results.csv 2>/dev/null
rm -f cleanup_analysis.txt 2>/dev/null

# Remove report files
rm -f librarian_*_report.json 2>/dev/null

echo "[CLEANUP] Log cleanup completed"