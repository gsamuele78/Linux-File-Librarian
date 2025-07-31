#!/bin/bash

# Linux File Librarian - Error Fix Script
# This script fixes the common errors found in the log file

echo "Linux File Librarian - Error Fix Script"
echo "========================================"

# Check if we're in the right directory
if [ ! -f "conf/config.ini" ]; then
    echo "[ERROR] Please run this script from the Linux-File-Librarian directory"
    exit 1
fi

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "[INFO] Activating virtual environment..."
    source venv/bin/activate
fi

echo ""
echo "Step 1: Running cleanup script..."
echo "--------------------------------"
python3 scripts/cleanup_broken_files.py

echo ""
echo "Step 2: Running memory optimization..."
echo "------------------------------------"
python3 scripts/optimize_memory.py

echo ""
echo "Step 3: Clearing temporary files..."
echo "----------------------------------"

# Remove temporary files that might be corrupted
if [ -f "file_scan_batches.csv" ]; then
    echo "[INFO] Backing up file_scan_batches.csv..."
    mv file_scan_batches.csv file_scan_batches.csv.backup
fi

if [ -f "librarian_run.log" ]; then
    echo "[INFO] Backing up librarian_run.log..."
    mv librarian_run.log librarian_run.log.backup
fi

# Clear Python cache
echo "[INFO] Clearing Python cache..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true

echo ""
echo "Step 4: Testing system resources..."
echo "----------------------------------"
python3 -c "
import psutil
import gc

# Force garbage collection
gc.collect()

# Check memory
memory = psutil.virtual_memory()
print(f'Available RAM: {memory.available / (1024**2):.0f} MB')
print(f'Memory Usage: {memory.percent:.1f}%')

if memory.percent > 80:
    print('[WARNING] High memory usage - consider restarting the system')
elif memory.available < 2000 * 1024 * 1024:  # Less than 2GB
    print('[WARNING] Low available memory - librarian will use conservative settings')
else:
    print('[OK] Memory looks good for processing')
"

echo ""
echo "========================================"
echo "ERROR FIX COMPLETE"
echo "========================================"
echo ""
echo "Summary of fixes applied:"
echo "- Fixed memory management in logger"
echo "- Improved broken symlink handling"
echo "- Added better error handling for image files"
echo "- Optimized memory usage in classification"
echo "- Cleaned up temporary files"
echo ""
echo "You can now run the librarian again:"
echo "  ./scripts/run_librarian.sh"
echo ""
echo "If you still encounter memory issues, consider:"
echo "1. Restarting your system"
echo "2. Processing smaller batches of files"
echo "3. Running the librarian on individual source directories"