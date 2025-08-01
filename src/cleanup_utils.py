import os
import glob

def cleanup_temp_files():
    """Clean up temporary files before starting operations"""
    temp_files = [
        'file_scan_batches.csv',
        'librarian_run.log',
        '.write_test',
        'temp_knowledge.sqlite',
        '/tmp/knowledge.sqlite'
    ]
    
    temp_patterns = [
        '*.tmp',
        '*.temp',
        'temp_*',
        '.temp_*',
        '*.log.old',
        '*.bak',
        '*~'
    ]
    
    cleaned_count = 0
    
    # Clean specific temp files
    for temp_file in temp_files:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                print(f"[CLEANUP] Removed: {temp_file}")
                cleaned_count += 1
            except Exception as e:
                print(f"[CLEANUP] Could not remove {temp_file}: {e}")
    
    # Clean temp patterns
    for pattern in temp_patterns:
        for file_path in glob.glob(pattern):
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"[CLEANUP] Removed: {file_path}")
                    cleaned_count += 1
            except Exception as e:
                print(f"[CLEANUP] Could not remove {file_path}: {e}")
    
    if cleaned_count > 0:
        print(f"[CLEANUP] Cleaned {cleaned_count} temporary files")
    else:
        print("[CLEANUP] No temporary files to clean")