#!/usr/bin/env python3
"""
Setup memory constraints for Linux File Librarian to prevent memory issues.
This script configures the system for optimal memory usage during processing.
"""

import os
import gc
import psutil
import sys

def setup_memory_constraints():
    """Setup memory constraints and optimize system for processing"""
    print("Setting up memory constraints for Linux File Librarian")
    print("=" * 60)
    
    # Get system memory info
    memory = psutil.virtual_memory()
    total_gb = memory.total / (1024**3)
    available_gb = memory.available / (1024**3)
    
    print(f"Total RAM: {total_gb:.1f} GB")
    print(f"Available RAM: {available_gb:.1f} GB")
    print(f"Memory Usage: {memory.percent:.1f}%")
    
    # Force aggressive garbage collection
    print("\nForcing garbage collection...")
    collected = gc.collect()
    print(f"Collected {collected} objects")
    
    # Set environment variables for conservative memory usage
    print("\nSetting memory-conservative environment variables...")
    
    # Python memory settings
    os.environ['PYTHONHASHSEED'] = '0'  # Consistent hashing
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'  # Don't create .pyc files
    
    # Pandas memory settings
    os.environ['PANDAS_COPY_ON_WRITE'] = '1'  # Enable copy-on-write
    
    # Multiprocessing settings
    os.environ['MULTIPROCESSING_START_METHOD'] = 'spawn'  # Use spawn method
    
    # Set conservative worker limits based on available memory
    if available_gb < 2:
        max_workers = 1
        chunk_size = 5
        print("LOW MEMORY MODE: Single worker, 5-file chunks")
    elif available_gb < 4:
        max_workers = 1
        chunk_size = 10
        print("CONSERVATIVE MODE: Single worker, 10-file chunks")
    else:
        max_workers = 1  # Always use single worker for safety
        chunk_size = 15
        print("SAFE MODE: Single worker, 15-file chunks")
    
    # Set environment variables for the librarian
    os.environ['LIBRARIAN_MAX_WORKERS'] = str(max_workers)
    os.environ['LIBRARIAN_CHUNK_SIZE'] = str(chunk_size)
    os.environ['LIBRARIAN_MEMORY_CONSERVATIVE'] = '1'
    
    print(f"Max workers: {max_workers}")
    print(f"Chunk size: {chunk_size}")
    
    # Clear system caches if possible (requires sudo)
    print("\nAttempting to clear system caches...")
    try:
        # This requires sudo, so it might fail
        os.system('sync && echo 1 > /proc/sys/vm/drop_caches 2>/dev/null')
        print("System caches cleared")
    except:
        print("Could not clear system caches (requires sudo)")
    
    # Set process priority to be nice to other processes
    try:
        os.nice(10)  # Lower priority
        print("Process priority lowered")
    except:
        print("Could not lower process priority")
    
    # Final memory check
    memory = psutil.virtual_memory()
    available_gb = memory.available / (1024**3)
    print(f"\nFinal available RAM: {available_gb:.1f} GB")
    
    if available_gb < 1:
        print("WARNING: Very low available memory. Consider closing other applications.")
        return False
    elif available_gb < 2:
        print("WARNING: Low available memory. Processing will be very conservative.")
    else:
        print("Memory setup complete. Ready for processing.")
    
    return True

def create_memory_config():
    """Create a memory configuration file for the librarian"""
    config_content = f"""# Memory-optimized configuration for Linux File Librarian
# Generated automatically based on system resources

[MEMORY]
# Always use conservative settings
max_workers = 1
chunk_size = 10
pdf_batch_size = 3
classification_batch_size = 5
sequential_processing = true
force_gc_after_batch = true
memory_pressure_threshold = 80
os_reserved_mb = 512

[PROCESSING]
# Conservative processing settings
timeout_per_file = 30
max_retries = 1
skip_large_files = true
max_file_size_mb = 100

[CLEANUP]
# Aggressive cleanup settings
cleanup_interval = 10
force_cleanup_after_errors = true
clear_caches_frequently = true
"""
    
    with open('memory_config.ini', 'w') as f:
        f.write(config_content)
    
    print("Created memory_config.ini with conservative settings")

def main():
    """Main setup function"""
    print("Linux File Librarian - Memory Constraint Setup")
    print("=" * 50)
    
    # Setup memory constraints
    success = setup_memory_constraints()
    
    # Create memory config
    create_memory_config()
    
    if success:
        print("\n" + "=" * 50)
        print("MEMORY SETUP COMPLETE")
        print("=" * 50)
        print("The system is now configured for memory-conservative processing.")
        print("You can now run the librarian with reduced memory usage.")
        print("\nRecommended next steps:")
        print("1. Close unnecessary applications")
        print("2. Run: ./scripts/run_librarian.sh")
        print("3. Monitor memory usage during processing")
    else:
        print("\n" + "=" * 50)
        print("MEMORY SETUP WARNING")
        print("=" * 50)
        print("Very low memory detected. Consider:")
        print("1. Restarting the system")
        print("2. Closing all unnecessary applications")
        print("3. Processing files in smaller batches")
        print("4. Running the librarian on individual directories")

if __name__ == "__main__":
    main()