#!/usr/bin/env python3
"""
Memory optimization script for Linux File Librarian
"""

import os
import gc
import psutil
import sys

def check_system_resources():
    """Check and report system resources"""
    print("System Resource Check")
    print("=" * 30)
    
    # Memory info
    memory = psutil.virtual_memory()
    print(f"Total RAM: {memory.total / (1024**3):.1f} GB")
    print(f"Available RAM: {memory.available / (1024**3):.1f} GB")
    print(f"Used RAM: {memory.used / (1024**3):.1f} GB")
    print(f"Memory Usage: {memory.percent:.1f}%")
    
    # Swap info
    swap = psutil.swap_memory()
    print(f"Total Swap: {swap.total / (1024**3):.1f} GB")
    print(f"Used Swap: {swap.used / (1024**3):.1f} GB")
    print(f"Swap Usage: {swap.percent:.1f}%")
    
    # CPU info
    print(f"CPU Cores: {psutil.cpu_count()}")
    print(f"CPU Usage: {psutil.cpu_percent(interval=1):.1f}%")
    
    # Disk space
    disk = psutil.disk_usage('/')
    print(f"Disk Space: {disk.free / (1024**3):.1f} GB free of {disk.total / (1024**3):.1f} GB")
    
    return memory.available / (1024**2)  # Return available MB

def optimize_system():
    """Perform system optimizations"""
    print("\nSystem Optimization")
    print("=" * 30)
    
    # Force garbage collection
    print("Running garbage collection...")
    collected = gc.collect()
    print(f"Collected {collected} objects")
    
    # Clear Python caches
    print("Clearing Python caches...")
    sys.modules.clear()
    
    # Suggest system optimizations
    print("\nRecommended system optimizations:")
    print("1. Close unnecessary applications")
    print("2. Clear browser caches")
    print("3. Restart the system if memory usage is high")
    
    memory = psutil.virtual_memory()
    if memory.percent > 80:
        print("\n[WARNING] High memory usage detected!")
        print("Consider:")
        print("- Restarting the system")
        print("- Closing other applications")
        print("- Running the librarian with smaller batch sizes")

def suggest_config_optimizations(available_mb):
    """Suggest configuration optimizations based on available memory"""
    print("\nConfiguration Recommendations")
    print("=" * 30)
    
    if available_mb < 2000:  # Less than 2GB
        print("LOW MEMORY SYSTEM DETECTED")
        print("Recommended settings:")
        print("- Use single-threaded processing")
        print("- Reduce batch sizes to 10-25 files")
        print("- Process files sequentially")
        print("- Consider processing in multiple runs")
        
    elif available_mb < 4000:  # Less than 4GB
        print("MODERATE MEMORY SYSTEM")
        print("Recommended settings:")
        print("- Use 1-2 worker processes")
        print("- Batch sizes of 25-50 files")
        print("- Monitor memory usage during processing")
        
    else:  # 4GB or more
        print("SUFFICIENT MEMORY SYSTEM")
        print("Recommended settings:")
        print("- Use 2-4 worker processes")
        print("- Batch sizes of 50-100 files")
        print("- Normal processing should work fine")

def clean_log_files():
    """Clean up large log files"""
    print("\nLog File Cleanup")
    print("=" * 30)
    
    log_files = ['librarian_run.log', 'file_scan_batches.csv']
    
    for log_file in log_files:
        if os.path.exists(log_file):
            size = os.path.getsize(log_file) / (1024**2)  # Size in MB
            print(f"{log_file}: {size:.1f} MB")
            
            if size > 100:  # If larger than 100MB
                print(f"[WARNING] {log_file} is large ({size:.1f} MB)")
                print(f"Consider backing up and truncating: mv {log_file} {log_file}.backup")
                
                # Offer to truncate
                response = input(f"Truncate {log_file}? (y/N): ").lower()
                if response == 'y':
                    try:
                        # Keep last 1000 lines
                        with open(log_file, 'r') as f:
                            lines = f.readlines()
                        
                        if len(lines) > 1000:
                            with open(f"{log_file}.backup", 'w') as f:
                                f.writelines(lines)
                            
                            with open(log_file, 'w') as f:
                                f.writelines(lines[-1000:])
                            
                            print(f"Truncated {log_file}, backup saved as {log_file}.backup")
                        else:
                            print(f"{log_file} is not large enough to truncate")
                            
                    except Exception as e:
                        print(f"Error truncating {log_file}: {e}")

def main():
    """Main optimization function"""
    print("Linux File Librarian - Memory Optimization")
    print("=" * 50)
    
    # Check system resources
    available_mb = check_system_resources()
    
    # Optimize system
    optimize_system()
    
    # Suggest configuration optimizations
    suggest_config_optimizations(available_mb)
    
    # Clean log files
    clean_log_files()
    
    print("\n" + "=" * 50)
    print("OPTIMIZATION COMPLETE")
    print("=" * 50)
    print("The system has been optimized for better performance.")
    print("Run the librarian again to see improved memory usage.")

if __name__ == "__main__":
    main()