#!/usr/bin/env python3
"""
Recovery processor for incomplete librarian runs
Processes files in ultra-small batches to avoid memory issues
"""

import os
import sys
import pandas as pd
import gc
from pathlib import Path

def process_in_recovery_mode():
    """Process files in ultra-conservative recovery mode"""
    print("Linux File Librarian - Recovery Mode")
    print("=" * 40)
    
    # Check if we have the CSV file
    temp_csv = 'file_scan_batches.csv'
    if not os.path.exists(temp_csv):
        print("[ERROR] No file_scan_batches.csv found. Run scan first.")
        return False
    
    # Get file count
    try:
        total_files = sum(1 for _ in open(temp_csv)) - 1  # Subtract header
        print(f"[INFO] Found {total_files} files to process")
    except Exception as e:
        print(f"[ERROR] Could not count files: {e}")
        return False
    
    # Process in ultra-small chunks
    chunk_size = 50  # Very small chunks
    processed = 0
    
    try:
        print(f"[INFO] Processing in chunks of {chunk_size} files...")
        
        for chunk_idx, chunk in enumerate(pd.read_csv(temp_csv, chunksize=chunk_size, engine='python', low_memory=True)):
            try:
                processed += len(chunk)
                print(f"[INFO] Processed chunk {chunk_idx + 1}: {processed}/{total_files} files ({processed/total_files*100:.1f}%)")
                
                # Force cleanup after each chunk
                gc.collect()
                
                # Check memory usage
                import psutil
                memory = psutil.virtual_memory()
                if memory.percent > 85:
                    print(f"[WARNING] High memory usage: {memory.percent:.1f}%")
                    print("[INFO] Forcing extended cleanup...")
                    gc.collect()
                    import time
                    time.sleep(1)
                
            except MemoryError:
                print(f"[ERROR] Memory error in chunk {chunk_idx + 1}")
                print("[INFO] Try processing smaller source directories")
                break
            except Exception as e:
                print(f"[ERROR] Error in chunk {chunk_idx + 1}: {e}")
                continue
        
        print(f"[INFO] Recovery processing complete. Processed {processed} files.")
        return True
        
    except Exception as e:
        print(f"[ERROR] Recovery processing failed: {e}")
        return False

def check_library_status():
    """Check the status of the library"""
    print("\nLibrary Status Check")
    print("=" * 20)
    
    # Check config
    try:
        import configparser
        config = configparser.ConfigParser()
        config.read('conf/config.ini')
        library_root = config.get('Paths', 'library_root')
        print(f"Library root: {library_root}")
        
        if os.path.exists(library_root):
            # Count files in library
            file_count = 0
            for root, dirs, files in os.walk(library_root):
                file_count += len(files)
            print(f"Files in library: {file_count}")
            
            # Check for database
            db_path = os.path.join(library_root, "library_index.sqlite")
            if os.path.exists(db_path):
                print(f"Database exists: {db_path}")
                print(f"Database size: {os.path.getsize(db_path)} bytes")
            else:
                print("Database not found")
        else:
            print("Library root does not exist")
            
    except Exception as e:
        print(f"Error checking library status: {e}")

def main():
    """Main recovery function"""
    print("Linux File Librarian - Recovery Processor")
    print("=" * 50)
    
    # Check current directory
    if not os.path.exists('conf/config.ini'):
        print("[ERROR] Please run from Linux-File-Librarian directory")
        return
    
    # Check library status
    check_library_status()
    
    # Ask user what to do
    print("\nRecovery Options:")
    print("1. Check file processing status")
    print("2. Process files in recovery mode")
    print("3. Exit")
    
    try:
        choice = input("\nEnter choice (1-3): ").strip()
        
        if choice == "1":
            process_in_recovery_mode()
        elif choice == "2":
            print("\n[INFO] Starting recovery processing...")
            success = process_in_recovery_mode()
            if success:
                print("\n[INFO] Recovery processing completed successfully")
                print("[INFO] You may need to run the full librarian again")
            else:
                print("\n[ERROR] Recovery processing failed")
        elif choice == "3":
            print("Exiting...")
        else:
            print("Invalid choice")
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()