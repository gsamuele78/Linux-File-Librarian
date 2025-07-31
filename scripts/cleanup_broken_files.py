#!/usr/bin/env python3
"""
Cleanup script to fix broken symlinks and problematic files
that are causing issues in the Linux File Librarian.
"""

import os
import sys
from pathlib import Path

def find_and_fix_broken_symlinks(source_paths):
    """Find and report broken symlinks in source paths"""
    broken_links = []
    
    for source_path in source_paths:
        if not os.path.exists(source_path):
            print(f"[WARNING] Source path does not exist: {source_path}")
            continue
            
        print(f"[INFO] Scanning {source_path} for broken symlinks...")
        
        for root, dirs, files in os.walk(source_path):
            # Check files
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.islink(file_path):
                    try:
                        # Check if the symlink target exists
                        if not os.path.exists(os.readlink(file_path)):
                            broken_links.append(file_path)
                            print(f"[BROKEN_SYMLINK] {file_path}")
                    except (OSError, IOError) as e:
                        broken_links.append(file_path)
                        print(f"[BROKEN_SYMLINK] {file_path} - Error: {e}")
            
            # Check directories
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                if os.path.islink(dir_path):
                    try:
                        if not os.path.exists(os.readlink(dir_path)):
                            broken_links.append(dir_path)
                            print(f"[BROKEN_SYMLINK] {dir_path}")
                    except (OSError, IOError) as e:
                        broken_links.append(dir_path)
                        print(f"[BROKEN_SYMLINK] {dir_path} - Error: {e}")
    
    return broken_links

def check_specific_broken_files():
    """Check the specific files mentioned in the log"""
    problematic_files = [
        '/home/jfs/Scrivania/00-Giochi_di_ruolo/Collegamento a pcgen.sh',
        '/home/jfs/Scrivania/00-Giochi_di_ruolo/000_Giochi_di_ruolo_final_clean/00_Campagne_Jfs_Master/00_Nuova_Campagna_valle_cannath/Forgotten_Realms_EN',
        '/home/jfs/Scrivania/00-Giochi_di_ruolo/000_Giochi_di_ruolo_final_clean/00_Campagne_Jfs_Master/00_Nuova_Campagna_valle_cannath/Forgotten_Realms_ITA',
        '/home/jfs/Scrivania/00-Giochi_di_ruolo/000_Giochi_di_ruolo_final_clean/00_Campagne_Jfs_Master/00_Nuova_Campagna_valle_cannath/Musica',
        '/home/jfs/Scrivania/00-Giochi_di_ruolo/000_Giochi_di_ruolo_final_clean/00_Campagne_Jfs_Master/00_Nuova_Campagna_valle_cannath/Red_hand_of_doom',
        '/home/jfs/Scrivania/00-Giochi_di_ruolo/000_Giochi_di_ruolo_final_clean/00_Campagne_Jfs_Master/00_Nuova_Campagna_valle_cannath/Splendente_Sud.pdf'
    ]
    
    print("\n[INFO] Checking specific problematic files...")
    for file_path in problematic_files:
        if os.path.exists(file_path):
            if os.path.islink(file_path):
                try:
                    target = os.readlink(file_path)
                    if os.path.exists(target):
                        print(f"[OK] Symlink exists and target is valid: {file_path} -> {target}")
                    else:
                        print(f"[BROKEN] Symlink target missing: {file_path} -> {target}")
                except Exception as e:
                    print(f"[ERROR] Cannot read symlink: {file_path} - {e}")
            else:
                print(f"[OK] File exists: {file_path}")
        else:
            print(f"[MISSING] File does not exist: {file_path}")

def clean_temp_files():
    """Clean up temporary files that might be causing issues"""
    temp_files = [
        'file_scan_batches.csv',
        'librarian_run.log'
    ]
    
    print("\n[INFO] Cleaning temporary files...")
    for temp_file in temp_files:
        if os.path.exists(temp_file):
            try:
                # Create backup before removing
                backup_name = f"{temp_file}.backup"
                if os.path.exists(backup_name):
                    os.remove(backup_name)
                os.rename(temp_file, backup_name)
                print(f"[CLEANED] Moved {temp_file} to {backup_name}")
            except Exception as e:
                print(f"[ERROR] Could not clean {temp_file}: {e}")

def main():
    """Main cleanup function"""
    print("Linux File Librarian - Cleanup Script")
    print("=" * 50)
    
    # Default source paths - adjust these based on your config
    source_paths = [
        '/home/jfs/Scrivania/00-Giochi_di_ruolo'
    ]
    
    # Check if config file exists to get actual source paths
    config_file = 'conf/config.ini'
    if os.path.exists(config_file):
        try:
            import configparser
            config = configparser.ConfigParser()
            config.read(config_file)
            if 'DEFAULT' in config and 'source_paths' in config['DEFAULT']:
                source_paths = [path.strip() for path in config['DEFAULT']['source_paths'].split(',')]
                print(f"[INFO] Using source paths from config: {source_paths}")
        except Exception as e:
            print(f"[WARNING] Could not read config file: {e}")
    
    # Find broken symlinks
    broken_links = find_and_fix_broken_symlinks(source_paths)
    
    # Check specific problematic files
    check_specific_broken_files()
    
    # Clean temporary files
    clean_temp_files()
    
    # Summary
    print("\n" + "=" * 50)
    print("CLEANUP SUMMARY")
    print("=" * 50)
    print(f"Broken symlinks found: {len(broken_links)}")
    
    if broken_links:
        print("\nBroken symlinks:")
        for link in broken_links[:10]:  # Show first 10
            print(f"  - {link}")
        if len(broken_links) > 10:
            print(f"  ... and {len(broken_links) - 10} more")
    
    print("\n[INFO] Cleanup complete. You can now run the librarian again.")
    print("[INFO] The system will now skip broken symlinks automatically.")

if __name__ == "__main__":
    main()