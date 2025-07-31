#!/usr/bin/env python3
"""
Fix permissions for the library destination directory
"""

import os
import sys
import configparser
import subprocess

def fix_destination_permissions():
    """Fix permissions for the destination directory"""
    print("Linux File Librarian - Permission Fix")
    print("=" * 40)
    
    # Read config
    try:
        config = configparser.ConfigParser()
        config.read('conf/config.ini')
        library_root = config.get('Paths', 'library_root')
        print(f"Library root: {library_root}")
    except Exception as e:
        print(f"[ERROR] Could not read config: {e}")
        return False
    
    # Check current permissions
    if not os.path.exists(library_root):
        print(f"[ERROR] Directory does not exist: {library_root}")
        return False
    
    print(f"Current permissions:")
    print(f"  Readable: {os.access(library_root, os.R_OK)}")
    print(f"  Writable: {os.access(library_root, os.W_OK)}")
    print(f"  Executable: {os.access(library_root, os.X_OK)}")
    
    # Get current user
    current_user = os.getenv('USER')
    print(f"Current user: {current_user}")
    
    # Check ownership
    try:
        stat_info = os.stat(library_root)
        import pwd
        owner = pwd.getpwuid(stat_info.st_uid).pw_name
        print(f"Directory owner: {owner}")
        
        if owner != current_user:
            print(f"[INFO] Directory is owned by {owner}, not {current_user}")
            print(f"[INFO] Need to change ownership or permissions")
    except Exception as e:
        print(f"[WARNING] Could not check ownership: {e}")
    
    # Try to fix permissions
    print("\nAttempting to fix permissions...")
    
    # Method 1: Try chmod
    try:
        os.chmod(library_root, 0o755)
        print("[INFO] chmod 755 applied")
    except Exception as e:
        print(f"[WARNING] chmod failed: {e}")
    
    # Method 2: Try with sudo
    if not os.access(library_root, os.W_OK):
        print("[INFO] Trying with sudo...")
        try:
            # Change ownership to current user
            cmd = f"sudo chown -R {current_user}:{current_user} '{library_root}'"
            print(f"Running: {cmd}")
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                print("[INFO] Ownership changed successfully")
                
                # Set permissions
                cmd2 = f"sudo chmod -R 755 '{library_root}'"
                print(f"Running: {cmd2}")
                result2 = subprocess.run(cmd2, shell=True, capture_output=True, text=True)
                
                if result2.returncode == 0:
                    print("[INFO] Permissions set successfully")
                else:
                    print(f"[ERROR] chmod failed: {result2.stderr}")
            else:
                print(f"[ERROR] chown failed: {result.stderr}")
                
        except Exception as e:
            print(f"[ERROR] sudo command failed: {e}")
    
    # Test final permissions
    print("\nFinal permission check:")
    print(f"  Readable: {os.access(library_root, os.R_OK)}")
    print(f"  Writable: {os.access(library_root, os.W_OK)}")
    print(f"  Executable: {os.access(library_root, os.X_OK)}")
    
    # Test write
    try:
        test_file = os.path.join(library_root, '.write_test')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        print("  Write test: PASSED")
        return True
    except Exception as e:
        print(f"  Write test: FAILED - {e}")
        return False

def main():
    """Main function"""
    if not os.path.exists('conf/config.ini'):
        print("[ERROR] Please run from Linux-File-Librarian directory")
        return
    
    success = fix_destination_permissions()
    
    if success:
        print("\n" + "=" * 40)
        print("PERMISSIONS FIXED SUCCESSFULLY")
        print("=" * 40)
        print("You can now run the librarian:")
        print("  ./scripts/run_librarian.sh")
    else:
        print("\n" + "=" * 40)
        print("PERMISSION FIX FAILED")
        print("=" * 40)
        print("Manual steps required:")
        print("1. Check if the USB drive is mounted read-only")
        print("2. Remount with write permissions")
        print("3. Or choose a different destination directory")

if __name__ == "__main__":
    main()