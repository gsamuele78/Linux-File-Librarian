#!/bin/bash

# Fix destination directory permissions

echo "Linux File Librarian - Destination Fix"
echo "======================================"

# Get library root from config
LIBRARY_ROOT=$(grep "^library_root" conf/config.ini | cut -d'=' -f2 | xargs)

if [ -z "$LIBRARY_ROOT" ]; then
    echo "[ERROR] Could not find library_root in config.ini"
    exit 1
fi

echo "Library root: $LIBRARY_ROOT"

# Check if directory exists
if [ ! -d "$LIBRARY_ROOT" ]; then
    echo "[ERROR] Directory does not exist: $LIBRARY_ROOT"
    exit 1
fi

# Check current permissions
echo "Current permissions:"
ls -ld "$LIBRARY_ROOT"

# Check if writable
if [ -w "$LIBRARY_ROOT" ]; then
    echo "[INFO] Directory is already writable"
    exit 0
fi

echo ""
echo "The destination directory is not writable."
echo "This is likely because it's on a USB drive mounted as root."
echo ""
echo "Solutions:"
echo "1. Fix permissions with sudo:"
echo "   sudo chown -R $USER:$USER '$LIBRARY_ROOT'"
echo "   sudo chmod -R 755 '$LIBRARY_ROOT'"
echo ""
echo "2. Or change destination to a local directory in config.ini:"
echo "   library_root = $HOME/Giochi_di_ruolo_merged"
echo ""

read -p "Do you want to try fixing permissions with sudo? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Fixing permissions..."
    sudo chown -R $USER:$USER "$LIBRARY_ROOT"
    sudo chmod -R 755 "$LIBRARY_ROOT"
    
    # Test write
    if touch "$LIBRARY_ROOT/.test" 2>/dev/null; then
        rm "$LIBRARY_ROOT/.test"
        echo "[SUCCESS] Permissions fixed! Directory is now writable."
    else
        echo "[ERROR] Permission fix failed. Directory is still not writable."
        exit 1
    fi
else
    echo "Please fix permissions manually or change the destination directory."
    exit 1
fi