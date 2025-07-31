#!/bin/bash

# Script to find and optionally remove broken symlinks
# Usage: ./cleanup_broken_links.sh [--remove] [path]

REMOVE_LINKS=false
SEARCH_PATH=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --remove)
            REMOVE_LINKS=true
            shift
            ;;
        *)
            SEARCH_PATH="$1"
            shift
            ;;
    esac
done

# If no path provided, read from config
if [ -z "$SEARCH_PATH" ]; then
    if [ -f "../conf/config.ini" ]; then
        SEARCH_PATH=$(python3 -c "
import configparser
config = configparser.ConfigParser()
config.read('../conf/config.ini')
paths = config.get('Paths', 'source_paths', fallback='').split(',')
for path in paths:
    path = path.strip()
    if path:
        print(path)
        break
")
    fi
fi

if [ -z "$SEARCH_PATH" ]; then
    echo "Error: No search path provided and couldn't read from config"
    echo "Usage: $0 [--remove] [path]"
    exit 1
fi

echo "Searching for broken symlinks in: $SEARCH_PATH"

# Find broken symlinks
BROKEN_LINKS=$(find "$SEARCH_PATH" -type l ! -exec test -e {} \; -print 2>/dev/null)

if [ -z "$BROKEN_LINKS" ]; then
    echo "No broken symlinks found."
    exit 0
fi

echo "Found broken symlinks:"
echo "$BROKEN_LINKS"

if [ "$REMOVE_LINKS" = true ]; then
    echo ""
    echo "Removing broken symlinks..."
    echo "$BROKEN_LINKS" | while read -r link; do
        if [ -L "$link" ] && [ ! -e "$link" ]; then
            echo "Removing: $link"
            rm "$link"
        fi
    done
    echo "Cleanup complete."
else
    echo ""
    echo "To remove these broken symlinks, run:"
    echo "$0 --remove \"$SEARCH_PATH\""
fi