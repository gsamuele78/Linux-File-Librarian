#!/bin/bash
# Enhanced Media Manager Installation Script
# Installs additional dependencies for comprehensive media and document processing

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Enhanced Media Manager Installation"
echo "=========================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Error: Virtual environment not found. Please run deployment/scripts/install.sh first."
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

echo "Installing enhanced Python dependencies..."

# Install enhanced requirements
pip install -r requirements-enhanced.txt

echo ""
echo "Checking system dependencies..."

# Check for ffmpeg
if command -v ffmpeg >/dev/null 2>&1; then
    echo "✓ ffmpeg found: $(ffmpeg -version | head -n1)"
else
    echo "⚠ ffmpeg not found - video metadata extraction will be limited"
    echo "  Install with:"
    echo "    Ubuntu/Debian: sudo apt install ffmpeg"
    echo "    CentOS/RHEL: sudo yum install ffmpeg"
    echo "    macOS: brew install ffmpeg"
fi

# Check for ffprobe specifically
if command -v ffprobe >/dev/null 2>&1; then
    echo "✓ ffprobe found"
else
    echo "⚠ ffprobe not found (usually comes with ffmpeg)"
fi

# Check for repair tools
echo "\nChecking repair tools..."

if command -v qpdf >/dev/null 2>&1; then
    echo "✓ qpdf found (PDF repair)"
else
    echo "⚠ qpdf not found - PDF repair will be limited"
    echo "  Install with: sudo apt install qpdf"
fi

if command -v gs >/dev/null 2>&1; then
    echo "✓ ghostscript found (PDF repair)"
else
    echo "⚠ ghostscript not found - PDF repair will be limited"
    echo "  Install with: sudo apt install ghostscript"
fi

if command -v unrar >/dev/null 2>&1; then
    echo "✓ unrar found (archive repair)"
else
    echo "⚠ unrar not found - RAR repair unavailable"
    echo "  Install with: sudo apt install unrar"
fi

if command -v 7z >/dev/null 2>&1; then
    echo "✓ 7z found (archive repair)"
else
    echo "⚠ 7z not found - 7z repair unavailable"
    echo "  Install with: sudo apt install p7zip-full"
fi

echo ""
echo "Testing enhanced functionality..."

# Test the enhanced classification engine with additional providers
python3 -c "
import sys
sys.path.insert(0, 'src')

try:
    from providers.enhanced_classification_engine import EnhancedClassificationEngine
    engine = EnhancedClassificationEngine({})
    status = engine.get_provider_status()
    
    print('Enhanced Classification Engine Status:')
    print(f'  Providers available: {status[\"providers_available\"]}')
    print(f'  Internet enrichment: {status[\"internet_enrichment_enabled\"]}')
    print(f'  Artwork download: {status[\"artwork_download_enabled\"]}')
    print(f'  Cache entries: {status[\"cache_entries\"]}')
    print('  Providers:')
    for provider in status['providers']:
        print(f'    ✓ {provider[\"name\"]}')
    
    print('  Note: Additional free providers available:')
    print('    - CrossRef: Academic papers (no API key needed)')
    print('    - arXiv: Scientific preprints (no API key needed)')
    print('    - Google Books: Book metadata (no API key needed)')
    print('    - Wikipedia: General knowledge (no API key needed)')
    print('    - Internet Archive: Historical documents (no API key needed)')
    print('  Configure TMDB/Fanart.tv API keys for movie/TV content')
        
except Exception as e:
    print(f'Error testing enhanced service: {e}')
    sys.exit(1)
"

echo ""
echo "=========================================="
echo "Enhanced installation completed!"
echo ""
echo "New capabilities:"
echo "• PDF metadata extraction and ISBN detection"
echo "• Video metadata extraction (requires ffmpeg)"
echo "• Audio metadata extraction"
echo "• Image EXIF data extraction"
echo "• Document metadata extraction"
echo "• NFO file generation (MediaElch-style)"
echo "• Enhanced directory organization"
echo "• Document repair (PDF, video, archive)"
echo "• Enhanced deduplication (exact, content, metadata)"
echo "• Intelligent duplicate resolution"
echo "• Internet metadata enrichment (TMDB, OpenLibrary)"
echo "• Automatic fanart/poster downloading"
echo "• Advanced content classification"
echo "• Gaming industry integration (Paizo, Wizards, DriveThruRPG, Giochi Uniti, Acheron Games)"
echo "• International TTRPG support (Italian publishers)"
echo "• Enterprise-grade processing pipeline"
echo "• Comprehensive quality metrics"
echo ""
echo "Run './deployment/scripts/run_professional.sh' to use enhanced features"
echo "=========================================="