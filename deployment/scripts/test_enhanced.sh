#!/bin/bash
# Test Enhanced Media Manager Functionality

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Testing Enhanced Media Manager"
echo "=========================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Error: Virtual environment not found. Please run scripts/install.sh first."
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

echo "Testing enhanced classification service..."

# Test the enhanced functionality
python3 -c "
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, 'src')

try:
    
    from providers.enhanced_classification_engine import EnhancedClassificationEngine
    #from enhanced_classification_service import EnhancedClassificationService
    from enhanced_media_manager import EnhancedMediaManager
    
    print('✓ Enhanced modules imported successfully')
    
    # Test service initialization
    service = EnhancedClassificationService({})
    print('✓ Enhanced classification service initialized')
    
    # Test media manager
    manager = EnhancedMediaManager()
    print('✓ Enhanced media manager initialized')
    
    # Check dependencies
    deps = manager.check_dependencies()
    print('\\nDependency Status:')
    for name, available in deps.items():
        status = '✓' if available else '✗'
        print(f'  {status} {name}')
    
    # Test with a dummy file
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        tmp.write(b'%PDF-1.4 dummy content')
        tmp_path = Path(tmp.name)
    
    try:
        result = manager.analyze_file(tmp_path)
        print(f'\\n✓ File analysis successful:')
        print(f'  Category: {result.category}')
        print(f'  Subcategory: {result.subcategory}')
        print(f'  Media Type: {result.media_type}')
        print(f'  Confidence: {result.confidence}')
        print(f'  Suggested Path: {\" > \".join(result.suggested_path)}')
    finally:
        tmp_path.unlink()
    
    # Test classification service
    file_info = {'path': '/tmp/test.pdf', 'size': 1024}
    classified = service.classify_file(file_info)
    print(f'\\n✓ Classification service test:')
    print(f'  Game System: {classified.get(\"game_system\", \"Unknown\")}')
    print(f'  Edition: {classified.get(\"edition\", \"Unknown\")}')
    print(f'  Category: {classified.get(\"category\", \"Unknown\")}')
    
    print('\\n✓ All tests passed!')
    
except ImportError as e:
    print(f'✗ Import error: {e}')
    print('Run scripts/install_enhanced.sh to install dependencies')
    sys.exit(1)
except Exception as e:
    print(f'✗ Test failed: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"

echo ""
echo "Testing repair utilities..."

python3 -c "
import sys
sys.path.insert(0, 'src')

try:
    from enhanced_repair_utils import EnhancedRepairManager
    
    manager = EnhancedRepairManager()
    print('✓ Enhanced repair manager initialized')
    
    # Check repair dependencies
    deps = manager.check_repair_dependencies()
    print('\nRepair Tool Status:')
    for name, available in deps.items():
        status = '✓' if available else '✗'
        print(f'  {status} {name}')
    
    # Get repair stats
    stats = manager.get_repair_stats()
    print(f'\n✓ Repair manager ready with {len(stats[\"available_repairers\"])} repairers')
    
except Exception as e:
    print(f'✗ Repair utilities test failed: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"

echo ""
echo "Testing deduplication utilities..."

python3 -c "
import sys
sys.path.insert(0, 'src')

try:
    from enhanced_deduplication import EnhancedDeduplicationManager
    
    manager = EnhancedDeduplicationManager()
    print('✓ Enhanced deduplication manager initialized')
    
    # Test with dummy data
    test_files = [
        {'path': '/tmp/test1.pdf', 'size': 1024},
        {'path': '/tmp/test2.pdf', 'size': 1024},  # Same size for testing
        {'path': '/tmp/test3.pdf', 'size': 2048}
    ]
    
    # This won't find real duplicates but tests the pipeline
    result = manager.deduplicate_files(test_files)
    print(f'✓ Deduplication test completed: {result.unique_count} unique files')
    
    # Get report
    report = manager.get_deduplication_report()
    print(f'✓ Deduplication report generated with {len(report[\"detectors\"])} detectors')
    
except Exception as e:
    print(f'✗ Deduplication utilities test failed: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"

echo ""
echo "Testing copy utilities..."

python3 -c "
import sys
sys.path.insert(0, 'src')

try:
    from enhanced_copy_utils import sanitize_filename, create_enhanced_destination_path
    from pathlib import Path
    
    # Test filename sanitization
    test_names = [
        'Normal File.pdf',
        'File with <bad> chars?.pdf',
        'File/with\\\\slashes.pdf',
        'Very long filename that exceeds normal limits and should be truncated properly while preserving the extension.pdf'
    ]
    
    print('Filename Sanitization Tests:')
    for name in test_names:
        sanitized = sanitize_filename(name)
        print(f'  \"{name}\" -> \"{sanitized}\"')
    
    # Test path creation
    library_root = Path('/tmp/test_library')
    file_info = {
        'game_system': 'Media',
        'edition': 'Video', 
        'category': 'Movies',
        'suggested_path': ['Media', 'Video', 'Movies'],
        'enhanced_metadata': {
            'year': '2023',
            'genre': 'Action'
        }
    }
    
    dest_path = create_enhanced_destination_path(library_root, file_info)
    print(f'\\nPath Creation Test:')
    print(f'  Created: {dest_path}')
    
    print('\\n✓ Copy utilities tests passed!')
    
except Exception as e:
    print(f'✗ Copy utilities test failed: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"

echo ""
echo "=========================================="
echo "Enhanced Media Manager Test Complete!"
echo ""
echo "If all tests passed, the enhanced features are ready to use."
echo "Run './scripts/run_professional.sh' to process files with enhanced capabilities."
echo "=========================================="