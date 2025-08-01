from src.library_builder import LibraryBuilder
# Dependency check and auto-install before any other imports
from src.dependency_manager import check_and_install_dependencies
check_and_install_dependencies()


# Linux-File-Librarian main orchestrator (modular version)
import os
import sys
import traceback
from src.config_loader import load_config
from src.resource_manager import ResourceManager
from src.classifier import Classifier
from src.pdf_manager import PDFManager
from src.logger import Logger

def main():
    # Set a hard memory limit for the process (Linux/Unix only)
    import resource
    from src.cleanup_utils import cleanup_temp_files
    
    # Clean up any existing temporary files before starting
    cleanup_temp_files()
    
    # Set a hard memory limit of 1GB for the process
    memory_limit = 1024 * 1024 * 1024  # 1GB in bytes
    logger = Logger(cleanup_old_log=True)  # Only cleanup in main entry point
    try:
        resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))
        logger.log_error("INFO", "librarian.py", f"[RESOURCE] Set hard memory limit: {memory_limit//(1024*1024)}MB (1GB)")
        config_path = os.environ.get("LIBRARIAN_CONFIG", os.path.join(os.path.dirname(__file__), "../conf/config.ini"))
        logger.log_error("INFO", "librarian.py", f"[INFO] Loading config from: {config_path}")
        config = load_config()
        settings = config.get('settings', config.get('Settings', {}))
        def get_cfg(key, default, typ):
            v = settings.get(key, default)
            try:
                return typ(v)
            except Exception:
                return default
        resource_mgr = ResourceManager(
            min_free_mb=get_cfg('os_reserved_mb', 1024, int),
            min_workers=1,
            max_workers=4,
            ram_per_worker_mb=256,
            os_reserved_mb=1024,
            max_ram_usage_ratio=1.0
        )
        resource_mgr.wait_for_free_ram()
        resource_mgr.print_resource_usage('Start')
        os.makedirs(config['library_root'], exist_ok=True)
        classifier = Classifier(config.get('knowledge_base_db_url') or "knowledge.sqlite")
        pdf_manager = PDFManager(logger.log_error)
        builder = LibraryBuilder(config, resource_mgr, classifier, pdf_manager, logger)
        
        # Step 1: Scan files
        logger.log_error("STEP", "librarian.py", '[STEP] Starting scan_files...')
        builder.scan_files()
        resource_mgr.force_cleanup()
        logger.log_error('MEMORY', 'scan_files', 'Memory cleanup after scan_files')
        
        # Step 2: Validate PDFs
        logger.log_error("STEP", "librarian.py", '[STEP] Starting validate_and_repair_pdfs...')
        builder.validate_and_repair_pdfs()
        resource_mgr.force_cleanup()
        logger.log_error('MEMORY', 'validate_pdfs', 'Memory cleanup after validate_and_repair_pdfs')
        
        # Step 3: Classify and analyze
        logger.log_error("STEP", "librarian.py", '[STEP] Starting classify_and_analyze...')
        builder.classify_and_analyze()
        resource_mgr.force_cleanup()
        logger.log_error('MEMORY', 'classify_analyze', 'Memory cleanup after classify_and_analyze')
        
        # Step 4: Deduplicate
        logger.log_error("STEP", "librarian.py", '[STEP] Starting deduplicate_files...')
        unique_files = builder.deduplicate_files()
        resource_mgr.force_cleanup()
        logger.log_error('MEMORY', 'deduplicate', 'Memory cleanup after deduplicate_files')
        
        # Step 5: Copy and index
        builder.copy_and_index(unique_files)
        resource_mgr.force_cleanup()
        logger.log_error('MEMORY', 'copy_index', 'Memory cleanup after copy_and_index')
        
        logger.print_summary()
        logger.log_error("STEP", "librarian.py", '[STEP] Workflow complete.')
    except Exception as e:
        import traceback
        logger.log_error("ERROR", "librarian.py", '[ERROR] Uncaught exception in main workflow:', extra=traceback.format_exc())
        logger.log_error("ERROR", "librarian.py", '[ERROR] librarian.py failed! See librarian_run.log for details.')

if __name__ == "__main__":
    main()
