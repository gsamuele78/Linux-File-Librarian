import os
import mimetypes
import traceback
import gc
import pandas as pd
from concurrent.futures import as_completed, ThreadPoolExecutor
from functools import partial
from tqdm import tqdm
import sqlite3
from src.services.enhanced_copy_utils import create_enhanced_destination_path, copy_file_enhanced

def get_file_hash_standalone(path, block_size=65536):
    import hashlib
    import os
    hasher = hashlib.sha256()
    try:
        # Check if file exists and is accessible
        if not os.path.exists(path) or not os.path.isfile(path):
            return None
        if os.path.islink(path) and not os.path.exists(os.readlink(path)):
            return None  # Skip broken symlinks
            
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    except (OSError, IOError, PermissionError):
        return None  # Silently skip inaccessible files
    except Exception as e:
        print(f"[ERROR] Could not hash file {path}: {e}")
        return None

def get_pdf_details_standalone(path):
    """Standalone function for PDF validation that can be pickled for multiprocessing"""
    from src.utils.pdf_manager import PDFManager
    def dummy_log_error(*args, **kwargs):
        pass
    try:
        pdf_manager = PDFManager(log_error=dummy_log_error)
        return pdf_manager.get_pdf_details(path)
    except Exception as e:
        print(f"[ERROR] PDF validation failed for {path}: {e}")
        return False, False, None, None, None

def analyze_row(row, knowledge_db_path, isbn_cache, pdf_validation):
    import os
    import gc
    import mimetypes
    from src.core.classifier import Classifier
    
    # This function is called from a ThreadPoolExecutor, so it needs to be robust.
    path = row.get('path')
    name = row.get('name')
    
    if not path or not os.path.exists(path):
        return None
    
    try:
        # Initialize classifier
        classifier = Classifier(knowledge_db_path)
        
        # Get mime type
        mime_type, _ = mimetypes.guess_type(path)
        if not mime_type:
            mime_type = 'application/octet-stream'
        
        # Get file hash
        file_hash = get_file_hash_standalone(path)
        
        # Get PDF validation data if available
        pdf_data = pdf_validation.get(path, (False, False, None, None, None))
        is_pdf_valid, has_ocr, pdf_version, pdf_creator, pdf_producer = pdf_data
        
        # Classify file using the correct method
        game_system, edition, category = classifier.classify(name, path, mime_type)
        
        # Use ISBN enricher as additional resource if primary classification is generic
        if (game_system in ['Miscellaneous', None] or category in ['Miscellaneous', None]) and \
           mime_type.startswith('application/pdf'):
            try:
                from src.core.isbn_enricher import enrich_file_with_isbn_metadata
                
                # Check cache first
                if path in isbn_cache:
                    isbn_results = isbn_cache[path]
                else:
                    isbn_results = enrich_file_with_isbn_metadata(path)
                    isbn_cache[path] = isbn_results
                
                if isbn_results:
                    meta = isbn_results[0]['metadata']
                    title = meta.get('title', '')
                    
                    # Check if ISBN metadata provides better classification
                    if title:
                        ntitle = classifier.normalize_text(title)
                        if ntitle in classifier.product_cache:
                            game_system, edition, category = classifier.product_cache[ntitle]
                        else:
                            # Use ISBN metadata for generic book classification
                            category = 'Books'
                            game_system = meta.get('publisher', 'Unknown Publisher')
                            
            except Exception as e:
                pass  # Continue with original classification if ISBN enrichment fails
        
        # Clean up classifier
        classifier.close()
        del classifier
        gc.collect()
        
        return {
            'path': path,
            'mime_type': mime_type,
            'hash': file_hash,
            'is_pdf_valid': is_pdf_valid,
            'has_ocr': has_ocr,
            'pdf_version': pdf_version,
            'pdf_creator': pdf_creator,
            'pdf_producer': pdf_producer,
            'game_system': game_system,
            'edition': edition,
            'category': category,
            'language': 'en'  # Default language
        }
        
    except Exception as e:
        print(f"[ERROR] Analysis failed for {path}: {e}")
        return None
    

class LibraryBuilder:
    def test_knowledge_db_concurrent_access(self, db_path=None, n_workers=4):
        
        import multiprocessing
        import traceback
        if db_path is None:
            db_path = self.config.get('knowledge_base_db_url') or 'knowledge.sqlite'
        print(f"[DIAGNOSTIC] Testing concurrent read access to {db_path} with {n_workers} workers...")
        log_lines = []
        def worker(idx, log_queue):
            try:
                conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                msg = f"[DIAGNOSTIC] [Worker {idx}] Success. Tables: {tables}"
                print(msg)
                log_queue.put(msg)
                conn.close()
            except Exception as e:
                err_msg = f"[DIAGNOSTIC] [Worker {idx}] ERROR: {e}\n{traceback.format_exc()}"
                print(err_msg)
                log_queue.put(err_msg)
        log_queue = multiprocessing.Queue()
        procs = []
        for i in range(n_workers):
            p = multiprocessing.Process(target=worker, args=(i, log_queue))
            p.start()
            procs.append(p)
        for p in procs:
            p.join()
        # Collect logs from queue
        while not log_queue.empty():
            log_lines.append(log_queue.get())
        with open('librarian_run.log', 'a') as logf:
            for line in log_lines:
                logf.write(line + '\n')
        print("[DIAGNOSTIC] Concurrent access test complete.")
    def preflight_check(self):
        print("[DIAGNOSTIC] Running preflight checks...")
        errors = []
        # Check source paths
        for src in self.config.get('source_paths', []):
            if not os.path.exists(src):
                errors.append(f"Source path does not exist: {src}")
            elif not os.path.isdir(src):
                errors.append(f"Source path is not a directory: {src}")
            elif not os.access(src, os.R_OK):
                errors.append(f"Source path is not readable: {src}")
        # Check library root
        library_root = self.config.get('library_root')
        if not library_root:
            errors.append("library_root not set in config.")
        else:
            if not os.path.exists(library_root):
                try:
                    os.makedirs(library_root, exist_ok=True)
                except Exception as e:
                    errors.append(f"Could not create library_root: {library_root}. Reason: {e}")
            if not os.access(library_root, os.W_OK):
                errors.append(f"library_root is not writable: {library_root}")
        
        # Log results
        if errors:
            print("[DIAGNOSTIC][ERROR] Preflight check failed:")
            for err in errors:
                print(f"  - {err}")
        else:
            print("[DIAGNOSTIC] All preflight checks passed.")
        return not errors
    def __init__(self, config, resource_mgr, classifier, pdf_manager, logger, processing_callback=None):
        self.config = config
        self.resource_mgr = resource_mgr
        self.classifier = classifier
        self.pdf_manager = pdf_manager
        self.logger = logger
        self.df = None
        self.pdf_validation = {}
        self.isbn_cache = {}
        self._temp_objects = []  # Track temporary objects for cleanup
        self.processing_callback = processing_callback
        self.files_processed_since_cleanup = 0
        self.intermediate_db_path = "intermediate_results.sqlite"
        
    def _cleanup_temp_objects(self):
        """Clean up temporary objects and force garbage collection"""
        for obj in self._temp_objects:
            try:
                del obj
            except Exception as e:
                pass
        self._temp_objects.clear()
        gc.collect()
        
    def _add_temp_object(self, obj):
        """Add object to temporary cleanup list"""
        self._temp_objects.append(obj)
        return obj
        
    def _increment_and_cleanup(self):
        if self.processing_callback:
            self.files_processed_since_cleanup += 1
            if self.files_processed_since_cleanup >= 1000:
                self.logger.log_error('INFO', 'library_builder', '1000 files processed, triggering resource cleanup.')
                self.processing_callback()
                self.files_processed_since_cleanup = 0

    def _get_intermediate_db_connection(self):
        
        conn = sqlite3.connect(self.intermediate_db_path)
        conn.row_factory = sqlite3.Row  # Access columns by name
        return conn
        


    def scan_files(self):
        print("[STEP] Starting scan_files...")
        self.logger.log_error('PROGRESS', 'scan_files', 'Starting file scan')
        
        
        
        source_paths = self.config['source_paths']
        batch_size = 1000  # Increased batch size for database inserts
        total_files = 0
        
        # Remove old intermediate DB file if it exists
        if os.path.exists(self.intermediate_db_path):
            os.remove(self.intermediate_db_path)
            self.logger.log_error('INFO', 'scan_files', f'Removed old intermediate DB: {self.intermediate_db_path}')

        conn = self._get_intermediate_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files_scanned (
                path TEXT PRIMARY KEY,
                name TEXT,
                size INTEGER
            )
        ''')
        conn.commit()

        def file_generator(src):
            try:
                for root, dirs, files in os.walk(src, followlinks=False):
                    for fname in files:
                        fpath = os.path.join(root, fname)
                        try:
                            # Check if file exists and is accessible
                            if not os.path.exists(fpath):
                                continue
                            
                            # Handle symlinks more carefully
                            if os.path.islink(fpath):
                                try:
                                    # Check if symlink target exists
                                    if not os.path.exists(os.readlink(fpath)):
                                        continue  # Skip broken symlinks silently
                                except (OSError, IOError):
                                    continue  # Skip broken symlinks silently
                            
                            stat = os.stat(fpath)
                            # Skip empty files and very small files that might be corrupted
                            if stat.st_size < 10:
                                continue
                                
                            # Skip hidden files and system files that might cause issues
                            if fname.startswith('.') and fname not in ['.htaccess', '.gitignore']:
                                continue
                                
                            yield {'path': fpath, 'name': fname, 'size': stat.st_size}
                        except (OSError, IOError, PermissionError) as e:
                            # Only log if it's not a common broken symlink and not a permission error
                            if not (os.path.islink(fpath) or 'Permission denied' in str(e)):
                                print(f"  [Warning] Could not stat file {fname}. Skipping. Reason: {e}")
                        except Exception as e:
                            # Reduce noise from common filesystem issues
                            if 'No such file' not in str(e) and 'Permission denied' not in str(e):
                                print(f"  [Warning] Unexpected error with file {fname}. Skipping. Reason: {e}")
            except Exception as e:
                print(f"[Warning] Error scanning {src}: {e}")
                self.logger.log_error('SCAN_ERROR', src, str(e))
        for src in source_paths:
            if not os.path.isdir(src):
                print(f"[Warning] Source path not found, skipping: {src}")
                continue
            batch = []
            for fileinfo in file_generator(src):
                batch.append((fileinfo['path'], fileinfo['name'], fileinfo['size']))
                if len(batch) >= batch_size:
                    cursor.executemany("INSERT OR IGNORE INTO files_scanned (path, name, size) VALUES (?, ?, ?)", batch)
                    conn.commit()
                    total_files += len(batch)
                    print(f"[INFO] Scanned {total_files} files so far...")
                    batch.clear()
                    self._cleanup_temp_objects()
                    import psutil
                    process = psutil.Process()
                    mem_info = process.memory_info()
                    print(f"[RESOURCE][SCAN] RAM after batch: {mem_info.rss//(1024*1024)}MB")
            if batch:
                cursor.executemany("INSERT OR IGNORE INTO files_scanned (path, name, size) VALUES (?, ?, ?)", batch)
                conn.commit()
                total_files += len(batch)
                print(f"[INFO] Scanned {total_files} files so far...")
                batch.clear()
                self._cleanup_temp_objects()
                import psutil
                process = psutil.Process()
                mem_info = process.memory_info()
                print(f"[RESOURCE][SCAN] RAM after batch: {mem_info.rss//(1024*1024)}MB")
        conn.close()
        print(f"[INFO] Found {total_files} total files.")
        self.logger.log_error('PROGRESS', 'scan_files', f'Found {total_files} total files')
        
        # self.df will now be a generator that reads from the DB
        self.df = self._get_scanned_files_from_db()
        print(f"[INFO] File scan batches written to {self.intermediate_db_path} and ready for streaming processing.")
        self.logger.log_error('PROGRESS', 'scan_files', 'File scan complete')
        # Clear scan-specific variables
        del source_paths, batch_size
        self._cleanup_temp_objects()
        return self.df

    def _get_scanned_files_from_db(self):
        conn = self._get_intermediate_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT path, name, size FROM files_scanned")
        for row in cursor:
            yield dict(row)
        conn.close()

    def validate_and_repair_pdfs(self):
        print("[STEP] Starting validate_and_repair_pdfs...")
        self.logger.log_error('PROGRESS', 'validate_pdfs', 'Starting PDF validation')
        
        import traceback
        import psutil
        import time
        
        
        conn = self._get_intermediate_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pdf_validation (
                path TEXT PRIMARY KEY,
                is_pdf_valid BOOLEAN,
                has_ocr BOOLEAN,
                pdf_version TEXT,
                pdf_creator TEXT,
                pdf_producer TEXT
            )
        ''')
        conn.commit()

        # Use a fixed chunk size of 10 for PDF validation
        chunk_size = 10
        pdf_validation = {}
        
        print(f"[INFO] Using fixed chunk size: {chunk_size} for PDF validation.")

        # Function to check system resources and adjust workers
        def get_adjusted_worker_count():
            try:
                return self.resource_mgr.get_safe_worker_count()
            except Exception as e:
                print(f"[WARNING] Error checking system resources: {e}")
                return 1

        # Function to enforce memory limits and cleanup
        def enforce_memory_limits():
            gc.collect()
            try:
                process = psutil.Process()
                mem_info = process.memory_info()
                # If process uses more than 75% of available memory, force cleanup
                if mem_info.rss > psutil.virtual_memory().available * 0.75:
                    print("[RESOURCE] High memory usage detected, forcing cleanup...")
                    gc.collect()
                    time.sleep(1)  # Give OS time to reclaim memory
            except Exception as e:
                print(f"[WARNING] Error enforcing memory limits: {e}")

        try:
            self.logger.log_error('INFO', 'validate_pdfs', f'Starting PDF validation with chunk size: {chunk_size}')
            
            # Read from files_scanned table in chunks
            files_cursor = conn.cursor()
            files_cursor.execute("SELECT path, name, size FROM files_scanned WHERE name LIKE '%.pdf'")
            
            while True:
                pdf_rows = files_cursor.fetchmany(chunk_size)
                if not pdf_rows:
                    break

                enforce_memory_limits()
                
                max_workers = get_adjusted_worker_count()
                if max_workers == 0: # Fallback if no workers are available
                    max_workers = 1

                pdf_info = [(row['path'], row['size']) for row in pdf_rows]
                if not pdf_info:
                    continue
                    
                # Sort by size ascending to process smaller files first
                pdf_info.sort(key=lambda x: x[1])
                pdf_paths = [p[0] for p in pdf_info]
                
                print(f"[INFO] Validating/Repairing PDF batch with {len(pdf_paths)} files...")
                print(f"[RESOURCE] Using {max_workers} workers for this batch")
                
                batch_results = []
                if max_workers > 1:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {executor.submit(get_pdf_details_standalone, path): path for path in pdf_paths}
                        completed_futures = []
                        for future in tqdm(as_completed(futures), total=len(pdf_paths), desc=f"PDF Batch"):
                            path = futures[future]
                            try:
                                is_valid, has_ocr, version, creator, producer = future.result()
                                batch_results.append((
                                    path, is_valid, has_ocr, version, creator, producer
                                ))
                            except Exception as e:
                                print(f"[ERROR] PDF validation failed for {path}: {e}")
                                self.logger.log_error('PDF_VALIDATION_ERROR', path, str(e))
                            finally:
                                # Critical: Clean up completed future immediately
                                completed_futures.append(future)
                                if len(completed_futures) >= 10:  # Clean every 10 futures
                                    for cf in completed_futures:
                                        try:
                                            del futures[cf]
                                            del cf
                                        except:
                                            pass
                                    completed_futures.clear()
                                    gc.collect()
                        # Final cleanup of remaining futures
                        for cf in completed_futures:
                            try:
                                del futures[cf]
                                del cf
                            except:
                                pass
                        del futures, completed_futures
                        gc.collect()
                else:
                    print(f"[LOG] Processing {len(pdf_paths)} PDF files sequentially...")
                    for path in tqdm(pdf_paths, desc=f"PDF Batch"):
                        try:
                            is_valid, has_ocr, version, creator, producer = get_pdf_details_standalone(path)
                            batch_results.append((
                                path, is_valid, has_ocr, version, creator, producer
                            ))
                        except Exception as e:
                            print(f"[ERROR] PDF validation failed for {path}: {e}")
                            self.logger.log_error('PDF_VALIDATION_ERROR', path, str(e))
                    
                if batch_results:
                    cursor.executemany("INSERT OR REPLACE INTO pdf_validation (path, is_pdf_valid, has_ocr, pdf_version, pdf_creator, pdf_producer) VALUES (?, ?, ?, ?, ?, ?)", batch_results)
                    conn.commit()
                    del batch_results # Clear batch results from memory
                
                print(f"[LOG] PDF batch completed.")
                
                # files_cursor.rownumber is not a standard attribute, use a counter instead
                if (files_cursor.rowcount % (chunk_size * 10) == 0) and files_cursor.rowcount > 0:
                    print("[RESOURCE] Extended cleanup after 10 batches...")
                    enforce_memory_limits()
                    time.sleep(2)
                
        except Exception as e:
            print(f"[ERROR] Exception during PDF validation/repair: {e}")
            print(f"[INFO] Continuing with limited PDF validation results...")
            self.logger.log_error('ERROR', 'validate_pdfs', f'Exception during PDF validation: {e}', extra=traceback.format_exc())
        
        enforce_memory_limits()
        conn.close()

        # Load all PDF validation results from the database into memory for subsequent steps
        # This might still be a memory concern for very large datasets, but it's how it was before.
        # For true streaming, subsequent steps would also need to read from the DB.
        conn = self._get_intermediate_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT path, is_pdf_valid, has_ocr, pdf_version, pdf_creator, pdf_producer FROM pdf_validation")
        self.pdf_validation = {row['path']: (bool(row['is_pdf_valid']), bool(row['has_ocr']), row['pdf_version'], row['pdf_creator'], row['pdf_producer']) for row in cursor}
        conn.close()

        print(f"[INFO] PDF validation/repair complete. Total PDFs processed: {len(self.pdf_validation)}")
        self.logger.log_error('PROGRESS', 'validate_pdfs', f'PDF validation complete. Processed: {len(self.pdf_validation)} PDFs')
        # Clear validation-specific variables
        del chunk_size
        self._cleanup_temp_objects()
        return self.pdf_validation

    def classify_and_analyze(self):
        print("[STEP] Starting classify_and_analyze...")
        self.logger.log_error('PROGRESS', 'classify_analyze', 'Starting classification and analysis')
        
        
        
        conn = self._get_intermediate_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analysis_results (
                path TEXT PRIMARY KEY,
                mime_type TEXT,
                hash TEXT,
                is_pdf_valid BOOLEAN,
                has_ocr BOOLEAN,
                pdf_version TEXT,
                pdf_creator TEXT,
                pdf_producer TEXT,
                game_system TEXT,
                edition TEXT,
                category TEXT,
                language TEXT
            )
        ''')
        conn.commit()

        knowledge_db_path = self.config.get('knowledge_base_db_url') or "knowledge.sqlite"
        
        # Use a fixed batch size for analysis.
        chunk_size = 10
        print(f"[INFO] Using fixed chunk size of {chunk_size} for classification.")
        print(f"[INFO] PDF validation results: {len(self.pdf_validation)} files processed")
        
        # Create picklable partial function
        analyze_row_partial = partial(
            analyze_row,
            knowledge_db_path=knowledge_db_path,
            isbn_cache={},
            pdf_validation=self.pdf_validation
        )
        
        total_analyzed = 0
        try:
            files_cursor = conn.cursor()
            files_cursor.execute("SELECT path, name, size FROM files_scanned")

            while True:
                file_rows = files_cursor.fetchmany(chunk_size)
                if not file_rows:
                    break

                row_dicts = [dict(row) for row in file_rows]
                print(f"[INFO] Processing classification chunk with {len(row_dicts)} files...")
                results_batch = []
                try:
                    from concurrent.futures import ThreadPoolExecutor
                    max_workers = self.resource_mgr.get_safe_worker_count()
                    if max_workers == 0: # Fallback if no workers are available
                        max_workers = 1

                    print(f"[INFO] Processing {len(row_dicts)} files with {max_workers} workers (Available RAM: {self.resource_mgr.get_available_ram_mb()}MB)")
                    
                    if max_workers > 1:
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            futures = {executor.submit(analyze_row_partial, row): row for row in row_dicts}
                            completed_futures = []
                            for future in tqdm(as_completed(futures), total=len(row_dicts), desc=f"Classification Chunk"):
                                row = futures[future]
                                try:
                                    result = future.result()
                                    if result:
                                        results_batch.append(result)
                                except Exception as e:
                                    row_info = row.get('path', 'unknown') if isinstance(row, dict) else 'unknown'
                                    print(f"[ERROR] Analysis failed for {row_info}: {e}")
                                    self.logger.log_error('ANALYSIS_ERROR', str(row_info), str(e)[:100])
                                finally:
                                    # Critical: Clean up completed future immediately
                                    completed_futures.append(future)
                                    if len(completed_futures) >= 5:  # Clean every 5 futures
                                        for cf in completed_futures:
                                            try:
                                                del futures[cf]
                                                del cf
                                            except:
                                                pass
                                        completed_futures.clear()
                                        gc.collect()
                            # Final cleanup of remaining futures
                            for cf in completed_futures:
                                try:
                                    del futures[cf]
                                    del cf
                                except:
                                    pass
                            del futures, completed_futures
                            gc.collect()
                    else: # Sequential processing if max_workers is 1
                        for row in tqdm(row_dicts, desc=f"Classification Chunk"):
                            try:
                                result = analyze_row_partial(row)
                                if result:
                                    results_batch.append(result)
                            except Exception as e:
                                row_info = row.get('path', 'unknown') if isinstance(row, dict) else 'unknown'
                                print(f"[ERROR] Analysis failed for {row_info}: {e}")
                                self.logger.log_error('ANALYSIS_ERROR', str(row_info), str(e)[:100])
                        
                    # Force garbage collection after each file and check memory pressure
                    self._cleanup_temp_objects()
                    if self.resource_mgr._detect_memory_pressure():
                        print("[RESOURCE] Memory pressure detected, pausing...")
                        import time
                        time.sleep(1)
                        self._cleanup_temp_objects()
                    self._increment_and_cleanup()
                    
                    if results_batch:
                        # Insert results into the database
                        insert_data = []
                        for res in results_batch:
                            insert_data.append((
                                res.get('path'), res.get('mime_type'), res.get('hash'),
                                res.get('is_pdf_valid'), res.get('has_ocr'),
                                res.get('pdf_version'), res.get('pdf_creator'), res.get('pdf_producer'),
                                res.get('game_system'), res.get('edition'), res.get('category'),
                                res.get('language')
                            ))
                        cursor.executemany("INSERT OR REPLACE INTO analysis_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", insert_data)
                        conn.commit()
                        total_analyzed += len(results_batch)

                    print("[LOG] Classification worker batch completed.")
                    
                    # Force garbage collection after each chunk
                    self._cleanup_temp_objects()
                except MemoryError as me:
                    print(f"[ERROR] MemoryError during classification chunk: {me}")
                    self.logger.log_error('MEMORY_ERROR', f'chunk_processing', 'Memory error during classification')
                    self._cleanup_temp_objects()
                    import time
                    time.sleep(2)  # Give system time to recover
                    continue
                except Exception as e:
                    print(f"[ERROR] Exception during classification chunk: {e}")
                    with open('librarian_run.log', 'a') as logf:
                        logf.write(f"[ERROR] Exception during classification chunk: {e}\n{traceback.format_exc()}\n")
                    self._cleanup_temp_objects()
                    continue
        except Exception as e:
            print(f"[ERROR] Exception during streaming classification: {e}")
            with open('librarian_run.log', 'a') as logf:
                logf.write(f"[ERROR] Exception during streaming classification: {e}\n{traceback.format_exc()}\n")

        conn.close()
        self.logger.log_error('PROGRESS', 'classify_analyze', f'Classification complete. Results: {total_analyzed}')
        print(f"[INFO] Classification and analysis complete.")
        # Clear classification-specific variables
        del knowledge_db_path, analyze_row_partial
        self._cleanup_temp_objects()


    def deduplicate_files(self):
        print("[STEP] Starting deduplicate_files...")
        self.logger.log_error('PROGRESS', 'deduplicate', 'Starting deduplication')

        conn = self._get_intermediate_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS deduplicated_files (
                path TEXT PRIMARY KEY,
                name TEXT,
                size INTEGER,
                mime_type TEXT,
                hash TEXT,
                is_pdf_valid BOOLEAN,
                has_ocr BOOLEAN,
                pdf_version TEXT,
                pdf_creator TEXT,
                pdf_producer TEXT,
                game_system TEXT,
                edition TEXT,
                category TEXT,
                language TEXT,
                quality_score INTEGER,
                name_occurrence INTEGER
            )
        ''')
        conn.commit()

        chunk_size = 1000 # Consistent chunk size
        final_best_files = {} # Initialize to an empty dictionary at the very beginning

        try:
            analysis_cursor = conn.cursor()
            analysis_cursor.execute("SELECT * FROM analysis_results")

            while True:
                rows = analysis_cursor.fetchmany(chunk_size)
                if not rows:
                    break

                df = pd.DataFrame([dict(row) for row in rows])
                
                current_chunk_best_files = {}
                df['quality_score'] = 0
                df['quality_score'] = df['quality_score'].astype(int)

                if 'has_ocr' in df.columns:
                    ocr_mask = df['has_ocr'] == 1 # SQLite stores BOOLEAN as INTEGER (0 or 1)
                    df['quality_score'] += (ocr_mask * 4)

                if 'is_pdf_valid' in df.columns:
                    valid_mask = df['is_pdf_valid'] == 1 # SQLite stores BOOLEAN as INTEGER (0 or 1)
                    df['quality_score'] += (valid_mask * 2)

                min_size = self.config.get('min_pdf_size_bytes', 1024)
                if 'size' in df.columns:
                    size_mask = (pd.to_numeric(df['size'], errors='coerce').fillna(0) > min_size)
                    df['quality_score'] += (size_mask * 1)

                for _, row in df.iterrows():
                    h = row.get('hash')
                    if h is None or pd.isna(h):
                        continue

                    score = row.get('quality_score', 0)
                    if h not in final_best_files or score > final_best_files[h].get('quality_score', 0):
                        final_best_files[h] = row.to_dict()
                
                del df, current_chunk_best_files
                self._cleanup_temp_objects()
                print(f"[INFO] Processed deduplication chunk.")
            
            if not final_best_files:
                print("[WARNING] No files with valid hashes found for final deduplication.")
                conn.close()
                return None

            unique_files_df = pd.DataFrame(list(final_best_files.values()))

            # Sort by quality and size
            sort_columns = []
            if 'quality_score' in unique_files_df.columns:
                sort_columns.append('quality_score')
            if 'size' in unique_files_df.columns:
                sort_columns.append('size')

            if sort_columns:
                unique_files_df = unique_files_df.sort_values(by=sort_columns, ascending=False)

            # Add name occurrence counter
            if 'name' in unique_files_df.columns:
                unique_files_df['name_occurrence'] = unique_files_df.groupby('name').cumcount()
            else:
                unique_files_df['name_occurrence'] = 0

            # Insert deduplicated files into the database
            insert_data = []
            for _, row in unique_files_df.iterrows():
                insert_data.append((
                    row.get('path'), row.get('name'), row.get('size'), row.get('mime_type'),
                    row.get('hash'), row.get('is_pdf_valid'), row.get('has_ocr'),
                    row.get('pdf_version'), row.get('pdf_creator'), row.get('pdf_producer'),
                    row.get('game_system'), row.get('edition'), row.get('category'),
                    row.get('language'), row.get('quality_score'), row.get('name_occurrence')
                ))
            cursor.executemany("INSERT OR REPLACE INTO deduplicated_files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", insert_data)
            conn.commit()
            conn.close()

            self.df = unique_files_df
            self.unique_files = unique_files_df

            self.logger.log_error('PROGRESS', 'deduplicate', f'Deduplication complete. Unique files: {len(unique_files_df)}')
            print(f"[INFO] Deduplication complete. Unique files: {len(unique_files_df)}")
            
            return unique_files_df

        except Exception as e:
            print(f"[ERROR] Error during deduplication: {e}")
            self.logger.log_error('DEDUPLICATION_ERROR', '', str(e))
            conn.close()
            return None

        finally:
            self._cleanup_temp_objects()
            # Ensure final_best_files is cleared
            del final_best_files        

    def build(self):
        try:
            self.scan_files()
            self._cleanup_temp_objects()
            
            self.validate_and_repair_pdfs()
            self._cleanup_temp_objects()
            
            # Automated concurrent access test for knowledge.sqlite
            self.test_knowledge_db_concurrent_access()
            self._cleanup_temp_objects()
            
            self.classify_and_analyze()
            self._cleanup_temp_objects()
            
            unique_files = self.deduplicate_files()
            self._cleanup_temp_objects()
            
            self.copy_and_index(unique_files)
            self._cleanup_temp_objects()
            
            self.logger.print_summary()
            print("[INFO] Library build process complete.")
        finally:
            # Final cleanup
            self._cleanup_temp_objects()
            # Clear large data structures
            self.pdf_validation = {}
            self.isbn_cache = {}
            if hasattr(self, 'df') and self.df is not None:
                del self.df
            if hasattr(self, 'unique_files') and hasattr(self, 'unique_files') and self.unique_files is not None:
                del self.unique_files
            if os.path.exists(self.intermediate_db_path):
                os.remove(self.intermediate_db_path)
                self.logger.log_error('INFO', 'build', f'Removed intermediate DB: {self.intermediate_db_path}')
            self._cleanup_temp_objects()

    def get_file_hash(self, path, block_size=65536):
        import hashlib
        hasher = hashlib.sha256()
        try:
            # Check if file exists and is accessible
            if not os.path.exists(path) or not os.path.isfile(path):
                return None
            if os.path.islink(path) and not os.path.exists(os.readlink(path)):
                return None  # Skip broken symlinks
                
            with open(path, 'rb') as f:
                for chunk in iter(lambda: f.read(block_size), b''):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except (OSError, IOError, PermissionError):
            return None  # Silently skip inaccessible files
        except Exception as e:
            print(f"[ERROR] Could not hash file {path}: {e}")
            return None

    def classify_with_isbn_fallback(self, classifier, filename, full_path, mime_type, isbn_cache):
        """Classify file using multiple fallback methods including ISBN enrichment"""
        try:
            from src.core.isbn_enricher import enrich_file_with_isbn_metadata
        except ImportError:
            print("[WARNING] ISBN enricher not available")
            enrich_file_with_isbn_metadata = None
        
        # 1. Try filename
        result = getattr(classifier, '_classify_by_filename', lambda x: None)(filename)
        if result:
            return result
        
        # 2. Try path
        result = getattr(classifier, '_classify_by_path', lambda x: None)(full_path)
        if result:
            return result
        
        # 3. Try mimetype
        result = getattr(classifier, '_classify_by_mimetype', lambda x: None)(mime_type)
        if result:
            return result
        
        # 4. Try ISBN enrichment only for PDFs and text files
        if enrich_file_with_isbn_metadata and (mime_type.startswith('application/pdf') or mime_type.startswith('text/')):
            try:
                if full_path in isbn_cache:
                    isbn_results = isbn_cache[full_path]
                else:
                    isbn_results = enrich_file_with_isbn_metadata(full_path)
                    isbn_cache[full_path] = isbn_results
                
                if isbn_results:
                    meta = isbn_results[0]['metadata']
                    title = meta.get('title')
                    if title:
                        ntitle = self.classifier.normalize_text(title)
                        if ntitle in self.classifier.product_cache:
                            print(f"[DEBUG] ISBN enrichment matched title in product cache: {title}")
                            return self.classifier.product_cache[ntitle]
                        print(f"[DEBUG] ISBN enrichment used Open Library metadata for: {title}")
                        return (title, None, "ISBN/Book")
            except Exception as e:
                print(f"[DEBUG] ISBN enrichment failed for {full_path}: {e}")
        
        print(f"[DEBUG] File {filename} is uncategorized after all attempts.")
        return ('Miscellaneous', None, None)

    def copy_and_index(self, unique_files):
        print("[STEP] Starting copy_and_index...")
        self.logger.log_error('PROGRESS', '', 'Starting copy and index phase')
        
        with self._get_intermediate_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM deduplicated_files")
            unique_files_data = cursor.fetchall()
        
        if not unique_files_data:
            print("[ERROR] No unique files to copy or index. Skipping DB creation.")
            print("[INFO] This may be due to memory errors during processing.")
            print("[INFO] Try running with smaller source directories or more RAM.")
            return

        unique_files = pd.DataFrame(unique_files_data, columns=[description[0] for description in cursor.description])