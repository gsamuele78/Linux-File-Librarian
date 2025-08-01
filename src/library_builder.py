import os
import gc
import mimetypes
import traceback
import pandas as pd
import weakref
from pathlib import Path
from concurrent.futures import as_completed
from functools import partial
from tqdm import tqdm

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
    from src.pdf_manager import PDFManager
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
    
    try:
        path = row.get('path')
        name = row.get('name')
        size = row.get('size')
        
        # Check if file still exists and is accessible
        if not path or not os.path.exists(path) or not os.path.isfile(path):
            return None
        if os.path.islink(path) and not os.path.exists(os.readlink(path)):
            return None  # Skip broken symlinks
        
        # Get mime type with better error handling
        try:
            import magic
            mime_type = magic.from_file(path)
        except Exception:
            try:
                mime_type = mimetypes.guess_type(path)[0] or 'application/octet-stream'
            except Exception:
                mime_type = 'application/octet-stream'
        
        # Skip image files that are causing analysis failures
        if mime_type and ('image/' in mime_type.lower() or name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff'))):
            file_hash = get_file_hash_standalone(path)
            if file_hash is None:
                return None
            return {
                'mime_type': mime_type,
                'hash': file_hash,
                'is_pdf_valid': None,
                'has_ocr': None,
                'pdf_version': None,
                'pdf_creator': None,
                'pdf_producer': None,
                'game_system': 'Images',
                'edition': None,
                'category': 'Graphics',
                'language': None
            }
        
        file_hash = get_file_hash_standalone(path)
        if file_hash is None:
            return None  # Skip files that can't be hashed
            
        # Initialize PDF variables
        is_pdf_valid = None
        has_ocr = None
        pdf_version = None
        pdf_creator = None
        pdf_producer = None
        
        # Handle PDF files
        if name and name.lower().endswith('.pdf'):
            pdf_info = pdf_validation.get(path) if pdf_validation else None
            if pdf_info:
                is_pdf_valid, has_ocr, pdf_version, pdf_creator, pdf_producer = pdf_info
            else:
                # Instantiate pdf_manager locally to avoid pickle issues
                try:
                    from src.pdf_manager import PDFManager
                    def dummy_log_error(*args, **kwargs):
                        pass
                    pdf_manager = PDFManager(log_error=dummy_log_error)
                    is_pdf_valid, has_ocr, pdf_version, pdf_creator, pdf_producer = pdf_manager.get_pdf_details(path)
                except Exception as e:
                    print(f"[ERROR][analyze_row] PDF details failed for {path}: {e}")
        
        # Classification with better error handling
        game_system = edition = category = None
        try:
            from src.classifier import Classifier
            classifier = Classifier(knowledge_db_path)
            game_system, edition, category = classifier.classify(name, path, mime_type)
            classifier.close()
        except Exception as e:
            print(f"[ERROR][analyze_row] Classifier failed for {name}: {e}")
            # Provide fallback classification
            if name:
                if any(ext in name.lower() for ext in ['.pdf', '.doc', '.txt']):
                    game_system, edition, category = 'Documents', None, 'Text'
                elif any(ext in name.lower() for ext in ['.mp3', '.wav', '.ogg']):
                    game_system, edition, category = 'Audio', None, 'Sound'
                elif any(ext in name.lower() for ext in ['.mp4', '.avi', '.mkv']):
                    game_system, edition, category = 'Video', None, 'Media'
                else:
                    game_system, edition, category = 'Miscellaneous', None, 'Other'
        
        language = None
        
        result = {
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
            'language': language
        }
        
        # Force garbage collection to prevent memory buildup
        gc.collect()
        return result
        
    except MemoryError:
        print(f"[MEMORY_ERROR] analyze_row failed for {row.get('path', 'unknown')}")
        gc.collect()
        return None
    except Exception as e:
        print(f"[ERROR][analyze_row] Unexpected error for {row.get('path', 'unknown')}: {e}")
        gc.collect()
        return None

class LibraryBuilder:
    def test_knowledge_db_concurrent_access(self, db_path=None, n_workers=4):
        import sqlite3
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
        # Check temp CSV
        temp_csv = 'file_scan_batches.csv'
        if os.path.exists(temp_csv) and not os.access(temp_csv, os.R_OK):
            errors.append(f"Temp CSV not readable: {temp_csv}")
        # Log results
        if errors:
            print("[DIAGNOSTIC][ERROR] Preflight check failed:")
            for err in errors:
                print(f"  - {err}")
        else:
            print("[DIAGNOSTIC] All preflight checks passed.")
        return not errors
    def __init__(self, config, resource_mgr, classifier, pdf_manager, logger):
        self.config = config
        self.resource_mgr = resource_mgr
        self.classifier = classifier
        self.pdf_manager = pdf_manager
        self.logger = logger
        self.df = None
        self.analysis_results = []
        self.pdf_validation = {}
        self.isbn_cache = {}
        self._temp_objects = []  # Track temporary objects for cleanup
        
    def _cleanup_temp_objects(self):
        """Clean up temporary objects and force garbage collection"""
        for obj in self._temp_objects:
            try:
                del obj
            except:
                pass
        self._temp_objects.clear()
        gc.collect()
        
    def _add_temp_object(self, obj):
        """Add object to temporary cleanup list"""
        self._temp_objects.append(obj)
        return obj

    def scan_files(self):
        print("[STEP] Starting scan_files...")
        import gc
        import csv
        from src.cleanup_utils import cleanup_temp_files
        
        # Clean up any existing temporary files
        cleanup_temp_files()
        
        source_paths = self.config['source_paths']
        batch_size = 100
        total_files = 0
        temp_csv = 'file_scan_batches.csv'
        # Write header
        with open(temp_csv, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['path','name','size'])
            writer.writeheader()
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
                batch.append(fileinfo)
                if len(batch) >= batch_size:
                    with open(temp_csv, 'a', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=['path','name','size'])
                        writer.writerows(batch)
                    total_files += len(batch)
                    print(f"[INFO] Scanned {total_files} files so far...")
                    batch.clear()
                    self._cleanup_temp_objects()
                    import psutil
                    process = psutil.Process()
                    mem_info = process.memory_info()
                    print(f"[RESOURCE][SCAN] RAM after batch: {mem_info.rss//(1024*1024)}MB")
            if batch:
                with open(temp_csv, 'a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=['path','name','size'])
                    writer.writerows(batch)
                total_files += len(batch)
                print(f"[INFO] Scanned {total_files} files so far...")
                batch.clear()
                self._cleanup_temp_objects()
                import psutil
                process = psutil.Process()
                mem_info = process.memory_info()
                print(f"[RESOURCE][SCAN] RAM after batch: {mem_info.rss//(1024*1024)}MB")
        print(f"[INFO] Found {total_files} total files.")
        # Streaming read from disk
        # Use a very small chunk size and python engine for robustness
        self.df = pd.read_csv(temp_csv, chunksize=25, engine='python')
        print(f"[INFO] File scan batches written to {temp_csv} and ready for streaming processing.")
        return self.df

    def validate_and_repair_pdfs(self):
        print("[STEP] Starting validate_and_repair_pdfs...")
        import sys
        import traceback
        import psutil
        import time
        from pandas.errors import ParserError
        
        if self.df is None:
            print("[ERROR] No files to validate.")
            self.pdf_validation = {}
            return self.pdf_validation

        # Ultra conservative chunk size to prevent memory errors
        available_mb = self.resource_mgr.get_available_ram_mb()
        chunk_size = max(1, min(3, available_mb // 1000))  # Ultra small batches
        temp_csv = 'file_scan_batches.csv'
        pdf_validation = {}
        
        print(f"[RESOURCE] Using conservative chunk size: {chunk_size} based on available RAM: {available_mb}MB")

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
            for chunk_idx, chunk in enumerate(pd.read_csv(temp_csv, chunksize=chunk_size, engine='python')):
                enforce_memory_limits()
                # Always use sequential processing for PDFs to prevent memory issues
                max_workers = 1
                
                try:
                    if not isinstance(chunk, pd.DataFrame):
                        chunk = pd.DataFrame([chunk])
                    
                    # Filter PDF paths and sort by size for better memory management
                    pdf_info = [(row['path'], row['size']) for _, row in chunk.iterrows() 
                              if row['name'].lower().endswith('.pdf')]
                    if not pdf_info:
                        continue
                        
                    # Sort by size ascending to process smaller files first
                    pdf_info.sort(key=lambda x: x[1])
                    pdf_paths = [p[0] for p in pdf_info]
                    
                    print(f"[INFO] Validating/Repairing PDF batch {chunk_idx+1} with {len(pdf_paths)} files...")
                    print(f"[RESOURCE] Using {max_workers} workers for this batch")
                    
                    batch_results = {}
                    # Always use sequential processing to prevent memory allocation errors
                    print(f"[LOG] Processing {len(pdf_paths)} PDF files sequentially to prevent memory issues...")
                    
                    for i, path in enumerate(tqdm(pdf_paths, desc=f"PDF Batch {chunk_idx+1}")):
                        try:
                            # Process each PDF individually without multiprocessing
                            from src.pdf_manager import PDFManager
                            def dummy_log_error(*args, **kwargs):
                                pass
                            
                            pdf_manager = PDFManager(log_error=dummy_log_error)
                            result = pdf_manager.get_pdf_details(path)
                            batch_results[path] = result
                            
                            # Cleanup after each file
                            del pdf_manager
                            self._cleanup_temp_objects()
                            
                            # Check memory pressure every 10 files
                            if (i + 1) % 10 == 0:
                                enforce_memory_limits()
                                
                        except MemoryError:
                            print(f"[ERROR] Memory error processing PDF: {path}")
                            self.logger.log_error('PDF_MEMORY_ERROR', path, 'PDF processing memory error')
                            enforce_memory_limits()
                            continue
                        except Exception as e:
                            print(f"[ERROR] PDF validation failed for {path}: {e}")
                            self.logger.log_error('PDF_VALIDATION_ERROR', path, str(e))
                            continue
                        
                    pdf_validation.update(batch_results)
                    print(f"[LOG] Batch {chunk_idx+1} completed. Total files processed: {len(pdf_validation)}")
                    
                except ParserError as pe:
                    print(f"[ERROR] ParserError in PDF chunk {chunk_idx+1}: {pe}")
                    enforce_memory_limits()
                    continue
                except MemoryError as me:
                    print(f"[ERROR] MemoryError in PDF chunk {chunk_idx+1}: {me}")
                    print(f"[ERROR] Stopping PDF processing due to memory constraints")
                    enforce_memory_limits()
                    break  # Stop processing instead of continuing
                except Exception as e:
                    print(f"[ERROR] Exception in PDF chunk {chunk_idx+1}: {e}")
                    enforce_memory_limits()
                    continue
                
                if chunk_idx > 0 and chunk_idx % 10 == 0:
                    print("[RESOURCE] Extended cleanup after 10 batches...")
                    enforce_memory_limits()
                    time.sleep(2)
                
        except Exception as e:
            print(f"[ERROR] Exception during PDF validation/repair: {e}")
            print(f"[INFO] Continuing with limited PDF validation results...")
            with open('librarian_run.log', 'a') as logf:
                logf.write(f"[ERROR] Exception during PDF validation/repair: {e}\n{traceback.format_exc()}\n")
        
        enforce_memory_limits()
        self.pdf_validation = pdf_validation
        print(f"[INFO] PDF validation/repair complete. Total PDFs processed: {len(pdf_validation)}")
        return self.pdf_validation

    def classify_and_analyze(self):
        print("[STEP] Starting classify_and_analyze...")
        import sys
        from pandas.errors import ParserError
        
        self.logger.log_error('PROGRESS', '', 'Starting classification and analysis')
        temp_csv = 'file_scan_batches.csv'
        
        if not os.path.exists(temp_csv):
            print("[ERROR] No scanned file batches found for classification.")
            self.analysis_results = []
            return self.analysis_results
        
        knowledge_db_path = self.config.get('knowledge_base_db_url') or "knowledge.sqlite"
        analysis_results = []
        
        # Very conservative chunk size for classification
        available_mb = self.resource_mgr.get_available_ram_mb()
        chunk_size = max(3, min(10, available_mb // 200))  # Much smaller chunks
        print(f"[RESOURCE] Using conservative chunk size: {chunk_size} for classification")
        print(f"[INFO] PDF validation results: {len(self.pdf_validation)} files processed")
        
        # Create picklable partial function
        analyze_row_partial = partial(
            analyze_row,
            knowledge_db_path=knowledge_db_path,
            isbn_cache={},
            pdf_validation=self.pdf_validation
        )
        try:
            for chunk_idx, chunk in enumerate(pd.read_csv(temp_csv, chunksize=chunk_size, engine='python')):
                import gc
                from pandas.errors import ParserError
                if not isinstance(chunk, pd.DataFrame):
                    chunk = pd.DataFrame([chunk])
                row_dicts = chunk.to_dict('records')
                print(f"[INFO] Processing classification chunk {chunk_idx+1} with {len(row_dicts)} files...")
                try:
                    # Always use single worker and sequential processing for memory safety
                    available_mb = self.resource_mgr.get_available_ram_mb()
                    print(f"[INFO] Processing {len(row_dicts)} files sequentially (Available RAM: {available_mb}MB)")
                    
                    # Always process sequentially to prevent memory issues
                    for row in tqdm(row_dicts, desc=f"Classification Chunk {chunk_idx+1}"):
                        try:
                            result = analyze_row_partial(row)
                            if result:
                                analysis_results.append(result)
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
                    
                    print("[LOG] Classification worker batch completed.")
                    
                    # Force garbage collection after each chunk
                    self._cleanup_temp_objects()
                except ParserError as pe:
                    print(f"[ERROR] ParserError in classification chunk {chunk_idx+1}: {pe}")
                    with open('librarian_run.log', 'a') as logf:
                        logf.write(f"[ERROR] ParserError in classification chunk {chunk_idx+1}: {pe}\n")
                    self._cleanup_temp_objects()
                    continue
                except MemoryError as me:
                    print(f"[ERROR] MemoryError during classification chunk {chunk_idx+1}: {me}")
                    self.logger.log_error('MEMORY_ERROR', f'chunk_{chunk_idx+1}', 'Memory error during classification')
                    # Emergency memory cleanup
                    analysis_results = analysis_results[-1000:] if len(analysis_results) > 1000 else analysis_results
                    self._cleanup_temp_objects()
                    import time
                    time.sleep(2)  # Give system time to recover
                    continue
                except Exception as e:
                    print(f"[ERROR] Exception during classification chunk {chunk_idx+1}: {e}")
                    with open('librarian_run.log', 'a') as logf:
                        logf.write(f"[ERROR] Exception during classification chunk {chunk_idx+1}: {e}\n{traceback.format_exc()}\n")
                    self._cleanup_temp_objects()
                    continue
        except Exception as e:
            print(f"[ERROR] Exception during streaming classification: {e}")
            with open('librarian_run.log', 'a') as logf:
                logf.write(f"[ERROR] Exception during streaming classification: {e}\n{traceback.format_exc()}\n")
        self.analysis_results = analysis_results
        self.logger.log_error('PROGRESS', '', 'Classification and analysis complete')
        print(f"[INFO] Classification and analysis complete.")
        return self.analysis_results


    def deduplicate_files(self):
        print("[STEP] Starting deduplicate_files...")
        self.logger.log_error('PROGRESS', '', 'Starting deduplication')

        if not self.analysis_results:
            print("[ERROR] No analysis results to deduplicate. Skipping deduplication.")
            return None

        temp_csv = 'file_scan_batches.csv'
        best_files = {}

        # Process analysis results in chunks to avoid memory issues
        analysis_chunks = [self.analysis_results[i:i+1000] for i in range(0, len(self.analysis_results), 1000)]

        try:
            chunk_idx = 0
            for chunk in pd.read_csv(temp_csv, chunksize=1000, engine='python'):
                if not isinstance(chunk, pd.DataFrame):
                    chunk = pd.DataFrame([chunk])

                # Get corresponding analysis chunk
                if chunk_idx < len(analysis_chunks):
                    analysis_chunk = pd.DataFrame(analysis_chunks[chunk_idx])

                    # Ensure chunks are same size
                    min_len = min(len(chunk), len(analysis_chunk))
                    chunk = chunk.iloc[:min_len].reset_index(drop=True)
                    analysis_chunk = analysis_chunk.iloc[:min_len].reset_index(drop=True)

                    df = pd.concat([chunk, analysis_chunk], axis=1)
                else:
                    # No more analysis results, skip remaining chunks
                    break

                chunk_idx += 1
                
                # --- FIX START ---
                # Vectorized quality scoring for better performance and type safety.
                # Initialize the column with a specific integer dtype.
                df['quality_score'] = 0
                df['quality_score'] = df['quality_score'].astype(int)

                if 'has_ocr' in df.columns:
                    ocr_mask = df['has_ocr'] == True  # noqa: E712
                    df['quality_score'] += (ocr_mask * 4) # Convert boolean to int (True=1, False=0) and add score

                if 'is_pdf_valid' in df.columns:
                    valid_mask = df['is_pdf_valid'] == True  # noqa: E712
                    df['quality_score'] += (valid_mask * 2)

                min_size = self.config.get('min_pdf_size_bytes', 1024)
                if 'size' in df.columns:
                    # Coerce to numeric, fill any conversion errors (NaN) with 0, then create the mask
                    size_mask = (pd.to_numeric(df['size'], errors='coerce').fillna(0) > min_size)
                    df['quality_score'] += (size_mask * 1)
                # --- FIX END ---
                    
                # Vectorized deduplication processing
                for _, row in df.iterrows():
                    h = row.get('hash')
                    if h is None or pd.isna(h):
                        continue

                    score = row.get('quality_score', 0)
                    if h not in best_files or score > best_files[h].get('quality_score', 0):
                        best_files[h] = row.to_dict()

                # Memory cleanup after each chunk
                del df
                self._cleanup_temp_objects()

        except Exception as e:
            print(f"[ERROR] Error during deduplication: {e}")
            self.logger.log_error('DEDUPLICATION_ERROR', '', str(e))
            return None

        finally:
            # Clean up analysis chunks
            del analysis_chunks
            self._cleanup_temp_objects()
            
        if not best_files:
            print("[WARNING] No files with valid hashes found for deduplication.")
            return None

        # Build DataFrame from best_files with memory efficiency
        try:
            unique_files = pd.DataFrame(list(best_files.values()))

            # Sort by quality and size
            sort_columns = []
            if 'quality_score' in unique_files.columns:
                sort_columns.append('quality_score')
            if 'size' in unique_files.columns:
                sort_columns.append('size')

            if sort_columns:
                unique_files = unique_files.sort_values(by=sort_columns, ascending=False)

            # Add name occurrence counter
            if 'name' in unique_files.columns:
                unique_files['name_occurrence'] = unique_files.groupby('name').cumcount()
            else:
                unique_files['name_occurrence'] = 0

            self.df = unique_files
            self.unique_files = unique_files

            self.logger.log_error('PROGRESS', '', f'Deduplication complete. Unique files: {len(unique_files)}')
            print(f"[INFO] Deduplication complete. Unique files: {len(unique_files)}")
            return unique_files

        except Exception as e:
            print(f"[ERROR] Error creating unique files DataFrame: {e}")
            self.logger.log_error('DATAFRAME_ERROR', '', str(e))
            return None

        finally:
            # Clean up best_files dict
            del best_files
            self._cleanup_temp_objects()        

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
            self.analysis_results = []
            self.pdf_validation = {}
            self.isbn_cache = {}
            if hasattr(self, 'df') and self.df is not None:
                del self.df
            if hasattr(self, 'unique_files') and self.unique_files is not None:
                del self.unique_files
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
            from src.isbn_enricher import enrich_file_with_isbn_metadata
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
        import shutil
        import sqlite3
        from tqdm import tqdm
        
        if unique_files is None or unique_files.empty:
            print("[ERROR] No unique files to copy or index. Skipping DB creation.")
            print("[INFO] This may be due to memory errors during processing.")
            print("[INFO] Try running with smaller source directories or more RAM.")
            return
        
        # Validate and test library_root from config
        LIBRARY_ROOT = self.config.get('library_root')
        if not LIBRARY_ROOT:
            print("[ERROR] library_root not configured in config.ini")
            return
        
        print(f"[INFO] Using library_root: {LIBRARY_ROOT}")
        
        # Test destination path accessibility
        print("[INFO] Testing destination path...")
        try:
            # Check if parent directory exists and is accessible
            parent_dir = os.path.dirname(LIBRARY_ROOT)
            if not os.path.exists(parent_dir):
                print(f"[ERROR] Parent directory does not exist: {parent_dir}")
                return
            
            if not os.access(parent_dir, os.R_OK | os.W_OK):
                print(f"[ERROR] Parent directory is not readable/writable: {parent_dir}")
                return
            
            # Create library root if it doesn't exist
            os.makedirs(LIBRARY_ROOT, exist_ok=True)
            print(f"[INFO] Library root created/verified: {LIBRARY_ROOT}")
            
            # Test write permissions with a test file
            test_file = os.path.join(LIBRARY_ROOT, '.write_test')
            try:
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
                print("[INFO] Write permissions verified")
            except Exception as e:
                print(f"[ERROR] Cannot write to destination: {e}")
                return
            
            # Check available space
            import shutil as sh
            total, used, free = sh.disk_usage(LIBRARY_ROOT)
            free_gb = free / (1024**3)
            print(f"[INFO] Available space: {free_gb:.1f} GB")
            
            if free_gb < 1:
                print("[WARNING] Less than 1GB free space available")
            
        except Exception as e:
            print(f"[ERROR] Could not access/create library root {LIBRARY_ROOT}: {e}")
            return
        
        DB_FILE = "library_index.sqlite"
        db_path = os.path.join(LIBRARY_ROOT, DB_FILE)
        print(f"[INFO] Database will be created at: {db_path}")
        
        # Remove old database
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
                print(f"[INFO] Removed existing database: {db_path}")
            except Exception as e:
                print(f"[ERROR] Could not remove old DB: {e}")
                return
        
        copied_count = 0
        failed_count = 0
        
        try:
            self.logger.log_error('PROGRESS', '', 'Copying files and building index')
            
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE files (
                        id INTEGER PRIMARY KEY,
                        filename TEXT,
                        path TEXT,
                        type TEXT,
                        size INTEGER,
                        game_system TEXT,
                        edition TEXT,
                        language TEXT,
                        hash TEXT
                    )
                ''')
                
                batch = []
                batch_size = 100
                
                for idx, row in tqdm(unique_files.iterrows(), 
                                   total=len(unique_files), 
                                   desc="Copying Files", 
                                   unit="file"):
                    try:
                        original_path = Path(row['path'])
                        if not original_path.exists() or not original_path.is_file():
                            failed_count += 1
                            continue
                        if original_path.is_symlink() and not original_path.exists():
                            failed_count += 1  # Skip broken symlinks
                            continue
                        
                        # Generate safe filename
                        name_occurrence = row.get('name_occurrence', 0)
                        if name_occurrence > 0:
                            new_filename = f"{original_path.stem}_{name_occurrence}{original_path.suffix}"
                        else:
                            new_filename = original_path.name
                        
                        # Create destination path
                        game_system = str(row.get('game_system', 'Misc')).replace('/', '_')
                        dest_subdir = Path(game_system)
                        
                        edition = row.get('edition')
                        if edition and pd.notna(edition):
                            dest_subdir = dest_subdir / str(edition).replace('/', '_')
                        
                        category = row.get('category')
                        if category and pd.notna(category):
                            dest_subdir = dest_subdir / str(category).replace('/', '_')
                        
                        destination_path = Path(LIBRARY_ROOT) / dest_subdir / new_filename
                        
                        # Create destination directory
                        os.makedirs(destination_path.parent, exist_ok=True)
                        
                        # Copy file with error handling
                        shutil.copy2(original_path, destination_path)
                        copied_count += 1
                        
                        # Add to database batch
                        batch.append((
                            new_filename,
                            str(destination_path),
                            row.get('mime_type', ''),
                            row.get('size', 0),
                            game_system,
                            edition,
                            row.get('language', ''),
                            row.get('hash', '')
                        ))
                        
                        # Process batch when full
                        if len(batch) >= batch_size:
                            cursor.executemany(
                                "INSERT INTO files (filename, path, type, size, game_system, edition, language, hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                batch
                            )
                            conn.commit()
                            batch.clear()
                            self._cleanup_temp_objects()  # Memory cleanup
                        
                    except Exception as e:
                        print(f"[Error] Could not copy {row.get('path', 'unknown')}: {e}")
                        self.logger.log_error('COPY_ERROR', str(row.get('path', 'unknown')), str(e))
                        failed_count += 1
                
                # Process remaining batch
                if batch:
                    cursor.executemany(
                        "INSERT INTO files (filename, path, type, size, game_system, edition, language, hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        batch
                    )
                    conn.commit()
                
                # Create indexes for better query performance
                cursor.execute("CREATE INDEX idx_game_system ON files(game_system)")
                cursor.execute("CREATE INDEX idx_hash ON files(hash)")
                conn.commit()
                
        except Exception as e:
            self.logger.log_error('ERROR', '', f'Could not build library DB: {e}')
            print(f"[Error] Could not build library DB: {e}")
        
        finally:
            print(f"[INFO] Copy complete. Copied: {copied_count}, Failed: {failed_count}")
            self._cleanup_temp_objects()

