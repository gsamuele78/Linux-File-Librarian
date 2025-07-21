import os
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import mimetypes
from functools import partial
from tqdm import tqdm
import os
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import mimetypes
from functools import partial
from tqdm import tqdm

def analyze_row(row, knowledge_db_path, isbn_cache, pdf_validation, pdf_manager, get_file_hash):
    from src.classifier import Classifier
    path = row.get('path')
    name = row.get('name')
    size = row.get('size')
    try:
        import magic
        mime_type = magic.from_file(path)
    except Exception:
        mime_type = mimetypes.guess_type(path)[0] or 'application/octet-stream'
    try:
        file_hash = get_file_hash(path)
    except Exception as e:
        file_hash = None
        print(f"[ERROR][analyze_row] Could not hash file {path}: {e}")
    is_pdf_valid = None
    has_ocr = None
    pdf_version = None
    pdf_creator = None
    pdf_producer = None
    if name and name.lower().endswith('.pdf'):
        pdf_info = pdf_validation.get(path) if pdf_validation else None
        if pdf_info:
            is_pdf_valid, has_ocr, pdf_version, pdf_creator, pdf_producer = pdf_info
        else:
            # Instantiate pdf_manager if not provided
            if pdf_manager is None:
                try:
                    from src.pdf_manager import PDFManager
                    # PDFManager requires log_error argument; use a dummy if not provided
                    def dummy_log_error(*args, **kwargs):
                        pass
                    pdf_manager = PDFManager(log_error=dummy_log_error)
                except Exception as e:
                    print(f"[ERROR][analyze_row] Could not instantiate PDFManager: {e}")
                    pdf_manager = None
            if pdf_manager:
                try:
                    is_pdf_valid, has_ocr, pdf_version, pdf_creator, pdf_producer = pdf_manager.get_pdf_details(path)
                except Exception as e:
                    print(f"[ERROR][analyze_row] PDF details failed for {path}: {e}")
    # Each worker creates its own Classifier
    try:
        classifier = Classifier(knowledge_db_path)
        game_system, edition, category = classifier.classify(name, path, mime_type)
        classifier.close()
    except Exception as e:
        print(f"[ERROR][analyze_row] Classifier failed for {name}: {e}")
        game_system = edition = category = None
    language = None
    return {
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

    def scan_files(self):
        print("[STEP] Starting scan_files...")
        import gc
        import csv
        source_paths = self.config['source_paths']
        batch_size = 100
        total_files = 0
        temp_csv = 'file_scan_batches.csv'
        # Remove old temp file if exists
        if os.path.exists(temp_csv):
            os.remove(temp_csv)
        # Write header
        with open(temp_csv, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['path','name','size'])
            writer.writeheader()
        def file_generator(src):
            try:
                for root, dirs, files in os.walk(src):
                    for fname in files:
                        fpath = os.path.join(root, fname)
                        try:
                            stat = os.stat(fpath)
                            yield {'path': fpath, 'name': fname, 'size': stat.st_size}
                        except Exception as e:
                            print(f"  [Warning] Could not stat file {fname}. Skipping. Reason: {e}")
            except Exception as e:
                print(f"[Warning] Error scanning {src}: {e}")
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
                    gc.collect()
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
                gc.collect()
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
        import sys, traceback, psutil, time
        if self.df is None:
            print("[ERROR] No files to validate.")
            self.pdf_validation = {}
            return self.pdf_validation
        import pandas as pd
        from pandas.errors import ParserError
        import gc

        # More aggressive chunk size to reduce memory pressure
        chunk_size = 10  # Reduced from 25
        temp_csv = 'file_scan_batches.csv'
        pdf_validation = {}

        # Function to check system resources and adjust workers
        def get_adjusted_worker_count():
            try:
                mem = psutil.virtual_memory()
                available_gb = mem.available / (1024**3)
                # More conservative worker count based on available memory
                suggested_workers = max(1, min(
                    self.resource_mgr.get_safe_worker_count(),
                    int(available_gb / 0.5)  # Assume each worker needs ~500MB
                ))
                print(f"[RESOURCE] Available memory: {available_gb:.2f}GB, Suggested workers: {suggested_workers}")
                return suggested_workers
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
                max_workers = get_adjusted_worker_count()
                
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
                    with ProcessPoolExecutor(max_workers=max_workers) as executor:
                        # Process files in smaller sub-batches for better memory management
                        sub_batch_size = max(1, len(pdf_paths) // max_workers)
                        for i in range(0, len(pdf_paths), sub_batch_size):
                            sub_batch = pdf_paths[i:i + sub_batch_size]
                            futures = {executor.submit(self.pdf_manager.get_pdf_details, path): path 
                                     for path in sub_batch}
                            
                            print(f"[LOG] Processing sub-batch {i//sub_batch_size + 1} of PDF files...")
                            for future in tqdm(as_completed(futures), 
                                            total=len(futures), 
                                            desc=f"PDF Sub-batch {i//sub_batch_size + 1}"):
                                try:
                                    result = future.result(timeout=120)  # Reduced timeout
                                    batch_results[futures[future]] = result
                                except TimeoutError:
                                    print(f"[ERROR] Timeout processing PDF: {futures[future]}")
                                except MemoryError:
                                    print(f"[ERROR] Memory error processing PDF: {futures[future]}")
                                    enforce_memory_limits()
                                except Exception as e:
                                    print(f"[ERROR] PDF validation/repair failed: {e}")
                            
                            enforce_memory_limits()
                        
                    pdf_validation.update(batch_results)
                    print(f"[LOG] Batch {chunk_idx+1} completed. Total files processed: {len(pdf_validation)}")
                    
                except ParserError as pe:
                    print(f"[ERROR] ParserError in PDF chunk {chunk_idx+1}: {pe}")
                    enforce_memory_limits()
                    continue
                except MemoryError as me:
                    print(f"[ERROR] MemoryError in PDF chunk {chunk_idx+1}: {me}")
                    enforce_memory_limits()
                    time.sleep(2)  # Additional delay to let system recover
                    continue
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
            with open('librarian_run.log', 'a') as logf:
                logf.write(f"[ERROR] Exception during PDF validation/repair: {e}\n{traceback.format_exc()}\n")
        
        enforce_memory_limits()
        self.pdf_validation = pdf_validation
        print(f"[INFO] PDF validation/repair complete. Total PDFs processed: {len(pdf_validation)}")
        return self.pdf_validation

    def classify_and_analyze(self):
        print("[STEP] Starting classify_and_analyze...")
        import sys, traceback
        self.logger.log_error('PROGRESS', '', 'Starting classification and analysis')
        import pandas as pd
        temp_csv = 'file_scan_batches.csv'
        if not os.path.exists(temp_csv):
            print("[ERROR] No scanned file batches found for classification.")
            self.analysis_results = []
            return self.analysis_results
        knowledge_db_path = self.config.get('knowledge_base_db_url') or "knowledge.sqlite"
        analysis_results = []
        # Use a top-level partial for analyze_row, which is picklable
        from functools import partial
        # Only pass primitive types and functions, not Classifier instance
        analyze_row_partial = partial(
            analyze_row,
            knowledge_db_path=knowledge_db_path,
            isbn_cache=self.isbn_cache,
            pdf_validation=self.pdf_validation,
            pdf_manager=None,  # pdf_manager should be instantiated inside analyze_row if needed
            get_file_hash=self.get_file_hash
        )
        chunk_size = 25
        try:
            for chunk_idx, chunk in enumerate(pd.read_csv(temp_csv, chunksize=chunk_size, engine='python')):
                import gc
                from pandas.errors import ParserError
                if not isinstance(chunk, pd.DataFrame):
                    chunk = pd.DataFrame([chunk])
                row_dicts = chunk.to_dict('records')
                print(f"[INFO] Processing classification chunk {chunk_idx+1} with {len(row_dicts)} files...")
                try:
                    with ProcessPoolExecutor(max_workers=max(1, min(2, self.resource_mgr.get_safe_worker_count()))) as executor:
                        futures = {executor.submit(analyze_row_partial, row): row for row in row_dicts}
                        print("[LOG] Waiting for classification worker batch to complete...")
                        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Classification Batch {chunk_idx+1}"):
                            try:
                                result = future.result(timeout=300)
                                analysis_results.append(result)
                            except Exception as e:
                                print(f"[ERROR] Hashing/analysis failed for a file: {e}")
                        print("[LOG] Classification worker batch completed.")
                    gc.collect()
                except ParserError as pe:
                    print(f"[ERROR] ParserError in classification chunk {chunk_idx+1}: {pe}")
                    with open('librarian_run.log', 'a') as logf:
                        logf.write(f"[ERROR] ParserError in classification chunk {chunk_idx+1}: {pe}\n")
                    gc.collect()
                    continue
                except MemoryError as me:
                    print(f"[ERROR] MemoryError during classification chunk {chunk_idx+1}: {me}")
                    with open('librarian_run.log', 'a') as logf:
                        logf.write(f"[ERROR] MemoryError during classification chunk {chunk_idx+1}: {me}\n{traceback.format_exc()}\n")
                    gc.collect()
                    continue
                except Exception as e:
                    print(f"[ERROR] Exception during classification chunk {chunk_idx+1}: {e}")
                    with open('librarian_run.log', 'a') as logf:
                        logf.write(f"[ERROR] Exception during classification chunk {chunk_idx+1}: {e}\n{traceback.format_exc()}\n")
                    gc.collect()
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
        import pandas as pd
        temp_csv = 'file_scan_batches.csv'
        # Prepare a dict to track unique hashes and best file per hash
        best_files = {}
        analysis_df = pd.DataFrame(self.analysis_results)
        chunk_idx = 0
        for chunk in pd.read_csv(temp_csv, chunksize=1000):
            chunk_idx += 1
            if not isinstance(chunk, pd.DataFrame):
                chunk = pd.DataFrame([chunk])
            # Align chunk with analysis results
            start = (chunk_idx-1)*1000
            end = start+len(chunk)
            analysis_chunk = analysis_df.iloc[start:end].reset_index(drop=True)
            df = pd.concat([chunk.reset_index(drop=True), analysis_chunk], axis=1)
            df['quality_score'] = 0
            def safe_add_quality(row, add):
                try:
                    if isinstance(row['quality_score'], (int, float)):
                        return row['quality_score'] + add
                except Exception:
                    pass
                return row['quality_score']
            df.loc[df['has_ocr'] == True, 'quality_score'] = df.loc[df['has_ocr'] == True].apply(lambda row: safe_add_quality(row, 4), axis=1)
            df.loc[df['is_pdf_valid'] == True, 'quality_score'] = df.loc[df['is_pdf_valid'] == True].apply(lambda row: safe_add_quality(row, 2), axis=1)
            mask = (df['size'].apply(lambda x: isinstance(x, (int, float))) & (df['size'] > self.config['min_pdf_size_bytes']))
            df.loc[mask, 'quality_score'] = df.loc[mask].apply(lambda row: safe_add_quality(row, 1), axis=1)
            # For each file, keep only the best per hash
            for _, row in df.iterrows():
                h = row.get('hash')
                if h is None:
                    continue
                score = row.get('quality_score', 0)
                if h not in best_files or score > best_files[h].get('quality_score', 0):
                    best_files[h] = row.to_dict()
        # Build DataFrame from best_files
        unique_files = pd.DataFrame(list(best_files.values()))
        unique_files = unique_files.sort_values(by=['quality_score', 'size'], ascending=False).copy()
        unique_files['name_occurrence'] = unique_files.groupby('name').cumcount()
        self.df = unique_files
        self.unique_files = unique_files
        self.logger.log_error('PROGRESS', '', f'Deduplication complete. Unique files: {len(unique_files)}')
        print(f"[INFO] Deduplication complete. Unique files: {len(unique_files)}")
        return unique_files

    def build(self):
        self.scan_files()
        self.validate_and_repair_pdfs()
        # Automated concurrent access test for knowledge.sqlite
        self.test_knowledge_db_concurrent_access()
        self.classify_and_analyze()
        unique_files = self.deduplicate_files()
        self.copy_and_index(unique_files)
        self.logger.print_summary()
        print("[INFO] Library build process complete.")

    def get_file_hash(self, path, block_size=65536):
        import hashlib
        hasher = hashlib.sha256()
        try:
            with open(path, 'rb') as f:
                for chunk in iter(lambda: f.read(block_size), b''):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            print(f"[ERROR] Could not hash file {path}: {e}")
            return None

    def classify_with_isbn_fallback(self, classifier, filename, full_path, mime_type, isbn_cache):
        from src.isbn_enricher import enrich_file_with_isbn_metadata
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
        if mime_type.startswith('application/pdf') or mime_type.startswith('text/'):
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
                            print(f"  [DEBUG] ISBN enrichment matched title in product cache: {title}")
                            return self.classifier.product_cache[ntitle]
                        print(f"  [DEBUG] ISBN enrichment used Open Library metadata for: {title}")
                        return (title, None, "ISBN/Book")
            except Exception as e:
                print(f"  [DEBUG] ISBN enrichment failed for {full_path}: {e}")
        print(f"  [DEBUG] File {filename} is uncategorized after all attempts.")
        return ('Miscellaneous', None, None)

    def copy_and_index(self, unique_files):
        print("[STEP] Starting copy_and_index...")
        self.logger.log_error('PROGRESS', '', 'Starting copy and index phase')
        import shutil
        import sqlite3
        from tqdm import tqdm
        LIBRARY_ROOT = self.config['library_root']
        DB_FILE = "library_index.sqlite"
        db_path = os.path.join(LIBRARY_ROOT, DB_FILE)
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir, exist_ok=True)
            except Exception as e:
                print(f"  [Error] Could not create DB directory: {e}")
                return
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception as e:
                print(f"  [Error] Could not remove old DB: {e}")
                return
        if unique_files is None or unique_files.empty:
            print("[ERROR] No unique files to copy or index. Skipping DB creation.")
            return
        try:
            self.logger.log_error('PROGRESS', '', 'Copying files and building index')
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('CREATE TABLE files (id INTEGER PRIMARY KEY, filename TEXT, path TEXT, type TEXT, size INTEGER, game_system TEXT, edition TEXT, language TEXT)')
                batch = []
                for _, row in tqdm(list(unique_files.iterrows()), desc="Copying Files", unit="file"):
                    original_path = Path(row['path'])
                    new_filename = f"{original_path.stem}_{row['name_occurrence']}{original_path.suffix}" if row['name_occurrence'] > 0 else original_path.name
                    dest_subdir = Path(row['game_system'] or 'Misc')
                    if row['edition'] and pd.notna(row['edition']): dest_subdir = dest_subdir / row['edition']
                    if row['category'] and pd.notna(row['category']): dest_subdir = dest_subdir / row['category']
                    destination_path = Path(LIBRARY_ROOT) / dest_subdir / new_filename
                    os.makedirs(destination_path.parent, exist_ok=True)
                    try:
                        shutil.copy2(original_path, destination_path)
                        language = getattr(row, 'language', None)
                        batch.append((new_filename, str(destination_path), row['mime_type'], row['size'], row['game_system'], row['edition'], language))
                    except Exception as e:
                        print(f"  [Error] Could not copy {original_path}. Reason: {e}")
                    if len(batch) >= 100:
                        cursor.executemany(
                            "INSERT INTO files (filename, path, type, size, game_system, edition, language) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            batch
                        )
                        batch = []
                if batch:
                    cursor.executemany(
                        "INSERT INTO files (filename, path, type, size, game_system, edition, language) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        batch
                    )
                conn.commit()
        except Exception as e:
            self.logger.log_error('ERROR', '', f'Could not build library DB: {e}')
            print(f"  [Error] Could not build library DB: {e}")
