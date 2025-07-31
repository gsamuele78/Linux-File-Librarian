#!/usr/bin/env python3
"""
Memory-constrained worker wrapper that enforces strict memory limits
and ensures proper cleanup after each task.
"""

import gc
import os
import psutil
import resource
import signal
import sys
from functools import wraps

class MemoryConstrainedWorker:
    """Wrapper that enforces memory limits on worker processes"""
    
    def __init__(self, max_memory_mb=400, cleanup_threshold_mb=300):
        self.max_memory_mb = max_memory_mb
        self.cleanup_threshold_mb = cleanup_threshold_mb
        self.process = psutil.Process()
        
    def set_memory_limit(self):
        """Set hard memory limit for the process"""
        try:
            # Set memory limit in bytes
            max_memory_bytes = self.max_memory_mb * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (max_memory_bytes, max_memory_bytes))
            print(f"[WORKER] Memory limit set to {self.max_memory_mb}MB")
        except Exception as e:
            print(f"[WORKER] Could not set memory limit: {e}")
    
    def check_memory_usage(self):
        """Check current memory usage and force cleanup if needed"""
        try:
            mem_info = self.process.memory_info()
            current_mb = mem_info.rss / (1024 * 1024)
            
            if current_mb > self.cleanup_threshold_mb:
                print(f"[WORKER] High memory usage ({current_mb:.1f}MB), forcing cleanup...")
                self.force_cleanup()
                
                # Check again after cleanup
                mem_info = self.process.memory_info()
                current_mb = mem_info.rss / (1024 * 1024)
                
                if current_mb > self.max_memory_mb:
                    print(f"[WORKER] Memory usage still high ({current_mb:.1f}MB), terminating...")
                    os._exit(1)  # Force exit if memory is still too high
                    
            return current_mb
        except Exception as e:
            print(f"[WORKER] Error checking memory: {e}")
            return 0
    
    def force_cleanup(self):
        """Force aggressive memory cleanup"""
        # Clear all possible caches
        gc.collect()
        
        # Clear module caches if possible
        try:
            if hasattr(sys, '_clear_type_cache'):
                sys._clear_type_cache()
        except:
            pass
        
        # Force another garbage collection
        gc.collect()
        
        print("[WORKER] Forced cleanup completed")
    
    def constrain_function(self, func):
        """Decorator to constrain a function's memory usage"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                # Set memory limit at start
                self.set_memory_limit()
                
                # Check memory before execution
                initial_memory = self.check_memory_usage()
                
                # Execute the function
                result = func(*args, **kwargs)
                
                # Check memory after execution and cleanup
                final_memory = self.check_memory_usage()
                self.force_cleanup()
                
                print(f"[WORKER] Task completed. Memory: {initial_memory:.1f}MB -> {final_memory:.1f}MB")
                
                return result
                
            except MemoryError:
                print("[WORKER] Memory error during task execution")
                self.force_cleanup()
                return None
            except Exception as e:
                print(f"[WORKER] Error during task execution: {e}")
                self.force_cleanup()
                return None
            finally:
                # Always cleanup at the end
                self.force_cleanup()
        
        return wrapper

# Global worker instance
_worker = MemoryConstrainedWorker()

def memory_constrained_pdf_worker(path):
    """Memory-constrained PDF processing worker"""
    @_worker.constrain_function
    def _process_pdf(path):
        from src.pdf_manager import PDFManager
        def dummy_log_error(*args, **kwargs):
            pass
        try:
            pdf_manager = PDFManager(log_error=dummy_log_error)
            result = pdf_manager.get_pdf_details(path)
            # Explicitly delete the manager
            del pdf_manager
            gc.collect()
            return result
        except Exception as e:
            print(f"[WORKER] PDF processing failed for {path}: {e}")
            return False, False, None, None, None
    
    return _process_pdf(path)

def memory_constrained_analysis_worker(row, knowledge_db_path, isbn_cache, pdf_validation):
    """Memory-constrained analysis worker"""
    @_worker.constrain_function
    def _analyze_row(row, knowledge_db_path, isbn_cache, pdf_validation):
        # Import the original analyze_row function
        from src.library_builder import analyze_row
        result = analyze_row(row, knowledge_db_path, isbn_cache, pdf_validation)
        return result
    
    return _analyze_row(row, knowledge_db_path, isbn_cache, pdf_validation)