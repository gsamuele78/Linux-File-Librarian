import sys

class Logger:
    def __init__(self, log_file="librarian_run.log", max_errors=10000):
        self.log_file = log_file
        self._logged_errors = set()
        self._error_counts = {}
        self.max_errors = max_errors
        self._total_logged = 0
        
        # Clean up old log file
        import os
        if os.path.exists(self.log_file):
            try:
                os.remove(self.log_file)
                print(f"[CLEANUP] Removed old log file: {self.log_file}")
            except Exception as e:
                print(f"[CLEANUP] Could not remove old log file {self.log_file}: {e}")

    def log_error(self, error_type, file_path, message, extra=None):
        try:
            # Prevent memory exhaustion by limiting stored errors
            if self._total_logged > self.max_errors:
                self._cleanup_old_errors()
            
            # Truncate long messages and paths to save memory
            truncated_message = message[:200] if len(message) > 200 else message
            truncated_path = file_path[-100:] if len(file_path) > 100 else file_path
            
            key = (error_type, truncated_path, truncated_message)
            if key in self._logged_errors:
                self._error_counts[key] = self._error_counts.get(key, 1) + 1
                return
            
            # Check memory before adding new entries
            if len(self._logged_errors) > self.max_errors // 2:
                self._cleanup_old_errors()
            
            self._logged_errors.add(key)
            self._error_counts[key] = 1
            self._total_logged += 1
            
            # Write to log file immediately to avoid memory buildup
            try:
                with open(self.log_file, "a", encoding="utf-8") as logf:
                    logf.write(f"[{error_type}] {truncated_path} | {truncated_message}\n")
                    if extra:
                        truncated_extra = extra[:300] if len(extra) > 300 else extra
                        logf.write(f"    {truncated_extra}\n")
            except Exception as e:
                print(f"[ERROR] Could not write to log file {self.log_file}: {e}", file=sys.stderr)
                
        except MemoryError:
            # Emergency handling for memory errors
            print(f"[MEMORY_ERROR] {error_type}: {file_path[:50]}...", file=sys.stderr)
            self._logged_errors.clear()
            self._error_counts.clear()
            self._total_logged = 0
        except Exception as e:
            print(f"[LOGGER_ERROR] {e}", file=sys.stderr)
    
    def _cleanup_old_errors(self):
        """Remove old errors to prevent memory exhaustion"""
        try:
            if len(self._logged_errors) > self.max_errors // 2:
                # Keep only the most frequent errors
                sorted_errors = sorted(self._error_counts.items(), key=lambda x: x[1], reverse=True)
                keep_count = min(self.max_errors // 4, 1000)  # Reduce memory footprint
                
                # Use more memory-efficient approach
                self._logged_errors.clear()
                self._error_counts.clear()
                
                for (key, count) in sorted_errors[:keep_count]:
                    self._logged_errors.add(key)
                    self._error_counts[key] = count
                
                self._total_logged = len(self._logged_errors)
                
                print(f"[LOGGER] Cleaned up old errors, keeping {keep_count} most frequent")
                
                # Force garbage collection
                import gc
                gc.collect()
        except MemoryError:
            # Emergency cleanup - clear everything
            self._logged_errors.clear()
            self._error_counts.clear()
            self._total_logged = 0
            print("[LOGGER] Emergency cleanup due to memory error")
        except Exception as e:
            print(f"[LOGGER] Error during cleanup: {e}")

    def print_summary(self):
        if not self._error_counts:
            print("  No errors or warnings logged.")
            return
        
        try:
            from collections import defaultdict
            type_counts = defaultdict(int)
            for (etype, fpath, msg), count in self._error_counts.items():
                type_counts[etype] += count
            
            print(f"  Total unique errors: {len(self._error_counts)}")
            for etype, count in sorted(type_counts.items()):
                print(f"  {etype}: {count} occurrences")
            
            print(f"  See {self.log_file} for details and file paths.")
            
            # Show most common errors (limit to prevent spam)
            most_common = sorted(self._error_counts.items(), key=lambda x: -x[1])[:5]
            if most_common:
                print("  Most frequent errors:")
                for (etype, fpath, msg), count in most_common:
                    # Truncate long messages
                    short_msg = msg[:100] + "..." if len(msg) > 100 else msg
                    short_path = fpath[-50:] if len(fpath) > 50 else fpath
                    print(f"    {etype} ({count}): {short_path} | {short_msg}")
        
        except Exception as e:
            print(f"  [ERROR] Could not generate summary: {e}")
    
    def clear_logs(self):
        """Clear all logged errors and counts"""
        self._logged_errors.clear()
        self._error_counts.clear()
        self._total_logged = 0
        print("[LOGGER] All logs cleared")
