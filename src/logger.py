import sys

class Logger:
    def __init__(self, log_file="librarian_run.log"):
        self.log_file = log_file
        self._logged_errors = set()
        self._error_counts = {}

    def log_error(self, error_type, file_path, message, extra=None):
        key = (error_type, file_path, message)
        if key in self._logged_errors:
            self._error_counts[key] = self._error_counts.get(key, 1) + 1
            return
        self._logged_errors.add(key)
        self._error_counts[key] = 1
        with open(self.log_file, "a", encoding="utf-8") as logf:
            logf.write(f"[{error_type}] {file_path} | {message}\n")
            if extra:
                logf.write(f"    {extra}\n")

    def print_summary(self):
        if not self._error_counts:
            print("  No errors or warnings logged.")
            return
        from collections import defaultdict
        type_counts = defaultdict(int)
        for (etype, fpath, msg), count in self._error_counts.items():
            type_counts[etype] += count
        for etype, count in type_counts.items():
            print(f"  {etype}: {count} occurrences")
        print(f"  See {self.log_file} for details and file paths.")
        most_common = sorted(self._error_counts.items(), key=lambda x: -x[1])[:5]
        if most_common:
            print("  Most frequent errors:")
            for (etype, fpath, msg), count in most_common:
                print(f"    {etype} ({count}): {fpath} | {msg}")
