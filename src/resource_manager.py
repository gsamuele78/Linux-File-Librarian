import os
import psutil
import time
import sys

class ResourceManager:
    """
    Dynamically detects available system resources and provides safe worker/thread counts.
    Enhanced with memory monitoring and automatic resource management.
    """
    def __init__(self, min_free_mb=512, min_workers=1, max_workers=2, ram_per_worker_mb=400, os_reserved_mb=512, max_ram_usage_ratio=0.3):
        self.os_reserved_mb = max(os_reserved_mb, 512)  # Always reserve 512MB for OS
        self.max_ram_usage_ratio = min(max_ram_usage_ratio, 0.4)  # Cap at 40% to be conservative
        self.min_free_mb = max(min_free_mb, 512)  # Always keep 512MB free
        self.min_workers = max(min_workers, 1)
        self.max_workers = min(max_workers, 2)  # Limit to 2 workers max
        self.ram_per_worker_mb = max(ram_per_worker_mb, 300)  # Minimum 300MB per worker
        self._last_check_time = 0
        self._cached_worker_count = None

    def get_available_ram_mb(self):
        # Use only available memory for worker calculation
        """
        Returns available RAM in MB, using /proc/meminfo for accuracy.
        Sums MemFree, Buffers, and Cached for conservative estimate.
        """
        meminfo = {}
        try:
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 2:
                        key = parts[0].rstrip(':')
                        value = int(parts[1])
                        meminfo[key] = value
            mem_free = meminfo.get('MemFree', 0)
            buffers = meminfo.get('Buffers', 0)
            cached = meminfo.get('Cached', 0)
            sreclaimable = meminfo.get('SReclaimable', 0)
            shmem = meminfo.get('Shmem', 0)
            # Linux kernel: MemAvailable is best, but fallback to sum
            mem_available = meminfo.get('MemAvailable', mem_free + buffers + cached + sreclaimable - shmem)
            available_mb = mem_available // 1024
            print(f"[RESOURCE] MemFree: {mem_free//1024}MB, Buffers: {buffers//1024}MB, Cached: {cached//1024}MB, SReclaimable: {sreclaimable//1024}MB, Shmem: {shmem//1024}MB, MemAvailable: {available_mb}MB")
            return available_mb
        except Exception as e:
            print(f"[RESOURCE][WARNING] Could not read /proc/meminfo: {e}")
            # Fallback to psutil
            import psutil
            vm = psutil.virtual_memory()
            available_mb = vm.available // (1024 * 1024)
            print(f"[RESOURCE][FALLBACK] psutil available: {available_mb}MB")
            return available_mb

    def get_total_ram_mb(self):
        return psutil.virtual_memory().total / (1024*1024)

    def get_safe_worker_count(self):
        import time
        
        # Cache worker count for 30 seconds to avoid excessive calculations
        current_time = time.time()
        if (self._cached_worker_count is not None and 
            current_time - self._last_check_time < 30):
            return self._cached_worker_count
        
        try:
            avail_mb = self.get_available_ram_mb()
            total_mb = self.get_total_ram_mb()
            
            # Always reserve 512MB for OS regardless of total RAM
            os_reserved_mb = 512
            
            # Calculate usable memory very conservatively
            usable_mb = max(0, avail_mb - os_reserved_mb - self.min_free_mb)
            
            # Cap at 30% of total RAM to be very conservative
            max_worker_ram = int(total_mb * 0.3)
            
            if usable_mb > max_worker_ram:
                usable_mb = max_worker_ram
                print(f"[RESOURCE] Capping worker RAM usage to 30% of total RAM ({max_worker_ram}MB)")
            
            # Very conservative worker calculation
            if usable_mb < 400:  # Less than 400MB usable
                workers = 1
                print(f"[RESOURCE][WARNING] Very low memory ({usable_mb}MB usable). Using single worker.")
            elif usable_mb < 800:  # Less than 800MB usable
                workers = 1
                print(f"[RESOURCE] Low memory ({usable_mb}MB usable). Using single worker.")
            else:
                # Calculate workers with strict memory per worker (400MB minimum)
                workers = min(self.max_workers, max(1, int(usable_mb // 400)))
                
                # Ensure each worker has at least 400MB
                while workers > 1 and (usable_mb / workers) < 400:
                    workers -= 1
                
                workers = max(1, workers)
            
            # Always check memory pressure and be very conservative
            memory_pressure = self._detect_memory_pressure()
            if memory_pressure or avail_mb < 1000:  # If memory pressure OR less than 1GB available
                workers = 1
                print(f"[RESOURCE] Memory pressure or low RAM detected, forcing single worker")
            
            # Final safety check - never use more than 1 worker if available RAM < 2GB
            if avail_mb < 2000:
                workers = 1
                print(f"[RESOURCE] Available RAM < 2GB ({avail_mb}MB), forcing single worker")
            
            ram_per_worker = usable_mb / workers if workers > 0 else 0
            print(f"[RESOURCE] Workers: {workers}, RAM per worker: {ram_per_worker:.1f}MB, OS reserved: {os_reserved_mb}MB")
            
            # Cache the result
            self._cached_worker_count = workers
            self._last_check_time = current_time
            
            return workers
            
        except Exception as e:
            print(f"[RESOURCE][ERROR] Error calculating worker count: {e}")
            return 1
    
    def _detect_memory_pressure(self):
        """Detect if system is under memory pressure - very sensitive"""
        try:
            vm = psutil.virtual_memory()
            # Memory pressure if less than 20% available (was 10%)
            if vm.percent > 80:
                return True
            
            # Check if available memory is less than 1GB
            if vm.available < 1024 * 1024 * 1024:
                return True
            
            swap = psutil.swap_memory()
            # Memory pressure if any significant swap usage (was 50%)
            if swap.total > 0 and swap.percent > 20:
                return True
                
            return False
        except Exception:
            return True  # Assume pressure if we can't check

    def wait_for_free_ram(self, min_free_mb=None, check_interval=5, max_wait=300):
        if min_free_mb is None:
            min_free_mb = self.min_free_mb
        
        # Adaptive minimum based on system total RAM
        total_mb = self.get_total_ram_mb()
        if total_mb < 4096:  # Less than 4GB system
            min_free_mb = min(min_free_mb, 256)
        
        waited = 0
        while waited < max_wait:
            free_mb = self.get_available_ram_mb()
            
            # Check for memory pressure
            if self._detect_memory_pressure():
                print(f"[RESOURCE] Memory pressure detected, forcing cleanup...")
                self.force_cleanup()
                time.sleep(2)  # Give system time to cleanup
            
            if free_mb >= min_free_mb:
                print(f"[RESOURCE] Sufficient free RAM: {free_mb:.1f}MB >= {min_free_mb}MB")
                break
                
            print(f"[RESOURCE] Waiting for free RAM: {free_mb:.1f}MB < {min_free_mb}MB (waited {waited}s)")
            time.sleep(check_interval)
            waited += check_interval
        
        if waited >= max_wait:
            print(f"[WARNING] Proceeding with limited RAM after {max_wait}s wait.", file=sys.stderr)

    def print_resource_usage(self, phase):
        try:
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            mem_mb = mem_info.rss / (1024*1024)
            
            # Get system memory info
            vm = psutil.virtual_memory()
            system_used_percent = vm.percent
            
            # Get file handles safely
            try:
                open_files = len(process.open_files())
            except (psutil.AccessDenied, AttributeError):
                open_files = 'N/A'
            
            # Get child processes
            try:
                children = len(process.children(recursive=True))
            except psutil.AccessDenied:
                children = 'N/A'
            
            print(f"[RESOURCE] {phase}: Process RAM={mem_mb:.1f}MB, System RAM={system_used_percent:.1f}%, Files={open_files}, Children={children}")
            
            # Warning if memory usage is high
            if mem_mb > 1024:  # More than 1GB
                print(f"[RESOURCE][WARNING] High memory usage detected: {mem_mb:.1f}MB")
                
        except Exception as e:
            print(f"[RESOURCE][ERROR] Could not get resource usage: {e}")
    
    def force_cleanup(self):
        """Force garbage collection and memory cleanup"""
        import gc
        self._cleanup_temp_objects()
        
        # Clear cached values
        self._cached_worker_count = None
        self._last_check_time = 0
        
        print("[RESOURCE] Forced cleanup completed")
        
    def _cleanup_temp_objects(self):
        """Clean up temporary objects and force garbage collection"""
        import gc
        gc.collect()
