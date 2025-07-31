import os
import psutil
import time
import sys

class ResourceManager:
    """
    Dynamically detects available system resources and provides safe worker/thread counts.
    Enhanced with memory monitoring and automatic resource management.
    """
    def __init__(self, min_free_mb=400, min_workers=1, max_workers=4, ram_per_worker_mb=600, os_reserved_mb=2048, max_ram_usage_ratio=0.5):
        self.os_reserved_mb = max(os_reserved_mb, 512)  # Minimum 512MB for OS
        self.max_ram_usage_ratio = min(max_ram_usage_ratio, 0.8)  # Cap at 80%
        self.min_free_mb = max(min_free_mb, 256)
        self.min_workers = max(min_workers, 1)
        self.max_workers = min(max_workers, os.cpu_count() or 4)
        self.ram_per_worker_mb = max(ram_per_worker_mb, 256)
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
            
            # Dynamic OS reservation based on total RAM
            if total_mb < 4096:  # Less than 4GB
                os_reserved_mb = 512
            elif total_mb < 8192:  # Less than 8GB
                os_reserved_mb = 1024
            else:
                os_reserved_mb = self.os_reserved_mb
            
            usable_mb = max(0, avail_mb - os_reserved_mb)
            max_worker_ram = int(total_mb * self.max_ram_usage_ratio)
            
            if usable_mb > max_worker_ram:
                usable_mb = max_worker_ram
                print(f"[RESOURCE] Capping worker RAM usage to {self.max_ram_usage_ratio*100:.0f}% of total RAM ({max_worker_ram}MB)")
            
            # Calculate workers based on available memory
            if usable_mb <= 0:
                workers = 1
                print(f"[RESOURCE][WARNING] Low memory condition. Using single worker.")
            else:
                # Base calculation
                workers = min(self.max_workers, max(1, int(usable_mb // self.ram_per_worker_mb)))
                
                # Ensure minimum RAM per worker
                min_ram_per_worker = max(256, self.ram_per_worker_mb // 2)
                while workers > self.min_workers and (usable_mb / workers) < min_ram_per_worker:
                    workers -= 1
                
                workers = max(self.min_workers, workers)
            
            # Memory pressure detection
            memory_pressure = self._detect_memory_pressure()
            if memory_pressure and workers > 1:
                workers = max(1, workers // 2)
                print(f"[RESOURCE] Memory pressure detected, reducing workers to {workers}")
            
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
        """Detect if system is under memory pressure"""
        try:
            vm = psutil.virtual_memory()
            # Memory pressure if less than 10% available or swap usage > 50%
            if vm.percent > 90:
                return True
            
            swap = psutil.swap_memory()
            if swap.total > 0 and swap.percent > 50:
                return True
                
            return False
        except Exception:
            return False

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
        gc.collect()
        
        # Clear cached values
        self._cached_worker_count = None
        self._last_check_time = 0
        
        print("[RESOURCE] Forced cleanup completed")
