import os
import psutil
import time
import sys

class ResourceManager:
    """
    Dynamically detects available system resources and provides safe worker/thread counts.
    Use this to avoid OOM and adapt to the running system.
    """
    def __init__(self, min_free_mb=400, min_workers=1, max_workers=4, ram_per_worker_mb=600, os_reserved_mb=2048, max_ram_usage_ratio=0.5):
        # Always reserve at least 2GB for the OS by default
        self.os_reserved_mb = os_reserved_mb
        self.max_ram_usage_ratio = max_ram_usage_ratio
        self.min_free_mb = max(min_free_mb, self.os_reserved_mb)
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.ram_per_worker_mb = ram_per_worker_mb

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
        avail_mb = self.get_available_ram_mb()
        total_mb = self.get_total_ram_mb()
        # If system RAM is low, allow os_reserved_mb to be as low as 512MB
        min_os_reserved = 512
        os_reserved_mb = max(self.os_reserved_mb, min_os_reserved)
        usable_mb = max(0, avail_mb - os_reserved_mb)
        max_worker_ram = int(total_mb * self.max_ram_usage_ratio)
        if usable_mb > max_worker_ram:
            usable_mb = max_worker_ram
            print(f"[RESOURCE] Capping worker RAM usage to {self.max_ram_usage_ratio*100:.0f}% of total RAM ({max_worker_ram}MB)")
        # Always allow at least 1 worker, even if usable_mb is very low
        min_ram_per_worker = 600
        workers = min(self.max_workers, int(usable_mb // self.ram_per_worker_mb)) if usable_mb > 0 else 1
        # Dynamically reduce workers until each gets at least min_ram_per_worker
        while workers > self.min_workers:
            if workers == 0:
                workers = 1
                break
            ram_per_worker = usable_mb / workers if workers else 1
            if ram_per_worker >= min_ram_per_worker:
                break
            print(f"[RESOURCE] Reducing workers to {workers-1} to ensure at least {min_ram_per_worker}MB RAM per worker (current: {ram_per_worker:.1f}MB)")
            workers -= 1
        if workers < self.min_workers:
            workers = self.min_workers
        # Never show 0MB per worker, set a floor of 1MB for display
        ram_per_worker_display = max(usable_mb / workers if workers else 1, 1)
        if usable_mb <= 0:
            print(f"[RESOURCE][WARNING] Usable RAM for workers is zero or negative after reserving {os_reserved_mb}MB for OS. Forcing 1 worker with minimal RAM. System may be under memory pressure. Consider freeing up RAM or lowering os_reserved_mb in config.")
        print(f"[RESOURCE] Final worker count: {workers} (RAM per worker: {ram_per_worker_display:.1f}MB, OS reserved: {os_reserved_mb}MB, max usage: {self.max_ram_usage_ratio*100:.0f}%)")
        return workers

    def wait_for_free_ram(self, min_free_mb=None, check_interval=5, max_wait=300):
        # Always reserve at least 1GB for the OS
        if min_free_mb is None:
            min_free_mb = self.min_free_mb
        min_free_mb = max(min_free_mb, self.os_reserved_mb)
        waited = 0
        while True:
            free_mb = self.get_available_ram_mb()
            if free_mb >= min_free_mb:
                print(f"[RESOURCE] Sufficient free RAM: {free_mb:.1f}MB >= {min_free_mb}MB")
                break
            print(f"[RESOURCE] Waiting for free RAM: {free_mb:.1f}MB < {min_free_mb}MB (waited {waited}s)")
            time.sleep(check_interval)
            waited += check_interval
            if waited >= max_wait:
                print(f"[WARNING] Still low on RAM after {max_wait}s, proceeding anyway.", file=sys.stderr)
                break

    def print_resource_usage(self, phase):
        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss / (1024*1024)
        open_files = len(process.open_files()) if hasattr(process, 'open_files') else 'N/A'
        children = len(process.children(recursive=True))
        print(f"[RESOURCE] {phase}: RAM={mem:.1f}MB, OpenFiles={open_files}, Children={children}")
