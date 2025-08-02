#!/usr/bin/env python3
import psutil
import time
import json
import sys

def monitor_performance(duration=3600):  # Monitor for 1 hour by default
    metrics = []
    start_time = time.time()
    
    while time.time() - start_time < duration:
        try:
            metric = {
                'timestamp': time.time(),
                'cpu_percent': psutil.cpu_percent(interval=1),
                'memory_percent': psutil.virtual_memory().percent,
                'memory_available_mb': psutil.virtual_memory().available / (1024 * 1024),
                'disk_io_read_mb': psutil.disk_io_counters().read_bytes / (1024 * 1024),
                'disk_io_write_mb': psutil.disk_io_counters().write_bytes / (1024 * 1024),
                'load_average': psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0
            }
            metrics.append(metric)
            time.sleep(10)  # Sample every 10 seconds
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Monitoring error: {e}", file=sys.stderr)
    
    # Save metrics
    with open(sys.argv[1], 'w') as f:
        json.dump(metrics, f, indent=2)

if __name__ == "__main__":
    monitor_performance()
