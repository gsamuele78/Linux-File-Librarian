#!/usr/bin/env python3
import json
import sys
from pathlib import Path

def generate_report(metrics_file, report_file):
    try:
        if Path(metrics_file).exists():
            with open(metrics_file, 'r') as f:
                metrics = json.load(f)
        else:
            metrics = []
        
        if not metrics:
            # Generate basic report even without metrics
            report = """
PROFESSIONAL PERFORMANCE REPORT
================================

No performance metrics collected.
This may indicate the monitoring process failed to start or collect data.

RECOMMENDATIONS:
- Check system permissions
- Verify Python psutil module is installed
- Review logs for monitoring errors
"""
            with open(report_file, 'w') as f:
                f.write(report)
            print(report)
            return
        
        # Calculate statistics
        cpu_avg = sum(m['cpu_percent'] for m in metrics) / len(metrics)
        cpu_max = max(m['cpu_percent'] for m in metrics)
        
        memory_avg = sum(m['memory_percent'] for m in metrics) / len(metrics)
        memory_max = max(m['memory_percent'] for m in metrics)
        
        load_avg = sum(m['load_average'] for m in metrics) / len(metrics)
        
        # Generate report
        report = f"""
PROFESSIONAL PERFORMANCE REPORT
================================

Monitoring Duration: {len(metrics) * 10 / 60:.1f} minutes
Sample Count: {len(metrics)}

CPU USAGE:
- Average: {cpu_avg:.1f}%
- Peak: {cpu_max:.1f}%

MEMORY USAGE:
- Average: {memory_avg:.1f}%
- Peak: {memory_max:.1f}%

SYSTEM LOAD:
- Average: {load_avg:.2f}

RECOMMENDATIONS:
"""
        
        if cpu_max > 90:
            report += "- High CPU usage detected. Consider reducing concurrent operations.\n"
        
        if memory_max > 85:
            report += "- High memory usage detected. Consider increasing available RAM.\n"
        
        if load_avg > 2.0:
            report += "- High system load detected. Consider optimizing workload distribution.\n"
        
        if cpu_max < 50 and memory_max < 50:
            report += "- System resources are underutilized. Consider increasing parallelism.\n"
        
        with open(report_file, 'w') as f:
            f.write(report)
        
        print(report)
        
    except Exception as e:
        print(f"Report generation error: {e}", file=sys.stderr)

if __name__ == "__main__":
    generate_report(sys.argv[1], sys.argv[2])
