# Workspace Cleanup Analysis

## Enterprise vs Legacy Components

### KEEP - Enterprise/Professional Components (Active)
- `scripts/run_professional.sh` - Main enterprise runner
- `src/professional_orchestrator.py` - Enterprise orchestrator
- `src/enterprise_*.py` - All enterprise modules
- `scripts/cleanup_logs.sh` - Log cleanup utility
- `src/cleanup_logs.py` - Python log cleanup

### REMOVE - Legacy Components (Superseded)
- `scripts/run_librarian.sh` - Superseded by run_professional.sh
- `scripts/run_search_gui.sh` - Superseded by run_enterprise_search.sh
- `src/librarian.py` - Superseded by professional_orchestrator.py
- `src/search_gui.py` - Superseded by enterprise_search_gui.py
- `src/logger.py` - Superseded by enterprise_logging.py
- `src/performance_monitor.py` - Superseded by enterprise_performance_monitor.py

### REMOVE - Unused/Redundant Scripts
- `scripts/fix_destination.sh` - One-time fix script
- `scripts/fix_errors.sh` - One-time fix script
- `scripts/recovery_mode.sh` - Redundant with enterprise error handling
- `scripts/cleanup_broken_links.sh` - Redundant functionality
- `scripts/analyze_unused_code.py` - Development tool, not needed in production
- `scripts/cleanup_unused_code.py` - Development tool, not needed in production
- `scripts/system_manager.sh` - Superseded by enterprise_system_manager.py

### REMOVE - Unused Source Files
- `src/integration_adapter.py` - Complex adapter no longer needed
- `src/system_optimization.py` - Functionality moved to enterprise modules
- `src/memory_constrained_worker.py` - Superseded by enterprise resource management
- `src/utility/` - Most utilities superseded by enterprise modules

### REMOVE - Cache and Temporary Files
- `.mypy_cache/` - Development cache
- `.ruff_cache/` - Development cache
- `logs/` - Will be recreated as needed
- `reports/` - Will be recreated as needed

### KEEP - Configuration and Documentation
- `conf/config.ini` - Main configuration
- `README.md` - Main documentation
- `requirements*.txt` - Dependencies
- Enterprise documentation files