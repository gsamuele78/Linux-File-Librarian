#!/usr/bin/env python3
"""
Enterprise Search GUI

Advanced search interface with enterprise features including:
- Modern UI with theming support
- Advanced search capabilities
- Export functionality
- Performance monitoring
- Accessibility features
"""

import os
import sys
import sqlite3
import json
import csv
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from subprocess import run, CalledProcessError, DEVNULL

# Import enterprise framework
from enterprise_gui_framework import (
    EnterpriseApplication, EnterpriseEntry, EnterpriseCombobox,
    EnterpriseTreeview, EnterpriseProgressBar, EnterpriseDialog,
    ThemeType, ComponentState
)

# Import configuration loader
try:
    from config_loader import load_config
except ImportError:
    from src.config_loader import load_config

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """Search result data structure"""
    filename: str
    game_system: str
    file_type: str
    size_bytes: int
    path: str
    language: str = ""
    tags: List[str] = None
    last_modified: float = 0.0
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class SearchCriteria:
    """Search criteria data structure"""
    filename_query: str = ""
    game_system: str = ""
    file_type: str = ""
    language: str = ""
    min_size_kb: int = 0
    max_size_kb: int = 0
    tags: List[str] = None
    date_from: str = ""
    date_to: str = ""
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []


class SearchDatabase:
    """Enterprise database interface for search operations"""
    
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self._validate_database()
    
    def _validate_database(self):
        """Validate database exists and has required structure"""
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'")
                if not cursor.fetchone():
                    raise ValueError("Database missing 'files' table")
        except sqlite3.Error as e:
            raise ValueError(f"Database validation failed: {e}")
    
    def get_filter_values(self) -> Dict[str, List[str]]:
        """Get unique values for filter dropdowns"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.cursor()
                
                filters = {}
                
                # Game systems
                cursor.execute("SELECT DISTINCT game_system FROM files WHERE game_system IS NOT NULL ORDER BY game_system")
                filters['game_systems'] = [row[0] for row in cursor.fetchall()]
                
                # File types
                cursor.execute("SELECT DISTINCT type FROM files WHERE type IS NOT NULL ORDER BY type")
                filters['file_types'] = [row[0] for row in cursor.fetchall()]
                
                # Languages
                cursor.execute("SELECT DISTINCT language FROM files WHERE language IS NOT NULL ORDER BY language")
                filters['languages'] = [row[0] for row in cursor.fetchall()]
                
                return filters
                
        except sqlite3.Error as e:
            logger.error(f"Failed to get filter values: {e}")
            return {'game_systems': [], 'file_types': [], 'languages': []}
    
    def search_files(self, criteria: SearchCriteria, limit: int = 1000) -> List[SearchResult]:
        """Search files based on criteria"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.cursor()
                
                # Build query
                query = "SELECT filename, game_system, type, size, path, language FROM files WHERE 1=1"
                params = []
                
                # Filename filter
                if criteria.filename_query:
                    query += " AND filename LIKE ?"
                    params.append(f"%{criteria.filename_query}%")
                
                # Game system filter
                if criteria.game_system and criteria.game_system != "-- ALL --":
                    query += " AND game_system = ?"
                    params.append(criteria.game_system)
                
                # File type filter
                if criteria.file_type and criteria.file_type != "-- ALL --":
                    query += " AND type = ?"
                    params.append(criteria.file_type)
                
                # Language filter
                if criteria.language and criteria.language != "-- ALL --":
                    query += " AND language = ?"
                    params.append(criteria.language)
                
                # Size filters
                if criteria.min_size_kb > 0:
                    query += " AND size >= ?"
                    params.append(criteria.min_size_kb * 1024)
                
                if criteria.max_size_kb > 0:
                    query += " AND size <= ?"
                    params.append(criteria.max_size_kb * 1024)
                
                # Add ordering and limit
                query += " ORDER BY game_system, filename LIMIT ?"
                params.append(limit)
                
                # Execute query
                cursor.execute(query, params)
                results = []
                
                for row in cursor.fetchall():
                    filename, game_system, file_type, size_bytes, path, language = row
                    results.append(SearchResult(
                        filename=filename or "",
                        game_system=game_system or "",
                        file_type=file_type or "",
                        size_bytes=size_bytes or 0,
                        path=path or "",
                        language=language or ""
                    ))
                
                return results
                
        except sqlite3.Error as e:
            logger.error(f"Search failed: {e}")
            raise
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.cursor()
                
                stats = {}
                
                # Total files
                cursor.execute("SELECT COUNT(*) FROM files")
                stats['total_files'] = cursor.fetchone()[0]
                
                # Total size
                cursor.execute("SELECT SUM(size) FROM files WHERE size IS NOT NULL")
                total_size = cursor.fetchone()[0] or 0
                stats['total_size_mb'] = total_size / (1024 * 1024)
                
                # File type distribution
                cursor.execute("SELECT type, COUNT(*) FROM files WHERE type IS NOT NULL GROUP BY type ORDER BY COUNT(*) DESC")
                stats['file_types'] = dict(cursor.fetchall())
                
                # Game system distribution
                cursor.execute("SELECT game_system, COUNT(*) FROM files WHERE game_system IS NOT NULL GROUP BY game_system ORDER BY COUNT(*) DESC LIMIT 10")
                stats['top_game_systems'] = dict(cursor.fetchall())
                
                return stats
                
        except sqlite3.Error as e:
            logger.error(f"Failed to get statistics: {e}")
            return {}


class ExportDialog(EnterpriseDialog):
    """Dialog for export options"""
    
    def __init__(self, parent, theme_manager, results_count: int):
        self.results_count = results_count
        self.export_format = tk.StringVar(value="csv")
        self.include_headers = tk.BooleanVar(value=True)
        self.export_path = tk.StringVar()
        
        super().__init__(parent, "Export Search Results", theme_manager, modal=True)
    
    def create_content(self):
        """Create export dialog content"""
        main_frame = ttk.Frame(self)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Info label
        info_label = ttk.Label(main_frame, text=f"Export {self.results_count} search results")
        info_label.pack(anchor=tk.W, pady=(0, 10))
        
        # Format selection
        format_frame = ttk.LabelFrame(main_frame, text="Export Format")
        format_frame.pack(fill=tk.X, pady=5)
        
        ttk.Radiobutton(format_frame, text="CSV (Comma Separated Values)", 
                       variable=self.export_format, value="csv").pack(anchor=tk.W, padx=10, pady=5)
        ttk.Radiobutton(format_frame, text="JSON (JavaScript Object Notation)", 
                       variable=self.export_format, value="json").pack(anchor=tk.W, padx=10, pady=5)
        ttk.Radiobutton(format_frame, text="TSV (Tab Separated Values)", 
                       variable=self.export_format, value="tsv").pack(anchor=tk.W, padx=10, pady=5)
        
        # Options
        options_frame = ttk.LabelFrame(main_frame, text="Options")
        options_frame.pack(fill=tk.X, pady=5)
        
        ttk.Checkbutton(options_frame, text="Include column headers", 
                       variable=self.include_headers).pack(anchor=tk.W, padx=10, pady=5)
        
        # File selection
        file_frame = ttk.LabelFrame(main_frame, text="Output File")
        file_frame.pack(fill=tk.X, pady=5)
        
        path_frame = ttk.Frame(file_frame)
        path_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.path_entry = ttk.Entry(path_frame, textvariable=self.export_path, width=50)
        self.path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        browse_btn = ttk.Button(path_frame, text="Browse...", command=self.browse_file)
        browse_btn.pack(side=tk.RIGHT, padx=(5, 0))
        
        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=20, pady=(0, 20))
        
        ttk.Button(btn_frame, text="Cancel", command=self.on_cancel).pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(btn_frame, text="Export", command=self.on_export).pack(side=tk.RIGHT)
        
        # Apply theme
        for widget in [main_frame, info_label, format_frame, options_frame, file_frame, btn_frame]:
            self.theme_manager.apply_theme(widget)
    
    def browse_file(self):
        """Browse for export file"""
        format_ext = self.export_format.get()
        filetypes = {
            'csv': [('CSV files', '*.csv'), ('All files', '*.*')],
            'json': [('JSON files', '*.json'), ('All files', '*.*')],
            'tsv': [('TSV files', '*.tsv'), ('All files', '*.*')]
        }
        
        filename = filedialog.asksaveasfilename(
            title="Export Search Results",
            filetypes=filetypes.get(format_ext, [('All files', '*.*')]),
            defaultextension=f".{format_ext}"
        )
        
        if filename:
            self.export_path.set(filename)
    
    def on_export(self):
        """Handle export button"""
        if not self.export_path.get():
            messagebox.showerror("Error", "Please select an output file")
            return
        
        self.result = {
            'format': self.export_format.get(),
            'path': self.export_path.get(),
            'include_headers': self.include_headers.get()
        }
        self.destroy()


class StatisticsDialog(EnterpriseDialog):
    """Dialog showing database statistics"""
    
    def __init__(self, parent, theme_manager, stats: Dict[str, Any]):
        self.stats = stats
        super().__init__(parent, "Library Statistics", theme_manager, modal=True, resizable=True)
    
    def create_content(self):
        """Create statistics dialog content"""
        main_frame = ttk.Frame(self)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Create notebook for different stat categories
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True)
        
        # General statistics
        general_frame = ttk.Frame(notebook)
        notebook.add(general_frame, text="General")
        
        general_text = f"""Total Files: {self.stats.get('total_files', 0):,}
Total Size: {self.stats.get('total_size_mb', 0):.1f} MB
Average File Size: {(self.stats.get('total_size_mb', 0) * 1024) / max(self.stats.get('total_files', 1), 1):.1f} KB"""
        
        ttk.Label(general_frame, text=general_text, justify=tk.LEFT).pack(anchor=tk.W, padx=10, pady=10)
        
        # File types
        types_frame = ttk.Frame(notebook)
        notebook.add(types_frame, text="File Types")
        
        types_tree = ttk.Treeview(types_frame, columns=("count",), show="tree headings")
        types_tree.heading("#0", text="File Type")
        types_tree.heading("count", text="Count")
        types_tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        for file_type, count in self.stats.get('file_types', {}).items():
            types_tree.insert("", "end", text=file_type, values=(count,))
        
        # Game systems
        systems_frame = ttk.Frame(notebook)
        notebook.add(systems_frame, text="Game Systems")
        
        systems_tree = ttk.Treeview(systems_frame, columns=("count",), show="tree headings")
        systems_tree.heading("#0", text="Game System")
        systems_tree.heading("count", text="Count")
        systems_tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        for system, count in self.stats.get('top_game_systems', {}).items():
            systems_tree.insert("", "end", text=system, values=(count,))
        
        # Close button
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=20, pady=(0, 20))
        
        ttk.Button(btn_frame, text="Close", command=self.on_cancel).pack(side=tk.RIGHT)
        
        # Apply theme
        self.theme_manager.apply_theme(main_frame)
        self.theme_manager.apply_theme(notebook)


class EnterpriseSearchApplication(EnterpriseApplication):
    """Enterprise search GUI application"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("Linux File Librarian - Enterprise Search", ThemeType.LIGHT)
        
        # Initialize components
        self.config = config
        self.library_root = Path(config['library_root']).resolve()
        self.db_path = self.library_root / "library_index.sqlite"
        
        # Validate paths
        self._validate_paths()
        
        # Initialize database
        self.database = SearchDatabase(self.db_path)
        
        # Search state
        self.current_results = []
        self.search_criteria = SearchCriteria()
        
        # Create UI
        self.create_interface()
        
        # Load initial data
        self.load_filter_data()
        
        # Set initial status
        self.set_status("Ready - Enter search criteria and click Search")
    
    def _validate_paths(self):
        """Validate configuration paths"""
        if not self.library_root.exists():
            raise FileNotFoundError(f"Library root not found: {self.library_root}")
        
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
    
    def create_interface(self):
        """Create the main interface"""
        # Main container
        main_container = ttk.PanedWindow(self, orient=tk.VERTICAL)
        main_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Search panel
        search_panel = self.create_search_panel(main_container)
        main_container.add(search_panel, weight=0)
        
        # Results panel
        results_panel = self.create_results_panel(main_container)
        main_container.add(results_panel, weight=1)
        
        # Create menu
        self.create_menu()
    
    def create_menu(self):
        """Create application menu"""
        menubar = tk.Menu(self)
        self.config(menu=menubar)
        
        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Export Results...", command=self.export_results, accelerator="Ctrl+E")
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.on_closing, accelerator="Ctrl+Q")
        
        # View menu
        view_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="View", menu=view_menu)
        view_menu.add_command(label="Statistics...", command=self.show_statistics)
        view_menu.add_command(label="Refresh", command=self.refresh_data, accelerator="F5")
        
        # Theme menu
        theme_menu = tk.Menu(view_menu, tearoff=0)
        view_menu.add_cascade(label="Theme", menu=theme_menu)
        theme_menu.add_command(label="Light", command=lambda: self.change_theme(ThemeType.LIGHT))
        theme_menu.add_command(label="Dark", command=lambda: self.change_theme(ThemeType.DARK))
        theme_menu.add_command(label="High Contrast", command=lambda: self.change_theme(ThemeType.HIGH_CONTRAST))
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About", command=self.show_about)
        
        # Bind keyboard shortcuts
        self.bind_all("<Control-e>", lambda e: self.export_results())
        self.bind_all("<Control-q>", lambda e: self.on_closing())
        self.bind_all("<F5>", lambda e: self.refresh_data())
    
    def create_search_panel(self, parent) -> ttk.Frame:
        """Create search criteria panel"""
        panel = ttk.LabelFrame(parent, text="Search Criteria")
        
        # Filters row 1
        filters_frame1 = ttk.Frame(panel)
        filters_frame1.pack(fill=tk.X, padx=10, pady=5)
        
        # Filename search
        ttk.Label(filters_frame1, text="Filename:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.filename_entry = EnterpriseEntry(filters_frame1, self.theme_manager, 
                                            placeholder="Enter filename to search...", width=30)
        self.filename_entry.widget.grid(row=0, column=1, sticky=tk.EW, padx=(0, 10))
        
        # Game system filter
        ttk.Label(filters_frame1, text="Game System:").grid(row=0, column=2, sticky=tk.W, padx=(0, 5))
        self.game_system_combo = EnterpriseCombobox(filters_frame1, self.theme_manager, width=25)
        self.game_system_combo.widget.grid(row=0, column=3, sticky=tk.EW, padx=(0, 10))
        
        # File type filter
        ttk.Label(filters_frame1, text="File Type:").grid(row=0, column=4, sticky=tk.W, padx=(0, 5))
        self.file_type_combo = EnterpriseCombobox(filters_frame1, self.theme_manager, width=20)
        self.file_type_combo.widget.grid(row=0, column=5, sticky=tk.EW)
        
        # Configure grid weights
        filters_frame1.grid_columnconfigure(1, weight=1)
        filters_frame1.grid_columnconfigure(3, weight=1)
        filters_frame1.grid_columnconfigure(5, weight=1)
        
        # Filters row 2
        filters_frame2 = ttk.Frame(panel)
        filters_frame2.pack(fill=tk.X, padx=10, pady=5)
        
        # Language filter
        ttk.Label(filters_frame2, text="Language:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.language_combo = EnterpriseCombobox(filters_frame2, self.theme_manager, width=15)
        self.language_combo.widget.grid(row=0, column=1, sticky=tk.EW, padx=(0, 10))
        
        # Size filters
        ttk.Label(filters_frame2, text="Size (KB):").grid(row=0, column=2, sticky=tk.W, padx=(0, 5))
        size_frame = ttk.Frame(filters_frame2)
        size_frame.grid(row=0, column=3, sticky=tk.EW, padx=(0, 10))
        
        self.min_size_entry = EnterpriseEntry(size_frame, self.theme_manager, placeholder="Min", width=8)
        self.min_size_entry.widget.pack(side=tk.LEFT, padx=(0, 5))
        ttk.Label(size_frame, text="to").pack(side=tk.LEFT, padx=5)
        self.max_size_entry = EnterpriseEntry(size_frame, self.theme_manager, placeholder="Max", width=8)
        self.max_size_entry.widget.pack(side=tk.LEFT, padx=(5, 0))
        
        # Search buttons
        btn_frame = ttk.Frame(filters_frame2)
        btn_frame.grid(row=0, column=4, sticky=tk.E)
        
        self.search_btn = ttk.Button(btn_frame, text="Search", command=self.perform_search)
        self.search_btn.pack(side=tk.LEFT, padx=5)
        
        self.clear_btn = ttk.Button(btn_frame, text="Clear", command=self.clear_search)
        self.clear_btn.pack(side=tk.LEFT)
        
        # Configure grid weights
        filters_frame2.grid_columnconfigure(1, weight=1)
        filters_frame2.grid_columnconfigure(3, weight=1)
        
        # Bind Enter key to search
        self.filename_entry.widget.bind("<Return>", lambda e: self.perform_search())
        
        return panel
    
    def create_results_panel(self, parent) -> ttk.Frame:
        """Create results display panel"""
        panel = ttk.LabelFrame(parent, text="Search Results")
        
        # Results treeview
        columns = ["filename", "game_system", "type", "size_kb", "language", "path"]
        headings = ["Filename", "Game System", "File Type", "Size (KB)", "Language", "Full Path"]
        
        self.results_tree = EnterpriseTreeview(panel, self.theme_manager, columns, headings)
        self.results_tree.widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Configure column widths
        tree = self.results_tree.tree
        tree.column("filename", width=300, anchor=tk.W)
        tree.column("game_system", width=200, anchor=tk.W)
        tree.column("type", width=100, anchor=tk.W)
        tree.column("size_kb", width=80, anchor=tk.E)
        tree.column("language", width=80, anchor=tk.W)
        tree.column("path", width=400, anchor=tk.W)
        
        # Bind double-click to open file
        tree.bind("<Double-1>", self.open_selected_file)
        tree.bind("<Button-3>", self.show_context_menu)  # Right-click context menu
        
        # Results info frame
        info_frame = ttk.Frame(panel)
        info_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        self.results_info = ttk.Label(info_frame, text="No search performed")
        self.results_info.pack(side=tk.LEFT)
        
        # Export button
        self.export_btn = ttk.Button(info_frame, text="Export Results", 
                                   command=self.export_results, state=tk.DISABLED)
        self.export_btn.pack(side=tk.RIGHT)
        
        return panel
    
    def load_filter_data(self):
        """Load data for filter dropdowns"""
        try:
            self.set_status("Loading filter data...")
            
            def load_data():
                return self.database.get_filter_values()
            
            def on_success(filter_data):
                # Update comboboxes
                self.game_system_combo.update_values(["-- ALL --"] + filter_data.get('game_systems', []))
                self.game_system_combo.set_value("-- ALL --")
                
                self.file_type_combo.update_values(["-- ALL --"] + filter_data.get('file_types', []))
                self.file_type_combo.set_value("-- ALL --")
                
                self.language_combo.update_values(["-- ALL --"] + filter_data.get('languages', []))
                self.language_combo.set_value("-- ALL --")
                
                self.set_status("Filter data loaded successfully")
            
            def on_error(error):
                self.show_error(f"Failed to load filter data: {error}")
                self.set_status("Error loading filter data")
            
            self.run_async_task(load_data, on_success, on_error)
            
        except Exception as e:
            self.show_error(f"Failed to initialize filters: {e}")
    
    def perform_search(self):
        """Perform search based on current criteria"""
        try:
            # Build search criteria
            criteria = SearchCriteria(
                filename_query=self.filename_entry.get_value(),
                game_system=self.game_system_combo.get_value(),
                file_type=self.file_type_combo.get_value(),
                language=self.language_combo.get_value()
            )
            
            # Parse size filters
            try:
                min_size = self.min_size_entry.get_value()
                criteria.min_size_kb = int(min_size) if min_size else 0
            except ValueError:
                criteria.min_size_kb = 0
            
            try:
                max_size = self.max_size_entry.get_value()
                criteria.max_size_kb = int(max_size) if max_size else 0
            except ValueError:
                criteria.max_size_kb = 0
            
            self.search_criteria = criteria
            
            # Disable search button and show progress
            self.search_btn.configure(state=tk.DISABLED)
            self.set_status("Searching...")
            
            def search_task():
                return self.database.search_files(criteria)
            
            def on_success(results):
                self.current_results = results
                self.display_results(results)
                self.search_btn.configure(state=tk.NORMAL)
                
                # Update status and export button
                count = len(results)
                self.set_status(f"Found {count} result{'s' if count != 1 else ''}")
                self.export_btn.configure(state=tk.NORMAL if count > 0 else tk.DISABLED)
            
            def on_error(error):
                self.show_error(f"Search failed: {error}")
                self.search_btn.configure(state=tk.NORMAL)
                self.set_status("Search failed")
            
            self.run_async_task(search_task, on_success, on_error)
            
        except Exception as e:
            self.show_error(f"Search error: {e}")
            self.search_btn.configure(state=tk.NORMAL)
    
    def display_results(self, results: List[SearchResult]):
        """Display search results in treeview"""
        # Clear existing results
        self.results_tree.clear()
        
        # Add new results
        for result in results:
            size_kb = f"{result.size_bytes / 1024:.1f}" if result.size_bytes else "0.0"
            
            self.results_tree.insert_row([
                result.filename,
                result.game_system,
                result.file_type,
                size_kb,
                result.language,
                result.path
            ])
        
        # Update info label
        count = len(results)
        self.results_info.configure(text=f"Showing {count} result{'s' if count != 1 else ''}")
    
    def clear_search(self):
        """Clear search criteria and results"""
        # Clear entry fields
        self.filename_entry.set_value("")
        self.min_size_entry.set_value("")
        self.max_size_entry.set_value("")
        
        # Reset comboboxes
        self.game_system_combo.set_value("-- ALL --")
        self.file_type_combo.set_value("-- ALL --")
        self.language_combo.set_value("-- ALL --")
        
        # Clear results
        self.results_tree.clear()
        self.current_results = []
        
        # Update UI
        self.results_info.configure(text="No search performed")
        self.export_btn.configure(state=tk.DISABLED)
        self.set_status("Search criteria cleared")
    
    def open_selected_file(self, event=None):
        """Open selected file with system default application"""
        selected_values = self.results_tree.get_selected_values()
        if not selected_values:
            return
        
        file_path = selected_values[5]  # Path is the 6th column
        
        try:
            if not Path(file_path).exists():
                self.show_error(f"File not found: {file_path}")
                return
            
            # Use platform-specific commands
            if sys.platform == "win32":
                os.startfile(file_path)
            elif sys.platform == "darwin":  # macOS
                run(["/usr/bin/open", file_path], check=True)
            else:  # Linux and other UNIX
                run(["/usr/bin/xdg-open", file_path], check=True)
                
            self.set_status(f"Opened: {Path(file_path).name}")
            
        except CalledProcessError as e:
            self.show_error(f"Failed to open file: {e}")
        except Exception as e:
            self.show_error(f"Error opening file: {e}")
    
    def show_context_menu(self, event):
        """Show context menu for results"""
        # Select item under cursor
        item = self.results_tree.tree.identify_row(event.y)
        if item:
            self.results_tree.tree.selection_set(item)
            
            # Create context menu
            context_menu = tk.Menu(self, tearoff=0)
            context_menu.add_command(label="Open File", command=self.open_selected_file)
            context_menu.add_command(label="Copy Path", command=self.copy_file_path)
            context_menu.add_command(label="Show in Folder", command=self.show_in_folder)
            
            # Show menu
            try:
                context_menu.tk_popup(event.x_root, event.y_root)
            finally:
                context_menu.grab_release()
    
    def copy_file_path(self):
        """Copy selected file path to clipboard"""
        selected_values = self.results_tree.get_selected_values()
        if selected_values:
            file_path = selected_values[5]
            self.clipboard_clear()
            self.clipboard_append(file_path)
            self.set_status("File path copied to clipboard")
    
    def show_in_folder(self):
        """Show selected file in file manager"""
        selected_values = self.results_tree.get_selected_values()
        if not selected_values:
            return
        
        file_path = Path(selected_values[5])
        folder_path = file_path.parent
        
        try:
            if sys.platform == "win32":
                os.startfile(str(folder_path))
            elif sys.platform == "darwin":  # macOS
                run(["/usr/bin/open", str(folder_path)], check=True)
            else:  # Linux
                run(["/usr/bin/xdg-open", str(folder_path)], check=True)
                
            self.set_status(f"Opened folder: {folder_path}")
            
        except Exception as e:
            self.show_error(f"Failed to open folder: {e}")
    
    def export_results(self):
        """Export search results"""
        if not self.current_results:
            self.show_warning("No results to export")
            return
        
        # Show export dialog
        dialog = ExportDialog(self, self.theme_manager, len(self.current_results))
        export_options = dialog.show()
        
        if not export_options:
            return
        
        try:
            self.set_status("Exporting results...")
            
            def export_task():
                self._export_results_to_file(export_options)
                return export_options['path']
            
            def on_success(file_path):
                self.set_status(f"Results exported to: {Path(file_path).name}")
                self.show_info(f"Results successfully exported to:\n{file_path}")
            
            def on_error(error):
                self.show_error(f"Export failed: {error}")
                self.set_status("Export failed")
            
            self.run_async_task(export_task, on_success, on_error)
            
        except Exception as e:
            self.show_error(f"Export error: {e}")
    
    def _export_results_to_file(self, options: Dict[str, Any]):
        """Export results to file"""
        file_path = Path(options['path'])
        format_type = options['format']
        include_headers = options['include_headers']
        
        if format_type == 'csv':
            self._export_csv(file_path, include_headers)
        elif format_type == 'json':
            self._export_json(file_path)
        elif format_type == 'tsv':
            self._export_tsv(file_path, include_headers)
    
    def _export_csv(self, file_path: Path, include_headers: bool):
        """Export results as CSV"""
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            if include_headers:
                writer.writerow(['Filename', 'Game System', 'File Type', 'Size (KB)', 'Language', 'Path'])
            
            for result in self.current_results:
                size_kb = f"{result.size_bytes / 1024:.1f}" if result.size_bytes else "0.0"
                writer.writerow([
                    result.filename, result.game_system, result.file_type,
                    size_kb, result.language, result.path
                ])
    
    def _export_json(self, file_path: Path):
        """Export results as JSON"""
        data = []
        for result in self.current_results:
            data.append({
                'filename': result.filename,
                'game_system': result.game_system,
                'file_type': result.file_type,
                'size_bytes': result.size_bytes,
                'size_kb': result.size_bytes / 1024 if result.size_bytes else 0,
                'language': result.language,
                'path': result.path
            })
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def _export_tsv(self, file_path: Path, include_headers: bool):
        """Export results as TSV"""
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')
            
            if include_headers:
                writer.writerow(['Filename', 'Game System', 'File Type', 'Size (KB)', 'Language', 'Path'])
            
            for result in self.current_results:
                size_kb = f"{result.size_bytes / 1024:.1f}" if result.size_bytes else "0.0"
                writer.writerow([
                    result.filename, result.game_system, result.file_type,
                    size_kb, result.language, result.path
                ])
    
    def show_statistics(self):
        """Show database statistics"""
        try:
            self.set_status("Loading statistics...")
            
            def load_stats():
                return self.database.get_statistics()
            
            def on_success(stats):
                dialog = StatisticsDialog(self, self.theme_manager, stats)
                dialog.show()
                self.set_status("Statistics displayed")
            
            def on_error(error):
                self.show_error(f"Failed to load statistics: {error}")
                self.set_status("Statistics load failed")
            
            self.run_async_task(load_stats, on_success, on_error)
            
        except Exception as e:
            self.show_error(f"Statistics error: {e}")
    
    def refresh_data(self):
        """Refresh filter data and current search"""
        self.load_filter_data()
        if self.current_results:
            self.perform_search()
    
    def change_theme(self, theme_type: ThemeType):
        """Change application theme"""
        self.theme_manager.set_theme(theme_type)
        # In a full implementation, you would recursively apply the theme to all widgets
        self.set_status(f"Theme changed to {theme_type.value}")
    
    def show_about(self):
        """Show about dialog"""
        about_text = """Linux File Librarian - Enterprise Search

Version: 2.0 Enterprise Edition

A robust, enterprise-grade file management and search system
designed for system administrators and organizations.

Features:
• Advanced search capabilities
• Multiple export formats
• Theme support
• Performance monitoring
• Accessibility features

© 2024 Linux File Librarian Project"""
        
        self.show_info(about_text, "About Linux File Librarian")


def main():
    """Main entry point"""
    try:
        # Load configuration
        config = load_config()
        
        # Validate configuration
        if not config.get('library_root') or "/path/to/" in config['library_root']:
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror(
                "Configuration Error",
                "Please configure 'library_root' in conf/config.ini before running the search GUI."
            )
            sys.exit(1)
        
        # Create and run application
        app = EnterpriseSearchApplication(config)
        app.mainloop()
        
    except FileNotFoundError as e:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Configuration Error", f"Configuration file not found: {e}")
        sys.exit(1)
    except Exception as e:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Fatal Error", f"Application startup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()