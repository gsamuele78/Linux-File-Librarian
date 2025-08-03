#!/usr/bin/env python3
"""
Enterprise GUI Configuration Manager

Provides comprehensive GUI configuration management with validation,
themes, accessibility settings, and user preferences.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum
import tkinter as tk
from tkinter import ttk, messagebox, colorchooser, font

logger = logging.getLogger(__name__)


class AccessibilityLevel(Enum):
    """Accessibility levels"""
    STANDARD = "standard"
    ENHANCED = "enhanced"
    HIGH_CONTRAST = "high_contrast"
    SCREEN_READER = "screen_reader"


@dataclass
class GUIPreferences:
    """GUI user preferences"""
    theme: str = "light"
    font_family: str = "Segoe UI"
    font_size: int = 10
    window_width: int = 1200
    window_height: int = 800
    window_maximized: bool = False
    show_tooltips: bool = True
    show_status_bar: bool = True
    auto_refresh: bool = True
    refresh_interval: int = 300
    accessibility_level: str = "standard"
    high_contrast_mode: bool = False
    large_fonts: bool = False
    keyboard_navigation: bool = True
    screen_reader_support: bool = False
    export_format: str = "csv"
    max_search_results: int = 1000
    search_history_size: int = 50
    recent_searches: List[str] = None
    
    def __post_init__(self):
        if self.recent_searches is None:
            self.recent_searches = []


@dataclass
class ThemeColors:
    """Theme color configuration"""
    background: str = "#ffffff"
    foreground: str = "#000000"
    select_background: str = "#0078d4"
    select_foreground: str = "#ffffff"
    button_background: str = "#f0f0f0"
    button_foreground: str = "#000000"
    entry_background: str = "#ffffff"
    entry_foreground: str = "#000000"
    error_color: str = "#d13438"
    success_color: str = "#107c10"
    warning_color: str = "#ff8c00"
    info_color: str = "#0078d4"
    border_color: str = "#cccccc"
    disabled_color: str = "#999999"


class EnterpriseGUIConfigManager:
    """Enterprise GUI configuration management"""
    
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.config_dir = self.project_root / "conf"
        self.gui_config_file = self.config_dir / "gui_config.json"
        self.themes_dir = self.config_dir / "themes"
        
        # Ensure directories exist
        self.config_dir.mkdir(exist_ok=True)
        self.themes_dir.mkdir(exist_ok=True)
        
        # Load configuration
        self.preferences = self.load_preferences()
        self.themes = self.load_themes()
    
    def load_preferences(self) -> GUIPreferences:
        """Load GUI preferences from file"""
        try:
            if self.gui_config_file.exists():
                with open(self.gui_config_file, 'r') as f:
                    data = json.load(f)
                    return GUIPreferences(**data.get('preferences', {}))
            else:
                # Create default configuration
                prefs = GUIPreferences()
                self.save_preferences(prefs)
                return prefs
        except Exception as e:
            logger.error(f"Failed to load GUI preferences: {e}")
            return GUIPreferences()
    
    def save_preferences(self, preferences: GUIPreferences):
        """Save GUI preferences to file"""
        try:
            config_data = {
                'preferences': asdict(preferences),
                'version': '2.0',
                'last_updated': str(Path().cwd())  # Simple timestamp
            }
            
            with open(self.gui_config_file, 'w') as f:
                json.dump(config_data, f, indent=2)
            
            self.preferences = preferences
            logger.info("GUI preferences saved successfully")
            
        except Exception as e:
            logger.error(f"Failed to save GUI preferences: {e}")
            raise
    
    def load_themes(self) -> Dict[str, ThemeColors]:
        """Load available themes"""
        themes = {
            'light': ThemeColors(),
            'dark': ThemeColors(
                background="#2d2d30",
                foreground="#ffffff",
                select_background="#0e639c",
                select_foreground="#ffffff",
                button_background="#3e3e42",
                button_foreground="#ffffff",
                entry_background="#1e1e1e",
                entry_foreground="#ffffff",
                border_color="#555555",
                disabled_color="#666666"
            ),
            'high_contrast': ThemeColors(
                background="#000000",
                foreground="#ffffff",
                select_background="#ffffff",
                select_foreground="#000000",
                button_background="#ffffff",
                button_foreground="#000000",
                entry_background="#000000",
                entry_foreground="#ffffff",
                error_color="#ff0000",
                success_color="#00ff00",
                warning_color="#ffff00",
                info_color="#00ffff",
                border_color="#ffffff",
                disabled_color="#808080"
            )
        }
        
        # Load custom themes from files
        for theme_file in self.themes_dir.glob("*.json"):
            try:
                with open(theme_file, 'r') as f:
                    theme_data = json.load(f)
                    theme_name = theme_file.stem
                    themes[theme_name] = ThemeColors(**theme_data)
            except Exception as e:
                logger.warning(f"Failed to load theme {theme_file}: {e}")
        
        return themes
    
    def save_custom_theme(self, name: str, colors: ThemeColors):
        """Save custom theme to file"""
        try:
            theme_file = self.themes_dir / f"{name}.json"
            with open(theme_file, 'w') as f:
                json.dump(asdict(colors), f, indent=2)
            
            self.themes[name] = colors
            logger.info(f"Custom theme '{name}' saved successfully")
            
        except Exception as e:
            logger.error(f"Failed to save custom theme '{name}': {e}")
            raise
    
    def get_theme_colors(self, theme_name: str = None) -> ThemeColors:
        """Get theme colors"""
        if not theme_name:
            theme_name = self.preferences.theme
        
        return self.themes.get(theme_name, self.themes['light'])
    
    def apply_accessibility_settings(self, widget: tk.Widget):
        """Apply accessibility settings to widget"""
        try:
            if self.preferences.large_fonts:
                current_font = widget.cget('font')
                if current_font:
                    font_obj = font.nametofont(current_font)
                    font_obj.configure(size=max(12, font_obj['size'] + 2))
            
            if self.preferences.high_contrast_mode:
                theme_colors = self.get_theme_colors('high_contrast')
                self._apply_colors_to_widget(widget, theme_colors)
            
            if self.preferences.keyboard_navigation:
                # Enable keyboard navigation
                widget.configure(takefocus=True)
        
        except tk.TclError:
            # Some widgets don't support all options
            pass
    
    def _apply_colors_to_widget(self, widget: tk.Widget, colors: ThemeColors):
        """Apply theme colors to widget"""
        try:
            widget_class = widget.winfo_class()
            
            if widget_class in ['Tk', 'Toplevel', 'Frame']:
                widget.configure(bg=colors.background)
            elif widget_class in ['Label']:
                widget.configure(bg=colors.background, fg=colors.foreground)
            elif widget_class in ['Button']:
                widget.configure(bg=colors.button_background, fg=colors.button_foreground)
            elif widget_class in ['Entry', 'Text']:
                widget.configure(bg=colors.entry_background, fg=colors.entry_foreground)
            elif widget_class in ['Listbox', 'Treeview']:
                widget.configure(bg=colors.background, fg=colors.foreground,
                               selectbackground=colors.select_background,
                               selectforeground=colors.select_foreground)
        
        except tk.TclError:
            pass
    
    def add_recent_search(self, search_query: str):
        """Add search to recent searches"""
        if search_query and search_query not in self.preferences.recent_searches:
            self.preferences.recent_searches.insert(0, search_query)
            
            # Limit history size
            if len(self.preferences.recent_searches) > self.preferences.search_history_size:
                self.preferences.recent_searches = self.preferences.recent_searches[:self.preferences.search_history_size]
            
            self.save_preferences(self.preferences)
    
    def get_recent_searches(self) -> List[str]:
        """Get recent search queries"""
        return self.preferences.recent_searches.copy()
    
    def clear_recent_searches(self):
        """Clear recent search history"""
        self.preferences.recent_searches.clear()
        self.save_preferences(self.preferences)
    
    def export_configuration(self, file_path: Path):
        """Export configuration to file"""
        try:
            config_data = {
                'preferences': asdict(self.preferences),
                'themes': {name: asdict(colors) for name, colors in self.themes.items()},
                'version': '2.0',
                'export_timestamp': str(Path().cwd())
            }
            
            with open(file_path, 'w') as f:
                json.dump(config_data, f, indent=2)
            
            logger.info(f"Configuration exported to {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to export configuration: {e}")
            raise
    
    def import_configuration(self, file_path: Path):
        """Import configuration from file"""
        try:
            with open(file_path, 'r') as f:
                config_data = json.load(f)
            
            # Import preferences
            if 'preferences' in config_data:
                self.preferences = GUIPreferences(**config_data['preferences'])
                self.save_preferences(self.preferences)
            
            # Import themes
            if 'themes' in config_data:
                for theme_name, theme_data in config_data['themes'].items():
                    if theme_name not in ['light', 'dark', 'high_contrast']:  # Don't overwrite built-in themes
                        self.save_custom_theme(theme_name, ThemeColors(**theme_data))
            
            logger.info(f"Configuration imported from {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to import configuration: {e}")
            raise


class GUIConfigurationDialog(tk.Toplevel):
    """GUI configuration dialog"""
    
    def __init__(self, parent: tk.Widget, config_manager: EnterpriseGUIConfigManager):
        super().__init__(parent)
        
        self.parent = parent
        self.config_manager = config_manager
        self.preferences = GUIPreferences(**asdict(config_manager.preferences))
        
        self.title("GUI Configuration")
        self.geometry("600x500")
        self.resizable(True, True)
        self.transient(parent)
        self.grab_set()
        
        # Center dialog
        self.update_idletasks()
        x = (self.winfo_screenwidth() - self.winfo_width()) // 2
        y = (self.winfo_screenheight() - self.winfo_height()) // 2
        self.geometry(f"+{x}+{y}")
        
        self.create_interface()
        
        # Handle close event
        self.protocol("WM_DELETE_WINDOW", self.on_cancel)
    
    def create_interface(self):
        """Create configuration interface"""
        # Create notebook for different categories
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Appearance tab
        self.create_appearance_tab(notebook)
        
        # Accessibility tab
        self.create_accessibility_tab(notebook)
        
        # Behavior tab
        self.create_behavior_tab(notebook)
        
        # Advanced tab
        self.create_advanced_tab(notebook)
        
        # Button frame
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        ttk.Button(btn_frame, text="Cancel", command=self.on_cancel).pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(btn_frame, text="Apply", command=self.on_apply).pack(side=tk.RIGHT)
        ttk.Button(btn_frame, text="OK", command=self.on_ok).pack(side=tk.RIGHT, padx=(0, 5))
    
    def create_appearance_tab(self, notebook: ttk.Notebook):
        """Create appearance configuration tab"""
        frame = ttk.Frame(notebook)
        notebook.add(frame, text="Appearance")
        
        # Theme selection
        theme_frame = ttk.LabelFrame(frame, text="Theme")
        theme_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.theme_var = tk.StringVar(value=self.preferences.theme)
        theme_combo = ttk.Combobox(theme_frame, textvariable=self.theme_var,
                                 values=list(self.config_manager.themes.keys()),
                                 state="readonly")
        theme_combo.pack(fill=tk.X, padx=10, pady=10)
        
        # Font settings
        font_frame = ttk.LabelFrame(frame, text="Font")
        font_frame.pack(fill=tk.X, padx=10, pady=5)
        
        font_inner = ttk.Frame(font_frame)
        font_inner.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Label(font_inner, text="Family:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.font_family_var = tk.StringVar(value=self.preferences.font_family)
        font_family_combo = ttk.Combobox(font_inner, textvariable=self.font_family_var,
                                       values=sorted(font.families()), width=20)
        font_family_combo.grid(row=0, column=1, sticky=tk.EW, padx=(0, 10))
        
        ttk.Label(font_inner, text="Size:").grid(row=0, column=2, sticky=tk.W, padx=(0, 5))
        self.font_size_var = tk.IntVar(value=self.preferences.font_size)
        font_size_spin = ttk.Spinbox(font_inner, from_=8, to=24, textvariable=self.font_size_var, width=5)
        font_size_spin.grid(row=0, column=3, sticky=tk.W)
        
        font_inner.grid_columnconfigure(1, weight=1)
        
        # Window settings
        window_frame = ttk.LabelFrame(frame, text="Window")
        window_frame.pack(fill=tk.X, padx=10, pady=5)
        
        window_inner = ttk.Frame(window_frame)
        window_inner.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Label(window_inner, text="Width:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.window_width_var = tk.IntVar(value=self.preferences.window_width)
        width_spin = ttk.Spinbox(window_inner, from_=800, to=2000, textvariable=self.window_width_var, width=8)
        width_spin.grid(row=0, column=1, sticky=tk.W, padx=(0, 10))
        
        ttk.Label(window_inner, text="Height:").grid(row=0, column=2, sticky=tk.W, padx=(0, 5))
        self.window_height_var = tk.IntVar(value=self.preferences.window_height)
        height_spin = ttk.Spinbox(window_inner, from_=600, to=1500, textvariable=self.window_height_var, width=8)
        height_spin.grid(row=0, column=3, sticky=tk.W)
        
        self.maximized_var = tk.BooleanVar(value=self.preferences.window_maximized)
        ttk.Checkbutton(window_inner, text="Start maximized", variable=self.maximized_var).grid(row=1, column=0, columnspan=4, sticky=tk.W, pady=(5, 0))
    
    def create_accessibility_tab(self, notebook: ttk.Notebook):
        """Create accessibility configuration tab"""
        frame = ttk.Frame(notebook)
        notebook.add(frame, text="Accessibility")
        
        # Accessibility level
        level_frame = ttk.LabelFrame(frame, text="Accessibility Level")
        level_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.accessibility_var = tk.StringVar(value=self.preferences.accessibility_level)
        levels = [("Standard", "standard"), ("Enhanced", "enhanced"), 
                 ("High Contrast", "high_contrast"), ("Screen Reader", "screen_reader")]
        
        for i, (text, value) in enumerate(levels):
            ttk.Radiobutton(level_frame, text=text, variable=self.accessibility_var, 
                          value=value).pack(anchor=tk.W, padx=10, pady=2)
        
        # Visual options
        visual_frame = ttk.LabelFrame(frame, text="Visual Options")
        visual_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.high_contrast_var = tk.BooleanVar(value=self.preferences.high_contrast_mode)
        ttk.Checkbutton(visual_frame, text="High contrast mode", 
                       variable=self.high_contrast_var).pack(anchor=tk.W, padx=10, pady=5)
        
        self.large_fonts_var = tk.BooleanVar(value=self.preferences.large_fonts)
        ttk.Checkbutton(visual_frame, text="Large fonts", 
                       variable=self.large_fonts_var).pack(anchor=tk.W, padx=10, pady=5)
        
        # Navigation options
        nav_frame = ttk.LabelFrame(frame, text="Navigation Options")
        nav_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.keyboard_nav_var = tk.BooleanVar(value=self.preferences.keyboard_navigation)
        ttk.Checkbutton(nav_frame, text="Enhanced keyboard navigation", 
                       variable=self.keyboard_nav_var).pack(anchor=tk.W, padx=10, pady=5)
        
        self.screen_reader_var = tk.BooleanVar(value=self.preferences.screen_reader_support)
        ttk.Checkbutton(nav_frame, text="Screen reader support", 
                       variable=self.screen_reader_var).pack(anchor=tk.W, padx=10, pady=5)
    
    def create_behavior_tab(self, notebook: ttk.Notebook):
        """Create behavior configuration tab"""
        frame = ttk.Frame(notebook)
        notebook.add(frame, text="Behavior")
        
        # Interface options
        interface_frame = ttk.LabelFrame(frame, text="Interface")
        interface_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.tooltips_var = tk.BooleanVar(value=self.preferences.show_tooltips)
        ttk.Checkbutton(interface_frame, text="Show tooltips", 
                       variable=self.tooltips_var).pack(anchor=tk.W, padx=10, pady=5)
        
        self.status_bar_var = tk.BooleanVar(value=self.preferences.show_status_bar)
        ttk.Checkbutton(interface_frame, text="Show status bar", 
                       variable=self.status_bar_var).pack(anchor=tk.W, padx=10, pady=5)
        
        # Auto-refresh options
        refresh_frame = ttk.LabelFrame(frame, text="Auto-refresh")
        refresh_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.auto_refresh_var = tk.BooleanVar(value=self.preferences.auto_refresh)
        ttk.Checkbutton(refresh_frame, text="Enable auto-refresh", 
                       variable=self.auto_refresh_var).pack(anchor=tk.W, padx=10, pady=5)
        
        refresh_inner = ttk.Frame(refresh_frame)
        refresh_inner.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(refresh_inner, text="Interval (seconds):").pack(side=tk.LEFT)
        self.refresh_interval_var = tk.IntVar(value=self.preferences.refresh_interval)
        ttk.Spinbox(refresh_inner, from_=30, to=3600, textvariable=self.refresh_interval_var, 
                   width=8).pack(side=tk.LEFT, padx=(5, 0))
        
        # Export options
        export_frame = ttk.LabelFrame(frame, text="Default Export Format")
        export_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.export_format_var = tk.StringVar(value=self.preferences.export_format)
        formats = [("CSV", "csv"), ("JSON", "json"), ("TSV", "tsv")]
        
        for text, value in formats:
            ttk.Radiobutton(export_frame, text=text, variable=self.export_format_var, 
                          value=value).pack(anchor=tk.W, padx=10, pady=2)
    
    def create_advanced_tab(self, notebook: ttk.Notebook):
        """Create advanced configuration tab"""
        frame = ttk.Frame(notebook)
        notebook.add(frame, text="Advanced")
        
        # Search options
        search_frame = ttk.LabelFrame(frame, text="Search Options")
        search_frame.pack(fill=tk.X, padx=10, pady=5)
        
        search_inner = ttk.Frame(search_frame)
        search_inner.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Label(search_inner, text="Max results:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.max_results_var = tk.IntVar(value=self.preferences.max_search_results)
        ttk.Spinbox(search_inner, from_=100, to=10000, textvariable=self.max_results_var, 
                   width=8).grid(row=0, column=1, sticky=tk.W)
        
        ttk.Label(search_inner, text="History size:").grid(row=1, column=0, sticky=tk.W, padx=(0, 5), pady=(5, 0))
        self.history_size_var = tk.IntVar(value=self.preferences.search_history_size)
        ttk.Spinbox(search_inner, from_=10, to=200, textvariable=self.history_size_var, 
                   width=8).grid(row=1, column=1, sticky=tk.W, pady=(5, 0))
        
        # Configuration management
        config_frame = ttk.LabelFrame(frame, text="Configuration Management")
        config_frame.pack(fill=tk.X, padx=10, pady=5)
        
        config_inner = ttk.Frame(config_frame)
        config_inner.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Button(config_inner, text="Export Configuration...", 
                  command=self.export_config).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(config_inner, text="Import Configuration...", 
                  command=self.import_config).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(config_inner, text="Reset to Defaults", 
                  command=self.reset_defaults).pack(side=tk.LEFT)
        
        # Clear data
        data_frame = ttk.LabelFrame(frame, text="Data Management")
        data_frame.pack(fill=tk.X, padx=10, pady=5)
        
        data_inner = ttk.Frame(data_frame)
        data_inner.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Button(data_inner, text="Clear Search History", 
                  command=self.clear_search_history).pack(side=tk.LEFT)
    
    def export_config(self):
        """Export configuration to file"""
        from tkinter import filedialog
        
        filename = filedialog.asksaveasfilename(
            title="Export Configuration",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            defaultextension=".json"
        )
        
        if filename:
            try:
                self.config_manager.export_configuration(Path(filename))
                messagebox.showinfo("Success", f"Configuration exported to {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export configuration: {e}")
    
    def import_config(self):
        """Import configuration from file"""
        from tkinter import filedialog
        
        filename = filedialog.askopenfilename(
            title="Import Configuration",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                self.config_manager.import_configuration(Path(filename))
                messagebox.showinfo("Success", "Configuration imported successfully. Please restart the application.")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to import configuration: {e}")
    
    def reset_defaults(self):
        """Reset configuration to defaults"""
        if messagebox.askyesno("Confirm Reset", "Reset all settings to defaults?"):
            self.preferences = GUIPreferences()
            self.update_interface()
    
    def clear_search_history(self):
        """Clear search history"""
        if messagebox.askyesno("Confirm Clear", "Clear all search history?"):
            self.config_manager.clear_recent_searches()
            messagebox.showinfo("Success", "Search history cleared")
    
    def update_interface(self):
        """Update interface with current preferences"""
        self.theme_var.set(self.preferences.theme)
        self.font_family_var.set(self.preferences.font_family)
        self.font_size_var.set(self.preferences.font_size)
        self.window_width_var.set(self.preferences.window_width)
        self.window_height_var.set(self.preferences.window_height)
        self.maximized_var.set(self.preferences.window_maximized)
        self.accessibility_var.set(self.preferences.accessibility_level)
        self.high_contrast_var.set(self.preferences.high_contrast_mode)
        self.large_fonts_var.set(self.preferences.large_fonts)
        self.keyboard_nav_var.set(self.preferences.keyboard_navigation)
        self.screen_reader_var.set(self.preferences.screen_reader_support)
        self.tooltips_var.set(self.preferences.show_tooltips)
        self.status_bar_var.set(self.preferences.show_status_bar)
        self.auto_refresh_var.set(self.preferences.auto_refresh)
        self.refresh_interval_var.set(self.preferences.refresh_interval)
        self.export_format_var.set(self.preferences.export_format)
        self.max_results_var.set(self.preferences.max_search_results)
        self.history_size_var.set(self.preferences.search_history_size)
    
    def apply_changes(self):
        """Apply changes to preferences"""
        self.preferences.theme = self.theme_var.get()
        self.preferences.font_family = self.font_family_var.get()
        self.preferences.font_size = self.font_size_var.get()
        self.preferences.window_width = self.window_width_var.get()
        self.preferences.window_height = self.window_height_var.get()
        self.preferences.window_maximized = self.maximized_var.get()
        self.preferences.accessibility_level = self.accessibility_var.get()
        self.preferences.high_contrast_mode = self.high_contrast_var.get()
        self.preferences.large_fonts = self.large_fonts_var.get()
        self.preferences.keyboard_navigation = self.keyboard_nav_var.get()
        self.preferences.screen_reader_support = self.screen_reader_var.get()
        self.preferences.show_tooltips = self.tooltips_var.get()
        self.preferences.show_status_bar = self.status_bar_var.get()
        self.preferences.auto_refresh = self.auto_refresh_var.get()
        self.preferences.refresh_interval = self.refresh_interval_var.get()
        self.preferences.export_format = self.export_format_var.get()
        self.preferences.max_search_results = self.max_results_var.get()
        self.preferences.search_history_size = self.history_size_var.get()
        
        self.config_manager.save_preferences(self.preferences)
    
    def on_ok(self):
        """Handle OK button"""
        self.apply_changes()
        self.destroy()
    
    def on_apply(self):
        """Handle Apply button"""
        self.apply_changes()
        messagebox.showinfo("Applied", "Settings applied successfully")
    
    def on_cancel(self):
        """Handle Cancel button"""
        self.destroy()


def main():
    """Demo configuration dialog"""
    root = tk.Tk()
    root.withdraw()
    
    config_manager = EnterpriseGUIConfigManager(Path.cwd())
    dialog = GUIConfigurationDialog(root, config_manager)
    
    root.wait_window(dialog)
    root.destroy()


if __name__ == '__main__':
    main()