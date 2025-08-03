# Enterprise GUI System Guide

## Overview

The Linux File Librarian now includes a comprehensive enterprise-grade GUI system with modern architecture, advanced features, and accessibility support designed for professional environments.

## Enterprise GUI Features

### 🎨 **Modern GUI Framework**
- **Component-Based Architecture**: Modular, reusable GUI components
- **Theme System**: Light, dark, and high-contrast themes with custom theme support
- **Accessibility Features**: Screen reader support, keyboard navigation, high contrast mode
- **Responsive Design**: Adaptive layouts that work across different screen sizes
- **Async Task Management**: Non-blocking operations with progress indicators

### 🔍 **Advanced Search Interface**
- **Multi-Criteria Search**: Filename, game system, file type, language, and size filters
- **Real-Time Results**: Instant search results with sorting and filtering
- **Export Capabilities**: CSV, JSON, and TSV export formats
- **Context Menus**: Right-click actions for file operations
- **Search History**: Persistent search history with quick access
- **Statistics Dashboard**: Comprehensive library statistics and analytics

### ⚙️ **Configuration Management**
- **User Preferences**: Comprehensive preference system with persistence
- **Theme Customization**: Custom theme creation and management
- **Accessibility Settings**: Configurable accessibility options
- **Import/Export**: Configuration backup and restore capabilities
- **Performance Tuning**: Configurable performance parameters

## Installation and Setup

### Prerequisites

The enterprise GUI system requires the following components:

```bash
# System packages (automatically installed by enterprise installer)
sudo apt-get install python3-tk python3-dev

# Python packages (included in requirements_enterprise.txt)
pip install tkinter  # Usually built-in
```

### Installation Methods

#### Method 1: Enterprise Installation (Recommended)

```bash
# Run enterprise installation
./scripts/enterprise_install.sh

# Launch enterprise search GUI
./scripts/run_enterprise_search.sh
```

#### Method 2: Manual Setup

```bash
# Install dependencies
pip install -r requirements_enterprise.txt

# Run search GUI directly
python3 src/enterprise_search_gui.py
```

## GUI Components

### Enterprise Framework Components

#### EnterpriseApplication
Base application class with enterprise features:

```python
from src.enterprise_gui_framework import EnterpriseApplication, ThemeType

class MyApp(EnterpriseApplication):
    def __init__(self):
        super().__init__("My Enterprise App", ThemeType.LIGHT)
        self.create_interface()
    
    def create_interface(self):
        # Create your interface here
        pass

app = MyApp()
app.mainloop()
```

#### EnterpriseWidget Components

**EnterpriseEntry**: Advanced text entry with validation
```python
entry = EnterpriseEntry(parent, theme_manager, placeholder="Enter text...")
entry.add_validation(lambda x: len(x) > 0, "Field cannot be empty")
entry.widget.pack()
```

**EnterpriseCombobox**: Enhanced combobox with dynamic values
```python
combo = EnterpriseCombobox(parent, theme_manager, values=["Option 1", "Option 2"])
combo.update_values(new_values)
combo.widget.pack()
```

**EnterpriseTreeview**: Advanced treeview with sorting and context menus
```python
tree = EnterpriseTreeview(parent, theme_manager, 
                         columns=["col1", "col2"], 
                         headings=["Column 1", "Column 2"])
tree.insert_row(["Value 1", "Value 2"])
tree.widget.pack()
```

### Search GUI Features

#### Advanced Search Capabilities

**Multi-Criteria Filtering**:
- Filename search with wildcards
- Game system/category filtering
- File type filtering
- Language filtering
- File size range filtering

**Search Interface**:
```python
# Search criteria example
criteria = SearchCriteria(
    filename_query="*.pdf",
    game_system="D&D 5e",
    file_type="PDF",
    min_size_kb=100,
    max_size_kb=10000
)

results = database.search_files(criteria)
```

#### Export Functionality

**Supported Formats**:
- **CSV**: Comma-separated values with optional headers
- **JSON**: Structured data format for programmatic use
- **TSV**: Tab-separated values for spreadsheet applications

**Export Example**:
```python
# Export search results
export_options = {
    'format': 'csv',
    'path': '/path/to/export.csv',
    'include_headers': True
}

app.export_results(export_options)
```

#### Context Menu Operations

**Available Actions**:
- Open file with system default application
- Copy file path to clipboard
- Show file in folder/file manager
- File properties and metadata

## Configuration System

### GUI Preferences

The enterprise GUI system includes comprehensive preference management:

```python
from src.enterprise_gui_config import EnterpriseGUIConfigManager, GUIPreferences

# Initialize configuration manager
config_manager = EnterpriseGUIConfigManager(project_root)

# Access preferences
prefs = config_manager.preferences
print(f"Current theme: {prefs.theme}")
print(f"Font size: {prefs.font_size}")

# Modify preferences
prefs.theme = "dark"
prefs.font_size = 12
config_manager.save_preferences(prefs)
```

### Configuration Categories

#### Appearance Settings
- **Theme Selection**: Light, dark, high-contrast, or custom themes
- **Font Configuration**: Family, size, and style options
- **Window Settings**: Default size, position, and maximization state
- **Color Customization**: Custom color schemes and theme creation

#### Accessibility Settings
- **Accessibility Levels**: Standard, enhanced, high-contrast, screen reader
- **Visual Options**: High contrast mode, large fonts, enhanced visibility
- **Navigation Options**: Keyboard navigation, screen reader support
- **Motor Accessibility**: Configurable click timing and gesture recognition

#### Behavior Settings
- **Interface Options**: Tooltips, status bar, toolbar visibility
- **Auto-refresh**: Configurable refresh intervals and triggers
- **Export Defaults**: Default export format and options
- **Search Behavior**: Result limits, history size, auto-complete

#### Advanced Settings
- **Performance Tuning**: Memory limits, cache sizes, thread counts
- **Logging Configuration**: Log levels, file rotation, audit trails
- **Security Settings**: Path validation, file access restrictions
- **Integration Options**: External tool integration, API endpoints

### Theme System

#### Built-in Themes

**Light Theme** (Default):
```json
{
  "background": "#ffffff",
  "foreground": "#000000",
  "select_background": "#0078d4",
  "button_background": "#f0f0f0",
  "error_color": "#d13438",
  "success_color": "#107c10"
}
```

**Dark Theme**:
```json
{
  "background": "#2d2d30",
  "foreground": "#ffffff",
  "select_background": "#0e639c",
  "button_background": "#3e3e42",
  "error_color": "#f14c4c",
  "success_color": "#73aa24"
}
```

**High Contrast Theme**:
```json
{
  "background": "#000000",
  "foreground": "#ffffff",
  "select_background": "#ffffff",
  "select_foreground": "#000000",
  "error_color": "#ff0000",
  "success_color": "#00ff00"
}
```

#### Custom Theme Creation

```python
from src.enterprise_gui_config import ThemeColors

# Create custom theme
custom_theme = ThemeColors(
    background="#1e1e1e",
    foreground="#d4d4d4",
    select_background="#264f78",
    button_background="#0e639c",
    error_color="#f48771",
    success_color="#89d185"
)

# Save custom theme
config_manager.save_custom_theme("my_custom_theme", custom_theme)
```

## Usage Examples

### Basic Search GUI Usage

```bash
# Launch with default settings
./scripts/run_enterprise_search.sh

# Launch with specific theme
GUI_THEME=dark ./scripts/run_enterprise_search.sh

# Launch with performance monitoring
./scripts/run_enterprise_search.sh --monitor

# Check prerequisites only
./scripts/run_enterprise_search.sh --check-only
```

### Advanced Search Operations

```python
# Initialize search application
from src.enterprise_search_gui import EnterpriseSearchApplication

config = load_config()
app = EnterpriseSearchApplication(config)

# Perform programmatic search
criteria = SearchCriteria(filename_query="character sheet")
results = app.database.search_files(criteria)

# Export results
app.export_results_to_file({
    'format': 'json',
    'path': 'search_results.json',
    'include_headers': True
})
```

### Configuration Management

```python
# Open configuration dialog
from src.enterprise_gui_config import GUIConfigurationDialog

config_manager = EnterpriseGUIConfigManager(project_root)
dialog = GUIConfigurationDialog(parent, config_manager)
dialog.show()

# Export/import configuration
config_manager.export_configuration(Path("my_config.json"))
config_manager.import_configuration(Path("backup_config.json"))
```

## Accessibility Features

### Screen Reader Support

The enterprise GUI includes comprehensive screen reader support:

- **ARIA Labels**: Proper labeling for all interactive elements
- **Keyboard Navigation**: Full keyboard accessibility
- **Focus Management**: Logical tab order and focus indicators
- **Announcements**: Status updates and error messages

### Visual Accessibility

- **High Contrast Mode**: Enhanced visibility for low vision users
- **Large Font Support**: Scalable fonts up to 24pt
- **Color Blind Support**: Color schemes that work with color blindness
- **Zoom Support**: Interface scaling for better visibility

### Motor Accessibility

- **Keyboard Shortcuts**: Comprehensive keyboard shortcuts for all functions
- **Configurable Timing**: Adjustable click timing and double-click intervals
- **Sticky Keys Support**: Compatible with system accessibility features
- **Voice Control**: Integration with system voice control features

## Performance Optimization

### Memory Management

```python
# Configure memory limits
preferences.max_memory_mb = 2048
preferences.enable_caching = True
preferences.cache_size_mb = 256
```

### Search Performance

```python
# Optimize search performance
preferences.max_search_results = 1000
preferences.search_timeout_seconds = 30
preferences.enable_search_indexing = True
```

### UI Responsiveness

```python
# Configure UI responsiveness
preferences.ui_update_interval_ms = 100
preferences.enable_async_operations = True
preferences.max_concurrent_tasks = 4
```

## Troubleshooting

### Common Issues

**GUI Won't Start**:
```bash
# Check prerequisites
./scripts/run_enterprise_search.sh --check-only

# Check display environment
echo $DISPLAY
echo $WAYLAND_DISPLAY

# Verify tkinter installation
python3 -c "import tkinter; print('tkinter OK')"
```

**Theme Issues**:
```bash
# Reset to default theme
python3 -c "
from src.enterprise_gui_config import EnterpriseGUIConfigManager, GUIPreferences
from pathlib import Path
config = EnterpriseGUIConfigManager(Path.cwd())
prefs = GUIPreferences()
config.save_preferences(prefs)
print('Theme reset to default')
"
```

**Performance Issues**:
```bash
# Enable performance monitoring
MONITOR_PERFORMANCE=true ./scripts/run_enterprise_search.sh

# Check performance logs
tail -f logs/gui_performance.log
```

### Debug Mode

```bash
# Launch in debug mode
./scripts/run_enterprise_search.sh --debug

# Check debug logs
tail -f logs/search_gui.log
```

### Configuration Reset

```bash
# Reset all GUI configuration
rm -f conf/gui_config.json
rm -rf conf/themes/

# Restart application to recreate defaults
./scripts/run_enterprise_search.sh
```

## Integration with Enterprise Systems

### API Integration

```python
# REST API endpoints for GUI integration
from src.enterprise_search_gui import SearchAPI

api = SearchAPI(config)

# Search endpoint
results = api.search({
    'query': 'character sheet',
    'filters': {'game_system': 'D&D 5e'}
})

# Export endpoint
api.export_results(results, 'csv', 'output.csv')
```

### Configuration Management Integration

```python
# Integration with configuration management tools
config_manager = EnterpriseGUIConfigManager(project_root)

# Export for Ansible/Puppet
config_data = config_manager.export_for_automation()

# Import from centralized configuration
config_manager.import_from_automation(central_config)
```

### Monitoring Integration

```python
# Prometheus metrics integration
from src.enterprise_gui_framework import MetricsCollector

metrics = MetricsCollector()
metrics.register_gui_metrics(app)

# Export metrics
metrics_data = metrics.get_prometheus_metrics()
```

## Security Considerations

### Input Validation

- **Path Traversal Protection**: All file paths validated and sanitized
- **SQL Injection Prevention**: Parameterized queries for database operations
- **XSS Prevention**: Input sanitization for display operations
- **File Access Control**: Restricted file system access

### Configuration Security

- **Secure Storage**: Configuration files with appropriate permissions
- **Credential Management**: No credentials stored in configuration files
- **Audit Logging**: All configuration changes logged
- **Access Control**: User-specific configuration isolation

### Network Security

- **No Network Access**: GUI operates entirely offline by default
- **Secure Export**: Export operations with path validation
- **File Permissions**: Exported files with secure permissions
- **Temporary Files**: Secure handling of temporary files

## Support and Documentation

### Documentation Structure

- **User Guide**: This document
- **API Reference**: `docs/api/gui_framework.md`
- **Developer Guide**: `docs/development/gui_development.md`
- **Accessibility Guide**: `docs/accessibility/gui_accessibility.md`
- **Theme Development**: `docs/theming/custom_themes.md`

### Support Resources

- **Issue Tracking**: GitHub issues for bug reports and feature requests
- **Community Forum**: User community for questions and discussions
- **Enterprise Support**: Professional support for enterprise deployments
- **Training Materials**: Video tutorials and training documentation

### Contributing

- **Code Contributions**: Guidelines for contributing to the GUI framework
- **Theme Contributions**: How to contribute custom themes
- **Accessibility Testing**: Guidelines for accessibility testing
- **Documentation**: How to contribute to documentation

For enterprise support and consulting services, please contact the development team.