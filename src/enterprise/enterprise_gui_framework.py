"""
Enterprise GUI Framework

Provides enterprise-grade GUI components with modern architecture,
comprehensive error handling, theming, and accessibility features.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
import tkinter as tk
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from tkinter import ttk
from typing import Any, Callable

logger = logging.getLogger(__name__)


class ThemeType(Enum):
    """GUI theme types"""
    LIGHT = "light"
    DARK = "dark"
    HIGH_CONTRAST = "high_contrast"
    SYSTEM = "system"


class ComponentState(Enum):
    """Component state enumeration"""
    NORMAL = "normal"
    DISABLED = "disabled"
    LOADING = "loading"
    ERROR = "error"
    SUCCESS = "success"


@dataclass
class GUITheme:
    """GUI theme configuration"""
    name: str
    background: str
    foreground: str
    select_background: str
    select_foreground: str
    button_background: str
    button_foreground: str
    entry_background: str
    entry_foreground: str
    error_color: str
    success_color: str
    warning_color: str
    info_color: str
    font_family: str = "Segoe UI"
    font_size: int = 10


class EnterpriseThemeManager:
    """Enterprise theme management"""

    def __init__(self):
        self.themes = {
            ThemeType.LIGHT: GUITheme(
                name="Light",
                background="#ffffff",
                foreground="#000000",
                select_background="#0078d4",
                select_foreground="#ffffff",
                button_background="#f0f0f0",
                button_foreground="#000000",
                entry_background="#ffffff",
                entry_foreground="#000000",
                error_color="#d13438",
                success_color="#107c10",
                warning_color="#ff8c00",
                info_color="#0078d4"
            ),
            ThemeType.DARK: GUITheme(
                name="Dark",
                background="#2d2d30",
                foreground="#ffffff",
                select_background="#0e639c",
                select_foreground="#ffffff",
                button_background="#3e3e42",
                button_foreground="#ffffff",
                entry_background="#1e1e1e",
                entry_foreground="#ffffff",
                error_color="#f14c4c",
                success_color="#73aa24",
                warning_color="#ffb900",
                info_color="#60cdff"
            ),
            ThemeType.HIGH_CONTRAST: GUITheme(
                name="High Contrast",
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
                info_color="#00ffff"
            )
        }
        self.current_theme = ThemeType.LIGHT

    def get_theme(self, theme_type: ThemeType | None = None) -> GUITheme | None:
        """Get theme configuration"""
        return self.themes.get(theme_type or self.current_theme)

    def set_theme(self, theme_type: ThemeType) -> None:
        """Set current theme"""
        self.current_theme = theme_type

    def apply_theme(self, widget: tk.Widget, theme: GUITheme | None = None) -> None:
        """Apply theme to widget"""
        if not theme:
            theme = self.get_theme()

        if not theme:
            return

        try:
            if isinstance(widget, (tk.Tk, tk.Toplevel, tk.Frame, ttk.Frame)):
                widget.configure(bg=theme.background)  # type: ignore[call-overload]
            elif isinstance(widget, tk.Label):
                widget.configure(bg=theme.background, fg=theme.foreground)
            elif isinstance(widget, ttk.Label):
                widget.configure(background=theme.background, foreground=theme.foreground)
            elif isinstance(widget, tk.Button):
                widget.configure(bg=theme.button_background)  # type: ignore
                widget.configure(fg=theme.button_foreground)  # type: ignore
            elif isinstance(widget, ttk.Button):
                widget.configure(background=theme.button_background, foreground=theme.button_foreground)
            elif isinstance(widget, tk.Entry):
                widget.configure(bg=theme.entry_background)  # type: ignore
                widget.configure(fg=theme.entry_foreground)  # type: ignore
            elif isinstance(widget, ttk.Entry):
                widget.configure(fieldbackground=theme.entry_background, foreground=theme.entry_foreground)
        except tk.TclError:
            # Some widgets don't support all configuration options
            pass


class AsyncTaskManager:
    """Manages asynchronous GUI tasks"""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.task_queue: queue.Queue = queue.Queue()
        self.result_queue: queue.Queue = queue.Queue()
        self.active_tasks: dict[int, threading.Thread] = {}
        self.task_counter = 0

        # Start result processor
        self._process_results()

    def submit_task(self, func: Callable[..., Any], callback: Callable[[Any], None] | None = None,
                   error_callback: Callable[[Exception], None] | None = None, *args, **kwargs) -> int:
        """Submit async task"""
        task_id = self.task_counter
        self.task_counter += 1

        def task_wrapper():
            try:
                result = func(*args, **kwargs)
                self.result_queue.put(("success", task_id, result, callback))
            except Exception as e:
                self.result_queue.put(("error", task_id, e, error_callback))

        thread = threading.Thread(target=task_wrapper, daemon=True)
        self.active_tasks[task_id] = thread
        thread.start()

        return task_id

    def _process_results(self):
        """Process task results in main thread"""
        try:
            while True:
                try:
                    status, task_id, result, callback = self.result_queue.get_nowait()

                    # Clean up task reference
                    self.active_tasks.pop(task_id, None)

                    # Execute callback if provided
                    if callback:
                        try:
                            if status == "success":
                                callback(result)
                            else:
                                callback(result)  # result is exception for errors
                        except Exception as e:
                            logger.error(f"Callback execution failed: {e}")

                except queue.Empty:
                    break
        except Exception as e:
            logger.error(f"Result processing failed: {e}")

        # Schedule next check
        self.root.after(100, self._process_results)


class EnterpriseWidget(ABC):
    """Base class for enterprise widgets"""

    def __init__(self, parent: tk.Widget, theme_manager: EnterpriseThemeManager):
        self.parent = parent
        self.theme_manager = theme_manager
        self.state = ComponentState.NORMAL
        self.widget: tk.Widget | None = None
        self.error_message: str | None = None
        self.validation_rules: list[tuple[Callable[[Any], bool], str]] = []

    @abstractmethod
    def create_widget(self) -> tk.Widget:
        """Create the actual widget"""

    def set_state(self, state: ComponentState, message: str | None = None) -> None:
        """Set widget state"""
        self.state = state
        self.error_message = message
        self._update_appearance()

    def _update_appearance(self) -> None:
        """Update widget appearance based on state"""
        if not self.widget:
            return

        theme = self.theme_manager.get_theme()

        try:
            if self.widget and theme:
                if self.state == ComponentState.ERROR:
                    self.widget.configure(bg=theme.error_color)  # type: ignore[call-arg]
                elif self.state == ComponentState.SUCCESS:
                    self.widget.configure(bg=theme.success_color)  # type: ignore[call-arg]
                elif self.state == ComponentState.DISABLED:
                    self.widget.configure(state="disabled")  # type: ignore[call-arg]
                else:
                    self.widget.configure(state="normal")  # type: ignore[call-arg]
                    self.theme_manager.apply_theme(self.widget)
        except tk.TclError:
            pass

    def add_validation(self, rule: Callable[[Any], bool], message: str) -> None:
        """Add validation rule"""
        self.validation_rules.append((rule, message))

    def validate(self) -> bool:
        """Validate widget value"""
        for rule, message in self.validation_rules:
            try:
                if not rule(self.get_value()):
                    self.set_state(ComponentState.ERROR, message)
                    return False
            except Exception as e:
                self.set_state(ComponentState.ERROR, f"Validation error: {e}")
                return False

        self.set_state(ComponentState.NORMAL)
        return True

    @abstractmethod
    def get_value(self) -> Any:
        """Get widget value"""

    @abstractmethod
    def set_value(self, value: Any):
        """Set widget value"""


class EnterpriseEntry(EnterpriseWidget):
    """Enterprise text entry widget"""

    def __init__(self, parent: tk.Widget, theme_manager: EnterpriseThemeManager,
                 placeholder: str = "", width: int = 20):
        super().__init__(parent, theme_manager)
        self.placeholder = placeholder
        self.width = width
        self.var = tk.StringVar()
        self.widget = self.create_widget()

    def create_widget(self) -> tk.Widget:
        """Create entry widget"""
        entry = ttk.Entry(self.parent, textvariable=self.var, width=self.width)

        # Add placeholder functionality
        if self.placeholder:
            self._setup_placeholder(entry)

        # Apply theme
        self.theme_manager.apply_theme(entry)

        return entry

    def _setup_placeholder(self, entry: ttk.Entry):
        """Setup placeholder text functionality"""
        def on_focus_in(_event):
            if self.var.get() == self.placeholder:
                self.var.set("")
                entry.configure(foreground="black")

        def on_focus_out(_event):
            if not self.var.get():
                self.var.set(self.placeholder)
                entry.configure(foreground="grey")

        # Set initial placeholder
        self.var.set(self.placeholder)
        entry.configure(foreground="grey")

        entry.bind("<FocusIn>", on_focus_in)
        entry.bind("<FocusOut>", on_focus_out)

    def get_value(self) -> str:
        """Get entry value"""
        value = self.var.get()
        return "" if value == self.placeholder else value

    def set_value(self, value: str) -> None:
        """Set entry value"""
        self.var.set(value)


class EnterpriseCombobox(EnterpriseWidget):
    """Enterprise combobox widget"""

    def __init__(self, parent: tk.Widget, theme_manager: EnterpriseThemeManager,
                 values: list[str] | None = None, width: int = 20):
        super().__init__(parent, theme_manager)
        self.values = values or []
        self.width = width
        self.var = tk.StringVar()
        self.widget = self.create_widget()

    def create_widget(self) -> tk.Widget:
        """Create combobox widget"""
        combo = ttk.Combobox(self.parent, textvariable=self.var,
                           values=self.values, width=self.width, state="readonly")

        # Apply theme
        self.theme_manager.apply_theme(combo)

        return combo

    def get_value(self) -> str:
        """Get combobox value"""
        return self.var.get()

    def set_value(self, value: str) -> None:
        """Set combobox value"""
        if value in self.values:
            self.var.set(value)

    def update_values(self, values: list[str]) -> None:
        """Update combobox values"""
        self.values = values
        if self.widget:
            self.widget["values"] = values  # type: ignore[index]


class EnterpriseTreeview(EnterpriseWidget):
    """Enterprise treeview widget with advanced features"""

    def __init__(self, parent: tk.Widget, theme_manager: EnterpriseThemeManager,
                 columns: list[str], headings: list[str] | None = None):
        super().__init__(parent, theme_manager)
        self.columns = columns
        self.headings = headings or columns
        self.data: list[list[str]] = []
        self.sort_column: str | None = None
        self.sort_reverse = False
        self.widget = self.create_widget()

    def create_widget(self) -> tk.Widget:
        """Create treeview widget with scrollbars"""
        # Create frame for treeview and scrollbars
        frame = ttk.Frame(self.parent)

        # Create treeview
        tree = ttk.Treeview(frame, columns=self.columns, show="headings")

        # Configure columns and headings
        for _i, (col, heading) in enumerate(zip(self.columns, self.headings)):
            tree.heading(col, text=heading, command=lambda c=col: self._sort_by_column(c))  # type: ignore[misc]
            tree.column(col, width=150, anchor=tk.W)

        # Create scrollbars
        v_scrollbar = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        h_scrollbar = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)

        # Pack components
        tree.grid(row=0, column=0, sticky="nsew")
        v_scrollbar.grid(row=0, column=1, sticky="ns")
        h_scrollbar.grid(row=1, column=0, sticky="ew")

        # Configure grid weights
        frame.grid_rowconfigure(0, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        # Store tree reference
        self.tree = tree

        # Apply theme
        self.theme_manager.apply_theme(tree)

        return frame

    def _sort_by_column(self, column: str) -> None:
        """Sort treeview by column"""
        if self.sort_column == column:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = column
            self.sort_reverse = False

        # Get all items
        items = [(self.tree.set(item, column), item) for item in self.tree.get_children("")]

        # Sort items
        items.sort(reverse=self.sort_reverse)

        # Rearrange items
        for index, (_, item) in enumerate(items):
            self.tree.move(item, "", index)

    def insert_row(self, values: list[str], tags: list[str] | None = None):
        """Insert row into treeview"""
        item = self.tree.insert("", "end", values=values, tags=tags or [])
        self.data.append(values)
        return item

    def clear(self) -> None:
        """Clear all items"""
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.data.clear()

    def get_selected_values(self) -> list[str]:
        """Get selected row values"""
        selection = self.tree.selection()
        if selection:
            values = self.tree.item(selection[0])["values"]
            return [str(v) for v in values] if values else []
        return []

    def get_value(self) -> list[list[str]]:
        """Get all treeview data"""
        return self.data

    def set_value(self, value: list[list[str]]) -> None:
        """Set treeview data"""
        self.clear()
        for row in value:
            self.insert_row(row)


class EnterpriseProgressBar(EnterpriseWidget):
    """Enterprise progress bar with status text"""

    def __init__(self, parent: tk.Widget, theme_manager: EnterpriseThemeManager,
                 length: int = 300, mode: str = "determinate"):
        super().__init__(parent, theme_manager)
        self.length = length
        self.mode = mode
        self.status_text = tk.StringVar()
        self.widget = self.create_widget()

    def create_widget(self) -> tk.Widget:
        """Create progress bar with status label"""
        frame = ttk.Frame(self.parent)

        # Progress bar
        self.progress = ttk.Progressbar(frame, length=self.length, mode=self.mode)  # type: ignore[arg-type]
        self.progress.pack(fill=tk.X, padx=5, pady=2)

        # Status label
        self.status_label = ttk.Label(frame, textvariable=self.status_text)
        self.status_label.pack(fill=tk.X, padx=5, pady=2)

        # Apply theme
        self.theme_manager.apply_theme(self.progress)
        self.theme_manager.apply_theme(self.status_label)

        return frame

    def set_progress(self, value: float, status: str = "") -> None:
        """Set progress value and status"""
        self.progress["value"] = value
        self.status_text.set(status)

    def start_indeterminate(self, status: str = "Processing...") -> None:
        """Start indeterminate progress"""
        self.progress.configure(mode="indeterminate")
        self.progress.start()
        self.status_text.set(status)

    def stop_indeterminate(self) -> None:
        """Stop indeterminate progress"""
        self.progress.stop()
        self.progress.configure(mode="determinate")

    def get_value(self) -> float:
        """Get progress value"""
        return float(self.progress["value"])

    def set_value(self, value: float) -> None:
        """Set progress value"""
        self.progress["value"] = value


class EnterpriseStatusBar:
    """Enterprise status bar with multiple sections"""

    def __init__(self, parent: tk.Widget, theme_manager: EnterpriseThemeManager):
        self.parent = parent
        self.theme_manager = theme_manager
        self.sections: dict[str, tk.StringVar] = {}
        self.frame = self._create_status_bar()

    def _create_status_bar(self) -> ttk.Frame:
        """Create status bar frame"""
        frame = ttk.Frame(self.parent, relief=tk.SUNKEN, borderwidth=1)

        # Apply theme
        self.theme_manager.apply_theme(frame)

        return frame

    def add_section(self, name: str, text: str = "", width: int = 20) -> tk.StringVar:
        """Add status section"""
        var = tk.StringVar(value=text)
        label = ttk.Label(self.frame, textvariable=var, width=width,
                         relief=tk.SUNKEN, borderwidth=1)
        label.pack(side=tk.LEFT, padx=1, pady=1)

        self.sections[name] = var
        self.theme_manager.apply_theme(label)

        return var

    def update_section(self, name: str, text: str) -> None:
        """Update status section"""
        if name in self.sections:
            self.sections[name].set(text)

    def pack(self, **kwargs) -> None:
        """Pack status bar"""
        self.frame.pack(**kwargs)


class EnterpriseDialog(tk.Toplevel):
    """Base class for enterprise dialogs"""

    def __init__(self, parent: tk.Widget, title: str, theme_manager: EnterpriseThemeManager,
                 modal: bool = True, resizable: bool = False):
        super().__init__(parent)

        self.parent = parent
        self.theme_manager = theme_manager
        self.result: bool | None = None

        # Configure dialog
        self.title(title)
        self.resizable(resizable, resizable)

        if modal:
            self.transient(parent)  # type: ignore[call-overload]
            self.grab_set()

        # Apply theme
        self.theme_manager.apply_theme(self)  # type: ignore[arg-type]

        # Center dialog
        self._center_dialog()

        # Create content
        self.create_content()

        # Handle close event
        self.protocol("WM_DELETE_WINDOW", self.on_cancel)

    def _center_dialog(self) -> None:
        """Center dialog on parent"""
        self.update_idletasks()

        # Get parent geometry
        parent_x = self.parent.winfo_rootx()
        parent_y = self.parent.winfo_rooty()
        parent_width = self.parent.winfo_width()
        parent_height = self.parent.winfo_height()

        # Calculate center position
        dialog_width = self.winfo_reqwidth()
        dialog_height = self.winfo_reqheight()

        x = parent_x + (parent_width - dialog_width) // 2
        y = parent_y + (parent_height - dialog_height) // 2

        self.geometry(f"+{x}+{y}")

    def create_content(self) -> None:
        """Override to create dialog content"""

    def on_ok(self) -> None:
        """Handle OK button"""
        self.result = True
        self.destroy()

    def on_cancel(self) -> None:
        """Handle Cancel button"""
        self.result = False
        self.destroy()

    def show(self) -> bool | None:
        """Show dialog and return result"""
        self.wait_window()
        return self.result


class EnterpriseMessageDialog(EnterpriseDialog):
    """Enterprise message dialog"""

    def __init__(self, parent: tk.Widget, title: str, message: str,
                 theme_manager: EnterpriseThemeManager, dialog_type: str = "info"):
        self.message = message
        self.dialog_type = dialog_type
        super().__init__(parent, title, theme_manager)

    def create_content(self) -> None:
        """Create message dialog content"""
        # Message frame
        msg_frame = ttk.Frame(self)
        msg_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # Icon (simplified - in production, use actual icons)
        icon_text = {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "❌",
            "question": "❓"
        }.get(self.dialog_type, "ℹ️")

        icon_label = ttk.Label(msg_frame, text=icon_text, font=("Arial", 24))
        icon_label.pack(side=tk.LEFT, padx=(0, 10))

        # Message text
        msg_label = ttk.Label(msg_frame, text=self.message, wraplength=400)
        msg_label.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Button frame
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=20, pady=(0, 20))

        # OK button
        ok_btn = ttk.Button(btn_frame, text="OK", command=self.on_ok)
        ok_btn.pack(side=tk.RIGHT, padx=(5, 0))

        # Apply theme
        self.theme_manager.apply_theme(msg_frame)
        self.theme_manager.apply_theme(icon_label)
        self.theme_manager.apply_theme(msg_label)
        self.theme_manager.apply_theme(btn_frame)
        self.theme_manager.apply_theme(ok_btn)


class EnterpriseApplication(tk.Tk):
    """Base enterprise application class"""

    def __init__(self, title: str = "Enterprise Application",
                 theme: ThemeType = ThemeType.LIGHT):
        super().__init__()

        # Initialize managers
        self.theme_manager = EnterpriseThemeManager()
        self.theme_manager.set_theme(theme)
        self.task_manager = AsyncTaskManager(self)

        # Configure application
        self.title(title)
        self.geometry("1200x800")

        # Apply theme
        self.theme_manager.apply_theme(self)  # type: ignore[arg-type]

        # Create status bar
        self.status_bar = EnterpriseStatusBar(self, self.theme_manager)  # type: ignore[arg-type]
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

        # Add default status sections
        self.status_bar.add_section("main", "Ready", 40)
        self.status_bar.add_section("progress", "", 20)
        self.status_bar.add_section("time", time.strftime("%H:%M:%S"), 15)

        # Update time periodically
        self._update_time()

        # Handle close event
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

    def _update_time(self) -> None:
        """Update time in status bar"""
        self.status_bar.update_section("time", time.strftime("%H:%M:%S"))
        self.after(1000, self._update_time)

    def show_message(self, title: str, message: str, dialog_type: str = "info") -> bool | None:
        """Show message dialog"""
        dialog = EnterpriseMessageDialog(self, title, message,  # type: ignore[arg-type]
                                       self.theme_manager, dialog_type)
        return dialog.show()

    def show_error(self, message: str, title: str = "Error"):
        """Show error message"""
        return self.show_message(title, message, "error")

    def show_warning(self, message: str, title: str = "Warning"):
        """Show warning message"""
        return self.show_message(title, message, "warning")

    def show_info(self, message: str, title: str = "Information"):
        """Show info message"""
        return self.show_message(title, message, "info")

    def set_status(self, message: str, section: str = "main") -> None:
        """Set status bar message"""
        self.status_bar.update_section(section, message)

    def run_async_task(self, func: Callable, success_callback: Callable | None = None,
                      error_callback: Callable | None = None, *args, **kwargs) -> int:
        """Run async task with callbacks"""
        def default_error_callback(error):
            self.show_error(f"Task failed: {error}")

        return self.task_manager.submit_task(
            func, success_callback, error_callback or default_error_callback,
            *args, **kwargs
        )

    def on_closing(self) -> None:
        """Handle application closing"""
        try:
            # Perform cleanup
            self.cleanup()
            self.destroy()
        except Exception as e:
            logger.error(f"Error during application shutdown: {e}")
            self.destroy()

    def cleanup(self) -> None:
        """Override for cleanup operations"""


def main() -> None:
    """Demo application"""
    class DemoApp(EnterpriseApplication):
        def __init__(self):
            super().__init__("Enterprise GUI Framework Demo")
            self.create_demo_content()

        def create_demo_content(self) -> None:
            # Main frame
            main_frame = ttk.Frame(self)
            main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

            # Entry demo
            entry_frame = ttk.LabelFrame(main_frame, text="Entry Demo")
            entry_frame.pack(fill=tk.X, pady=5)

            self.entry = EnterpriseEntry(entry_frame, self.theme_manager,
                                       placeholder="Enter text here...")
            if self.entry.widget:
                self.entry.widget.pack(padx=10, pady=10)

            # Combobox demo
            combo_frame = ttk.LabelFrame(main_frame, text="Combobox Demo")
            combo_frame.pack(fill=tk.X, pady=5)

            self.combo = EnterpriseCombobox(combo_frame, self.theme_manager,
                                          values=["Option 1", "Option 2", "Option 3"])
            if self.combo.widget:
                self.combo.widget.pack(padx=10, pady=10)

            # Treeview demo
            tree_frame = ttk.LabelFrame(main_frame, text="Treeview Demo")
            tree_frame.pack(fill=tk.BOTH, expand=True, pady=5)

            self.tree = EnterpriseTreeview(tree_frame, self.theme_manager,
                                         columns=["col1", "col2", "col3"],
                                         headings=["Column 1", "Column 2", "Column 3"])
            if self.tree.widget:
                self.tree.widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

            # Add sample data
            for i in range(10):
                self.tree.insert_row([f"Item {i+1}", f"Value {i+1}", f"Data {i+1}"])

            # Button frame
            btn_frame = ttk.Frame(main_frame)
            btn_frame.pack(fill=tk.X, pady=5)

            ttk.Button(btn_frame, text="Show Info",
                      command=lambda: self.show_info("This is an info message")).pack(side=tk.LEFT, padx=5)
            ttk.Button(btn_frame, text="Show Warning",
                      command=lambda: self.show_warning("This is a warning")).pack(side=tk.LEFT, padx=5)
            ttk.Button(btn_frame, text="Show Error",
                      command=lambda: self.show_error("This is an error")).pack(side=tk.LEFT, padx=5)

    app = DemoApp()
    app.mainloop()


if __name__ == "__main__":
    main()
