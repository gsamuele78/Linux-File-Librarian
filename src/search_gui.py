import os
import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
import subprocess
import sys

# Import the centralized configuration loader from within the src directory
try:
    from src.config_loader import load_config
except ModuleNotFoundError:
    # This fallback allows the script to be run directly for testing,
    # though the primary method should be via the wrapper scripts.
    from config_loader import load_config


class SearchApp(tk.Tk):
    def __init__(self, config):
        super().__init__()
        
        # Clean up any temporary GUI files
        from src.cleanup_utils import cleanup_temp_files
        cleanup_temp_files()
        
        # Load configuration settings
        self.library_root = config['library_root']
        self.db_path = os.path.join(self.library_root, "library_index.sqlite")

        # Essential check: Ensure the database exists before building the GUI
        if not os.path.exists(self.db_path):
            messagebox.showerror("Error", f"Database not found at:\n{self.db_path}\n\nPlease run the librarian script first to build the library.")
            self.destroy()
            return
            
        self.title("Library Search")
        self.geometry("1100x700")

        # --- Filter Frame ---
        self.filter_frame = ttk.LabelFrame(self, text="Filters")
        self.filter_frame.pack(fill=tk.X, padx=10, pady=5)

        # Game System Filter
        ttk.Label(self.filter_frame, text="Game System:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.game_system_var = tk.StringVar()
        self.game_system_combo = ttk.Combobox(self.filter_frame, textvariable=self.game_system_var, state="readonly", width=30)
        self.game_system_combo.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        # File Type Filter
        ttk.Label(self.filter_frame, text="File Type:").grid(row=0, column=2, padx=(20, 5), pady=5, sticky="w")
        self.file_type_var = tk.StringVar()
        self.file_type_combo = ttk.Combobox(self.filter_frame, textvariable=self.file_type_var, state="readonly", width=30)
        self.file_type_combo.grid(row=0, column=3, padx=5, pady=5, sticky="ew")

        # Language Filter
        ttk.Label(self.filter_frame, text="Language:").grid(row=0, column=4, padx=(20, 5), pady=5, sticky="w")
        self.language_var = tk.StringVar()
        self.language_combo = ttk.Combobox(self.filter_frame, textvariable=self.language_var, state="readonly", width=20)
        self.language_combo.grid(row=0, column=5, padx=5, pady=5, sticky="ew")

        # Make combobox columns expandable
        self.filter_frame.grid_columnconfigure(1, weight=1)
        self.filter_frame.grid_columnconfigure(3, weight=1)
        self.filter_frame.grid_columnconfigure(5, weight=1)
        
        # Populate the dropdowns with data from the database
        self.populate_filters()

        # --- Search Frame ---
        self.search_frame = ttk.LabelFrame(self, text="Filename Search")
        self.search_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(self.search_frame, textvariable=self.search_var)
        self.search_entry.pack(fill=tk.X, side=tk.LEFT, expand=True, padx=5, pady=5)
        
        # Bind the <Return> key to the search function for convenience
        self.search_entry.bind("<Return>", self.perform_search)

        self.search_button = ttk.Button(self.search_frame, text="Search", command=self.perform_search)
        self.search_button.pack(side=tk.LEFT, padx=(0, 5), pady=5)

        # --- Results Treeview ---
        self.tree_frame = ttk.Frame(self)
        self.tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.tree = ttk.Treeview(self.tree_frame, columns=("filename", "game_system", "type", "size", "path"), show="headings")
        self.tree.heading("filename", text="Filename")
        self.tree.heading("game_system", text="Game System/Category")
        self.tree.heading("type", text="File Type")
        self.tree.heading("size", text="Size (KB)")
        self.tree.heading("path", text="Full Path")

        # Define column widths
        self.tree.column("filename", width=350, anchor=tk.W)
        self.tree.column("game_system", width=180, anchor=tk.W)
        self.tree.column("type", width=150, anchor=tk.W)
        self.tree.column("size", width=80, anchor=tk.E)
        self.tree.column("path", width=500, anchor=tk.W)

        # Vertical and Horizontal Scrollbars
        vsb = ttk.Scrollbar(self.tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self.tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.tree.pack(fill=tk.BOTH, expand=True)

        # Bind double-click event to open a file
        self.tree.bind("<Double-1>", self.open_selected_file)

    def populate_filters(self):
        """Populates the comboboxes with data from the library index."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get unique Game Systems/Categories
                cursor.execute("SELECT DISTINCT game_system FROM files WHERE game_system IS NOT NULL ORDER BY game_system")
                systems = [row[0] for row in cursor.fetchall()]
                self.game_system_combo['values'] = ["-- ALL --"] + systems
                self.game_system_combo.set("-- ALL --")
                
                # Get unique File Types
                cursor.execute("SELECT DISTINCT type FROM files WHERE type IS NOT NULL ORDER BY type")
                types = [row[0] for row in cursor.fetchall()]
                self.file_type_combo['values'] = ["-- ALL --"] + types
                self.file_type_combo.set("-- ALL --")
                
                # Get unique Languages
                cursor.execute("SELECT DISTINCT language FROM files WHERE language IS NOT NULL ORDER BY language")
                langs = [row[0] for row in cursor.fetchall()]
                self.language_combo['values'] = ["-- ALL --"] + langs
                self.language_combo.set("-- ALL --")
                
        except sqlite3.Error as e:
            messagebox.showerror("Database Error", f"Could not populate filters: {e}")

    def perform_search(self, event=None):
        """Executes the search query against the database using all filters."""
        # Clear previous results from the treeview
        for item in self.tree.get_children():
            self.tree.delete(item)

        # Build the SQL query dynamically based on filter selections
        query = "SELECT filename, game_system, type, size, path FROM files WHERE 1=1"
        params = []

        # Add filename filter (case-insensitive)
        filename_query = self.search_var.get().strip()
        if filename_query:
            query += " AND filename LIKE ?"
            params.append(f"%{filename_query}%")

        # Add game system filter
        system_filter = self.game_system_var.get()
        if system_filter and system_filter != "-- ALL --":
            query += " AND game_system = ?"
            params.append(system_filter)

        # Add file type filter
        type_filter = self.file_type_var.get()
        if type_filter and type_filter != "-- ALL --":
            query += " AND type = ?"
            params.append(type_filter)

        # Add language filter
        lang_filter = getattr(self, 'language_var', None)
        if lang_filter and lang_filter.get() and lang_filter.get() != "-- ALL --":
            query += " AND language = ?"
            params.append(lang_filter.get())

        query += " ORDER BY game_system, filename"

        # Execute the query and populate the results tree
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query, tuple(params))
                for row in cursor.fetchall():
                    filename, game_sys, file_type, size_bytes, path = row
                    size_kb = f"{size_bytes / 1024:.1f}" if size_bytes else "0.0"
                    self.tree.insert("", tk.END, values=(filename, game_sys or '', file_type or '', size_kb, path))
        except sqlite3.Error as e:
            messagebox.showerror("Database Error", f"Search failed: {e}")
            
    def open_selected_file(self, event=None):
        """Opens the selected file in the treeview using the system's default application."""
        selected_item = self.tree.focus()
        if not selected_item:
            return
            
        # The path is the 5th value in our treeview column setup
        file_path = self.tree.item(selected_item)['values'][4]

        try:
            if not os.path.exists(file_path):
                messagebox.showerror("File Not Found", f"The file could not be found at:\n{file_path}")
                return

            # Use platform-specific commands to open the file
            if sys.platform == "win32":
                os.startfile(file_path)
            elif sys.platform == "darwin": # macOS
                subprocess.run(["open", file_path], check=True)
            else: # linux and other UNIX
                subprocess.run(["xdg-open", file_path], check=True)
        except subprocess.CalledProcessError as e:
            messagebox.showerror("Error", f"Could not open file (subprocess error):\n{e}")
        except Exception as e:
            messagebox.showerror("Error", f"Could not open file:\n{e}")

# Main execution block to launch the application
if __name__ == "__main__":
    try:
        # Load configuration from conf/config.ini
        config = load_config()
        
        # Configuration validation before starting the GUI
        if not config['library_root'] or "/path/to/" in config['library_root']:
            # Create a temporary root window just to show an error message
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror("Configuration Error", "Please set 'library_root' in conf/config.ini before running the GUI.")
            sys.exit(1)
            
        # If config is valid, create and run the application
        app = SearchApp(config)
        app.mainloop()

    except FileNotFoundError as e:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Fatal Error", f"A required file was not found. Please check your setup.\n\nError: {e}")
        sys.exit(1)
    except Exception as e:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Fatal Error", f"An unexpected error occurred on startup:\n\n{e}")
        sys.exit(1)
