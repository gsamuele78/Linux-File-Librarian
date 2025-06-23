# src/search_gui.py
import os
import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
import subprocess
import sys

# --- CONFIGURATION ---
LIBRARY_ROOT = "/path/to/your/new_unified_library"
DB_FILE = "library_index.sqlite"

class SearchApp(tk.Tk):
    def __init__(self, db_path):
        super().__init__()
        # ... (initial error checking is the same)
        if not os.path.exists(db_path):
            messagebox.showerror("Error", f"Database not found at:\n{db_path}")
            self.destroy()
            return
            
        self.db_path = db_path
        self.title("Library Search")
        self.geometry("1000x700")

        # --- Filter Frame ---
        self.filter_frame = ttk.LabelFrame(self, text="Filters")
        self.filter_frame.pack(fill=tk.X, padx=10, pady=5)

        # Game System Filter
        ttk.Label(self.filter_frame, text="Game System:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.game_system_var = tk.StringVar()
        self.game_system_combo = ttk.Combobox(self.filter_frame, textvariable=self.game_system_var, state="readonly")
        self.game_system_combo.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        # File Type Filter
        ttk.Label(self.filter_frame, text="File Type:").grid(row=0, column=2, padx=5, pady=5, sticky="w")
        self.file_type_var = tk.StringVar()
        self.file_type_combo = ttk.Combobox(self.filter_frame, textvariable=self.file_type_var, state="readonly")
        self.file_type_combo.grid(row=0, column=3, padx=5, pady=5, sticky="ew")

        self.filter_frame.grid_columnconfigure(1, weight=1)
        self.filter_frame.grid_columnconfigure(3, weight=1)
        
        self.populate_filters()

        # --- Search Frame ---
        self.search_frame = ttk.LabelFrame(self, text="Filename Search")
        self.search_frame.pack(fill=tk.X, padx=10, pady=5)
        # ... (Search Entry and Button are the same)
        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(self.search_frame, textvariable=self.search_var)
        self.search_entry.pack(fill=tk.X, side=tk.LEFT, expand=True, padx=5, pady=5)
        self.search_button = ttk.Button(self.search_frame, text="Search", command=self.perform_search)
        self.search_button.pack(side=tk.LEFT, padx=5)
        self.search_entry.bind("<Return>", self.perform_search)

        # --- Results Treeview (with new columns) ---
        self.tree_frame = ttk.Frame(self)
        self.tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        self.tree = ttk.Treeview(self.tree_frame, columns=("filename", "game_system", "type", "size", "path"), show="headings")
        self.tree.heading("filename", text="Filename")
        self.tree.heading("game_system", text="Game System")
        self.tree.heading("type", text="File Type")
        self.tree.heading("size", text="Size (KB)")
        self.tree.heading("path", text="Full Path")
        # ... (Scrollbars and double-click binding are the same)
        vsb = ttk.Scrollbar(self.tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self.tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.tree.pack(fill=tk.BOTH, expand=True)
        self.tree.bind("<Double-1>", self.open_selected_file)

    def populate_filters(self):
        """Populates the comboboxes with data from the library index."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Game Systems
            cursor.execute("SELECT DISTINCT game_system FROM files WHERE game_system IS NOT NULL ORDER BY game_system")
            systems = [row[0] for row in cursor.fetchall()]
            self.game_system_combo['values'] = ["-- ALL --"] + systems
            self.game_system_combo.set("-- ALL --")
            
            # File Types
            cursor.execute("SELECT DISTINCT type FROM files WHERE type IS NOT NULL ORDER BY type")
            types = [row[0] for row in cursor.fetchall()]
            self.file_type_combo['values'] = ["-- ALL --"] + types
            self.file_type_combo.set("-- ALL --")
            
            conn.close()
        except sqlite3.Error as e:
            messagebox.showerror("Database Error", f"Could not populate filters: {e}")

    def perform_search(self, event=None):
        """Executes search with filters."""
        for item in self.tree.get_children(): self.tree.delete(item)

        query = "SELECT filename, game_system, type, size, path FROM files WHERE 1=1"
        params = []

        # Add filename filter
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

        query += " ORDER BY filename"

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(query, params)
            for row in cursor.fetchall():
                filename, game_sys, file_type, size_bytes, path = row
                size_kb = f"{size_bytes / 1024:.1f}" if size_bytes else "0.0"
                self.tree.insert("", tk.END, values=(filename, game_sys, file_type, size_kb, path))
            conn.close()
        except sqlite3.Error as e:
            messagebox.showerror("Database Error", f"Search failed: {e}")
            
    def open_selected_file(self, event=None):
        # ... (This function is the same as before)
        selected_item = self.tree.focus()
        if not selected_item: return
        file_path = self.tree.item(selected_item)['values'][4]
        try:
            if sys.platform == "win32": os.startfile(file_path)
            elif sys.platform == "darwin": subprocess.run(["open", file_path], check=True)
            else: subprocess.run(["xdg-open", file_path], check=True)
        except Exception as e:
            messagebox.showerror("Error", f"Could not open file:\n{e}")

if __name__ == "__main__":
    if "/path/to/" in LIBRARY_ROOT:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Configuration Error", "Please edit the script and set the LIBRARY_ROOT.")
    else:
        db_path = os.path.join(LIBRARY_ROOT, DB_FILE)
        app = SearchApp(db_path)
        app.mainloop()
