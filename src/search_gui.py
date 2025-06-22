import os
import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
import subprocess
import sys

# --- CONFIGURATION ---
# This should point to the unified library you created.
# The script will automatically look for the index file inside this directory.
LIBRARY_ROOT = "/path/to/your/new_unified_library"
DB_FILE = "library_index.sqlite"

# --- APPLICATION CLASS ---

class SearchApp(tk.Tk):
    def __init__(self, db_path):
        super().__init__()

        if not os.path.exists(db_path):
            messagebox.showerror("Error", f"Database not found at:\n{db_path}\n\nPlease run the librarian.py script first to build the library.")
            self.destroy()
            return

        self.db_path = db_path
        self.title("Library Search")
        self.geometry("800x600")

        # --- Widgets ---
        self.search_frame = ttk.Frame(self)
        self.search_frame.pack(fill=tk.X, padx=10, pady=10)

        self.search_label = ttk.Label(self.search_frame, text="Search Filename:")
        self.search_label.pack(side=tk.LEFT, padx=(0, 5))

        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(self.search_frame, textvariable=self.search_var, width=50)
        self.search_entry.pack(fill=tk.X, expand=True, side=tk.LEFT, padx=5)
        self.search_entry.bind("<Return>", self.perform_search)

        self.search_button = ttk.Button(self.search_frame, text="Search", command=self.perform_search)
        self.search_button.pack(side=tk.LEFT, padx=5)

        # --- Results Treeview ---
        self.tree_frame = ttk.Frame(self)
        self.tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        self.tree = ttk.Treeview(self.tree_frame, columns=("filename", "type", "size", "path"), show="headings")
        self.tree.heading("filename", text="Filename")
        self.tree.heading("type", text="File Type")
        self.tree.heading("size", text="Size (KB)")
        self.tree.heading("path", text="Full Path")

        # Column widths
        self.tree.column("filename", width=300)
        self.tree.column("type", width=150)
        self.tree.column("size", width=80, anchor=tk.E)
        self.tree.column("path", width=400)

        # Scrollbars
        vsb = ttk.Scrollbar(self.tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self.tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.tree.bind("<Double-1>", self.open_selected_file) # Double-click to open

        # --- Action Buttons ---
        self.button_frame = ttk.Frame(self)
        self.button_frame.pack(fill=tk.X, padx=10, pady=(0,10))
        
        self.open_button = ttk.Button(self.button_frame, text="Open Selected File", command=self.open_selected_file)
        self.open_button.pack(side=tk.RIGHT)

    def perform_search(self, event=None):
        """Executes the search query against the database."""
        query = self.search_var.get().strip()
        
        # Clear previous results
        for item in self.tree.get_children():
            self.tree.delete(item)

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Use LIKE for partial matching. Add '%' wildcards.
            search_pattern = f"%{query}%"
            cursor.execute(
                "SELECT filename, type, size, path FROM files WHERE filename LIKE ? ORDER BY filename",
                (search_pattern,)
            )

            for row in cursor.fetchall():
                filename, file_type, size_bytes, path = row
                size_kb = f"{size_bytes / 1024:.2f}"
                self.tree.insert("", tk.END, values=(filename, file_type, size_kb, path))

            conn.close()
        except sqlite3.Error as e:
            messagebox.showerror("Database Error", f"An error occurred: {e}")

    def open_selected_file(self, event=None):
        """Opens the selected file using the system's default application."""
        selected_item = self.tree.focus()
        if not selected_item:
            messagebox.showwarning("No Selection", "Please select a file to open.")
            return
            
        file_details = self.tree.item(selected_item)
        file_path = file_details['values'][3] # Path is the 4th value

        try:
            if sys.platform == "win32":
                os.startfile(file_path)
            elif sys.platform == "darwin": # macOS
                subprocess.run(["open", file_path], check=True)
            else: # linux and other UNIX
                subprocess.run(["xdg-open", file_path], check=True)
        except Exception as e:
            messagebox.showerror("Error", f"Could not open file:\n{file_path}\n\nReason: {e}")

# --- Main execution block ---
if __name__ == "__main__":
    if "/path/to/" in LIBRARY_ROOT:
        # Create a dummy window to show the error message if path is not set
        root = tk.Tk()
        root.withdraw() # Hide the main window
        messagebox.showerror("Configuration Error", "Please edit the script and set the LIBRARY_ROOT variable.")
    else:
        db_path = os.path.join(LIBRARY_ROOT, DB_FILE)
        app = SearchApp(db_path)
        app.mainloop()
