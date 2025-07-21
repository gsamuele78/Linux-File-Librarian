import sys
import subprocess

REQUIRED_MODULES = [
    ("magic", "python-magic"),
    ("fitz", "pymupdf"),
    ("rapidfuzz", "rapidfuzz"),
    ("psutil", "psutil"),
    ("tqdm", "tqdm"),
]

def check_and_install_dependencies():
    """
    Check for required modules and auto-install any that are missing.
    Exits the program if a dependency cannot be installed.
    """
    for mod, pipname in REQUIRED_MODULES:
        try:
            __import__(mod)
        except ImportError:
            print(f"[INFO] {mod} not found. Attempting to install {pipname}...", file=sys.stderr)
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', pipname])
                __import__(mod)
                print(f"[INFO] {mod} installed successfully.")
            except Exception as e:
                print(f"[FATAL] Could not install {pipname} automatically: {e}", file=sys.stderr)
                sys.exit(1)
