import os
import sys
import sqlite3
import re
import unicodedata
from pathlib import Path
from rapidfuzz import fuzz

class Classifier:
    """
    An intelligent engine for categorizing files using a multi-tiered approach.
    Enhanced for multilingual/Unicode support and fuzzy matching.
    """
    def __init__(self, knowledge_db_path):
        self.conn = None
        self.product_cache = {}
        self.path_keywords = {}
        # Support loading knowledge base from a web URL if provided
        if knowledge_db_path.startswith("http://") or knowledge_db_path.startswith("https://"):
            local_tmp = "/tmp/knowledge.sqlite"
            try:
                import requests
                r = requests.get(knowledge_db_path, timeout=30)
                r.raise_for_status()
                with open(local_tmp, "wb") as f:
                    f.write(r.content)
                knowledge_db_path = local_tmp
            except Exception as e:
                print(f"[WARNING] Could not download remote knowledge base: {e}", file=sys.stderr)
                knowledge_db_path = None
        if knowledge_db_path and os.path.exists(knowledge_db_path):
            try:
                self.conn = sqlite3.connect(f"file:{knowledge_db_path}?mode=ro", uri=True)
                self.load_products_to_cache()
                self.load_path_keywords()
                self.load_alternate_keywords()
            except sqlite3.Error as e:
                print(f"[WARNING] Could not connect to knowledge base. TTRPG classification will be limited. Error: {e}", file=sys.stderr)
        else:
            print("[WARNING] Knowledge base not found. Run build_knowledgebase.sh for full TTRPG classification.", file=sys.stderr)

    def load_products_to_cache(self):
        if not self.conn: return
        cursor = self.conn.cursor()
        cursor.execute("SELECT product_code, title, game_system, edition, category FROM products")
        for code, title, system, edition, category in cursor.fetchall():
            if code:
                nkey = self.normalize_text(code)
                self.product_cache[nkey] = (system, edition, category)
            if title:
                nkey = self.normalize_text(title)
                self.product_cache[nkey] = (system, edition, category)

    def load_path_keywords(self):
        if not self.conn: return
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT game_system, edition FROM products WHERE game_system IS NOT NULL")
        for system, edition in cursor.fetchall():
            nsys = self.normalize_text(system)
            self.path_keywords[nsys] = system
            if edition:
                nedit = self.normalize_text(edition)
                self.path_keywords[nedit] = system

    def load_alternate_keywords(self):
        if not self.conn: return
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT alt_title, product_code, game_system, edition, category FROM alternate_titles")
            for alt_title, code, system, edition, category in cursor.fetchall():
                if alt_title:
                    nkey = self.normalize_text(alt_title)
                    self.product_cache[nkey] = (system, edition, category)
                    self.path_keywords[nkey] = system
        except sqlite3.Error:
            pass

    @staticmethod
    def normalize_text(text):
        if not text:
            return ''
        text = text.lower()
        text = unicodedata.normalize('NFKD', text)
        text = ''.join(c for c in text if not unicodedata.combining(c))
        text = re.sub(r'[^a-z0-9]', '', text)
        return text

    def _classify_by_filename(self, filename):
        clean_filename = self.normalize_text(filename)
        for title_key in sorted(self.product_cache.keys(), key=len, reverse=True):
            if title_key in clean_filename:
                return self.product_cache[title_key]
        for title_key in self.product_cache.keys():
            if fuzz.partial_ratio(clean_filename, title_key) >= 90:
                return self.product_cache[title_key]
        return None

    def _classify_by_path(self, full_path):
        if not self.path_keywords: return None
        for part in reversed(Path(full_path).parts[:-1]):
            clean_part = self.normalize_text(part)
            if clean_part in self.path_keywords:
                game_system = self.path_keywords[clean_part]
                return game_system, "From Folder", "Heuristic"
            for kw in self.path_keywords.keys():
                if fuzz.partial_ratio(clean_part, kw) >= 90:
                    game_system = self.path_keywords[kw]
                    return game_system, "From Folder", "Heuristic"
        return None

    def _classify_by_mimetype(self, mime_type):
        major_type = mime_type.split('/')[0]
        if major_type == 'video': return ('Media', 'Video', None)
        if major_type == 'audio': return ('Media', 'Audio', None)
        if major_type == 'image': return ('Media', 'Images', None)
        if 'zip' in mime_type or 'rar' in mime_type or '7z' in mime_type: return ('Archives', None, None)
        if 'pdf' in mime_type: return ('Documents', 'PDF', None)
        if 'msword' in mime_type or 'officedocument' in mime_type: return ('Documents', 'Office', None)
        if major_type == 'text': return ('Documents', 'Text', None)
        if 'application' in mime_type: return ('Software & Data', None, None)
        return None

    def classify(self, filename, full_path, mime_type):
        result = self._classify_by_filename(filename)
        if result: return result
        result = self._classify_by_path(full_path)
        if result: return result
        result = self._classify_by_mimetype(mime_type)
        if result: return result
        return ('Miscellaneous', None, None)

    def close(self):
        if self.conn: self.conn.close()
