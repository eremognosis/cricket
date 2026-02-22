#  =========================================================
#  FILE  : `metaregis.py`
#  DESCRIPTION : This module defines the MetadataRegistry class, which provides a simple interface for tracking the processing status of files based on their content hash. It uses an SQLite database to store file paths, their last processed hash, category, and timestamp. This allows other scripts to check if a file has already been processed and to mark files as processed after handling them.
# 
# CREATED BY : RAJ
# LAST UPDATED : 2026-02-22
#  =========================================================


import sqlite3
import hashlib
from contextlib import contextmanager
import os

class MetadataRegistry:
    def __init__(self, db_path='./data/stageddata/registry.db'):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._bootstrap()

    def _bootstrap(self):
        with self._get_conn() as conn:
            conn.execute('PRAGMA journal_mode=WAL;') 
            conn.execute('PRAGMA synchronous=NORMAL;')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS file_registry (
                    file_path TEXT PRIMARY KEY,
                    category TEXT,
                    last_hash TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def get_file_hash(self, filepath):
        hasher = hashlib.md5()
        try:
            with open(filepath, 'rb') as f:
                hasher.update(f.read())
            return hasher.hexdigest()
        except Exception:
            return None

    def is_processed(self, file_path, current_hash):
        with self._get_conn() as conn:
            cursor = conn.execute("SELECT last_hash FROM file_registry WHERE file_path = ?", (file_path,))
            row = cursor.fetchone()
            return row is not None and row[0] == current_hash

    def mark_processed(self, file_path, category, current_hash):
        with self._get_conn() as conn:
            conn.execute('''
                INSERT INTO file_registry (file_path, category, last_hash, processed_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(file_path) DO UPDATE SET 
                    last_hash = excluded.last_hash,
                    processed_at = CURRENT_TIMESTAMP
            ''', (file_path, category, current_hash))