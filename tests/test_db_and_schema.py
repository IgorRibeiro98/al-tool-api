import sqlite3
from pathlib import Path
import os
import tempfile

from app.core.config import settings
from app.core.db import init_db, get_connection
from app.repo import schema


def test_init_db_creates_file_and_tables(tmp_path, monkeypatch):
    # point DB_PATH to a temp file
    temp_db = tmp_path / "test.db"
    monkeypatch.setattr(settings, "DB_PATH", str(temp_db))

    # ensure file does not exist
    if temp_db.exists():
        temp_db.unlink()

    init_db()
    assert temp_db.exists()

    conn = get_connection()
    try:
        # create tables using schema.create_tables
        schema.create_tables(conn)

        # check that datasets table exists
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='datasets'")
        assert cur.fetchone() is not None

        # check reconciliation_runs exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='reconciliation_runs'")
        assert cur.fetchone() is not None
    finally:
        conn.close()
