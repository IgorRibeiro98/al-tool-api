import sqlite3
from pathlib import Path

from app.core.config import settings
from app.core.db import init_db, get_connection
from app.repo import schema
from app.repo.datasets import create_dataset_record


def test_create_dataset_record_inserts_and_returns_id(tmp_path, monkeypatch):
    # Setup an isolated DB
    temp_db = tmp_path / "test.db"
    monkeypatch.setattr(settings, "DB_PATH", str(temp_db))

    init_db()
    conn = get_connection()
    try:
        schema.create_tables(conn)
        dataset_id = create_dataset_record(conn, "file.csv", "fiscal", "csv", "/some/path/file.csv")
        assert isinstance(dataset_id, int)

        cur = conn.cursor()
        cur.execute("SELECT id, name, base_type, format, storage_path FROM datasets WHERE id = ?", (dataset_id,))
        row = cur.fetchone()
        assert row is not None
        assert row[1] == "file.csv"
        assert row[2] == "fiscal"
        assert row[3] == "csv"
        assert row[4] == "/some/path/file.csv"
    finally:
        conn.close()
