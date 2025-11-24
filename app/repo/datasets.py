"""Repository helpers for dataset records.

Provides a minimal function to insert a dataset record into the
`datasets` table and return its generated id.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
import sqlite3


def create_dataset_record(
    conn: sqlite3.Connection | Any,
    name: str,
    base_type: str,
    format: str,
    storage_path: str,
    header_row: int | None = None,
    header_col: str | None = None,
) -> int:
    """Insert a new dataset record and return the inserted id.

    The function commits the transaction.
    """
    created_at = datetime.utcnow().isoformat()
    cur = conn.cursor()
    # Insert including optional header_row/header_col if columns exist (migration may have run).
    # Determine columns present.
    try:
        cur.execute("PRAGMA table_info(datasets)")
        cols = {r[1] for r in cur.fetchall()}
    except Exception:
        cols = {"name","base_type","format","storage_path","status","row_count","created_at"}

    if "header_row" in cols and "header_col" in cols:
        cur.execute(
            """
            INSERT INTO datasets (name, base_type, format, storage_path, status, row_count, created_at, header_row, header_col)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (name, base_type, format, storage_path, None, None, created_at, header_row, header_col),
        )
    else:
        cur.execute(
            """
            INSERT INTO datasets (name, base_type, format, storage_path, status, row_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (name, base_type, format, storage_path, None, None, created_at),
        )
    conn.commit()
    return cur.lastrowid


__all__ = ["create_dataset_record"]
