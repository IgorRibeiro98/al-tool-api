"""Database helpers.

Provides a minimal `get_connection` and `init_db` functions.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from .config import settings


def get_connection() -> sqlite3.Connection:
    """Return a new sqlite3 connection using configured DB path."""
    return sqlite3.connect(str(settings.DB_PATH))


def init_db() -> None:
    """Ensure the database file exists by creating parent directories
    and opening/closing a connection if the file is missing.

    This function does NOT create any tables — it only guarantees the
    file and directories exist so later tasks can create schema.
    """
    db_path = Path(settings.DB_PATH)
    db_dir = db_path.parent
    if not db_dir.exists():
        db_dir.mkdir(parents=True, exist_ok=True)

    if not db_path.exists():
        # Connecting will create the sqlite file on disk.
        conn = sqlite3.connect(str(db_path))
        conn.close()


__all__ = ["get_connection", "init_db"]
