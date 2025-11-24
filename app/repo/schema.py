"""Database schema for repository layer.

Defines SQL for the `datasets` table and helpers to create tables.
"""
from __future__ import annotations

from typing import Any
import sqlite3


DATASETS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS datasets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    base_type TEXT,
    format TEXT,
    storage_path TEXT,
    status TEXT,
    row_count INTEGER,
    created_at TEXT
);
"""


DATASET_COLUMNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dataset_columns (
    id INTEGER PRIMARY KEY,
    dataset_id INTEGER,
    name TEXT,
    logical_name TEXT,
    data_type TEXT,
    is_value_column INTEGER,
    is_candidate_key INTEGER
);
"""


RECONCILIATION_CONFIGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reconciliation_configs (
    id INTEGER PRIMARY KEY,
    name TEXT,
    base_a_id INTEGER,
    base_b_id INTEGER,
    value_column_a TEXT,
    value_column_b TEXT,
    invert_a INTEGER,
    invert_b INTEGER,
    threshold REAL
);
"""


RECONCILIATION_RESULTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reconciliation_results (
    id INTEGER PRIMARY KEY,
    run_id INTEGER,
    dataset_id INTEGER,
    row_identifier TEXT,
    status TEXT,
    group_name TEXT,
    key_used TEXT,
    difference REAL
);
"""


RECONCILIATION_KEYS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reconciliation_keys (
    id INTEGER PRIMARY KEY,
    config_id INTEGER,
    name TEXT,
    base_a_columns TEXT,
    base_b_columns TEXT
);
"""


PREPROCESS_RUNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS preprocess_runs (
    id INTEGER PRIMARY KEY,
    dataset_id INTEGER,
    status TEXT,
    started_at TEXT,
    finished_at TEXT,
    summary_json TEXT
);
"""


RECONCILIATION_RUNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id INTEGER PRIMARY KEY,
    config_id INTEGER,
    status TEXT,
    started_at TEXT,
    finished_at TEXT,
    summary_json TEXT
);
"""


REVERSAL_CONFIGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reversal_configs (
    id INTEGER PRIMARY KEY,
    dataset_id INTEGER,
    column_a TEXT,
    column_b TEXT,
    value_column TEXT
);
"""


CANCELLATION_CONFIGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cancellation_configs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id INTEGER NOT NULL,
    indicator_column TEXT NOT NULL,
    canceled_value TEXT NOT NULL,
    active_value TEXT NOT NULL,
    UNIQUE(dataset_id)
);
"""


def create_tables(conn: sqlite3.Connection | Any) -> None:
    """Create required tables on the given SQLite connection.

    The function will execute DDL statements and commit the transaction.
    """
    cur = conn.cursor()
    cur.execute(DATASETS_TABLE_SQL)
    cur.execute(DATASET_COLUMNS_TABLE_SQL)
    cur.execute(RECONCILIATION_CONFIGS_TABLE_SQL)
    cur.execute(RECONCILIATION_RESULTS_TABLE_SQL)
    cur.execute(RECONCILIATION_KEYS_TABLE_SQL)
    cur.execute(PREPROCESS_RUNS_TABLE_SQL)
    cur.execute(RECONCILIATION_RUNS_TABLE_SQL)
    cur.execute(REVERSAL_CONFIGS_TABLE_SQL)
    cur.execute(CANCELLATION_CONFIGS_TABLE_SQL)
    conn.commit()

    # --- lightweight migration for new columns (header_row, header_col) ---
    try:
        cur.execute("PRAGMA table_info(datasets)")
        existing_cols = {r[1] for r in cur.fetchall()}
        alters = []
        if "header_row" not in existing_cols:
            alters.append("ALTER TABLE datasets ADD COLUMN header_row INTEGER")
        if "header_col" not in existing_cols:
            alters.append("ALTER TABLE datasets ADD COLUMN header_col TEXT")
        for stmt in alters:
            cur.execute(stmt)
        if alters:
            conn.commit()
    except Exception:
        # Do not fail overall table creation if migration cannot be applied.
        pass


__all__ = [
    "DATASETS_TABLE_SQL",
    "DATASET_COLUMNS_TABLE_SQL",
    "RECONCILIATION_CONFIGS_TABLE_SQL",
    "RECONCILIATION_RESULTS_TABLE_SQL",
    "RECONCILIATION_KEYS_TABLE_SQL",
    "PREPROCESS_RUNS_TABLE_SQL",
    "RECONCILIATION_RUNS_TABLE_SQL",
    "REVERSAL_CONFIGS_TABLE_SQL",
    "CANCELLATION_CONFIGS_TABLE_SQL",
    "create_tables",
]

