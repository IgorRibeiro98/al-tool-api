"""Cancellation helpers for the pipeline.

This module provides two responsibilities:
- an in-memory helper `mark_canceled` that marks a pandas DataFrame;
- a SQL helper `mark_canceled_sql` that updates the SQLite `dataset_{id}` table.

The functions are written to be small, predictable and easy to test.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd


# Constants
PAGE_SIZE = 1000
HEADER_COL_NAME = "__is_canceled__"
TRUTHY_TOKENS = ("S", "1", "TRUE", "YES", "Y", "T")


def _ensure_bool_column(df: pd.DataFrame, column: str) -> None:
    """Ensure `column` exists on `df` as boolean dtype (in-place)."""
    if column not in df.columns:
        df[column] = False
    df[column] = df[column].astype(bool)


def mark_canceled(df: pd.DataFrame, indicator: str, canceled_value: Any) -> pd.DataFrame:
    """Mark rows in `df` as canceled based on `indicator == canceled_value`.

    Returns the modified DataFrame (same object). Missing indicator column
    results in a boolean column set to False.
    """
    _ensure_bool_column(df, HEADER_COL_NAME)

    if indicator not in df.columns:
        return df

    try:
        mask = df[indicator] == canceled_value
    except Exception:
        mask = df[indicator].astype(str) == str(canceled_value)

    df[HEADER_COL_NAME] = mask.fillna(False).astype(bool)
    return df


def _table_exists(cur, table: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def _get_table_columns(cur, table: str) -> Tuple[str, ...]:
    cur.execute(f"PRAGMA table_info(\"{table}\")")
    return tuple(r[1] for r in cur.fetchall())


def _get_dataset_header_row(cur, dataset_id: int) -> Optional[int]:
    try:
        cur.execute("PRAGMA table_info(datasets)")
        cols = {r[1] for r in cur.fetchall()}
        if "header_row" in cols:
            cur.execute("SELECT header_row FROM datasets WHERE id = ?", (dataset_id,))
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None
    return None


def _safe_identifier(name: str) -> str:
    """Return a double-quoted SQL identifier, escaping double quotes inside the name."""
    return f'"{name.replace('"', '""')}"'


def mark_canceled_sql(conn, dataset_id: int, indicator_column: str, canceled_value: Any) -> Dict[str, int]:
    """Mark rows as canceled in `dataset_{dataset_id}` using SQL.

    Returns a dict with `canceled_rows` and `active_rows` counts (only data rows
    are considered when `datasets.header_row` is set).
    """
    cur = conn.cursor()
    table = f"dataset_{int(dataset_id)}"

    if not _table_exists(cur, table):
        raise ValueError(f"Table not found: {table}")

    cols = set(_get_table_columns(cur, table))

    # ensure flag column exists
    if HEADER_COL_NAME not in cols:
        cur.execute(f"ALTER TABLE { _safe_identifier(table) } ADD COLUMN {HEADER_COL_NAME} INTEGER")
        cols.add(HEADER_COL_NAME)

    # If indicator column does not exist, just return current counts
    if indicator_column not in cols:
        header_row = _get_dataset_header_row(cur, dataset_id)
        canceled, active = _count_flags(cur, table, header_row)
        conn.commit()
        return {"canceled_rows": canceled, "active_rows": active}

    # Normalize target and choose matching strategy
    target = str(canceled_value or "").strip().upper()
    header_row = _get_dataset_header_row(cur, dataset_id)
    row_filter_sql, params_tail = (" AND row_index > ?", (int(header_row),)) if header_row and header_row > 0 else ("", ())

    safe_col = _safe_identifier(indicator_column)

    if target in TRUTHY_TOKENS:
        placeholders = ",".join(["?" for _ in TRUTHY_TOKENS])
        sql = f"UPDATE { _safe_identifier(table) } SET {HEADER_COL_NAME} = 1 WHERE UPPER(TRIM({safe_col})) IN ({placeholders}){row_filter_sql}"
        cur.execute(sql, tuple(TRUTHY_TOKENS) + params_tail)
    else:
        sql = f"UPDATE { _safe_identifier(table) } SET {HEADER_COL_NAME} = 1 WHERE UPPER(TRIM({safe_col})) = ?{row_filter_sql}"
        cur.execute(sql, (target,) + params_tail)

    # Normalize NULL flags to 0 only for data rows
    if row_filter_sql:
        cur.execute(f"UPDATE { _safe_identifier(table) } SET {HEADER_COL_NAME} = 0 WHERE {HEADER_COL_NAME} IS NULL AND row_index > ?", (int(header_row),))
    else:
        cur.execute(f"UPDATE { _safe_identifier(table) } SET {HEADER_COL_NAME} = 0 WHERE {HEADER_COL_NAME} IS NULL")

    canceled, active = _count_flags(cur, table, header_row)
    conn.commit()
    return {"canceled_rows": canceled, "active_rows": active}


def _count_flags(cur, table: str, header_row: Optional[int]) -> Tuple[int, int]:
    """Return (canceled_count, active_count) considering header_row when present."""
    if header_row and header_row > 0:
        cur.execute(f"SELECT COUNT(*) FROM { _safe_identifier(table) } WHERE {HEADER_COL_NAME} = 1 AND row_index > ?", (int(header_row),))
        canceled = int(cur.fetchone()[0] or 0)
        cur.execute(f"SELECT COUNT(*) FROM { _safe_identifier(table) } WHERE {HEADER_COL_NAME} = 0 AND row_index > ?", (int(header_row),))
        active = int(cur.fetchone()[0] or 0)
    else:
        cur.execute(f"SELECT COUNT(*) FROM { _safe_identifier(table) } WHERE {HEADER_COL_NAME} = 1")
        canceled = int(cur.fetchone()[0] or 0)
        cur.execute(f"SELECT COUNT(*) FROM { _safe_identifier(table) } WHERE {HEADER_COL_NAME} = 0")
        active = int(cur.fetchone()[0] or 0)
    return canceled, active


__all__ = ["mark_canceled", "mark_canceled_sql"]
