"""Utilities to detect and mark reversal pairs inside a dataset SQL table.

This module provides a single, focused function `detect_and_mark_reversals_sql`
that keeps responsibilities small and testable: validate input, ensure the
marker column exists, find reversal pairs using SQL, and mark the involved
rows. The implementation favors clarity, early returns, and safe SQL
identifier handling.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, List, Optional, Tuple

LOG = logging.getLogger(__name__)

# Small constants to avoid magic numbers/strings
REVERSAL_FLAG = "__is_reversal__"
HEADER_ROW_COLUMN = "header_row"
DATASETS_TABLE = "datasets"
DEFAULT_TOLERANCE = 1e-9


def _quote_ident(name: str) -> str:
    """Return a safely quoted SQL identifier for SQLite.

    This is minimal quoting suitable for building PRAGMA and DDL statements
    where identifiers cannot be parameterized.
    """
    return f'"{name.replace('"', '""')}"'


def _ensure_flag_column(cur, table: str) -> None:
    cur.execute(f"PRAGMA table_info({_quote_ident(table)})")
    cols = [r[1] for r in cur.fetchall()]
    if REVERSAL_FLAG not in cols:
        cur.execute(f"ALTER TABLE {_quote_ident(table)} ADD COLUMN {REVERSAL_FLAG} INTEGER")


def _get_header_row_if_present(cur, dataset_id: int) -> Optional[int]:
    try:
        cur.execute(f"PRAGMA table_info({_quote_ident(DATASETS_TABLE)})")
        dcols = [r[1] for r in cur.fetchall()]
        if HEADER_ROW_COLUMN in dcols:
            cur.execute(f"SELECT {HEADER_ROW_COLUMN} FROM {_quote_ident(DATASETS_TABLE)} WHERE id = ?", (dataset_id,))
            hr = cur.fetchone()
            if hr and isinstance(hr[0], int):
                return hr[0]
    except Exception:
        # Be conservative: if any error occurs, treat as header_row unknown
        LOG.debug("Unable to read header_row for dataset %s", dataset_id, exc_info=True)
    return None


def _validate_table_exists(cur, table: str) -> None:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    if not cur.fetchone():
        raise ValueError(f"Table not found: {table}")


def detect_and_mark_reversals_sql(conn, dataset_id: int, col_a: str, col_b: str, value_col: str, tolerance: float = DEFAULT_TOLERANCE) -> Dict[str, Any]:
    """Detect reversal pairs in `dataset_{id}` and mark the involved rows.

    A reversal pair (i, j) satisfies the conditions:
      - value in column `col_a` for row i equals column `col_b` for row j
      - value in column `col_b` for row i equals column `col_a` for row j
      - the numeric values in `value_col` sum to approximately zero

    Returns a dictionary with summary information and the concrete rows/pairs.
    """
    cur = conn.cursor()
    table = f"dataset_{int(dataset_id)}"

    _validate_table_exists(cur, table)

    # Ensure marker column exists; this is idempotent
    _ensure_flag_column(cur, table)

    header_row = _get_header_row_if_present(cur, dataset_id)

    # Ensure required columns exist in the dataset table
    cur.execute(f"PRAGMA table_info({_quote_ident(table)})")
    existing_cols = {r[1] for r in cur.fetchall()}
    for required in (col_a, col_b, value_col):
        if required not in existing_cols:
            conn.commit()
            return {"reversals": 0, "rows_marked": 0, "rows": [], "pairs": []}

    # Build safe fully-qualified column references for SQL
    a_col_a = f"a.{_quote_ident(col_a)}"
    a_col_b = f"a.{_quote_ident(col_b)}"
    a_val = f"a.{_quote_ident(value_col)}"
    b_col_a = f"b.{_quote_ident(col_a)}"
    b_col_b = f"b.{_quote_ident(col_b)}"
    b_val = f"b.{_quote_ident(value_col)}"

    # Use ABS(sum) <= ? so we can pass a tolerance parameter
    where_sum = f"ABS(COALESCE({a_val}, 0) + COALESCE({b_val}, 0)) <= ?"

    select_sql = (
        f"SELECT a.row_index, b.row_index FROM {_quote_ident(table)} a "
        f"JOIN {_quote_ident(table)} b ON {a_col_a} = {b_col_b} AND {a_col_b} = {b_col_a} "
        f"WHERE {where_sum} AND a.row_index < b.row_index"
    )

    params: List[Any] = [tolerance]
    if header_row is not None and isinstance(header_row, int) and header_row > 0:
        select_sql += " AND a.row_index > ? AND b.row_index > ?"
        params.extend([header_row, header_row])

    try:
        cur.execute(select_sql, tuple(params))
        pairs: List[Tuple[int, int]] = cur.fetchall()
    except Exception as exc:
        LOG.exception("Failed to execute reversal detection SQL for table %s", table)
        raise RuntimeError("Reversal detection query failed") from exc

    if not pairs:
        conn.commit()
        return {"reversals": 0, "rows_marked": 0, "rows": [], "pairs": []}

    rows_to_mark = {int(r) for pair in pairs for r in pair}

    placeholders = ",".join(["?"] * len(rows_to_mark))
    rows_param = tuple(sorted(rows_to_mark))

    try:
        if header_row is not None and isinstance(header_row, int) and header_row > 0:
            update_sql = f"UPDATE {_quote_ident(table)} SET {REVERSAL_FLAG} = 1 WHERE row_index IN ({placeholders}) AND row_index > ?"
            cur.execute(update_sql, rows_param + (header_row,))
        else:
            update_sql = f"UPDATE {_quote_ident(table)} SET {REVERSAL_FLAG} = 1 WHERE row_index IN ({placeholders})"
            cur.execute(update_sql, rows_param)
    except Exception as exc:
        LOG.exception("Failed to mark reversal rows for table %s", table)
        raise RuntimeError("Failed to mark reversal rows") from exc

    conn.commit()

    return {"reversals": len(pairs), "rows_marked": len(rows_to_mark), "rows": sorted(list(rows_to_mark)), "pairs": pairs}


__all__ = ["detect_and_mark_reversals_sql"]
