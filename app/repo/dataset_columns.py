"""Repository helpers for dataset columns.

Provides `save_detected_columns` which inserts one row per DataFrame
column into the `dataset_columns` table.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union
import sqlite3

import pandas as pd
from pandas.api import types as pdtypes


def _col_letter_to_index(letter: str) -> int:
    """Convert Excel-style column letter (A, B, ..., Z, AA, AB, ...) to 0-based index.

    Returns 0 for 'A', 7 for 'H', etc.
    """
    s = (letter or "").strip().upper()
    if not s:
        raise ValueError("Empty column letter")
    idx = 0
    for ch in s:
        if not ('A' <= ch <= 'Z'):
            raise ValueError(f"Invalid column letter: {letter}")
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx - 1


def save_detected_columns(
    conn: Any,
    dataset_id: int,
    df: pd.DataFrame,
    col_types: Dict[str, str],
    header_row: int = 1,
    header_col: Optional[Union[str, int]] = None,
) -> List[int]:
    """Persist detected columns for a dataset.

        Inserts one row per column into `dataset_columns`.

        Behavior:
        - By default (header_row=1 and header_col=None) the function behaves
            as before: it iterates `df.columns` and persists each column name.
        - If `header_row` and/or `header_col` are provided, the function will
            read header values from the DataFrame at the given row/column
            coordinates. `header_row` is 1-based (1 = first row). `header_col`
            can be a column letter (e.g. 'H') or a 1-based integer index. The
            function will collect headers starting at that column until the
            last non-empty header in the same row.

        The `col_types` mapping is used when a header name matches a key in
        the mapping; otherwise the `data_type` defaults to 'string'.

    Returns a list of inserted row ids.
    """
    cur = conn.cursor()
    inserted_ids: List[int] = []

    # Determine header names and their corresponding column indexes
    headers: List[str] = []
    col_indexes: List[int] = []

    # Default (existing) behavior: use df.columns
    if (header_row == 1 or header_row is None) and header_col is None:
        # If column names contain NaN/empty values, attempt to auto-detect a header
        # row within the first few rows. Otherwise, use columns but replace
        # NaN/empty names with generated names.
        blank_cols = [c for c in df.columns if c is None or (isinstance(c, float) and pd.isna(c)) or str(c).strip() == "" or str(c).startswith("Unnamed")]
        if blank_cols and len(blank_cols) > 0:
            # attempt to find a header row among the first N rows
            max_rows = min(10, len(df.index))
            best_row = None
            best_count = -1
            for r in range(0, max_rows):
                row_vals = list(df.iloc[r].values)
                non_empty = sum(1 for v in row_vals if v is not None and not (isinstance(v, float) and pd.isna(v)) and str(v).strip() != "")
                if non_empty > best_count:
                    best_count = non_empty
                    best_row = r

            # Heuristic: accept best_row if it has more than 30% non-empty cells
            if best_row is not None and best_count >= max(1, int(0.3 * len(df.columns))):
                # Use this row as header (row index is 0-based)
                row_values = list(df.iloc[best_row].values)
                last_idx = -1
                for j in range(0, len(row_values)):
                    v = row_values[j]
                    if v is not None and not (isinstance(v, float) and pd.isna(v)) and str(v).strip() != "":
                        last_idx = j
                if last_idx >= 0:
                    for j in range(0, last_idx + 1):
                        raw = row_values[j]
                        name = str(raw).strip() if raw is not None and not (isinstance(raw, float) and pd.isna(raw)) else f"column_{j}"
                        uniq_name = name
                        suffix = 1
                        while uniq_name in headers:
                            uniq_name = f"{name}_{suffix}"
                            suffix += 1
                        headers.append(uniq_name)
                        col_indexes.append(j)
            else:
                # fallback: use df.columns but sanitize NaN/Unnamed
                for i, col in enumerate(df.columns):
                    if col is None or (isinstance(col, float) and pd.isna(col)) or str(col).strip() == "" or str(col).startswith("Unnamed"):
                        name = f"column_{i}"
                    else:
                        name = str(col).strip()
                    uniq_name = name
                    suffix = 1
                    while uniq_name in headers:
                        uniq_name = f"{name}_{suffix}"
                        suffix += 1
                    headers.append(uniq_name)
                    col_indexes.append(i)
        else:
            for i, col in enumerate(df.columns):
                headers.append(str(col))
                col_indexes.append(i)
    else:
        # Compute start column index
        if header_col is None:
            start_idx = 0
        elif isinstance(header_col, str):
            start_idx = _col_letter_to_index(header_col)
        else:
            # integer provided: accept 1-based or 0-based (assume 1-based if >=1)
            try:
                ival = int(header_col)
            except Exception:
                raise ValueError(f"Invalid header_col value: {header_col}")
            start_idx = ival - 1 if ival > 0 else ival

        # header_row is 1-based
        hr = int(header_row) - 2 if header_row and header_row > 0 else 0

        # Ensure DataFrame has enough columns
        ncols = len(df.columns)

        if hr < 0 or hr >= len(df.index):
            raise IndexError(f"header_row {header_row} is out of range for DataFrame with {len(df.index)} rows")

        # Try to access header values in the provided row by position
        # Use iloc for robust positional indexing
        row_values = list(df.iloc[hr].values)

        # Find last non-empty column starting from start_idx
        last_idx = start_idx - 1
        for j in range(start_idx, len(row_values)):
            v = row_values[j]
            if v is not None and (not (isinstance(v, float) and pd.isna(v))) and str(v).strip() != "":
                last_idx = j

        if last_idx < start_idx:
            # no headers found in the requested row/cols
            raise ValueError(f"No header values found starting at column {header_col} on row {header_row}")

        for j in range(start_idx, last_idx + 1):
            raw = row_values[j]
            name = str(raw).strip() if raw is not None and not (isinstance(raw, float) and pd.isna(raw)) else f"column_{j}"
            # ensure uniqueness
            uniq_name = name
            suffix = 1
            while uniq_name in headers:
                uniq_name = f"{name}_{suffix}"
                suffix += 1
            headers.append(uniq_name)
            col_indexes.append(j)

    # Persist the detected headers. Infer data type per series position to avoid
    # relying on `col_types` keys that may not match stringified header names.
    for idx, header_name in zip(col_indexes, headers):
        logical_name = header_name
        # infer data type from the column series
        try:
            ser = df.iloc[:, idx]
            if pdtypes.is_numeric_dtype(ser):
                data_type = "number"
            elif pdtypes.is_datetime64_any_dtype(ser) or pdtypes.is_timedelta64_dtype(ser):
                data_type = "date"
            else:
                data_type = "string"
        except Exception:
            data_type = col_types.get(header_name, "string")

        is_value_column = 1 if data_type == "number" else 0
        is_candidate_key = 0

        cur.execute(
            """
            INSERT INTO dataset_columns (
                dataset_id, name, logical_name, data_type, is_value_column, is_candidate_key
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (dataset_id, header_name, logical_name, data_type, is_value_column, is_candidate_key),
        )
        inserted_ids.append(cur.lastrowid)

    conn.commit()
    return inserted_ids


def get_dataset_headers(conn: Any, dataset_id: int) -> List[str]:
    """Return ordered header names for a dataset from `dataset_columns`.

    Prefers `logical_name` when present and non-empty, otherwise falls back to
    the raw `name`. Results are ordered by the insertion order (id).
    Returns an empty list if no rows are found.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT name, logical_name FROM dataset_columns WHERE dataset_id = ? ORDER BY id",
        (dataset_id,),
    )
    rows = cur.fetchall()
    headers: List[str] = []
    for name, logical in rows:
        if logical is not None and str(logical).strip() != "":
            headers.append(str(logical))
        else:
            headers.append(str(name) if name is not None else "")
    return headers


__all__ = ["save_detected_columns", "get_dataset_headers"]
