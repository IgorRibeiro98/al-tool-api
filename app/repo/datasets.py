"""Repository helpers for dataset records.

Provides a minimal function to insert a dataset record into the
`datasets` table and return its generated id.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
import sqlite3
import pandas as pd
from pandas.api import types as ptypes


def create_dataset_record(
    conn: Any,
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


def ingest_dataset_to_sql(conn: sqlite3.Connection, dataset_id: int, df: pd.DataFrame) -> dict:
    """Ingest a pandas DataFrame into a dedicated SQLite table `dataset_{id}`.

    Rules:
      - Create table `dataset_{id}` with `row_index INTEGER PRIMARY KEY AUTOINCREMENT` and
        one column per DataFrame column mapped to SQLite types (TEXT/REAL).
      - Dates (datetime dtypes) are converted to ISO8601 strings.
      - Populate the table with the DataFrame contents.
      - Return {"dataset_id": id, "rows": number_of_rows}

    The function uses parameterized INSERTs and commits the transaction.
    """
    cur = conn.cursor()
    table = f"dataset_{int(dataset_id)}"

    # Ensure column names are strings and make them unique if duplicates exist
    orig_cols = [str(c) for c in df.columns]
    seen: dict = {}
    cols: list = []
    for c in orig_cols:
        if c in seen:
            seen[c] += 1
            newc = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
            newc = c
        cols.append(newc)

    if cols != orig_cols:
        df = df.copy()
        df.columns = cols
    else:
        cols = orig_cols

    # Infer SQLite column types and prepare any necessary conversions
    col_types: dict = {}
    for c in cols:
        dtype = df[c].dtype if c in df.columns else object
        if ptypes.is_integer_dtype(dtype) or ptypes.is_float_dtype(dtype) or ptypes.is_numeric_dtype(dtype):
            col_types[c] = "REAL"
        elif ptypes.is_datetime64_any_dtype(dtype):
            col_types[c] = "TEXT"
            # convert datetimes to ISO strings in-place
            df[c] = df[c].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        else:
            col_types[c] = "TEXT"

    # Drop existing table and create fresh one with desired schema
    cur.execute(f"DROP TABLE IF EXISTS \"{table}\"")
    cols_sql = ",\n".join([f'"{c}" {col_types[c]}' for c in cols])
    create_sql = f"CREATE TABLE \"{table}\" (\n    row_index INTEGER PRIMARY KEY AUTOINCREMENT,\n    {cols_sql}\n)"
    cur.execute(create_sql)

    # Prepare insert statement
    col_list = ", ".join([f'"{c}"' for c in cols])
    placeholders = ",".join(["?" for _ in cols])
    insert_sql = f"INSERT INTO \"{table}\" ({col_list}) VALUES ({placeholders})"

    # Prepare rows for insertion, converting NaN to None and ensuring types
    to_insert = []
    for _, row in df.iterrows():
        vals = []
        for c in cols:
            v = row.get(c) if c in row.index else None
            if pd.isna(v):
                vals.append(None)
            else:
                if col_types[c] == "REAL":
                    try:
                        vals.append(float(v))
                    except Exception:
                        vals.append(None)
                else:
                    vals.append(str(v))
        to_insert.append(tuple(vals))

    if to_insert:
        cur.executemany(insert_sql, to_insert)
    conn.commit()

    return {"dataset_id": int(dataset_id), "rows": len(to_insert)}

__all__.append("ingest_dataset_to_sql")
