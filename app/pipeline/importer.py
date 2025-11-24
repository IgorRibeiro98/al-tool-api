"""Excel importer utilities for the pipeline.

This module provides a minimal `load_excel` function that wraps
`pandas.read_excel` and returns a DataFrame.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import sqlite3
from ..core.config import settings



def load_excel(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    """Load an Excel file at `path` into a pandas DataFrame.

    Parameters:
    - path: file path to the Excel file (str or Path).
    - **kwargs: forwarded to `pandas.read_excel`.

    Raises `FileNotFoundError` if the file does not exist.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Excel file not found: {p}")

    return pd.read_excel(p, **kwargs)


def load_txt(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    """Load a text/CSV file at `path` into a pandas DataFrame using `read_csv`.

    Parameters:
    - path: file path to the text file (str or Path).
    - **kwargs: forwarded to `pandas.read_csv`.

    Raises `FileNotFoundError` if the file does not exist.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Text file not found: {p}")

    return pd.read_csv(p, **kwargs)


def load_fiscal(path: str | Path, format: str, **kwargs: Any) -> pd.DataFrame:
    """Load a fiscal base according to `format`.

    - if `format` indicates an Excel file (xls/xlsx/excel) -> use `load_excel`
    - if `format` indicates a text/csv file -> use `load_txt`

    Raises `ValueError` for unsupported formats.
    """
    fmt = (format or "").strip().lower()
    if fmt in ("excel", "xls", "xlsx"):
        return load_excel(path, **kwargs)
    if fmt in ("txt", "csv"):
        return load_txt(path, **kwargs)
    raise ValueError(f"Unsupported fiscal format: {format}")


__all__ = ["load_excel", "load_txt", "load_fiscal"]


def load_dataset_dataframe(conn: sqlite3.Connection, dataset_id: int) -> pd.DataFrame:
    """Load a dataset DataFrame by `dataset_id` using the repository `conn`.

    The function queries the `datasets` table for `storage_path` and `format`,
    then delegates to `load_fiscal` to load and return a DataFrame.

    Raises `ValueError` if the dataset is not found.
    """
    cur = conn.cursor()
    cur.execute("SELECT storage_path, format FROM datasets WHERE id = ?", (dataset_id,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Dataset not found: {dataset_id}")

    storage_path, fmt = row[0], row[1]
    return load_fiscal(storage_path, fmt)


def load_preprocessed_dataset_dataframe(dataset_id: int) -> pd.DataFrame:
    """Load a preprocessed DataFrame saved under `storage/pre/<dataset_id>.csv`.

    Raises `FileNotFoundError` if the preprocessed CSV does not exist.
    """
    pre_path = Path(settings.STORAGE_PATH) / "pre" / f"{int(dataset_id)}.csv"
    if not pre_path.exists():
        raise FileNotFoundError(f"Preprocessed CSV not found for dataset {dataset_id}: {pre_path}")
    # Attempt to apply stored header_row/header_col if present in datasets table.
    try:
        from ..core.db import init_db, get_connection
        init_db()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(datasets)")
        cols = {r[1] for r in cur.fetchall()}
        header_row = None
        header_col = None
        if "header_row" in cols or "header_col" in cols:
            cur.execute("SELECT header_row, header_col FROM datasets WHERE id = ?", (dataset_id,))
            row = cur.fetchone()
            if row:
                header_row, header_col = row[0], row[1]
        try:
            conn.close()
        except Exception:
            pass
    except Exception:
        header_row = None
        header_col = None

    # If no special header row, load normally.
    if not header_row or header_row in (0, 1):
        return pd.read_csv(pre_path)

    # header_row is 1-based. Read raw, then promote the specified row to header.
    raw = pd.read_csv(pre_path, header=None)
    hr_index = int(header_row) - 1
    if hr_index >= len(raw.index):
        # fallback
        return pd.read_csv(pre_path)
    new_header = raw.iloc[hr_index].tolist()
    df = raw.iloc[hr_index + 1 :].copy()
    df.columns = [str(c) for c in new_header]

    # Optionally slice columns starting at header_col if provided.
    if header_col:
        try:
            from ..repo.dataset_columns import _col_letter_to_index
            if header_col.isalpha():
                start_idx = _col_letter_to_index(header_col)
            else:
                start_idx = int(header_col) - 1
            if 0 <= start_idx < len(df.columns):
                df = df.iloc[:, start_idx:]
        except Exception:
            pass
    return df


__all__ = ["load_excel", "load_txt", "load_fiscal", "load_preprocessed_dataset_dataframe"]
