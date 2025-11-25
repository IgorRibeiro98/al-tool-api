"""Importer utilities for the pipeline.

Small, well-named helpers to read incoming files (Excel/CSV) and to load
preprocessed dataset CSVs while respecting stored header coordinates.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import logging
import sqlite3

import pandas as pd

from ..core.config import settings


LOG = logging.getLogger(__name__)

# Supported formats
EXCEL_FORMATS = {"excel", "xls", "xlsx"}
TEXT_FORMATS = {"txt", "csv"}


def _path_to_pathlike(path: str | Path) -> Path:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    return p


def load_excel(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    """Load an Excel file into a DataFrame.

    Raises FileNotFoundError when the path does not exist.
    """
    p = _path_to_pathlike(path)
    return pd.read_excel(p, **kwargs)


def load_csv(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    """Load a CSV/text file into a DataFrame.

    Raises FileNotFoundError when the path does not exist.
    """
    p = _path_to_pathlike(path)
    return pd.read_csv(p, **kwargs)


def load_fiscal(path: str | Path, fmt: str, **kwargs: Any) -> pd.DataFrame:
    """Dispatch loader depending on `fmt` (case-insensitive).

    `fmt` is expected to be a simple token like 'excel' or 'csv'.
    """
    fmt_low = (fmt or "").strip().lower()
    if fmt_low in EXCEL_FORMATS:
        return load_excel(path, **kwargs)
    if fmt_low in TEXT_FORMATS:
        return load_csv(path, **kwargs)
    raise ValueError(f"Unsupported fiscal format: {fmt}")


__all__ = ["load_excel", "load_csv", "load_fiscal"]


def load_dataset_dataframe(conn: sqlite3.Connection, dataset_id: int) -> pd.DataFrame:
    """Load dataset source file using metadata in `datasets` table.

    Queries `storage_path` and `format` and delegates to `load_fiscal`.
    """
    cur = conn.cursor()
    cur.execute("SELECT storage_path, format FROM datasets WHERE id = ?", (dataset_id,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Dataset not found: {dataset_id}")
    storage_path, fmt = row[0], row[1]
    return load_fiscal(storage_path, fmt)


def _get_dataset_header_coords(dataset_id: int) -> Dict[str, Optional[Any]]:
    """Return dict with header_row and header_col for a dataset or empty dict.

    This helper initializes DB access locally; it tolerates failures and
    returns empty dict on error (caller will fallback to default behavior).
    """
    try:
        from ..core.db import init_db, get_connection

        init_db()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(datasets)")
        cols = {r[1] for r in cur.fetchall()}
        if "header_row" not in cols and "header_col" not in cols:
            try:
                conn.close()
            except Exception:
                pass
            return {}

        cur.execute("SELECT header_row, header_col FROM datasets WHERE id = ?", (dataset_id,))
        row = cur.fetchone()
        try:
            conn.close()
        except Exception:
            pass
        if not row:
            return {}
        return {"header_row": row[0], "header_col": row[1]}
    except Exception:
        LOG.debug("Could not read header coords for dataset %s", dataset_id, exc_info=True)
        return {}


def load_preprocessed_dataset_dataframe(dataset_id: int) -> pd.DataFrame:
    """Load `storage/pre/<dataset_id>.csv` and promote header row if configured.

    If `datasets.header_row` is not set (or equals 0/1) the CSV is read normally.
    When a header_row is set (1-based), the module will read raw CSV and
    promote the specified row to the DataFrame header.
    """
    pre_path = Path(settings.STORAGE_PATH) / "pre" / f"{int(dataset_id)}.csv"
    if not pre_path.exists():
        raise FileNotFoundError(f"Preprocessed CSV not found for dataset {dataset_id}: {pre_path}")

    coords = _get_dataset_header_coords(dataset_id)
    header_row = coords.get("header_row")
    header_col = coords.get("header_col")

    # Normal load when no special header row is defined
    if not header_row or header_row in (0, 1):
        return pd.read_csv(pre_path)

    # Read raw CSV with no header and promote header_row (1-based)
    raw = pd.read_csv(pre_path, header=None)
    hr_index = int(header_row) - 1
    if hr_index >= len(raw.index):
        LOG.warning("Configured header_row %s out of bounds for dataset %s", header_row, dataset_id)
        return pd.read_csv(pre_path)

    new_header = raw.iloc[hr_index].tolist()
    df = raw.iloc[hr_index + 1 :].copy()
    df.columns = [str(c) for c in new_header]

    # Optionally slice columns starting at header_col if provided.
    if header_col:
        try:
            from ..repo.dataset_columns import _col_letter_to_index

            if isinstance(header_col, str) and header_col.isalpha():
                start_idx = _col_letter_to_index(header_col)
            else:
                start_idx = int(header_col) - 1
            if 0 <= start_idx < len(df.columns):
                df = df.iloc[:, start_idx:]
        except Exception:
            LOG.debug("Failed to apply header_col slice for dataset %s", dataset_id, exc_info=True)

    return df


__all__ = [
    "load_excel",
    "load_csv",
    "load_fiscal",
    "load_dataset_dataframe",
    "load_preprocessed_dataset_dataframe",
]
