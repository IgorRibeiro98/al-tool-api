"""Pipeline step: fill null values according to schema info.

This module provides two small responsibilities:
- `fill_nulls` operates on a pandas DataFrame and returns a new DataFrame
  with nulls / empty values normalized according to a simple `schema_info`.
- `fill_nulls_sql` applies the same rules using SQL updates against the
  `dataset_{id}` table.

Design goals: small helpers, named constants, input validation and clear
error messages. Keep transformations explicit to make testing straightforward.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional

import pandas as pd


LOG = logging.getLogger(__name__)

# Constants / defaults
STRING_TYPE = "string"
NUMBER_TYPE = "number"
STRING_REPLACEMENT = "NULL"
NUMBER_REPLACEMENT = 0


def _quote_identifier(name: str) -> str:
    """Return a double-quoted SQL identifier, escaping internal quotes.

    This is a minimal helper for building PRAGMA and UPDATE statements.
    """
    return f'"{name.replace('"', '""')}"'


def _ensure_schema(schema_info: Optional[Dict[str, str]]) -> Dict[str, str]:
    if not schema_info:
        return {}
    if not isinstance(schema_info, dict):
        raise TypeError("schema_info must be a dict[str, str]")
    return schema_info


def _replace_empty_strings(series: pd.Series, replacement: Any) -> pd.Series:
    """Replace None/NaN/empty-or-whitespace strings with `replacement`.

    Works safely when the Series has mixed dtypes.
    """
    # Fill NaN first
    out = series.fillna(replacement)
    # Replace empty/whitespace-only strings
    try:
        mask_empty = out.astype(str).str.strip() == ""
    except Exception:
        # Fallback: if conversion fails, return filled series
        return out
    out = out.where(~mask_empty, replacement)
    return out


def fill_nulls(df: pd.DataFrame, schema_info: Optional[Dict[str, str]]) -> pd.DataFrame:
    """Return a new DataFrame with nulls filled according to `schema_info`.

    The function is conservative: it only modifies columns listed in
    `schema_info`. Supported types: 'string' and 'number'. Other types are
    ignored (left unchanged).
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")

    schema = _ensure_schema(schema_info)
    if not schema:
        return df.copy()

    result = df.copy(deep=True)

    for col, raw_type in schema.items():
        if col not in result.columns:
            LOG.debug("fill_nulls: column %s not in DataFrame, skipping", col)
            continue

        t = (raw_type or "").strip().lower()
        if t == STRING_TYPE:
            result[col] = _replace_empty_strings(result[col], STRING_REPLACEMENT)
        elif t == NUMBER_TYPE:
            # Fill NaN with number replacement then convert empty strings
            filled = result[col].fillna(NUMBER_REPLACEMENT)
            try:
                mask_empty = filled.astype(str).str.strip() == ""
                filled = filled.where(~mask_empty, NUMBER_REPLACEMENT)
            except Exception:
                # If conversion fails, keep filled as-is
                pass
            result[col] = filled
        else:
            # unsupported types intentionally ignored
            LOG.debug("fill_nulls: unsupported type %s for column %s", raw_type, col)

    return result


def fill_nulls_sql(conn: Any, dataset_id: int, schema_info: Optional[Dict[str, str]]) -> Dict[str, int]:
    """Fill nulls in `dataset_{dataset_id}` using SQL updates.

    Returns counts: {"filled_string": int, "filled_number": int}.
    """
    if not hasattr(conn, "cursor"):
        raise TypeError("conn must be a DB-API connection with a cursor() method")
    table = f"dataset_{int(dataset_id)}"
    schema = _ensure_schema(schema_info)

    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    if not cur.fetchone():
        raise ValueError(f"Table not found: {table}")

    filled_string = 0
    filled_number = 0

    for col, raw_type in schema.items():
        t = (raw_type or "").strip().lower()
        quoted_col = _quote_identifier(col)
        quoted_table = _quote_identifier(table)

        if t == STRING_TYPE:
            sql = f"UPDATE {quoted_table} SET {quoted_col} = ? WHERE {quoted_col} IS NULL OR TRIM({quoted_col}) = ''"
            cur.execute(sql, (STRING_REPLACEMENT,))
            filled_string += cur.rowcount or 0
        elif t == NUMBER_TYPE:
            sql = f"UPDATE {quoted_table} SET {quoted_col} = ? WHERE {quoted_col} IS NULL OR TRIM({quoted_col}) = ''"
            cur.execute(sql, (NUMBER_REPLACEMENT,))
            filled_number += cur.rowcount or 0
        else:
            # ignore unsupported types
            continue

    conn.commit()
    return {"filled_string": int(filled_string), "filled_number": int(filled_number)}


__all__ = ["fill_nulls", "fill_nulls_sql"]
