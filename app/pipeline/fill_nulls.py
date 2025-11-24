"""Pipeline step: fill null values according to schema info.

This module provides `fill_nulls(df, schema_info)` which fills missing
values based on a simple schema mapping: strings -> "NULL", numbers -> 0.
"""
from __future__ import annotations

from typing import Dict

import pandas as pd


def fill_nulls(df: pd.DataFrame, schema_info: Dict[str, str]) -> pd.DataFrame:
    """Return a new DataFrame with nulls filled according to `schema_info`.

    - For columns mapped to 'string' replace nulls with the literal "NULL".
    - For columns mapped to 'number' replace nulls with 0.

    Columns not present in `schema_info` are left unchanged.
    """
    result = df.copy()
    for col, col_type in schema_info.items():
        if col not in result.columns:
            continue
        t = (col_type or "").strip().lower()
        if t == "string":
            result[col] = result[col].fillna("NULL")
        elif t == "number":
            result[col] = result[col].fillna(0)
        # other types (e.g., date) are not modified here
    return result


__all__ = ["fill_nulls"]
