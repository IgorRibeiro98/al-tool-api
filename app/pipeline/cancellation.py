"""Cancellation helpers for the pipeline.

Provides a utility to mark rows as canceled based on an indicator column.
"""
from __future__ import annotations

from typing import Any

import pandas as pd


def mark_canceled(df: pd.DataFrame, column_name: str, canceled_value: Any) -> pd.DataFrame:
    """Create or replace column `__is_canceled__` on `df`.

    - Sets True where `df[column_name] == canceled_value`.
    - Sets False otherwise.

    The function modifies `df` (assigns the column) and returns it.

    Behavior notes:
    - If `column_name` does not exist in `df`, the function will add
      `__is_canceled__` filled with `False` (does not raise).
    - Comparisons handle missing values: any NA is treated as not canceled.
    - The result column is always boolean dtype.
    """
    # If the indicator column is not present, add the boolean column as all False
    if column_name not in df.columns:
        df["__is_canceled__"] = False
        df["__is_canceled__"] = df["__is_canceled__"].astype(bool)
        return df

    # Perform comparison; handle NA values safely
    try:
        mask = df[column_name] == canceled_value
    except Exception:
        # Fallback: compare string representations
        mask = df[column_name].astype(str) == str(canceled_value)

    # Replace NA in mask with False and ensure boolean dtype
    mask = mask.fillna(False).astype(bool)
    df["__is_canceled__"] = mask
    return df


__all__ = ["mark_canceled"]
