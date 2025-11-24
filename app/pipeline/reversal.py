"""Reversal detection utilities for the pipeline.

Provides a function to detect pairs of rows that are reversals of each
other on two key columns and whose `value_col` values sum to zero.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd


def detect_reversals(df: pd.DataFrame, colA: str, colB: str, value_col: str) -> List[Tuple[Any, Any]]:
    """Detect reversal pairs in `df`.

    A reversal pair is defined as two rows i and j such that:
      - df.loc[i, colA] == df.loc[j, colB]
      - df.loc[i, colB] == df.loc[j, colA]
      - df.loc[i, value_col] + df.loc[j, value_col] == 0 (within numeric tolerance)

    Returns a list of tuples `(idx_i, idx_j)` where `idx_*` are the original
    DataFrame index labels. Each pair is reported once with the smaller
    index first (based on Python ordering of the labels).
    """
    if colA not in df.columns or colB not in df.columns or value_col not in df.columns:
        raise KeyError("One or more specified columns are not present in the DataFrame")

    # Work with copies to avoid modifying caller's df
    working = df[[colA, colB, value_col]].copy()

    # Convert value_col to numeric if possible
    working[value_col] = pd.to_numeric(working[value_col], errors="coerce")

    # Build mapping from (a,b) -> list of (index_label, value)
    pairs_map: Dict[Tuple[Any, Any], List[Tuple[Any, float]]] = {}
    for idx, row in working.iterrows():
        key = (row[colA], row[colB])
        val = float(row[value_col]) if pd.notna(row[value_col]) else 0.0
        pairs_map.setdefault(key, []).append((idx, val))

    results: List[Tuple[Any, Any]] = []
    seen = set()
    tol = 1e-9

    for (a, b), entries in pairs_map.items():
        rev_key = (b, a)
        if rev_key not in pairs_map:
            continue

        # iterate cross-product of entries for key and its reverse
        for idx_i, val_i in entries:
            for idx_j, val_j in pairs_map[rev_key]:
                # avoid pairing the exact same row (possible when a==b)
                if idx_i == idx_j:
                    continue

                # Ensure we report each unordered pair once
                pair = (idx_i, idx_j)
                ordered = tuple(sorted(pair, key=lambda x: (str(x))))
                if ordered in seen:
                    continue

                if abs((val_i + val_j)) <= tol:
                    results.append(ordered)
                    seen.add(ordered)

    # Mark rows that are part of any detected reversal
    indices_involved = {i for pair in results for i in pair}
    # create boolean Series aligned with df.index
    is_rev = pd.Series(False, index=df.index)
    # set True for involved indices if present in the DataFrame index
    present = [idx for idx in indices_involved if idx in df.index]
    if present:
        is_rev.loc[present] = True
    # assign column (modifies DataFrame in place)
    df["__is_reversal__"] = is_rev

    return results


__all__ = ["detect_reversals"]
