"""Reversal detection utilities.

Provides `detect_reversals` which finds pairs of rows that are reverse
matches of each other (key columns swapped and values summing to zero).
"""

from __future__ import annotations

import math
import logging
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

LOG = logging.getLogger(__name__)

# Constants
DEFAULT_TOLERANCE = 1e-9
_ROUND_PRECISION = 9
REVERSAL_FLAG_COLUMN = "__is_reversal__"


def _validate_inputs(df: pd.DataFrame, col_a: str, col_b: str, value_col: str) -> None:
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")
    missing = [c for c in (col_a, col_b, value_col) if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in DataFrame: {missing}")


def _to_numeric_or_zero(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s.fillna(0.0).astype(float)


def _group_entries(df: pd.DataFrame, col_a: str, col_b: str, value_col: str) -> Dict[Tuple[Any, Any], List[Tuple[Any, float]]]:
    groups: Dict[Tuple[Any, Any], List[Tuple[Any, float]]] = defaultdict(list)
    for idx, a_val, b_val, v in df[[col_a, col_b, value_col]].itertuples(index=True, name=None):
        groups[(a_val, b_val)].append((idx, float(v)))
    return groups


def _match_pairs(entries: List[Tuple[Any, float]], rev_entries: List[Tuple[Any, float]], tol: float) -> List[Tuple[Any, Any]]:
    """Match entries from `entries` with `rev_entries` where values sum to ~0.

    Uses rounding to _ROUND_PRECISION to bucket candidate matches, avoiding
    O(N*M) cross-product in common duplicate scenarios.
    """
    # Build lookup from rounded value -> deque of indices in rev_entries
    buckets: Dict[float, deque] = defaultdict(deque)
    for idx, val in rev_entries:
        key = round(val, _ROUND_PRECISION)
        buckets[key].append((idx, val))

    matches: List[Tuple[Any, Any]] = []
    for idx_i, val_i in entries:
        target_key = round(-val_i, _ROUND_PRECISION)
        candidates = buckets.get(target_key)
        if not candidates:
            continue
        # pop candidate to avoid reuse
        idx_j, val_j = candidates.popleft()
        if math.isfinite(val_i) and math.isfinite(val_j) and abs(val_i + val_j) <= tol:
            matches.append((idx_i, idx_j))
        else:
            # if popped candidate didn't satisfy tolerance, try to find a valid one
            found = False
            # check remaining candidates with same key
            for _ in range(len(candidates)):
                cand_idx, cand_val = candidates.popleft()
                if math.isfinite(val_i) and math.isfinite(cand_val) and abs(val_i + cand_val) <= tol:
                    matches.append((idx_i, cand_idx))
                    found = True
                    break
                else:
                    # keep the candidate for potential future matches
                    buckets[target_key].append((cand_idx, cand_val))
            if not found:
                # nothing matched for this idx_i; continue
                continue

    return matches


def detect_reversals(df: pd.DataFrame, colA: str, colB: str, value_col: str, mark_column: bool = True, tolerance: float = DEFAULT_TOLERANCE) -> List[Tuple[Any, Any]]:
    """Detect reversal pairs in `df`.

    A reversal pair (i, j) satisfies:
      - df.loc[i, colA] == df.loc[j, colB]
      - df.loc[i, colB] == df.loc[j, colA]
      - df.loc[i, value_col] + df.loc[j, value_col] == 0 (within `tolerance`)

    Returns a list of unique ordered tuples `(idx_i, idx_j)` where the tuple
    ordering is determined by Python's ordering on the index labels. By
    default the DataFrame is annotated with a boolean column
    `__is_reversal__` marking involved rows; pass `mark_column=False` to
    avoid modifying the DataFrame in-place.
    """
    _validate_inputs(df, colA, colB, value_col)
    if tolerance is None or tolerance < 0:
        raise ValueError("tolerance must be a non-negative float")

    # Work on a view with only the necessary columns
    working = df[[colA, colB, value_col]].copy()
    working[value_col] = _to_numeric_or_zero(working[value_col])

    groups = _group_entries(working, colA, colB, value_col)
    results: List[Tuple[Any, Any]] = []
    seen: set = set()

    for (a, b), entries in groups.items():
        rev_key = (b, a)
        if rev_key not in groups:
            continue
        rev_entries = groups[rev_key]

        # match entries <-> rev_entries
        matches = _match_pairs(entries, rev_entries, tolerance)
        for i, j in matches:
            if i == j:
                # skip self-pair (possible when a == b)
                continue
            ordered = tuple(sorted((i, j), key=lambda x: (str(x))))
            if ordered in seen:
                continue
            seen.add(ordered)
            results.append(ordered)

    # annotate DataFrame if requested
    if mark_column:
        indices_involved = {idx for pair in results for idx in pair}
        is_rev = pd.Series(False, index=df.index)
        present = [idx for idx in indices_involved if idx in df.index]
        if present:
            is_rev.loc[present] = True
        df[REVERSAL_FLAG_COLUMN] = is_rev

    LOG.info("Detected %d reversal pairs", len(results))
    return results


__all__ = ["detect_reversals"]
