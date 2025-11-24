"""Pipeline utilities.

Contains helpers used across pipeline steps.
"""
from __future__ import annotations

from typing import Dict, Any, List, Union
from typing import Tuple

import pandas as pd
from pandas.api import types as pdtypes


def infer_column_types(df: pd.DataFrame) -> Dict[str, str]:
    """Infer simple column types for a DataFrame.

    For each column returns one of: 'number', 'date', 'string'.

    - 'number' if the column has a numeric dtype
    - 'date' if the column has a datetime/timedelta dtype
    - 'string' otherwise
    """
    types: Dict[str, str] = {}
    for col in df.columns:
        ser = df[col]
        if pdtypes.is_numeric_dtype(ser):
            types[col] = "number"
        elif pdtypes.is_datetime64_any_dtype(ser) or pdtypes.is_timedelta64_dtype(ser):
            types[col] = "date"
        else:
            types[col] = "string"
    return types


def build_key(row: Union[pd.Series, Dict[str, Any]], columns: List[str], sep: str = "|") -> str:
    """Build a composite key from `row` by concatenating the values of `columns`.

    - `row` may be a `pandas.Series` or a `dict`-like object.
    - `columns` is a list of column names to include in order.
    - `sep` is the separator used between values (default: `"|"`).

    Missing or NaN values are treated as empty strings.
    """
    parts: List[str] = []
    for col in columns:
        value = None
        try:
            value = row[col]
        except Exception:
            # fallback for dict-like objects
            if isinstance(row, dict):
                value = row.get(col)
            else:
                # last resort: attempt attribute access
                value = getattr(row, col, None)
        # normalize pandas NA
        try:
            if pd.isna(value):
                parts.append("")
                continue
        except Exception:
            pass
        parts.append("" if value is None else str(value))

    return sep.join(parts)


__all__ = ["infer_column_types", "build_key"]


def generate_keys(df: pd.DataFrame, columns: List[str], sep: str = "|") -> pd.Series:
    """Generate a pandas Series of composite keys for each row in `df`.

    - `columns` is a list of column names (order matters).
    - `sep` is the separator used between values (default: `"|"`).

    Missing columns are treated as empty strings. NaN values are treated as
    empty strings as well. The returned Series has the same index as `df`.
    """
    # If columns list is empty, return empty strings
    if not columns:
        return pd.Series([""] * len(df), index=df.index)

    # Garantir que `columns` é lista de strings
    columns = [str(c) for c in columns]

    # Não mutar o df original: criamos um df “seguro” só com as colunas relevantes
    existing = [c for c in columns if c in df.columns]
    missing = [c for c in columns if c not in df.columns]

    # Reindexa colunas existentes, preenchendo com NaN onde não houver
    safe_df = df.copy()
    for c in missing:
        # cria coluna ausente com None/NaN
        safe_df[c] = None

    # Garante a ordem das colunas conforme `columns`
    safe_df = safe_df[columns]

    # Replace NA with empty string and convert to string, then join per row
    safe = safe_df.applymap(lambda x: "" if pd.isna(x) else str(x))

    key_series = safe.agg(sep.join, axis=1)

    # Garantir que é Series 1-D alinhada ao índice original
    if not isinstance(key_series, pd.Series):
        key_series = pd.Series(list(key_series), index=df.index)

    return key_series

    """Generate a pandas Series of composite keys for each row in `df`.

    - `columns` is a list of column names (order matters).
    - `sep` is the separator used between values (default: `"|"`).

    Missing columns are treated as empty strings. NaN values are treated as
    empty strings as well. The returned Series has the same index as `df`.
    """
    # If columns list is empty, return empty strings
    if not columns:
        return pd.Series([""] * len(df), index=df.index)

    # Ensure missing columns exist in df (as None)
    missing = [c for c in columns if c not in df.columns]
    if missing:
        for c in missing:
            df[c] = None

    # Replace NA with empty string and convert to string, then join per row
    safe = df[columns].applymap(lambda x: "" if pd.isna(x) else str(x))
    return safe.agg(sep.join, axis=1)


def filter_concilable(df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame filtered to rows eligible for reconciliation.

    Removes rows marked as reversals (`__is_reversal__`) or cancelled
    (`__is_canceled__`). If those marker columns are missing they are
    treated as `False` (i.e. rows are kept).

    The original DataFrame is not modified; a filtered copy is returned.
    """
    if df is None:
        return df

    # work on a view but avoid mutating original
    series_index = df.index

    def _truthy_series(s: pd.Series) -> pd.Series:
        # normalize different representations into boolean Series
        if s is None:
            return pd.Series([False] * len(df), index=series_index)
        # boolean dtype -> straightforward
        if pd.api.types.is_bool_dtype(s):
            return s.fillna(False)
        # numeric dtype -> non-zero is truthy
        if pd.api.types.is_numeric_dtype(s):
            return s.fillna(0).astype(bool)
        # otherwise try to coerce strings like 'true','1','yes'
        return s.fillna("").astype(str).str.lower().isin({"1", "true", "yes", "y", "t"})

    mask = pd.Series([True] * len(df), index=series_index)
    if "__is_reversal__" in df.columns:
        mask &= ~_truthy_series(df["__is_reversal__"])
    if "__is_canceled__" in df.columns:
        mask &= ~_truthy_series(df["__is_canceled__"])

    return df.loc[mask].copy()


__all__ = ["infer_column_types", "build_key", "generate_keys", "filter_concilable"]


def invert_value(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Return a DataFrame where `column` values are multiplied by -1.

        - Does not modify the original DataFrame; returns a copy.
        - Non-numeric values are coerced to numeric where possible; values that
            cannot be converted become `NaN` and remain unchanged except for the
            coercion.
        - Raises `KeyError` if `column` is not present in `df`.
        """
        if column not in df.columns:
                raise KeyError(f"Column not found: {column}")

        out = df.copy()
        # coerce to numeric where possible; keep NaN for non-coercible
        coerced = pd.to_numeric(out[column], errors="coerce")
        out[column] = -1 * coerced
        return out


__all__ = ["infer_column_types", "build_key", "generate_keys", "filter_concilable", "invert_value"]


def group_by_key(df: pd.DataFrame, key_series: Union[pd.Series, List[Any]], value_column: str) -> Dict[Any, float]:
    """Group `df` by `key_series` and sum `value_column`.

    - `key_series` can be a `pandas.Series` aligned with `df`, or a list/array
      with the same length as `df`.
    - `value_column` must exist in `df`.

    Returns a dict mapping key -> summed numeric value (float). Non-numeric
    values in `value_column` are coerced to numeric; non-convertible values
    are treated as 0.
    """
    if value_column not in df.columns:
        # Include sample of available columns to aid debugging.
        cols_preview = ", ".join(list(map(str, df.columns))[:25])
        raise KeyError(
            f"Value column not found: {value_column}. "
            f"Available columns ({len(df.columns)}): {cols_preview}"
        )

    # normalize key_series to a Series aligned with df.index
    try:
        if isinstance(key_series, pd.Series):
            ks = key_series.reindex(df.index)
        else:
            # assume list-like
            if len(key_series) != len(df):
                raise ValueError(f"key_series length mismatch: {len(key_series)} != {len(df)}")
            ks = pd.Series(list(key_series), index=df.index)
    except Exception as e:
        raise ValueError(f"Failed to normalize key_series: {e}") from e

    # ensure ks is 1-D
    if getattr(ks, "ndim", 1) != 1:
        raise ValueError("key_series must be 1-D after normalization")

    # guard against non-hashable entries (convert to string; tratar None/NaN)
    try:
        ks = ks.apply(
            lambda x: "" if (x is None or (isinstance(x, float) and pd.isna(x))) else str(x)
        )
    except Exception:
        ks = ks.astype(str)

    # sanitize value column: if cells are list/tuple/dict, attempt to extract a numeric scalar
    raw_vals = df[value_column].copy()

    def _extract_scalar(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, (list, tuple, set)):
            if len(v) == 0:
                return None
            for item in v:
                if item is not None and not (isinstance(item, float) and pd.isna(item)):
                    return item
            return None
        if isinstance(v, dict):
            for k in ("value", "val", "amount", "amt"):
                if k in v:
                    return v[k]
            try:
                return next(iter(v.values()))
            except Exception:
                return None
        return v

    try:
        if raw_vals.apply(lambda x: isinstance(x, (list, tuple, set, dict))).any():
            raw_vals = raw_vals.apply(_extract_scalar)
    except Exception:
        pass

    # optional normalization of numeric-looking strings (handles decimal comma & thousands separators)
    def _normalize_numeric_string(s: Any) -> Any:
        if s is None:
            return None
        if isinstance(s, (int, float)):
            return s
        if not isinstance(s, str):
            return s
        t = s.strip()
        if t == "":
            return None
        # handle negative in parentheses e.g. (1.234,56)
        neg = False
        if t.startswith("(") and t.endswith(")"):
            neg = True
            t = t[1:-1].strip()
        # remove currency symbols and spaces
        for sym in ["R$", "$", "€", "£"]:
            if sym in t:
                t = t.replace(sym, "")
        t = t.replace(" ", "")
        # unify thousand/decimal separators
        if "," in t and "." in t:
            last_comma = t.rfind(",")
            last_dot = t.rfind(".")
            if last_comma > last_dot:
                # pattern like 1.234,56
                t = t.replace(".", "")
                t = t.replace(",", ".")
            else:
                # pattern like 1,234.56
                t = t.replace(",", "")
        elif "," in t and "." not in t:
            # decimal comma
            if t.count(",") == 1:
                t = t.replace(",", ".")
            else:
                # multiple commas -> thousands
                t = t.replace(",", "")
        elif "." in t and "," not in t:
            # could be decimal point or thousands; if more than one dot treat as thousands
            if t.count(".") > 1:
                t = t.replace(".", "")
        # remove any trailing non-numeric chars (e.g. '%', ';', ',', '.')
        while t and t[-1] in ["%", ";", ":", ",", "."]:
            t = t[:-1]
        if neg:
            t = "-" + t
        return t

    try:
        # apply only if many strings present
        ser_strings = raw_vals.apply(lambda x: isinstance(x, str))
        if ser_strings.sum() > 0 and (ser_strings.sum() / max(len(raw_vals), 1)) >= 0.3:
            raw_vals = raw_vals.apply(_normalize_numeric_string)
    except Exception:
        pass

    # Agora garantimos que vals é uma Series 1-D
    vals = pd.to_numeric(raw_vals, errors="coerce").fillna(0)

    # Construímos um DataFrame auxiliar para evitar problemas com `by` do groupby
    tmp = pd.DataFrame({"__key__": ks, "__value__": vals})

    try:
        grouped = tmp.groupby("__key__")["__value__"].sum()
    except Exception as e:
        sample_vals = raw_vals.head(5).tolist()
        sample_keys = ks.head(5).tolist()
        raise RuntimeError(
            f"GroupBy failed (value_column={value_column}, rows={len(df)}, "
            f"unique_keys={ks.nunique()}, sample_keys={sample_keys}, "
            f"sample_values={sample_vals}): {e}"
        ) from e

    # convert to plain Python dict (keys may be numpy types)
    return {k: float(v) for k, v in grouped.items()}


__all__ = ["infer_column_types", "build_key", "generate_keys", "filter_concilable", "invert_value", "group_by_key"]


def classify(valueA: Any, valueB: Any, threshold: float = 0.0) -> Tuple[str, str, float]:
    """Classify two numeric values for reconciliation.

    Returns a tuple `(status, group, difference)` where:
    - `status` is a short string describing the relation: one of
      `"both_zero"`, `"match"`, `"only_a"`, `"only_b"`, `"mismatch"`.
    - `group` is a coarser classification: `"matched"` or `"unmatched"`.
    - `difference` is `valueA - valueB` (float), where missing/non-numeric
      inputs are treated as `0.0` for the arithmetic.

    `threshold` is a non-negative tolerance: if `abs(difference) <= threshold`
    the pair is considered a `match`.
    """
    # coerce to numeric where possible; treat non-convertible as NaN
    try:
        a = float(valueA)
    except Exception:
        a = float("nan")
    try:
        b = float(valueB)
    except Exception:
        b = float("nan")

    # treat NaN as 0.0 for reconciliation arithmetic
    ai = 0.0 if pd.isna(a) else a
    bi = 0.0 if pd.isna(b) else b
    diff = float(ai - bi)
    absdiff = abs(diff)

    if ai == 0.0 and bi == 0.0:
        return "both_zero", "matched", diff
    if absdiff <= float(threshold):
        return "match", "matched", diff
    if ai == 0.0 and bi != 0.0:
        return "only_b", "unmatched", diff
    if bi == 0.0 and ai != 0.0:
        return "only_a", "unmatched", diff
    return "mismatch", "unmatched", diff


def find_missing_keys(keysA: List[Any], keysB: List[Any]) -> Tuple[List[Any], List[Any]]:
    """Return (onlyA, onlyB) where:

    - `onlyA` are keys present in `keysA` but not in `keysB`.
    - `onlyB` are keys present in `keysB` but not in `keysA`.

    Inputs can be any iterable of hashable values. The returned lists are
    sorted to provide deterministic ordering.
    """
    setA = set(keysA or [])
    setB = set(keysB or [])

    onlyA = sorted(setA - setB)
    onlyB = sorted(setB - setA)
    return onlyA, onlyB


__all__ = [
    "infer_column_types",
    "build_key",
    "generate_keys",
    "filter_concilable",
    "invert_value",
    "group_by_key",
    "classify",
    "find_missing_keys",
]


def attach_results(
    df: pd.DataFrame,
    mapping_dict: Dict[Any, Any],
    key_column: str = "key",
    status_col: str = "status",
    group_col: str = "group",
    diff_col: str = "difference",
) -> pd.DataFrame:
    """Attach reconciliation results to `df`.

    Parameters
    - `df`: DataFrame to annotate (a copy is returned; original not mutated).
    - `mapping_dict`: mapping from key -> result. Each result can be either:
        - a tuple/list like `(status, group, difference)` or
        - a dict with keys `status`, `group`, `difference` (or any subset).
    - `key_column`: the column in `df` that contains the key used to lookup results.
    - `status_col`, `group_col`, `diff_col`: names for the output columns to add.

    For rows whose key is not present in `mapping_dict`, the function fills
    `status_col` and `group_col` with `None` and `diff_col` with `NaN`.
    """
    if key_column not in df.columns:
        raise KeyError(f"Key column not found in DataFrame: {key_column}")

    out = df.copy()

    statuses = []
    groups = []
    diffs = []

    for k in out[key_column]:
        res = mapping_dict.get(k)
        if res is None:
            statuses.append(None)
            groups.append(None)
            diffs.append(float("nan"))
            continue

        if isinstance(res, (list, tuple)):
            # expect (status, group, difference)
            st = res[0] if len(res) > 0 else None
            gr = res[1] if len(res) > 1 else None
            dfv = res[2] if len(res) > 2 else float("nan")
        elif isinstance(res, dict):
            st = res.get("status")
            gr = res.get("group")
            dfv = res.get("difference", float("nan"))
        else:
            # unknown shape; store whole object in status for debugging
            st = str(res)
            gr = None
            dfv = float("nan")

        statuses.append(st)
        groups.append(gr)
        try:
            diffs.append(float(dfv) if dfv is not None else float("nan"))
        except Exception:
            diffs.append(float("nan"))

    out[status_col] = statuses
    out[group_col] = groups
    out[diff_col] = diffs

    return out
