"""Pipeline utilities.

Contém helpers usados em vários passos da pipeline:
- infer_column_types
- build_key / generate_keys
- filter_concilable
- invert_value
- group_by_key
- classify
- find_missing_keys
- attach_results
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple, Union

import pandas as pd
from pandas.api import types as pdtypes


# ---------------------------------------------------------------------------
# Inferência simples de tipos de coluna
# ---------------------------------------------------------------------------

def infer_column_types(df: pd.DataFrame) -> Dict[str, str]:
    """Inferir tipo simples por coluna.

    Retorna um dict col -> {'number','date','string'}.
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


# ---------------------------------------------------------------------------
# Construção de chaves
# ---------------------------------------------------------------------------

def build_key(row: Union[pd.Series, Dict[str, Any]], columns: List[str], sep: str = "|") -> str:
    """Monta uma chave composta a partir de um row e uma lista de colunas."""
    parts: List[str] = []
    for col in columns:
        value = None
        try:
            value = row[col]
        except Exception:
            if isinstance(row, dict):
                value = row.get(col)
            else:
                value = getattr(row, col, None)

        try:
            if pd.isna(value):
                parts.append("")
                continue
        except Exception:
            pass

        parts.append("" if value is None else str(value))
    return sep.join(parts)


def generate_keys(df: pd.DataFrame, columns: List[str], sep: str = "|") -> pd.Series:
    """Gera uma Series de chaves compostas para cada linha de `df`.

    - `columns`: lista de nomes de colunas (ordem importa).
    - Colunas ausentes são tratadas como vazias.
    - NaN é tratado como string vazia.
    - Retorna uma Series 1-D alinhada ao índice de `df`.
    """
    if not columns:
        return pd.Series([""] * len(df), index=df.index)

    # Garante que são strings
    columns = [str(c) for c in columns]

    # Não mutar df original
    safe_df = df.copy()

    # Cria colunas ausentes como None
    for c in columns:
        if c not in safe_df.columns:
            safe_df[c] = None

    # Garante ordem exata
    safe_df = safe_df[columns]

    # Substitui NA por "" e converte tudo para string
    safe_df = safe_df.fillna("").astype(str)

    key_series = safe_df.agg(sep.join, axis=1)

    if not isinstance(key_series, pd.Series):
        key_series = pd.Series(list(key_series), index=df.index)

    return key_series


# ---------------------------------------------------------------------------
# Filtro de linhas conciliáveis (remove estornos e canceladas)
# ---------------------------------------------------------------------------

def filter_concilable(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra DataFrame para linhas elegíveis à conciliação.

    Remove:
    - linhas marcadas como reversals (`__is_reversal__`)
    - linhas marcadas como canceladas (`__is_canceled__`)
    """
    if df is None:
        return df

    series_index = df.index

    def _truthy_series(s: pd.Series) -> pd.Series:
        if s is None:
            return pd.Series([False] * len(df), index=series_index)
        if pd.api.types.is_bool_dtype(s):
            return s.fillna(False)
        if pd.api.types.is_numeric_dtype(s):
            return s.fillna(0).astype(bool)
        return s.fillna("").astype(str).str.lower().isin({"1", "true", "yes", "y", "t"})

    mask = pd.Series([True] * len(df), index=series_index)
    if "__is_reversal__" in df.columns:
        mask &= ~_truthy_series(df["__is_reversal__"])
    if "__is_canceled__" in df.columns:
        mask &= ~_truthy_series(df["__is_canceled__"])

    return df.loc[mask].copy()


def invert_value(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Retorna uma cópia do DataFrame com a coluna multiplicada por -1."""
    if column not in df.columns:
        raise KeyError(f"Column not found: {column}")
    out = df.copy()
    coerced = pd.to_numeric(out[column], errors="coerce")
    out[column] = -1 * coerced
    return out


# ---------------------------------------------------------------------------
# Agrupamento por chave (núcleo da conciliação)
# ---------------------------------------------------------------------------

def group_by_key(
    df: pd.DataFrame,
    key_series: Union[pd.Series, List[Any]],
    value_column: str,
) -> Dict[Any, float]:
    """Agrupa `df` por `key_series` e soma `value_column`.

    - `key_series`: Series ou iterável com mesmo length de df.
    - `value_column`: deve existir em df.

    Retorna dict key -> soma(float).
    """
    if value_column not in df.columns:
        cols_preview = ", ".join(list(map(str, df.columns))[:25])
        raise KeyError(
            f"Value column not found: {value_column}. "
            f"Available columns ({len(df.columns)}): {cols_preview}"
        )

    # 1) Normalizar key_series para Series 1-D alinhada com df.index
    try:
        if isinstance(key_series, pd.Series):
            ks = key_series.reindex(df.index)
        else:
            if len(key_series) != len(df):
                raise ValueError(f"key_series length mismatch: {len(key_series)} != {len(df)}")
            ks = pd.Series(list(key_series), index=df.index)
    except Exception as e:
        raise ValueError(f"Failed to normalize key_series: {e}") from e

    # Garante 1-D e string (tratando None/NaN como vazio)
    try:
        ks = ks.apply(
            lambda x: "" if (x is None or (isinstance(x, float) and pd.isna(x))) else str(x)
        )
    except Exception:
        ks = ks.astype(str)

    # 2) Sanitizar valores da coluna de valor
    raw_vals = df[value_column].copy()

    # Se a seleção por `value_column` retornou múltiplas colunas (DataFrame)
    # — por exemplo quando há nomes de coluna duplicados — reduza para uma
    # Series antes de aplicar conversões. Heurística:
    # - se maioria/total das colunas for numérica, soma-se por linha;
    # - caso contrário, escolhe-se o primeiro valor não-nulo por linha.
    if isinstance(raw_vals, pd.DataFrame):
        try:
            is_num = [pd.api.types.is_numeric_dtype(raw_vals[c]) for c in raw_vals.columns]
            if len(is_num) > 0 and (all(is_num) or sum(is_num) >= (len(is_num) / 2)):
                # converte cada coluna para numérico e soma por linha (NaN -> 0)
                raw_vals = raw_vals.apply(lambda col: pd.to_numeric(col, errors="coerce").fillna(0))
                raw_vals = raw_vals.sum(axis=1)
            else:
                def _first_nonnull(row):
                    for v in row:
                        try:
                            if not pd.isna(v):
                                return v
                        except Exception:
                            if v is not None:
                                return v
                    return None

                raw_vals = raw_vals.apply(_first_nonnull, axis=1)
        except Exception:
            try:
                raw_vals = raw_vals.iloc[:, 0]
            except Exception:
                raw_vals = pd.Series([None] * len(df), index=df.index)

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
        neg = False
        if t.startswith("(") and t.endswith(")"):
            neg = True
            t = t[1:-1].strip()
        for sym in ["R$", "$", "€", "£"]:
            t = t.replace(sym, "")
        t = t.replace(" ", "")
        if "," in t and "." in t:
            last_comma = t.rfind(",")
            last_dot = t.rfind(".")
            if last_comma > last_dot:
                t = t.replace(".", "").replace(",", ".")
            else:
                t = t.replace(",", "")
        elif "," in t and "." not in t:
            if t.count(",") == 1:
                t = t.replace(",", ".")
            else:
                t = t.replace(",", "")
        elif "." in t and "," not in t and t.count(".") > 1:
            t = t.replace(".", "")
        while t and t[-1] in ["%", ";", ":", ",", "."]:
            t = t[:-1]
        if neg:
            t = "-" + t
        return t

    try:
        ser_strings = raw_vals.apply(lambda x: isinstance(x, str))
        if ser_strings.sum() > 0 and (ser_strings.sum() / max(len(raw_vals), 1)) >= 0.3:
            raw_vals = raw_vals.apply(_normalize_numeric_string)
    except Exception:
        pass

    vals = pd.to_numeric(raw_vals, errors="coerce").fillna(0)

    # 3) Agregação manual (sem usar groupby)
    agg: Dict[Any, float] = {}
    for k, v in zip(ks.values, vals.values):
        try:
            fv = float(v)
        except Exception:
            fv = 0.0
        agg[k] = agg.get(k, 0.0) + fv

    return agg




# ---------------------------------------------------------------------------
# Classificação A x B e chaves faltantes
# ---------------------------------------------------------------------------

def classify(valueA: Any, valueB: Any, threshold: float = 0.0) -> Tuple[str, str, float]:
    """Classifica dois valores numéricos para conciliação."""
    try:
        a = float(valueA)
    except Exception:
        a = float("nan")
    try:
        b = float(valueB)
    except Exception:
        b = float("nan")

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
    """Retorna (onlyA, onlyB) para chaves presentes só em uma das listas."""
    setA = set(keysA or [])
    setB = set(keysB or [])

    onlyA = sorted(setA - setB)
    onlyB = sorted(setB - setA)
    return onlyA, onlyB


# ---------------------------------------------------------------------------
# Anexar resultados por chave em um DF
# ---------------------------------------------------------------------------

def attach_results(
    df: pd.DataFrame,
    mapping_dict: Dict[Any, Any],
    key_column: str = "key",
    status_col: str = "status",
    group_col: str = "group",
    diff_col: str = "difference",
) -> pd.DataFrame:
    """Anexa colunas de resultado de conciliação em um DataFrame."""
    if key_column not in df.columns:
        raise KeyError(f"Key column not found in DataFrame: {key_column}")

    out = df.copy()

    statuses: List[Any] = []
    groups: List[Any] = []
    diffs: List[float] = []

    for k in out[key_column]:
        res = mapping_dict.get(k)
        if res is None:
            statuses.append(None)
            groups.append(None)
            diffs.append(float("nan"))
            continue

        if isinstance(res, (list, tuple)):
            st = res[0] if len(res) > 0 else None
            gr = res[1] if len(res) > 1 else None
            dfv = res[2] if len(res) > 2 else float("nan")
        elif isinstance(res, dict):
            st = res.get("status")
            gr = res.get("group")
            dfv = res.get("difference", float("nan"))
        else:
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


__all__ = [
    "infer_column_types",
    "build_key",
    "generate_keys",
    "filter_concilable",
    "invert_value",
    "group_by_key",
    "classify",
    "find_missing_keys",
    "attach_results",
]
