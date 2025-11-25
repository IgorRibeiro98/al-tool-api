"""Reconciliation pipeline.

This module implements reconciliation workflows and utilities used by the
reconciliation API and pipeline. The implementation is split into small,
well-named helpers to keep responsibilities clear and make testing easier.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..core.db import get_connection, init_db
from ..pipeline.importer import load_preprocessed_dataset_dataframe
from ..pipeline.utils import classify, filter_concilable, generate_keys, group_by_key
from ..repo.reconciliation_results import get_results_for_run


LOG = logging.getLogger(__name__)

# Constants
ENRICHMENT_COLUMNS: Tuple[str, ...] = (
    "status",
    "group_name",
    "key_used",
)
FLAG_COLUMNS: Tuple[str, ...] = ("__is_canceled__", "__is_reversal__")
ALL_ENRICHMENT_COLUMNS: Tuple[str, ...] = FLAG_COLUMNS + ENRICHMENT_COLUMNS


def _quote_ident(name: str) -> str:
    """Safely quote an SQL identifier for use in f-strings.

    This is minimal but sufficient for our sqlite use-case.
    """
    return f'"{name.replace('"', '""')}"'


def _safe_json_load(text: Optional[str]) -> List[str]:
    if not text:
        return []
    try:
        return json.loads(text)
    except Exception:
        LOG.debug("Failed to parse JSON: %s", text, exc_info=True)
        return []


def _load_config_and_keys(conn, config_id: int) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, base_a_id, base_b_id, value_column_a, value_column_b, invert_a, invert_b, threshold FROM reconciliation_configs WHERE id = ?",
        (config_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Reconciliation config not found: {config_id}")

    cfg = {
        "id": int(row[0]),
        "name": row[1],
        "base_a_id": row[2],
        "base_b_id": row[3],
        "value_column_a": row[4],
        "value_column_b": row[5],
        "invert_a": bool(row[6]),
        "invert_b": bool(row[7]),
        "threshold": float(row[8]) if row[8] is not None else 0.0,
    }

    cur.execute(
        "SELECT id, name, base_a_columns, base_b_columns FROM reconciliation_keys WHERE config_id = ? ORDER BY id",
        (config_id,),
    )
    keys: List[Dict[str, Any]] = []
    for kr in cur.fetchall():
        keys.append({
            "id": kr[0],
            "name": kr[1],
            "base_a_columns": _safe_json_load(kr[2]),
            "base_b_columns": _safe_json_load(kr[3]),
        })

    cfg["keys"] = keys
    return cfg


def _coerce_series(values, index) -> pd.Series:
    if isinstance(values, pd.Series):
        return values
    try:
        return pd.Series(list(values), index=index)
    except Exception:
        return pd.Series([""] * len(index), index=index)


def _resolve_value_column(conn, dataset_id: Optional[int], df: pd.DataFrame, requested: Optional[str]) -> Optional[str]:
    """Resolve a requested value column to an actual column name present in `df`.

    Strategy (in order):
    - exact match in df.columns
    - match `dataset_columns.logical_name` exactly
    - case-insensitive match on `name`
    - substring match on `name` or `logical_name`
    """
    if not requested or df.empty:
        return None
    if requested in df.columns:
        return requested

    try:
        cur = conn.cursor()
        cur.execute("SELECT name, logical_name FROM dataset_columns WHERE dataset_id = ?", (dataset_id,))
        rows = cur.fetchall()
        target = str(requested).strip().lower()

        # exact logical_name
        for name, logical in rows:
            if logical and str(logical).strip().lower() == target and name in df.columns:
                return name

        # exact name
        for name, _ in rows:
            if name and str(name).strip().lower() == target and name in df.columns:
                return name

        # substring matches
        for name, logical in rows:
            if name and target in str(name).strip().lower() and name in df.columns:
                return name
            if logical and target in str(logical).strip().lower() and logical in df.columns:
                return logical
    except Exception:
        LOG.debug("Failed to resolve value column '%s' for dataset %s", requested, dataset_id, exc_info=True)
    return None


def _aggregate_by_key(df: pd.DataFrame, key_series: pd.Series, value_column: Optional[str]) -> Dict[Any, float]:
    if value_column:
        return group_by_key(df, key_series, value_column)
    return {k: 0.0 for k in key_series.unique()} if isinstance(key_series, pd.Series) else {}


def _persist_run_and_results(conn, config_id: int, started_at: str, results: List[Dict[str, Any]]) -> Tuple[int, Dict[str, Any]]:
    finished_at = datetime.utcnow().isoformat()
    total = len(results)
    matched = sum(1 for r in results if r.get("group") == "Conciliados")

    summary = {
        "config_id": config_id,
        "config_name": _load_config_and_keys(conn, config_id).get("name"),
        "keys_evaluated": int(total),
        "matched_keys": int(matched),
        "unmatched_keys": int(total - matched),
    }

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO reconciliation_runs (config_id, status, started_at, finished_at, summary_json) VALUES (?, ?, ?, ?, ?)",
        (config_id, "completed", started_at, finished_at, json.dumps(summary)),
    )
    conn.commit()
    run_id = cur.lastrowid

    for r in results:
        cur.execute(
            "INSERT INTO reconciliation_results (run_id, dataset_id, row_identifier, status, group_name, key_used, difference) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_id, None, r.get("key"), r.get("status"), r.get("group"), r.get("key_name"), r.get("difference")),
        )
    conn.commit()
    return run_id, summary


def run_reconciliation(conn, config_id: int) -> Dict[str, Any]:
    """Pandas-based reconciliation runner.

    Loads preprocessed datasets, aggregates values by configured keys and
    persists a run and its results.
    """
    if conn is None:
        raise ValueError("A valid DB connection is required")
    started_at = datetime.utcnow().isoformat()
    cfg = _load_config_and_keys(conn, config_id)

    df_a = load_preprocessed_dataset_dataframe(cfg.get("base_a_id")) if cfg.get("base_a_id") else pd.DataFrame()
    df_b = load_preprocessed_dataset_dataframe(cfg.get("base_b_id")) if cfg.get("base_b_id") else pd.DataFrame()

    df_a = filter_concilable(df_a)
    df_b = filter_concilable(df_b)

    results: List[Dict[str, Any]] = []
    threshold = float(cfg.get("threshold") or 0.0)

    for key_def in cfg.get("keys", []):
        key_name = key_def.get("name")
        a_cols = key_def.get("base_a_columns") or []
        b_cols = key_def.get("base_b_columns") or []

        keys_a = _coerce_series(generate_keys(df_a, a_cols), df_a.index)
        keys_b = _coerce_series(generate_keys(df_b, b_cols), df_b.index)

        val_col_a = cfg.get("value_column_a")
        val_col_b = cfg.get("value_column_b")

        resolved_a = _resolve_value_column(conn, cfg.get("base_a_id"), df_a, val_col_a) if val_col_a else None
        resolved_b = _resolve_value_column(conn, cfg.get("base_b_id"), df_b, val_col_b) if val_col_b else None
        if resolved_a:
            val_col_a = resolved_a
        if resolved_b:
            val_col_b = resolved_b

        try:
            agg_a = _aggregate_by_key(df_a, keys_a, val_col_a)
        except Exception as exc:  # pragma: no cover - surface friendly error
            raise RuntimeError(f"Failed to aggregate A for key '{key_name}': {exc}") from exc

        try:
            agg_b = _aggregate_by_key(df_b, keys_b, val_col_b)
        except Exception as exc:  # pragma: no cover - surface friendly error
            cols_snapshot = ", ".join([str(c) for c in df_b.columns][:30])
            raise RuntimeError(f"Failed to aggregate B for key '{key_name}': {exc}. Columns: {cols_snapshot}") from exc

        all_keys = set(agg_a.keys()) | set(agg_b.keys())

        for k in sorted(all_keys):
            a_val = float(agg_a.get(k, 0.0) or 0.0)
            b_val = float(agg_b.get(k, 0.0) or 0.0)
            status, group, diff = classify(a_val, b_val, threshold)
            results.append(
                {
                    "key_name": key_name,
                    "key": k,
                    "value_a": a_val,
                    "value_b": b_val,
                    "status": status,
                    "group": group,
                    "difference": float(diff),
                }
            )

    run_id, summary = _persist_run_and_results(conn, config_id, started_at, results)
    return {"run_id": run_id, "summary": summary, "details": results}


__all__ = ["run_reconciliation", "run_sql_reconciliation", "merge_results_with_dataframe", "enrich_dataset_table"]


def run_sql_reconciliation(conn, config_id: int) -> Dict[str, Any]:
    """SQL-based reconciliation using aggregated queries.

    This function constructs aggregated SELECTs over the dataset tables and
    classifies the results similarly to the pandas runner.
    """
    if conn is None:
        raise ValueError("A valid DB connection is required")
    started_at = datetime.utcnow().isoformat()
    cfg = _load_config_and_keys(conn, config_id)
    cur = conn.cursor()

    results: List[Dict[str, Any]] = []
    threshold = float(cfg.get("threshold") or 0.0)

    for key_def in cfg.get("keys", []):
        key_name = key_def.get("name")
        a_cols = key_def.get("base_a_columns") or []
        b_cols = key_def.get("base_b_columns") or []

        if not isinstance(a_cols, (list, tuple)):
            a_cols = list(a_cols) if a_cols else []
        if not isinstance(b_cols, (list, tuple)):
            b_cols = list(b_cols) if b_cols else []

        key_expr_a = "''" if not a_cols else " || '|' || ".join([f"COALESCE({_quote_ident(c)}, '')" for c in a_cols])
        key_expr_b = "''" if not b_cols else " || '|' || ".join([f"COALESCE({_quote_ident(c)}, '')" for c in b_cols])

        val_col_a = cfg.get("value_column_a") or cfg.get("value_column") or ""
        val_col_b = cfg.get("value_column_b") or cfg.get("value_column") or ""

        base_a = f"dataset_{int(cfg.get('base_a_id'))}" if cfg.get("base_a_id") else None
        base_b = f"dataset_{int(cfg.get('base_b_id'))}" if cfg.get("base_b_id") else None

        concilable = f"({_quote_ident(FLAG_COLUMNS[0])} IS NULL OR {_quote_ident(FLAG_COLUMNS[0])} = 0) AND ({_quote_ident(FLAG_COLUMNS[1])} IS NULL OR {_quote_ident(FLAG_COLUMNS[1])} = 0)"

        def build_agg(table_name: Optional[str], val_col: str, invert: bool) -> Optional[str]:
            if not table_name or not val_col:
                return None
            val_expr = f"COALESCE({_quote_ident(val_col)}, 0)"
            if invert:
                val_expr = f"(-1) * ({val_expr})"
            return f"SELECT ({key_expr_a if table_name == base_a else key_expr_b}) AS key, SUM({val_expr}) as total FROM {_quote_ident(table_name)} WHERE {concilable} GROUP BY key"

        agg_a = build_agg(base_a, val_col_a, bool(cfg.get("invert_a")))
        agg_b = build_agg(base_b, val_col_b, bool(cfg.get("invert_b")))

        if not agg_a and not agg_b:
            continue

        parts = []
        if agg_a:
            parts.append(f"SELECT key, total as valueA, 0 as valueB FROM ({agg_a})")
        if agg_b:
            parts.append(f"SELECT key, 0 as valueA, total as valueB FROM ({agg_b})")

        union = " UNION ALL ".join(parts)
        final_sql = f"SELECT key, SUM(valueA) as valueA, SUM(valueB) as valueB FROM ({union}) t GROUP BY key"

        cur.execute(final_sql)
        for row in cur.fetchall():
            key_val = row[0]
            a_val = float(row[1] or 0.0)
            b_val = float(row[2] or 0.0)
            status, group, diff = classify(a_val, b_val, threshold)
            results.append({"key_name": key_name, "key": key_val, "value_a": a_val, "value_b": b_val, "status": status, "group": group, "difference": float(diff)})

    run_id, summary = _persist_run_and_results(conn, config_id, started_at, results)
    total_keys = int(summary.get("keys_evaluated", 0))
    matched = int(summary.get("matched_keys", 0))
    return {"run_id": run_id, "matched": matched, "unmatched": int(total_keys - matched), "total_keys": total_keys, "details": results}


def merge_results_with_dataframe(dataset_id: int, run_id: int) -> pd.DataFrame:
    """Return preprocessed dataframe enriched with reconciliation results.

    Loads preprocessed CSV for `dataset_id` and attaches reconciliation
    result columns from `run_id` where the generated composite key matches.
    """
    init_db()
    conn = get_connection()
    try:
        cfg = _load_config_and_keys(conn, _get_config_id_for_run(conn, run_id))
        df = load_preprocessed_dataset_dataframe(dataset_id)
        out = df.copy()

        out["recon_key_name"] = None
        out["recon_status"] = None
        out["recon_group"] = None
        out["recon_difference"] = float("nan")

        results = get_results_for_run(conn, run_id)
        results_by_key: Dict[str, List[Dict[str, Any]]] = {}
        for r in results:
            kn = r.get("key_used")
            if not kn:
                continue
            results_by_key.setdefault(kn, []).append(r)

        side = None
        if cfg.get("base_a_id") == dataset_id:
            side = "a"
        elif cfg.get("base_b_id") == dataset_id:
            side = "b"

        for key_def in cfg.get("keys", []):
            name = key_def.get("name")
            if side == "a":
                cols = key_def.get("base_a_columns") or []
            elif side == "b":
                cols = key_def.get("base_b_columns") or []
            else:
                cols = (key_def.get("base_a_columns") or []) + (key_def.get("base_b_columns") or [])

            if not cols:
                continue
            if not isinstance(cols, (list, tuple)):
                cols = [cols]

            key_series = _coerce_series(generate_keys(out, cols), out.index)

            for res in results_by_key.get(name, []):
                row_id = res.get("row_identifier")
                if row_id is None:
                    continue
                mask = key_series == row_id
                if not mask.any():
                    continue
                out.loc[mask, "recon_key_name"] = name
                out.loc[mask, "recon_status"] = res.get("status")
                out.loc[mask, "recon_group"] = res.get("group_name")
                try:
                    out.loc[mask, "recon_difference"] = float(res.get("difference"))
                except Exception:
                    out.loc[mask, "recon_difference"] = float("nan")

        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _get_config_id_for_run(conn, run_id: int) -> int:
    cur = conn.cursor()
    cur.execute("SELECT config_id FROM reconciliation_runs WHERE id = ?", (run_id,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Run not found: {run_id}")
    return int(row[0])


def enrich_dataset_table(conn, dataset_id: int, run_id: int) -> None:
    """Persist reconciliation results into the dataset SQL table.

    Adds missing enrichment columns to `dataset_{id}` if necessary, updates
    data rows (avoiding pre-header rows) and ensures `dataset_columns` contains
    entries for the created columns so exporters can reconstruct headers.
    """
    if conn is None:
        raise ValueError("A valid DB connection is required")

    cur = conn.cursor()
    table = f"dataset_{int(dataset_id)}"

    # verify table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    if not cur.fetchone():
        raise ValueError(f"Table not found: {table}")

    # ensure enrichment columns exist in SQL table
    cur.execute(f"PRAGMA table_info({ _quote_ident(table) })")
    existing = {r[1] for r in cur.fetchall()}
    for col in ALL_ENRICHMENT_COLUMNS:
        if col not in existing:
            col_type = "integer" if col in FLAG_COLUMNS else "text"
            cur.execute(f"ALTER TABLE { _quote_ident(table) } ADD COLUMN { _quote_ident(col) } {col_type}")
    conn.commit()

    # determine header_row mapping so we do not overwrite pre-header rows
    header_row_sql: Optional[int] = None
    try:
        cur.execute("PRAGMA table_info(datasets)")
        cols = {r[1] for r in cur.fetchall()}
        if "header_row" in cols:
            cur.execute("SELECT header_row FROM datasets WHERE id = ?", (dataset_id,))
            row = cur.fetchone()
            if row and row[0] is not None:
                hri = int(row[0])
                header_row_sql = hri - 1 if hri > 0 else hri
    except Exception:
        LOG.debug("Could not determine header_row for dataset %s", dataset_id, exc_info=True)

    # load run config and results
    cur.execute("SELECT config_id FROM reconciliation_runs WHERE id = ?", (int(run_id),))
    rr = cur.fetchone()
    if not rr:
        raise ValueError(f"Run not found: {run_id}")
    config_id = int(rr[0])

    cur.execute("SELECT base_a_id, base_b_id FROM reconciliation_configs WHERE id = ?", (config_id,))
    cfg_row = cur.fetchone() or (None, None)
    base_a_id, base_b_id = cfg_row[0], cfg_row[1]

    side = "a" if base_a_id == dataset_id else ("b" if base_b_id == dataset_id else None)

    results = get_results_for_run(conn, run_id)

    cur.execute("SELECT name, base_a_columns, base_b_columns FROM reconciliation_keys WHERE config_id = ?", (config_id,))
    key_map: Dict[str, Dict[str, List[str]]] = {}
    for name, a_js, b_js in cur.fetchall():
        key_map[name] = {"a": _safe_json_load(a_js), "b": _safe_json_load(b_js)}

    for res in results:
        key_name = res.get("key_used")
        row_id = res.get("row_identifier")
        status = res.get("status")
        group = res.get("group_name")

        if not key_name or row_id is None:
            continue

        km = key_map.get(key_name)
        if not km:
            continue

        if side == "a":
            cols = km.get("a") or []
        elif side == "b":
            cols = km.get("b") or []
        else:
            cols = (km.get("a") or []) + (km.get("b") or [])

        if not cols:
            continue

        key_expr = " || '|' || ".join([f"COALESCE({_quote_ident(c)}, '')" for c in cols])

        try:
            if header_row_sql is not None and header_row_sql >= 0:
                cur.execute(
                    f"UPDATE { _quote_ident(table) } SET { _quote_ident('status') } = ?, { _quote_ident('group_name') } = ?, { _quote_ident('key_used') } = ? WHERE ({key_expr}) = ? AND row_index > ? AND ({ _quote_ident('key_used') } IS NULL OR { _quote_ident('key_used') } = '')",
                    (status, group, key_name, row_id, int(header_row_sql)),
                )
            else:
                cur.execute(
                    f"UPDATE { _quote_ident(table) } SET { _quote_ident('status') } = ?, { _quote_ident('group_name') } = ?, { _quote_ident('key_used') } = ? WHERE ({key_expr}) = ? AND ({ _quote_ident('key_used') } IS NULL OR { _quote_ident('key_used') } = '')",
                    (status, group, key_name, row_id),
                )
        except Exception:
            LOG.debug("Failed to update row for key %s on dataset %s", key_name, dataset_id, exc_info=True)

    conn.commit()

    # Ensure dataset_columns contains entries for enrichment columns
    try:
        for col in ALL_ENRICHMENT_COLUMNS:
            cur.execute("SELECT COUNT(1) FROM dataset_columns WHERE dataset_id = ? AND name = ?", (dataset_id, col))
            c = cur.fetchone()
            if not c or int(c[0]) == 0:
                dtype = "integer" if col in FLAG_COLUMNS else "string"
                cur.execute(
                    "INSERT INTO dataset_columns (dataset_id, name, logical_name, data_type, is_value_column, is_candidate_key) VALUES (?, ?, ?, ?, ?, ?)",
                    (dataset_id, col, col, dtype, 0, 0),
                )
        conn.commit()
    except Exception:
        LOG.debug("Failed to ensure dataset_columns for %s", dataset_id, exc_info=True)

    # If header_row known, populate header cells for enrichment columns (best-effort)
    if header_row_sql is not None and header_row_sql >= 0:
        try:
            for col in ALL_ENRICHMENT_COLUMNS:
                try:
                    cur.execute(
                        f"UPDATE { _quote_ident(table) } SET { _quote_ident(col) } = ? WHERE row_index = ? AND ({ _quote_ident(col) } IS NULL OR { _quote_ident(col) } = '')",
                        (col, int(header_row_sql)),
                    )
                except Exception:
                    LOG.debug("Failed to set header cell for %s on %s", col, table, exc_info=True)
            conn.commit()
        except Exception:
            LOG.debug("Failed to write header cells for dataset %s", dataset_id, exc_info=True)
