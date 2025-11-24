"""Reconciliation pipeline.

Provides `run_reconciliation(conn, config_id)` which executes a reconciliation
flow for a given config id and stores a run record and per-key results.
"""
from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Dict, List

import pandas as pd

from ..pipeline.importer import load_preprocessed_dataset_dataframe
from ..pipeline.utils import (
    filter_concilable,
    generate_keys,
    group_by_key,
    classify,
)
from ..core.db import init_db, get_connection
from ..repo.reconciliation_results import get_results_for_run


def _load_config_and_keys(conn, config_id: int) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, base_a_id, base_b_id, value_column_a, value_column_b, invert_a, invert_b, threshold FROM reconciliation_configs WHERE id = ?",
        (config_id,),
    )
    r = cur.fetchone()
    if not r:
        raise ValueError(f"Reconciliation config not found: {config_id}")
    cfg = {
        "id": int(r[0]),
        "name": r[1],
        "base_a_id": r[2],
        "base_b_id": r[3],
        "value_column_a": r[4],
        "value_column_b": r[5],
        "invert_a": bool(r[6]),
        "invert_b": bool(r[7]),
        "threshold": float(r[8]) if r[8] is not None else 0.0,
    }

    cur.execute("SELECT id, name, base_a_columns, base_b_columns FROM reconciliation_keys WHERE config_id = ?", (config_id,))
    keys = []
    for kr in cur.fetchall():
        try:
            a_cols = json.loads(kr[2]) if kr[2] else []
        except Exception:
            a_cols = []
        try:
            b_cols = json.loads(kr[3]) if kr[3] else []
        except Exception:
            b_cols = []
        keys.append({"id": kr[0], "name": kr[1], "base_a_columns": a_cols, "base_b_columns": b_cols})

    cfg["keys"] = keys
    return cfg


def run_reconciliation(conn, config_id: int) -> Dict[str, Any]:
    """Run reconciliation for `config_id` and persist a run + results.

    Returns a dict with `summary` and `details`.
    """
    started_at = datetime.utcnow().isoformat()

    cfg = _load_config_and_keys(conn, config_id)

    # load preprocessed dataframes (after loader enhancement, headers may be reconstructed)
    df_a = load_preprocessed_dataset_dataframe(cfg["base_a_id"]) if cfg.get("base_a_id") else pd.DataFrame()
    df_b = load_preprocessed_dataset_dataframe(cfg["base_b_id"]) if cfg.get("base_b_id") else pd.DataFrame()

    # Fallback: if declared value columns are missing, attempt rename using dataset_columns logical_name.
    def _maybe_apply_column_mapping(dataset_id: int, df: pd.DataFrame) -> pd.DataFrame:
        if dataset_id is None or df.empty:
            return df
        try:
            from ..core.db import get_connection, init_db
            init_db()
            conn2 = get_connection()
            cur2 = conn2.cursor()
            cur2.execute(
                "SELECT name, logical_name FROM dataset_columns WHERE dataset_id = ? AND logical_name IS NOT NULL AND logical_name <> ''",
                (dataset_id,),
            )
            rows = cur2.fetchall()
            mapping = {r[0]: r[1] for r in rows if r[0] in df.columns and r[1] not in df.columns}
            if mapping:
                df = df.rename(columns=mapping)
            try:
                conn2.close()
            except Exception:
                pass
        except Exception:
            pass
        return df

    df_a = _maybe_apply_column_mapping(cfg.get("base_a_id"), df_a)
    df_b = _maybe_apply_column_mapping(cfg.get("base_b_id"), df_b)

    # filter concilable
    df_a = filter_concilable(df_a)
    df_b = filter_concilable(df_b)

    results_all: List[Dict[str, Any]] = []
    total_keys = 0
    matched = 0

    threshold = float(cfg.get("threshold") or 0.0)

    for key_def in cfg.get("keys", []):
        name = key_def.get("name")
        a_cols = key_def.get("base_a_columns") or []
        b_cols = key_def.get("base_b_columns") or []

        # generate key series
        keys_a = generate_keys(df_a, a_cols)
        keys_b = generate_keys(df_b, b_cols)

        # ensure key series are pandas Series (coerce if needed)
        if not isinstance(keys_a, pd.Series):
            try:
                keys_a = pd.Series(list(keys_a), index=df_a.index)
            except Exception:
                keys_a = pd.Series([""] * len(df_a), index=df_a.index)
        if not isinstance(keys_b, pd.Series):
            try:
                keys_b = pd.Series(list(keys_b), index=df_b.index)
            except Exception:
                keys_b = pd.Series([""] * len(df_b), index=df_b.index)

        # helper: resolve requested value column name to an actual column in df
        def _resolve_value_column(dataset_id: int, df: pd.DataFrame, requested: str) -> str | None:
            if not requested:
                return None
            if requested in df.columns:
                return requested
            # try to find mapping in dataset_columns
            try:
                cur = conn.cursor()
                cur.execute("SELECT name, logical_name FROM dataset_columns WHERE dataset_id = ?", (dataset_id,))
                rows = cur.fetchall()
                # direct logical_name match
                for r in rows:
                    nm, logical = r[0], r[1]
                    if logical is not None and str(logical).strip().lower() == str(requested).strip().lower():
                        if nm in df.columns:
                            return nm
                # direct name match (case-insensitive)
                for r in rows:
                    nm = r[0]
                    if nm is not None and str(nm).strip().lower() == str(requested).strip().lower():
                        if nm in df.columns:
                            return nm
                # substring match on names/logical
                for r in rows:
                    nm, logical = r[0], r[1]
                    if nm and str(requested).strip().lower() in str(nm).strip().lower() and nm in df.columns:
                        return nm
                    if logical and str(requested).strip().lower() in str(logical).strip().lower() and logical in df.columns:
                        return logical
            except Exception:
                pass
            return None

        # aggregate by key
        val_col_a = cfg.get("value_column_a")
        val_col_b = cfg.get("value_column_b")

        # attempt to resolve configured value column to actual column names in preprocessed DF
        try:
            resolved_a = _resolve_value_column(cfg.get("base_a_id"), df_a, val_col_a) if val_col_a else None
            if resolved_a:
                val_col_a = resolved_a
            resolved_b = _resolve_value_column(cfg.get("base_b_id"), df_b, val_col_b) if val_col_b else None
            if resolved_b:
                val_col_b = resolved_b
        except Exception:
            # ignore resolution errors; aggregation will surface meaningful message
            pass

        try:
            if val_col_a:
                agg_a = group_by_key(df_a, keys_a, val_col_a)
            else:
                # ensure keys_a is a Series
                if isinstance(keys_a, pd.Series):
                    agg_a = {k: 0.0 for k in keys_a.unique()}
                else:
                    agg_a = {}
        except Exception as e:
            raise RuntimeError(f"Failed to aggregate A for key '{name}': {e}") from e

        try:
            if val_col_b:
                agg_b = group_by_key(df_b, keys_b, val_col_b)
            else:
                if isinstance(keys_b, pd.Series):
                    agg_b = {k: 0.0 for k in keys_b.unique()}
                else:
                    agg_b = {}
        except Exception as e:
            # Add debug snapshot of columns/dtypes for diagnosis
            cols_snapshot = ", ".join([str(c) for c in df_b.columns][:30])
            raise RuntimeError(
                f"Failed to aggregate B for key '{name}': {e}. Columns({len(df_b.columns)}): {cols_snapshot}"
            ) from e

        # union of keys
        all_keys = set(agg_a.keys()) | set(agg_b.keys())
        total_keys += len(all_keys)

        for k in sorted(all_keys):
            a_val = agg_a.get(k, 0.0)
            b_val = agg_b.get(k, 0.0)
            status, group, diff = classify(a_val, b_val, threshold)
            if group == "matched":
                matched += 1

            results_all.append(
                {
                    "key_name": name,
                    "key": k,
                    "value_a": float(a_val),
                    "value_b": float(b_val),
                    "status": status,
                    "group": group,
                    "difference": float(diff),
                }
            )

    # persist reconciliation_runs
    finished_at = datetime.utcnow().isoformat()
    summary = {
        "config_id": config_id,
        "config_name": cfg.get("name"),
        "keys_evaluated": int(total_keys),
        "matched_keys": int(matched),
        "unmatched_keys": int(total_keys - matched),
    }

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO reconciliation_runs (config_id, status, started_at, finished_at, summary_json) VALUES (?, ?, ?, ?, ?)",
        (config_id, "completed", started_at, finished_at, json.dumps(summary)),
    )
    conn.commit()
    run_id = cur.lastrowid

    # persist per-key results
    for r in results_all:
        cur.execute(
            "INSERT INTO reconciliation_results (run_id, dataset_id, row_identifier, status, group_name, key_used, difference) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_id, None, r["key"], r["status"], r["group"], r["key_name"], r["difference"]),
        )
    conn.commit()

    return {"run_id": run_id, "summary": summary, "details": results_all}


__all__ = ["run_reconciliation"]


def merge_results_with_dataframe(dataset_id: int, run_id: int) -> pd.DataFrame:
    """Return the preprocessed DataFrame for `dataset_id` enriched with reconciliation results from `run_id`.

    The function will:
    - Load the preprocessed DataFrame from storage (`storage/pre/<dataset_id>.csv`).
    - Load the run record to determine the reconciliation config and its keys.
    - For each key definition applicable to the run, generate the composite keys
      for this dataset and attach result columns when a match is found.

    The returned DataFrame contains additional columns:
    - `recon_key_name`, `recon_status`, `recon_group`, `recon_difference`.

    If no result matches a row, the added columns will contain `None`/NaN.
    """
    # ensure DB exists and get a connection
    init_db()
    conn = get_connection()
    try:
        # reuse config loader to get config and keys
        cfg = _load_config_and_keys(conn, _get_config_id_for_run(conn, run_id))

        # load preprocessed dataframe
        df = load_preprocessed_dataset_dataframe(dataset_id)
        out = df.copy()

        # prepare result columns
        out_cols = {
            "recon_key_name": None,
            "recon_status": None,
            "recon_group": None,
            "recon_difference": float("nan"),
        }
        for c, v in out_cols.items():
            out[c] = v

        # load results for run
        results = get_results_for_run(conn, run_id)
        # group results by key_name for quick access
        results_by_key = {}
        for r in results:
            key_name = r.get("key_used")
            if key_name is None:
                continue
            results_by_key.setdefault(key_name, []).append(r)

        # determine which side this dataset belongs to (a or b)
        side = None
        if cfg.get("base_a_id") == dataset_id:
            side = "a"
        elif cfg.get("base_b_id") == dataset_id:
            side = "b"

        # iterate key definitions
        for key_def in cfg.get("keys", []):
            name = key_def.get("name")
            # which columns to use for this dataset
            if side == "a":
                cols = key_def.get("base_a_columns") or []
            elif side == "b":
                cols = key_def.get("base_b_columns") or []
            else:
                # dataset not explicitly base_a or base_b: try both
                cols = (key_def.get("base_a_columns") or []) + (key_def.get("base_b_columns") or [])

            if not cols:
                continue

            # Ensure `cols` is a list of strings
            if not isinstance(cols, (list, tuple)):
                cols = [cols]

            key_series = generate_keys(out, cols)
            if not isinstance(key_series, pd.Series):
                # try to coerce
                try:
                    key_series = pd.Series(list(key_series), index=out.index)
                except Exception:
                    # fallback: generate empty-series of same length
                    key_series = pd.Series([""] * len(out), index=out.index)

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
