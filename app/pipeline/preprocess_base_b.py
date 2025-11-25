"""Preprocessing utilities specific for base type B.

This module exposes `run_preprocess_base_b` which orchestrates a small
preprocessing pipeline for base type B datasets:
- load dataset source
- apply null-filling (prefer SQL; fall back to in-memory)
- mark canceled rows (prefer SQL; fall back to in-memory)
- persist preprocessed CSV and register a preprocess_runs entry

The implementation favors small helpers, clear errors and logging so the
behavior is easier to reason about and test.
"""

from __future__ import annotations

from datetime import datetime
import json
import logging
from typing import Any, Dict, Optional

import pandas as pd

from ..pipeline.importer import load_dataset_dataframe
from ..pipeline.utils import infer_column_types
from ..pipeline.fill_nulls import fill_nulls_sql, fill_nulls
from ..pipeline.cancellation import mark_canceled_sql, mark_canceled
from ..core.storage import save_preprocessed_df
from ..repo.cancellation_config import get_cancellation_config
from ..repo.dataset_columns import get_dataset_headers


LOG = logging.getLogger(__name__)


def _dataset_name(conn, dataset_id: int) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM datasets WHERE id = ?", (dataset_id,))
    row = cur.fetchone()
    return row[0] if row else None


def _load_table_as_df(conn, dataset_id: int) -> pd.DataFrame:
    cur = conn.cursor()
    table = f"dataset_{int(dataset_id)}"
    cur.execute(f"SELECT * FROM \"{table}\"")
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame.from_records(rows, columns=cols)


def _apply_fill_nulls(conn, dataset_id: int, df: pd.DataFrame) -> pd.DataFrame:
    """Try SQL fill; if fails, fall back to in-memory fill using `fill_nulls`."""
    try:
        schema = infer_column_types(df)
    except Exception:
        LOG.debug("infer_column_types failed for dataset %s", dataset_id, exc_info=True)
        schema = {}

    if schema:
        try:
            fill_nulls_sql(conn, dataset_id, schema)
            return df
        except Exception:
            LOG.warning("fill_nulls_sql failed for dataset %s; falling back to in-memory", dataset_id, exc_info=True)

    # fallback: in-memory filling
    try:
        return fill_nulls(df, schema)
    except Exception:
        LOG.exception("In-memory fill_nulls failed for dataset %s", dataset_id)
        return df.copy()


def _apply_cancellation(conn, dataset_id: int, cfg: Dict[str, Any], df: pd.DataFrame) -> pd.DataFrame:
    """Try SQL cancel marking; on failure, mark in-memory using `mark_canceled`."""
    indicator = cfg.get("indicator_column")
    canceled_value = cfg.get("canceled_value")

    try:
        mark_canceled_sql(conn, dataset_id, indicator, canceled_value)
        return df
    except Exception:
        LOG.warning("mark_canceled_sql failed for dataset %s; falling back to in-memory", dataset_id, exc_info=True)
        try:
            return mark_canceled(df.copy(), indicator, canceled_value)
        except Exception:
            LOG.exception("In-memory mark_canceled failed for dataset %s", dataset_id)
            return df


def _apply_headers_if_available(conn, dataset_id: int, df: pd.DataFrame) -> pd.DataFrame:
    try:
        headers = get_dataset_headers(conn, dataset_id)
        if headers and len(headers) == len(df.columns):
            out = df.copy()
            out.columns = headers
            return out
    except Exception:
        LOG.debug("Failed to apply dataset headers for %s", dataset_id, exc_info=True)
    return df


def run_preprocess_base_b(conn, dataset_id: int) -> Dict[str, int]:
    """Run preprocessing for a base B dataset and record a preprocess_runs entry.

    Returns a summary dict with `total_rows`, `canceled_rows` and `active_rows`.
    """
    if not hasattr(conn, "cursor"):
        raise TypeError("conn must be a DB-API connection")

    started_at = datetime.utcnow().isoformat()

    # 1. load DataFrame source
    df = load_dataset_dataframe(conn, dataset_id)

    # 2. load cancellation config
    dataset_name = _dataset_name(conn, dataset_id)
    if not dataset_name:
        raise ValueError(f"Dataset {dataset_id} not found or has no name")

    cfg = get_cancellation_config(conn, dataset_name)
    if cfg is None:
        raise ValueError(f"No cancellation configuration found for dataset name '{dataset_name}'")

    # 3. fill nulls (prefer SQL)
    df = _apply_fill_nulls(conn, dataset_id, df)

    # 4. mark canceled (prefer SQL)
    df = _apply_cancellation(conn, dataset_id, cfg, df)

    # 5. load processed table (best-effort) to include flags inserted by SQL
    try:
        df_pre = _load_table_as_df(conn, dataset_id)
    except Exception:
        LOG.warning("Failed to load dataset table dataset_%s; falling back to in-memory DF", dataset_id, exc_info=True)
        df_pre = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()

    # 6. apply repository headers if they match
    df_pre = _apply_headers_if_available(conn, dataset_id, df_pre)

    # 7. persist preprocessed CSV
    storage_path = save_preprocessed_df(df_pre, dataset_id)

    # 8. record preprocess run
    finished_at = datetime.utcnow().isoformat()
    total_rows = int(df_pre.shape[0])
    canceled_rows = int(df_pre.get("__is_canceled__", pd.Series([False] * total_rows)).sum())
    active_rows = int(total_rows - canceled_rows)

    summary: Dict[str, Any] = {
        "total_rows": total_rows,
        "canceled_rows": canceled_rows,
        "active_rows": active_rows,
        "storage_path": storage_path,
    }

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO preprocess_runs (dataset_id, status, started_at, finished_at, summary_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (dataset_id, "completed", started_at, finished_at, json.dumps(summary)),
    )
    conn.commit()

    return {"total_rows": total_rows, "canceled_rows": canceled_rows, "active_rows": active_rows}


__all__ = ["run_preprocess_base_b"]
