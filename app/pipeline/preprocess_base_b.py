"""Preprocessing utilities specific for base type B.

Provides `run_preprocess_base_b` which runs a preprocessing flow that
applies null-filling and cancellation marking for datasets of base_type B.
"""
from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Dict

import pandas as pd

from ..pipeline.importer import load_dataset_dataframe
from ..pipeline.utils import infer_column_types
from ..pipeline.fill_nulls import fill_nulls
from ..pipeline.cancellation import mark_canceled
from ..core.storage import save_preprocessed_df
from ..repo.cancellation_config import get_cancellation_config
from ..repo.dataset_columns import get_dataset_headers


def run_preprocess_base_b(conn, dataset_id: int) -> Dict[str, int]:
    """Run preprocessing for a base B dataset.

    Steps:
    1. Load DataFrame using `load_dataset_dataframe(conn, dataset_id)`
    2. Obtain cancellation config via `get_cancellation_config(conn, dataset_id)`
       - If missing, raise `ValueError`
    3. Apply `fill_nulls(df, schema_info)`
    4. Apply `mark_canceled(df, config.indicator_column, config.canceled_value)`
    5. Save preprocessed DF via `save_preprocessed_df(df, dataset_id)`
    6. Insert a record into `preprocess_runs` (status 'completed')
    7. Return summary dict with counts
    """
    # 1. load DataFrame
    df: pd.DataFrame = load_dataset_dataframe(conn, dataset_id)

    # 2. get cancellation config
    cfg = get_cancellation_config(conn, dataset_id)
    if cfg is None:
        raise ValueError(f"No cancellation configuration for dataset {dataset_id}")

    indicator_column = cfg["indicator_column"]
    canceled_value = cfg["canceled_value"]
    # active_value = cfg["active_value"]  # not used directly here

    # 3. fill nulls
    col_types = infer_column_types(df)
    df_pre = fill_nulls(df, col_types)

    # 4. mark canceled
    try:
        df_pre = mark_canceled(df_pre, indicator_column, canceled_value)
    except KeyError as exc:
        raise ValueError(str(exc)) from exc

    # Apply headers from repository if available and matching count
    try:
        headers = get_dataset_headers(conn, dataset_id)
        if headers and len(headers) == len(df_pre.columns):
            df_pre = df_pre.copy()
            df_pre.columns = headers
    except Exception:
        pass

    # 5. save preprocessed DF
    storage_path = save_preprocessed_df(df_pre, dataset_id)

    # 6. insert preprocess_runs record
    started_at = datetime.utcnow().isoformat()
    finished_at = datetime.utcnow().isoformat()
    summary = {
        "total_rows": int(df_pre.shape[0]),
        "canceled_rows": int(df_pre["__is_canceled__"].sum()),
        "active_rows": int((~df_pre["__is_canceled__"]).sum()),
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
    _run_id = cur.lastrowid

    # 7. return summary counts
    return {
        "total_rows": summary["total_rows"],
        "canceled_rows": summary["canceled_rows"],
        "active_rows": summary["active_rows"],
    }


__all__ = ["run_preprocess_base_b"]
