"""Repository helpers for cancellation configuration.

Provides functions to save and retrieve a cancellation configuration for a dataset.
"""
from __future__ import annotations

from typing import Any, Dict, Optional
import sqlite3


def save_cancellation_config(
    conn: sqlite3.Connection | Any,
    dataset_id: int,
    indicator_column: str,
    canceled_value: str,
    active_value: str,
) -> int:
    """Insert or update a cancellation configuration for `dataset_id`.

    Returns the id of the saved configuration.
    """
    cur = conn.cursor()
    # Check if a config already exists for this dataset
    cur.execute("SELECT id FROM cancellation_configs WHERE dataset_id = ?", (dataset_id,))
    row = cur.fetchone()
    if row:
        cfg_id = int(row[0])
        cur.execute(
            """
            UPDATE cancellation_configs
            SET indicator_column = ?, canceled_value = ?, active_value = ?
            WHERE id = ?
            """,
            (indicator_column, canceled_value, active_value, cfg_id),
        )
    else:
        cur.execute(
            """
            INSERT INTO cancellation_configs (dataset_id, indicator_column, canceled_value, active_value)
            VALUES (?, ?, ?, ?)
            """,
            (dataset_id, indicator_column, canceled_value, active_value),
        )
        cfg_id = cur.lastrowid

    conn.commit()
    return int(cfg_id)


def get_cancellation_config(conn: sqlite3.Connection | Any, dataset_id: int) -> Optional[Dict[str, Any]]:
    """Retrieve the cancellation configuration for `dataset_id`.

    Returns a dict with keys (`id`, `dataset_id`, `indicator_column`, `canceled_value`, `active_value`)
    or `None` if no configuration exists.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, dataset_id, indicator_column, canceled_value, active_value
        FROM cancellation_configs
        WHERE dataset_id = ?
        """,
        (dataset_id,),
    )
    row = cur.fetchone()
    if not row:
        return None

    return {
        "id": int(row[0]),
        "dataset_id": int(row[1]),
        "indicator_column": row[2],
        "canceled_value": row[3],
        "active_value": row[4],
    }


__all__ = ["save_cancellation_config", "get_cancellation_config"]
