"""Repository helpers for cancellation configuration.

Provides functions to save and retrieve a cancellation configuration.
Cancellation configs may be global (dataset_id is NULL) or dataset-specific.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


def save_cancellation_config(
    conn: Any,
    name: str,
    indicator_column: str | None = None,
    canceled_value: str | None = None,
    active_value: str | None = None,
) -> int:
    """Create or update a cancellation_config identified by `name`.

    Returns the id of the saved cancellation_configs row.
    """
    cur = conn.cursor()

    # Upsert by name: if an existing row for this name exists, update it; otherwise insert.
    cur.execute("SELECT id FROM cancellation_configs WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        cfg_id = int(row[0])
        cur.execute(
            "UPDATE cancellation_configs SET indicator_column = ?, canceled_value = ?, active_value = ? WHERE id = ?",
            (indicator_column or "", canceled_value or "", active_value or "", cfg_id),
        )
    else:
        cur.execute(
            "INSERT INTO cancellation_configs (name, indicator_column, canceled_value, active_value) VALUES (?, ?, ?, ?)",
            (name, indicator_column or "", canceled_value or "", active_value or ""),
        )
        cfg_id = cur.lastrowid

    conn.commit()
    return int(cfg_id)


def get_cancellation_config(conn: Any, name: str) -> Optional[Dict[str, Any]]:
    """Retrieve the cancellation configuration by `name`.

    Returns a dict with keys (`id`, `name`, `indicator_column`, `canceled_value`, `active_value`)
    or `None` if no configuration exists.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name, indicator_column, canceled_value, active_value
        FROM cancellation_configs
        WHERE name = ?
        """,
        (name,),
    )
    row = cur.fetchone()
    if not row:
        return None

    cfg = {
        "id": int(row[0]),
        "name": row[1],
        "indicator_column": row[2],
        "canceled_value": row[3],
        "active_value": row[4],
    }

    return cfg


def get_cancellation_config_by_id(conn: Any, cfg_id: int) -> Optional[Dict[str, Any]]:
    """Retrieve cancellation config by its id.

    Returns same dict shape as `get_cancellation_config` or None.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name, indicator_column, canceled_value, active_value
        FROM cancellation_configs
        WHERE id = ?
        """,
        (int(cfg_id),),
    )
    row = cur.fetchone()
    if not row:
        return None

    cfg = {
        "id": int(row[0]),
        "name": row[1],
        "indicator_column": row[2],
        "canceled_value": row[3],
        "active_value": row[4],
    }

    return cfg


__all__ = ["save_cancellation_config", "get_cancellation_config", "get_cancellation_config_by_id"]
