"""API endpoints to manage cancellation configuration for datasets."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Any

from ..core.db import init_db, get_connection
from ..repo.schema import create_tables
from ..repo.cancellation_config import save_cancellation_config, get_cancellation_config

router = APIRouter()


class CancellationConfigIn(BaseModel):
    indicator_column: str
    canceled_value: str
    active_value: str


@router.put("/datasets/{dataset_id}/config/cancellation")
def put_cancellation_config(dataset_id: int, payload: CancellationConfigIn):
    """Validate dataset and save cancellation configuration.

    Steps:
    1. Validate dataset exists
    2. Validate dataset.base_type == 'B'
    3. Save config via repository
    4. Return saved config
    """
    init_db()
    conn = get_connection()
    try:
        create_tables(conn)

        cur = conn.cursor()
        cur.execute("SELECT id, name, base_type FROM datasets WHERE id = ?", (dataset_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

        _, name, base_type = row[0], row[1], row[2]
        if (base_type or "").strip().upper() != "B":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cancellation config can only be set for base_type 'B'")

        cfg_id = save_cancellation_config(
            conn,
            dataset_id,
            payload.indicator_column,
            payload.canceled_value,
            payload.active_value,
        )

        cfg = get_cancellation_config(conn, dataset_id)
        return {"cancellation_config_id": cfg_id, "cancellation_config": cfg}

    finally:
        try:
            conn.close()
        except Exception:
            pass


__all__ = ["router"]
