"""API endpoints to manage cancellation configuration.

Cancellation configs are standalone resources (may be global when `dataset_id` is NULL).
The application no longer exposes a PUT endpoint under `/datasets/{dataset_id}` to
link configs — configs should be created via `POST /cancellation-configs` and
passed to the upload endpoint (`POST /datasets`) using `cancellation_config_id`.
"""
from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from ..core.db import init_db, get_connection
from ..repo.schema import create_tables
from ..repo.cancellation_config import save_cancellation_config, get_cancellation_config_by_id

router = APIRouter()


def _init_db_conn():
    init_db()
    conn = get_connection()
    create_tables(conn)
    cur = conn.cursor()
    return conn, cur


class CancellationConfigCreate(BaseModel):
    name: str
    indicator_column: str
    canceled_value: str
    active_value: str


@router.post("/cancellation-configs")
def post_cancellation_config(payload: CancellationConfigCreate):
    """Create a global cancellation_config (not bound to a dataset).

    Use `cancellation_config_id` returned here when uploading datasets to apply
    cancellation rules during ingest.
    """
    conn, cur = _init_db_conn()
    try:
        cid = save_cancellation_config(conn, payload.name, payload.indicator_column, payload.canceled_value, payload.active_value)
        cfg = get_cancellation_config_by_id(conn, cid)
        return {"cancellation_config_id": cid, "cancellation_config": cfg}
    finally:
        try:
            conn.close()
        except Exception:
            pass


__all__ = ["router"]
