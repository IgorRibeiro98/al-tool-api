"""API endpoints for reversal configuration management.

Provides a POST endpoint to create a `reversal_configs` record.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Optional

from ..core.db import init_db, get_connection
from ..repo.schema import create_tables


def _init_db_conn():
    init_db()
    conn = get_connection()
    create_tables(conn)
    cur = conn.cursor()
    return conn, cur

router = APIRouter()


class ReversalConfigCreate(BaseModel):
    name: str
    column_a: str
    column_b: str
    value_column: Optional[str] = None


@router.post("/reversal-configs")
def create_reversal_config(payload: ReversalConfigCreate):
    """Create a reversal_configs record and return its id and saved data."""
    try:
        conn, cur = _init_db_conn()
        cur.execute(
            """
            INSERT INTO reversal_configs (name, column_a, column_b, value_column)
            VALUES (?, ?, ?, ?)
            """,
            (payload.name, payload.column_a, payload.column_b, payload.value_column),
        )
        conn.commit()
        rc_id = cur.lastrowid
        saved = {
            "id": rc_id,
            "name": payload.name,
            "column_a": payload.column_a,
            "column_b": payload.column_b,
            "value_column": payload.value_column,
        }
    except Exception as exc:
        try:
            conn.close()
        except Exception:
            pass
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create reversal config") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"reversal_config_id": rc_id, "reversal_config": saved}


__all__ = ["router"]
