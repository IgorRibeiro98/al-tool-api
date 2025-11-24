"""API endpoints for preprocessing datasets.

Provides a single endpoint to run a simple preprocessing step: load,
fill nulls, save preprocessed CSV and register a preprocess_runs entry.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from datetime import datetime
import json
from typing import Any, Optional

from ..core.db import init_db, get_connection
from ..repo.schema import create_tables
from ..pipeline.importer import load_dataset_dataframe
from ..pipeline.utils import infer_column_types
from ..pipeline.fill_nulls import fill_nulls
from ..pipeline.reversal import detect_reversals
from ..core.storage import save_preprocessed_df, export_to_excel
from pathlib import Path
from ..core.config import settings
from fastapi.responses import FileResponse
from ..pipeline.importer import load_preprocessed_dataset_dataframe
from ..pipeline.preprocess_base_b import run_preprocess_base_b
from ..repo.dataset_columns import get_dataset_headers

router = APIRouter()


@router.post("/datasets/{dataset_id}/preprocess")
def run_preprocess(
    dataset_id: int,
    col_a: Optional[str] = None,
    col_b: Optional[str] = None,
    value_column: Optional[str] = None,
):
    """Run a simple preprocess: load DF, fill nulls, save and register run.

    Returns the created `preprocess_run` id and summary information.
    """
    init_db()
    conn = get_connection()
    try:
        # ensure tables exist
        create_tables(conn)

        started_at = datetime.utcnow().isoformat()

        # load DataFrame using repository-aware loader
        try:
            df = load_dataset_dataframe(conn, dataset_id)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

        # infer simple column types and apply fill_nulls
        col_types = infer_column_types(df)
        df_pre = fill_nulls(df, col_types)

        # optionally run reversal detection when columns are provided
        reversals: list = []
        if col_a and col_b and value_column:
            try:
                reversals = detect_reversals(df_pre, col_a, col_b, value_column)
            except KeyError as exc:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        # apply headers from repository if available and matching count
        try:
            headers = get_dataset_headers(conn, dataset_id)
            if headers and len(headers) == len(df_pre.columns):
                df_pre = df_pre.copy()
                df_pre.columns = headers
        except Exception:
            pass

        # save preprocessed dataframe (ensures __is_reversal__ exists)
        storage_path = save_preprocessed_df(df_pre, dataset_id)

        finished_at = datetime.utcnow().isoformat()

        # prepare summary
        summary = {
            "rows": int(df_pre.shape[0]),
            "columns": int(df_pre.shape[1]),
            "storage_path": storage_path,
            "reversals_count": len(reversals),
        }

        # insert preprocess_runs record
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO preprocess_runs (dataset_id, status, started_at, finished_at, summary_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (dataset_id, "completed", started_at, finished_at, json.dumps(summary)),
        )
        conn.commit()
        run_id = cur.lastrowid

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Preprocess failed") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"dataset_id": dataset_id, "preprocess_run_id": run_id, "summary": summary}


@router.post("/datasets/{dataset_id}/preprocess/base-b/run")
def run_base_b_pipeline(dataset_id: int):
    """Run the Base B preprocessing pipeline for a dataset.

    Flow:
    1. Validate dataset exists and base_type == 'B'
    2. Call `run_preprocess_base_b(conn, dataset_id)`
    3. Return summary and status
    """
    init_db()
    conn = get_connection()
    try:
        create_tables(conn)

        cur = conn.cursor()
        cur.execute("SELECT id, base_type FROM datasets WHERE id = ?", (dataset_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

        base_type = (row[1] or "").strip().upper()
        if base_type != "B":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Dataset is not of base_type 'B'")

        try:
            summary = run_preprocess_base_b(conn, dataset_id)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Preprocess Base B failed") from exc

    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"dataset_id": dataset_id, "summary": summary, "status": "completed"}


__all__ = ["router"]


@router.get("/datasets/{dataset_id}/preprocess/export")
def export_preprocessed_dataset(dataset_id: int):
    """Export the preprocessed CSV for `dataset_id` to an XLSX file and return it.

    The function looks for `storage/pre/<dataset_id>.csv`, loads it as a
    DataFrame and writes an Excel file to `storage/exports/preprocessed_{dataset_id}.xlsx`.
    Returns a `FileResponse` for download.
    """
    init_db()
    conn = get_connection()
    try:
        create_tables(conn)
        try:
            df = load_preprocessed_dataset_dataframe(dataset_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

        filename = f"preprocessed_{dataset_id}.xlsx"
        export_path = Path(settings.STORAGE_PATH) / "exports" / filename
        export_path.parent.mkdir(parents=True, exist_ok=True)

        # use helper to export
        export_to_excel(df, export_path)

        return FileResponse(str(export_path), filename=filename, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    finally:
        try:
            conn.close()
        except Exception:
            pass
