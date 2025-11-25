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
from ..pipeline.utils import infer_column_types, select_concilable_rows_sql
from ..pipeline.fill_nulls import fill_nulls_sql
from ..pipeline.reversals import detect_and_mark_reversals_sql
from ..core.storage import save_preprocessed_df, export_to_excel
from pathlib import Path
from ..core.config import settings
from fastapi.responses import FileResponse
from ..pipeline.importer import load_preprocessed_dataset_dataframe
from ..pipeline.preprocess_base_b import run_preprocess_base_b
from ..repo.dataset_columns import get_dataset_headers

router = APIRouter()


def _init_db_conn():
    init_db()
    conn = get_connection()
    create_tables(conn)
    cur = conn.cursor()
    return conn, cur


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
    conn, cur = _init_db_conn()
    try:
        started_at = datetime.utcnow().isoformat()

        # Perform SQL-only preprocessing:
        # 1) Build schema_info from dataset_columns to drive fill_nulls_sql
        try:
            cur = conn.cursor()
            cur.execute("SELECT name, data_type FROM dataset_columns WHERE dataset_id = ? ORDER BY id", (dataset_id,))
            rows = cur.fetchall()
            schema_info = {r[0]: (r[1] or "string") for r in rows} if rows else {}
        except Exception:
            schema_info = {}

        # 2) Fill nulls via SQL
        try:
            if schema_info:
                fill_nulls_sql(conn, dataset_id, schema_info)
        except Exception:
            # tolerate failures and continue
            LOG.debug("fill_nulls_sql failed for dataset %s", dataset_id, exc_info=True)

        # 3) Optionally run reversal detection via SQL when columns are provided
        reversals_count = 0
        try:
            if col_a and col_b and value_column:
                res = detect_and_mark_reversals_sql(conn, dataset_id, col_a, col_b, value_column)
                reversals_count = int(res.get("rows_marked", 0) if isinstance(res, dict) else 0)
        except Exception:
            # treat as zero if detection fails
            reversals_count = 0

        # 4) Load the table (after SQL preprocessing) into a DataFrame to save preprocessed CSV
        try:
            table = f"dataset_{int(dataset_id)}"
            cur.execute(f'SELECT * FROM "{table}"')
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description] if cur.description else []
            df_pre = pd.DataFrame.from_records(rows, columns=cols)
        except Exception:
            # Fallback: try loading original file into DataFrame
            try:
                df_pre = load_dataset_dataframe(conn, dataset_id)
            except Exception:
                df_pre = pd.DataFrame()

        # Apply headers from repository if available and matching count
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
            "columns": int(df_pre.shape[1]) if hasattr(df_pre, 'shape') else 0,
            "storage_path": storage_path,
            "reversals_count": int(reversals_count),
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
        LOG.exception("Preprocess failed for dataset %s", dataset_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Preprocess failed") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return {"dataset_id": dataset_id, "summary": summary, "status": "completed"}


@router.post("/datasets/{dataset_id}/preprocess/base-b/run")
def run_base_b_pipeline(dataset_id: int):
    """Run the Base B preprocessing pipeline for a dataset.

    Flow:
    1. Validate dataset exists and base_type == 'B'
    2. Call `run_preprocess_base_b(conn, dataset_id)`
    3. Return summary and status
    """
    conn, cur = _init_db_conn()
    try:
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
        except Exception:
            LOG.exception("run_preprocess_base_b failed for %s", dataset_id)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Preprocess Base B failed")

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
    conn, cur = _init_db_conn()
    try:
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
