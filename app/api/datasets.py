"""API router for dataset uploads.

Provides POST /datasets which accepts a multipart file along with
`base_type` and `format`, saves the file and registers a dataset record.
"""
from __future__ import annotations

from fastapi import APIRouter, File, Form, UploadFile, HTTPException, status
from pydantic import BaseModel
from typing import List, Optional

from ..core.storage import save_file
from ..core.db import init_db, get_connection
from ..repo.schema import create_tables
from ..repo.datasets import create_dataset_record
from ..pipeline.importer import load_fiscal
from ..pipeline.utils import infer_column_types
from ..repo.dataset_columns import save_detected_columns
from ..core.config import settings
from pathlib import Path

router = APIRouter()

# formatos aceitos ao fazer upload/preview
ALLOWED_FORMATS = {"csv", "txt", "excel", "xls", "xlsx"}


@router.post("/datasets")
async def upload_dataset(
    file: UploadFile = File(...),
    base_type: str = Form(...),
    format: str = Form(...),
    header_row: Optional[int] = Form(None),
    header_col: Optional[str] = Form(None),
):
    """Receive a dataset file, save it and register a dataset record.

    Returns the created `dataset_id` and basic info.
    """
    try:
        content = await file.read()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Failed to read uploaded file") from exc

    fmt = (format or "").strip().lower()
    if fmt not in ALLOWED_FORMATS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Formato inválido: {format}. Use um de: {sorted(ALLOWED_FORMATS)}")

    # Save file to storage
    try:
        storage_path = save_file(content, file.filename)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to save file") from exc

    # Ensure DB file exists and tables are created (schema module contains DDL)
    try:
        init_db()
        conn = get_connection()
        create_tables(conn)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database initialization error") from exc

    try:
        # Persist header_row/header_col directly in datasets table (if migrated)
        dataset_id = create_dataset_record(
            conn,
            file.filename,
            base_type,
            fmt,
            storage_path,
            header_row=int(header_row) if header_row is not None else None,
            header_col=header_col if header_col is not None else None,
        )
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create dataset record") from exc
    else:
        # Try to load the saved file and persist detected columns. Do not fail the upload if this step fails.
        try:
                df = load_fiscal(storage_path, fmt)
                col_types = infer_column_types(df)
                try:
                    # If header_row not provided, save_detected_columns defaults to header_row=1 behavior.
                    hr = int(header_row) if header_row is not None else None
                    hc = header_col if header_col is not None else None
                    if hr is None and hc is None:
                        save_detected_columns(conn, dataset_id, df, col_types)
                    else:
                        # pass values through; save_detected_columns accepts header_row (1-based) and header_col
                        save_detected_columns(conn, dataset_id, df, col_types, header_row=hr or 1, header_col=hc)
                except Exception:
                    # Don't fail the upload for metadata persistence errors
                    pass
        except Exception:
            # If loading/preprocessing fails, skip persisting columns
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {
        "dataset_id": dataset_id,
        "name": file.filename,
        "base_type": base_type,
        "format": fmt,
        "storage_path": storage_path,
    }



@router.get("/datasets/{dataset_id}/columns")
def get_dataset_columns(dataset_id: int):
    """Return registered columns for a dataset as JSON."""
    try:
        init_db()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, dataset_id, name, logical_name, data_type, is_value_column, is_candidate_key
            FROM dataset_columns
            WHERE dataset_id = ?
            """,
            (dataset_id,)
        )
        rows = cur.fetchall()
        cols = [dict(zip([c[0] for c in cur.description], row)) for row in rows]
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to read dataset columns") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"dataset_id": dataset_id, "columns": cols}


@router.get("/datasets/{dataset_id}/mapped-columns")
def get_mapped_columns(dataset_id: int):
    """Return columns for a dataset that have been mapped (logical_name present)."""
    try:
        init_db()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, dataset_id, name, logical_name, data_type, is_value_column, is_candidate_key
            FROM dataset_columns
            WHERE dataset_id = ? AND logical_name IS NOT NULL AND logical_name <> ''
            """,
            (dataset_id,)
        )
        rows = cur.fetchall()
        cols = [dict(zip([c[0] for c in cur.description], row)) for row in rows]
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to read mapped columns") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"dataset_id": dataset_id, "mapped_columns": cols}



class ColumnUpdate(BaseModel):
    id: int
    logical_name: Optional[str] = None
    data_type: Optional[str] = None
    is_value_column: Optional[int] = None
    is_candidate_key: Optional[int] = None


@router.put("/datasets/{dataset_id}/columns")
def update_dataset_columns(dataset_id: int, updates: List[ColumnUpdate]):
    """Update `logical_name`, `data_type` and flag fields for dataset_columns.

    Expects a JSON array of objects with at least `id` and optional fields to update.
    """
    if not updates:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No updates provided")

    try:
        init_db()
        conn = get_connection()
        cur = conn.cursor()
        updated_ids = []
        for u in updates:
            # Build parameters and SQL dynamically to update only provided fields
            fields = []
            params = []
            if u.logical_name is not None:
                fields.append("logical_name = ?")
                params.append(u.logical_name)
            if u.data_type is not None:
                fields.append("data_type = ?")
                params.append(u.data_type)
            if u.is_value_column is not None:
                fields.append("is_value_column = ?")
                params.append(int(u.is_value_column))
            if u.is_candidate_key is not None:
                fields.append("is_candidate_key = ?")
                params.append(int(u.is_candidate_key))

            if not fields:
                # nothing to update for this item
                continue

            params.extend([u.id, dataset_id])
            sql = f"UPDATE dataset_columns SET {', '.join(fields)} WHERE id = ? AND dataset_id = ?"
            cur.execute(sql, params)
            if cur.rowcount and cur.rowcount > 0:
                updated_ids.append(u.id)

        conn.commit()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update columns") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"dataset_id": dataset_id, "updated_count": len(updated_ids), "updated_ids": updated_ids}



@router.post("/datasets/preview")
def preview_dataset(storage_path: str, format: str):
    """Load a dataset from `storage_path` according to `format` and return first 50 rows as JSON.

    Query/body parameters:
    - `storage_path`: absolute or relative path to the stored file
    - `format`: one of 'csv', 'txt', 'excel', 'xls', 'xlsx'
    """
    try:
        df = load_fiscal(storage_path, format)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to load dataset") from exc

    # take first 50 rows and convert to JSON-serializable structure
    try:
        preview = df.head(50).to_dict(orient="records")
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to serialize preview") from exc

    return {"rows": preview, "row_count": len(preview)}


@router.post("/datasets/{dataset_id}/re-detect-headers")
def redetect_headers(
    dataset_id: int,
    header_row: int = Form(...),
    header_col: Optional[str] = Form(None),
):
    """Re-detect headers and refresh `dataset_columns` using provided coordinates.

    Steps:
    - Update `datasets.header_row` / `datasets.header_col` (if columns exist).
    - Load original dataset file.
    - Clear existing `dataset_columns` for the dataset.
    - Re-run `save_detected_columns` with given `header_row` / `header_col`.
    Returns the refreshed columns.
    """
    try:
        init_db()
        conn = get_connection()
        create_tables(conn)
        cur = conn.cursor()
        # Ensure dataset exists & get path/format
        cur.execute("SELECT storage_path, format FROM datasets WHERE id = ?", (dataset_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset não encontrado")
        storage_path, fmt = row[0], row[1]

        # Update header coords if columns present
        try:
            cur.execute("PRAGMA table_info(datasets)")
            cols = {r[1] for r in cur.fetchall()}
            if "header_row" in cols:
                cur.execute("UPDATE datasets SET header_row = ? WHERE id = ?", (int(header_row), dataset_id))
            if "header_col" in cols:
                cur.execute("UPDATE datasets SET header_col = ? WHERE id = ?", (header_col, dataset_id))
            conn.commit()
        except Exception:
            pass

        # Load original file (raw) then detect columns
        try:
            df = load_fiscal(storage_path, fmt)
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Falha ao carregar dataset: {exc}") from exc

        # Basic column type inference
        from ..pipeline.utils import infer_column_types
        col_types = infer_column_types(df)

        # Clear existing dataset_columns
        cur.execute("DELETE FROM dataset_columns WHERE dataset_id = ?", (dataset_id,))
        conn.commit()

        try:
            save_detected_columns(
                conn,
                dataset_id,
                df,
                col_types,
                header_row=int(header_row),
                header_col=header_col if header_col else None,
            )
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Falha ao detectar headers: {exc}") from exc

        # Return refreshed columns
        cur.execute(
            """
            SELECT id, dataset_id, name, logical_name, data_type, is_value_column, is_candidate_key
            FROM dataset_columns WHERE dataset_id = ?
            """,
            (dataset_id,),
        )
        rows = cur.fetchall()
        cols = [dict(zip([c[0] for c in cur.description], r)) for r in rows]
        return {
            "dataset_id": dataset_id,
            "header_row": header_row,
            "header_col": header_col,
            "columns": cols,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass


__all__ = ["router"]


@router.delete("/datasets/{dataset_id}")
def delete_dataset(dataset_id: int):
    """Delete a dataset record, related metadata and stored files.

    This will:
    - remove rows in `dataset_columns`, `preprocess_runs`, `reconciliation_results`,
      `reversal_configs`, `cancellation_configs` that reference the dataset_id
    - remove reconciliation configs where this dataset is base_a or base_b (and
      related keys and runs/results)
    - delete the stored original file and the preprocessed CSV under `storage/pre/{id}.csv` if present
    """
    try:
        init_db()
        conn = get_connection()
        create_tables(conn)

        cur = conn.cursor()
        # check dataset exists and get storage path
        cur.execute("SELECT storage_path FROM datasets WHERE id = ?", (dataset_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

        storage_path = row[0]

        # Delete dataset-linked reconciliation configs (base_a or base_b)
        cur.execute(
            "SELECT id FROM reconciliation_configs WHERE base_a_id = ? OR base_b_id = ?",
            (dataset_id, dataset_id),
        )
        config_rows = cur.fetchall()
        config_ids = [r[0] for r in config_rows]
        if config_ids:
            # delete keys
            q = f"DELETE FROM reconciliation_keys WHERE config_id IN ({','.join('?' for _ in config_ids)})"
            cur.execute(q, tuple(config_ids))

            # find runs for these configs and delete their results
            qsel = f"SELECT id FROM reconciliation_runs WHERE config_id IN ({','.join('?' for _ in config_ids)})"
            cur.execute(qsel, tuple(config_ids))
            run_rows = cur.fetchall()
            run_ids = [r[0] for r in run_rows]
            if run_ids:
                qdelr = f"DELETE FROM reconciliation_results WHERE run_id IN ({','.join('?' for _ in run_ids)})"
                cur.execute(qdelr, tuple(run_ids))

            # delete runs and configs
            qdelruns = f"DELETE FROM reconciliation_runs WHERE config_id IN ({','.join('?' for _ in config_ids)})"
            cur.execute(qdelruns, tuple(config_ids))
            qdelcfg = f"DELETE FROM reconciliation_configs WHERE id IN ({','.join('?' for _ in config_ids)})"
            cur.execute(qdelcfg, tuple(config_ids))

        # delete any reconciliation_results that reference this dataset directly
        cur.execute("DELETE FROM reconciliation_results WHERE dataset_id = ?", (dataset_id,))

        # delete other dataset-linked records
        cur.execute("DELETE FROM dataset_columns WHERE dataset_id = ?", (dataset_id,))
        cur.execute("DELETE FROM preprocess_runs WHERE dataset_id = ?", (dataset_id,))
        cur.execute("DELETE FROM reversal_configs WHERE dataset_id = ?", (dataset_id,))
        cur.execute("DELETE FROM cancellation_configs WHERE dataset_id = ?", (dataset_id,))

        # finally delete dataset record
        cur.execute("DELETE FROM datasets WHERE id = ?", (dataset_id,))

        conn.commit()

        # remove files: original and preprocessed
        try:
            if storage_path:
                p = Path(storage_path)
                if p.exists():
                    p.unlink()
        except Exception:
            pass

        try:
            pre_path = Path(settings.STORAGE_PATH) / "pre" / f"{int(dataset_id)}.csv"
            if pre_path.exists():
                pre_path.unlink()
        except Exception:
            pass

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete dataset") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"dataset_id": dataset_id, "deleted": True}
