"""API endpoints to manage reconciliation configurations and keys."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import List, Optional, Any
import json

from ..core.db import init_db, get_connection
from ..repo.schema import create_tables
import json
from typing import Dict, Any, List
from fastapi import BackgroundTasks
from ..repo.reconciliation_results import get_results_for_run, query_reconciliation_results
from fastapi.responses import FileResponse
import pandas as pd
from ..core.storage import export_to_excel
from fastapi import APIRouter
from pathlib import Path
from ..core.config import settings

router = APIRouter()


class ReconciliationKeyIn(BaseModel):
    name: str
    base_a_columns: List[str]
    base_b_columns: List[str]


class ReconciliationConfigIn(BaseModel):
    name: str
    base_a_id: int
    base_b_id: int
    value_column_a: Optional[str] = None
    value_column_b: Optional[str] = None
    invert_a: Optional[int] = 0
    invert_b: Optional[int] = 0
    threshold: Optional[float] = None
    keys: Optional[List[ReconciliationKeyIn]] = []


@router.post("/reconciliation-configs")
def create_reconciliation_config(payload: ReconciliationConfigIn):
    """Create a reconciliation_config and its reconciliation_keys.

    Expects JSON with config fields and optional `keys` array. Returns created
    config id and created keys.
    """
    init_db()
    conn = get_connection()
    try:
        create_tables(conn)
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO reconciliation_configs
            (name, base_a_id, base_b_id, value_column_a, value_column_b, invert_a, invert_b, threshold)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.name,
                payload.base_a_id,
                payload.base_b_id,
                payload.value_column_a,
                payload.value_column_b,
                int(payload.invert_a or 0),
                int(payload.invert_b or 0),
                payload.threshold,
            ),
        )
        conn.commit()
        cfg_id = cur.lastrowid

        created_keys = []
        for k in payload.keys or []:
            cur.execute(
                """
                INSERT INTO reconciliation_keys (config_id, name, base_a_columns, base_b_columns)
                VALUES (?, ?, ?, ?)
                """,
                (cfg_id, k.name, json.dumps(k.base_a_columns), json.dumps(k.base_b_columns)),
            )
            created_keys.append({
                "id": cur.lastrowid,
                "config_id": cfg_id,
                "name": k.name,
                "base_a_columns": k.base_a_columns,
                "base_b_columns": k.base_b_columns,
            })
        conn.commit()

        return {"reconciliation_config_id": cfg_id, "reconciliation_keys": created_keys}

    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass


@router.get("/reconciliations/labels")
def get_reconciliation_labels():
    """Return the fixed set of statuses and groups used by reconciliation."""
    statuses = [
        {"key": "both_zero", "label": "Both Zero", "description": "Both sides are zero"},
        {"key": "match", "label": "Match", "description": "Values match within threshold"},
        {"key": "only_a", "label": "Only A", "description": "Only present in base A"},
        {"key": "only_b", "label": "Only B", "description": "Only present in base B"},
        {"key": "mismatch", "label": "Mismatch", "description": "Both present but values differ beyond threshold"},
    ]

    groups = [
        {"key": "matched", "label": "Matched", "description": "Considered matched"},
        {"key": "unmatched", "label": "Unmatched", "description": "Considered unmatched"},
    ]

    return {"statuses": statuses, "groups": groups}


@router.get("/health")
def health_check():
    """Simple health check endpoint."""
    return {"status": "ok"}


@router.get("/reconciliations/runs/{run_id}")
def get_run_summary(run_id: int):
    """Return summary and results for a reconciliation run by id."""
    init_db()
    conn = get_connection()
    try:
        create_tables(conn)
        cur = conn.cursor()
        cur.execute("SELECT id, config_id, status, started_at, finished_at, summary_json FROM reconciliation_runs WHERE id = ?", (run_id,))
        r = cur.fetchone()
        if not r:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        run = {
            "id": r[0],
            "config_id": r[1],
            "status": r[2],
            "started_at": r[3],
            "finished_at": r[4],
            "summary": json.loads(r[5]) if r[5] else None,
        }

        details = get_results_for_run(conn, run_id)
        return {"run": run, "details": details}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass


@router.get("/reconciliations/results")
def list_reconciliation_results(
    run_id: int | None = None,
    status: str | None = None,
    key_used: str | None = None,
    group_name: str | None = None,
    limit: int = 100,
    offset: int = 0,
):
    """List reconciliation results with optional filters and pagination."""
    init_db()
    conn = get_connection()
    try:
        create_tables(conn)
        payload = query_reconciliation_results(
            conn,
            run_id=run_id,
            status=status,
            key_used=key_used,
            group_name=group_name,
            limit=limit,
            offset=offset,
        )
        return {"results": payload["results"], "total": payload["total"], "limit": limit, "offset": offset}
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass


@router.get("/reconciliations/runs/{run_id}/export")
def export_run_to_excel(run_id: int):
    """Export a reconciliation run (summary + details) to an Excel file and return it for download."""
    init_db()
    conn = get_connection()
    try:
        create_tables(conn)
        cur = conn.cursor()
        cur.execute("SELECT id, config_id, status, started_at, finished_at, summary_json FROM reconciliation_runs WHERE id = ?", (run_id,))
        r = cur.fetchone()
        if not r:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

        run = {
            "id": r[0],
            "config_id": r[1],
            "status": r[2],
            "started_at": r[3],
            "finished_at": r[4],
            "summary": json.loads(r[5]) if r[5] else None,
        }

        details = get_results_for_run(conn, run_id)

        # build DataFrames
        summary_df = pd.DataFrame([run])
        details_df = pd.DataFrame(details)

        # export to excel with two sheets
        filename = f"reconciliation_run_{run_id}.xlsx"
        export_path = Path(settings.STORAGE_PATH) / "exports" / filename
        # use pandas ExcelWriter to write multiple sheets
        export_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(export_path) as writer:
            summary_df.to_excel(writer, sheet_name="summary", index=False)
            details_df.to_excel(writer, sheet_name="details", index=False)

        return FileResponse(str(export_path), filename=filename, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
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


@router.get("/reconciliations/configs")
def list_reconciliation_configs():
    """Return all reconciliation configurations with their keys."""
    init_db()
    conn = get_connection()
    try:
        create_tables(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, base_a_id, base_b_id, value_column_a, value_column_b, invert_a, invert_b, threshold FROM reconciliation_configs"
        )
        rows = cur.fetchall()
        configs: List[Dict[str, Any]] = []
        for r in rows:
            cfg_id = int(r[0])
            cfg = {
                "id": cfg_id,
                "name": r[1],
                "base_a_id": r[2],
                "base_b_id": r[3],
                "value_column_a": r[4],
                "value_column_b": r[5],
                "invert_a": r[6],
                "invert_b": r[7],
                "threshold": r[8],
            }
            # load keys
            cur.execute("SELECT id, name, base_a_columns, base_b_columns FROM reconciliation_keys WHERE config_id = ?", (cfg_id,))
            key_rows = cur.fetchall()
            keys = []
            for kr in key_rows:
                try:
                    a_cols = json.loads(kr[2]) if kr[2] else []
                except Exception:
                    a_cols = []
                try:
                    b_cols = json.loads(kr[3]) if kr[3] else []
                except Exception:
                    b_cols = []
                keys.append({"id": kr[0], "name": kr[1], "base_a_columns": a_cols, "base_b_columns": b_cols})
            cfg["keys"] = keys
            configs.append(cfg)
        return {"reconciliation_configs": configs}
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass


@router.get("/reconciliations/configs/{config_id}")
def get_reconciliation_config(config_id: int):
    """Return a single reconciliation configuration (with keys) by id."""
    init_db()
    conn = get_connection()
    try:
        create_tables(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, base_a_id, base_b_id, value_column_a, value_column_b, invert_a, invert_b, threshold FROM reconciliation_configs WHERE id = ?",
            (config_id,),
        )
        r = cur.fetchone()
        if not r:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reconciliation config not found")
        cfg = {
            "id": int(r[0]),
            "name": r[1],
            "base_a_id": r[2],
            "base_b_id": r[3],
            "value_column_a": r[4],
            "value_column_b": r[5],
            "invert_a": r[6],
            "invert_b": r[7],
            "threshold": r[8],
        }
        cur.execute("SELECT id, name, base_a_columns, base_b_columns FROM reconciliation_keys WHERE config_id = ?", (config_id,))
        key_rows = cur.fetchall()
        keys = []
        for kr in key_rows:
            try:
                a_cols = json.loads(kr[2]) if kr[2] else []
            except Exception:
                a_cols = []
            try:
                b_cols = json.loads(kr[3]) if kr[3] else []
            except Exception:
                b_cols = []
            keys.append({"id": kr[0], "name": kr[1], "base_a_columns": a_cols, "base_b_columns": b_cols})
        cfg["keys"] = keys
        return {"reconciliation_config": cfg}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass



@router.post("/reconciliations/configs/{config_id}/run")
def start_reconciliation_run(config_id: int, enrich_preprocessed: bool = False):
    """Start a reconciliation run for the given config id and return the run result.

    Optional query parameter: `enrich_preprocessed` (bool) — when set to true,
    the endpoint will enrich the preprocessed datasets referenced by the
    reconciliation config (both `base_a_id` and `base_b_id`, if present) using
    `merge_results_with_dataframe` and save the enriched files to
    `storage/exports/enriched_dataset_{dataset_id}_run_{run_id}.xlsx`.

    The endpoint returns the run result and, when enrichment is requested, an
    `enriched_files` list with saved file paths and `enrich_errors` list with
    any enrichment errors (the reconciliation run itself does not fail if
    enrichment fails).
    """
    init_db()
    conn = get_connection()
    try:
        create_tables(conn)
        # import here to avoid circular import at module load
        from ..pipeline.reconciliation import run_reconciliation

        result = run_reconciliation(conn, config_id)

        # optional enrichment of preprocessed datasets referenced by the config
        if enrich_preprocessed:
            enriched_files = []
            enrich_errors = []
            try:
                # get base ids from config
                cur = conn.cursor()
                cur.execute("SELECT base_a_id, base_b_id FROM reconciliation_configs WHERE id = ?", (config_id,))
                cfg_row = cur.fetchone()
                if cfg_row:
                    base_a_id, base_b_id = cfg_row[0], cfg_row[1]
                else:
                    base_a_id, base_b_id = None, None
            except Exception as e:
                base_a_id, base_b_id = None, None
                enrich_errors.append(f"failed to load config bases: {e}")

            # import merge helper locally to avoid circular import at module load
            if base_a_id:
                try:
                    from ..pipeline.reconciliation import merge_results_with_dataframe

                    df_a = merge_results_with_dataframe(int(base_a_id), int(result.get("run_id")))
                    path_a = Path(settings.STORAGE_PATH) / "exports" / f"enriched_dataset_{base_a_id}_run_{result.get('run_id')}.xlsx"
                    path_a.parent.mkdir(parents=True, exist_ok=True)
                    df_a.to_excel(path_a, index=False)
                    enriched_files.append(str(path_a))
                except Exception as e:
                    enrich_errors.append(f"base_a ({base_a_id}) error: {e}")

            if base_b_id:
                try:
                    from ..pipeline.reconciliation import merge_results_with_dataframe

                    df_b = merge_results_with_dataframe(int(base_b_id), int(result.get("run_id")))
                    path_b = Path(settings.STORAGE_PATH) / "exports" / f"enriched_dataset_{base_b_id}_run_{result.get('run_id')}.xlsx"
                    path_b.parent.mkdir(parents=True, exist_ok=True)
                    df_b.to_excel(path_b, index=False)
                    enriched_files.append(str(path_b))
                except Exception as e:
                    enrich_errors.append(f"base_b ({base_b_id}) error: {e}")

            # attach enrichment info to result (do not raise)
            if enriched_files:
                result["enriched_files"] = enriched_files
            if enrich_errors:
                result["enrich_errors"] = enrich_errors

        return {"run": result}
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(ve)) from ve
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass
