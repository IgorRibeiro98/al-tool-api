"""Export helpers: build XLSX files from SQL dataset and reconciliation results.

This module focuses on a clear, testable flow: fetch metadata, stream rows
page-by-page, transform presentation-only columns and write a final XLSX.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


LOG = logging.getLogger(__name__)

# Constants
EXPORT_DIR = os.path.join("storage", "exports")
DEFAULT_SHEET = "sheet1"
PAGE_SIZE = 1000
HEADER_TRANS = {
    "group_name": "Grupo",
    "key_used": "Chave Usada",
    "__is_canceled__": "Cancelado",
    "__is_reversal__": "Estorno",
    "status": "Status",
}

STATUS_TRANSLATIONS = {
    "matched": "Conciliado",
    "reconciled": "Conciliado",
    "unmatched": "Não conciliado",
    "mismatch": "Divergência",
    "both_zero": "Ambos zero",
    "Conferem": "Conferem",
    "Apenas A": "Apenas A",
    "Apenas B": "Apenas B",
}


def _safe_identifier(name: str) -> str:
    return f'"{name.replace('"', '""')}"'


def _ensure_export_dir() -> None:
    os.makedirs(EXPORT_DIR, exist_ok=True)


def _bool_to_pt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return "Sim" if int(value) != 0 else "Não"
    s = str(value).strip().lower()
    if s in ("1", "true", "t", "y", "yes", "sim"):
        return "Sim"
    if s in ("0", "false", "f", "n", "no", "nao", "não"):
        return "Não"
    return str(value)


def _humanize_key(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if s.upper() == s and ("_" in s or s.isupper()):
        parts = [p.capitalize() for p in s.split("_") if p]
        return " ".join(parts)
    return s


def _get_dataset_header(conn, dataset_id: int) -> int:
    cur = conn.cursor()
    cur.execute("SELECT header_row FROM datasets WHERE id = ?", (dataset_id,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Dataset not found: {dataset_id}")
    return int(row[0]) if row[0] is not None else 1


def _get_table_columns(conn, table_name: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({ _safe_identifier(table_name) })")
    return [r[1] for r in cur.fetchall() if r[1] != "row_index"]


def _get_max_row_index(conn, table_name: str) -> Optional[int]:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT MAX(row_index) FROM { _safe_identifier(table_name) }")
        r = cur.fetchone()
        return int(r[0]) if r and r[0] is not None else None
    except Exception:
        LOG.exception("Failed to get max row index for %s", table_name)
        return None


def _init_matrix(rows: int, cols: int) -> List[List[Optional[Any]]]:
    return [[None] * cols for _ in range(max(0, rows))]


def _write_excel(df: pd.DataFrame, out_path: str, sheet: str = DEFAULT_SHEET) -> None:
    _ensure_export_dir()
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet, index=False, header=False)


def generate_dataset_xlsx(conn: Any, dataset_id: int) -> Dict[str, Any]:
    """Export `dataset_{id}` to XLSX using a streaming approach."""
    if not isinstance(dataset_id, int):
        raise TypeError("dataset_id must be an int")

    header_row = _get_dataset_header(conn, dataset_id)
    table_name = f"dataset_{int(dataset_id)}"
    sql_cols = _get_table_columns(conn, table_name)
    ncols = len(sql_cols)

    # Prepare SQL column list
    col_list_sql = ", ".join([_safe_identifier(c) for c in sql_cols]) if sql_cols else ""

    max_row_idx = _get_max_row_index(conn, table_name)
    max_row = max_row_idx if max_row_idx and max_row_idx > header_row else header_row

    cur = conn.cursor()
    offset = 0

    _ensure_export_dir()
    out_path = os.path.join(EXPORT_DIR, f"dataset_{int(dataset_id)}.xlsx")

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:

        # Stream rows in pages and write directly to the file
        row_offset = 1  # Start writing data after the header
        while True:
            if ncols:
                sql = f"SELECT row_index, {col_list_sql} FROM { _safe_identifier(table_name) } ORDER BY row_index ASC LIMIT ? OFFSET ?"
            else:
                sql = f"SELECT row_index FROM { _safe_identifier(table_name) } ORDER BY row_index ASC LIMIT ? OFFSET ?"

            cur.execute(sql, (PAGE_SIZE, offset))
            batch = cur.fetchall()
            if not batch:
                break

            data = []
            for row in batch:
                ridx = int(row[0])
                tidx = max(0, ridx - 1)
                if ncols == 0:
                    continue

                row_data = []
                if ridx == header_row - 1:
                    for j in range(ncols):
                        colname = sql_cols[j]
                        orig = row[1 + j]
                        if colname in HEADER_TRANS:
                            row_data.append(HEADER_TRANS[colname])
                        else:
                            row_data.append(orig)
                    data.append(row_data)
                    continue
                else:
                    for j in range(ncols):
                        val = row[1 + j]
                        row_data.append(val)
                        # colname = sql_cols[j]
                        # if colname in ("__is_canceled__", "__is_reversal__"):
                        #     row_data.append(_bool_to_pt(val))
                        # elif colname == "status":
                        #     row_data.append(STATUS_TRANSLATIONS.get(str(val).strip(), str(val) if val is not None else ""))
                        # elif colname == "key_used":
                        #     row_data.append(_humanize_key(val))
                        # else:
                        #     row_data.append(val)
                data.append(row_data)

            pd.DataFrame(data).to_excel(
                writer, sheet_name=DEFAULT_SHEET, index=False, header=False, startrow=row_offset
            )
            row_offset += len(data)
            offset += PAGE_SIZE

    return {"path": out_path, "rows": max_row, "header_row": header_row, "columns": sql_cols}


def generate_reconciliation_xlsx(conn: Any, run_id: int) -> Dict[str, Any]:
    """Generate an XLSX file for a reconciliation run.

    Kept intentionally small: fetch summary and results, build DataFrames
    and write two sheets: `summary` and `details`.
    """
    if not isinstance(run_id, int):
        raise TypeError("run_id must be an int")

    cur = conn.cursor()
    cur.execute(
        "SELECT id, config_id, status, started_at, finished_at, summary_json FROM reconciliation_runs WHERE id = ?",
        (run_id,),
    )
    run_row = cur.fetchone()
    if not run_row:
        raise ValueError(f"reconciliation_run not found: {run_id}")

    summary_json = run_row[5]
    try:
        summary = json.loads(summary_json) if summary_json else {}
    except Exception:
        LOG.exception("Failed to parse summary_json for run %s", run_id)
        summary = {"raw": summary_json}

    cur.execute(
        "SELECT id, run_id, dataset_id, row_identifier, status, group_name, key_used, difference FROM reconciliation_results WHERE run_id = ? ORDER BY id ASC",
        (run_id,),
    )
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description] if cur.description else []

    details_df = pd.DataFrame.from_records(rows, columns=cols)
    summary_records = [
        {
            "run_id": run_row[0],
            "config_id": run_row[1],
            "status": run_row[2],
            "started_at": run_row[3],
            "finished_at": run_row[4],
            **(summary or {}),
        }
    ]
    summary_df = pd.DataFrame.from_records(summary_records)

    _ensure_export_dir()
    out_path = os.path.join(EXPORT_DIR, f"reconciliation_run_{int(run_id)}.xlsx")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        details_df.to_excel(writer, sheet_name="details", index=False)

    return {"path": out_path, "rows": len(details_df), "summary": summary}


__all__ = ["generate_reconciliation_xlsx", "generate_dataset_xlsx"]
