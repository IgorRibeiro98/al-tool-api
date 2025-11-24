"""Repository helpers for reconciliation results.

Provides helpers to save single results or batches and to query results
for a reconciliation run.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
import sqlite3


def save_reconciliation_result(
    conn: sqlite3.Connection,
    run_id: int,
    dataset_id: Optional[int],
    row_identifier: str,
    status: str,
    group_name: Optional[str],
    key_used: Optional[str],
    difference: Optional[float],
) -> int:
    """Insert a single reconciliation result and return the inserted id.

    Commits the transaction after insert.
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO reconciliation_results
        (run_id, dataset_id, row_identifier, status, group_name, key_used, difference)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, dataset_id, row_identifier, status, group_name, key_used, difference),
    )
    conn.commit()
    return cur.lastrowid


def save_reconciliation_results(conn: sqlite3.Connection, run_id: int, results: Iterable[Dict[str, Any]]) -> List[int]:
    """Insert multiple reconciliation result records.

    `results` is an iterable of dicts with keys:
      - row_identifier, status, group, key_name, difference, dataset_id (optional)

    Returns list of inserted ids.
    """
    cur = conn.cursor()
    ids: List[int] = []
    for r in results:
        dataset_id = r.get("dataset_id") if isinstance(r, dict) else None
        row_identifier = r.get("row_identifier") or r.get("key")
        status = r.get("status")
        group_name = r.get("group") or r.get("group_name")
        key_used = r.get("key_name") or r.get("key_used")
        difference = r.get("difference")

        cur.execute(
            """
            INSERT INTO reconciliation_results
            (run_id, dataset_id, row_identifier, status, group_name, key_used, difference)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, dataset_id, row_identifier, status, group_name, key_used, difference),
        )
        ids.append(cur.lastrowid)

    conn.commit()
    return ids


def get_results_for_run(conn: sqlite3.Connection, run_id: int) -> List[Dict[str, Any]]:
    """Return all reconciliation results for a given `run_id` as list of dicts."""
    cur = conn.cursor()
    cur.execute(
        "SELECT id, run_id, dataset_id, row_identifier, status, group_name, key_used, difference FROM reconciliation_results WHERE run_id = ?",
        (run_id,),
    )
    rows = cur.fetchall()
    results: List[Dict[str, Any]] = []
    for r in rows:
        results.append(
            {
                "id": r[0],
                "run_id": r[1],
                "dataset_id": r[2],
                "row_identifier": r[3],
                "status": r[4],
                "group_name": r[5],
                "key_used": r[6],
                "difference": r[7],
            }
        )
    return results


__all__ = ["save_reconciliation_result", "save_reconciliation_results", "get_results_for_run"]


def query_reconciliation_results(
    conn: sqlite3.Connection,
    run_id: int | None = None,
    status: str | None = None,
    key_used: str | None = None,
    group_name: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """Query reconciliation results with optional filters and pagination.

    Returns dict with keys: `results` (list of dicts) and `total` (int total matching rows).
    """
    params: list = []
    where_clauses: list = []
    if run_id is not None:
        where_clauses.append("run_id = ?")
        params.append(run_id)
    if status is not None:
        where_clauses.append("status = ?")
        params.append(status)
    if key_used is not None:
        where_clauses.append("key_used = ?")
        params.append(key_used)
    if group_name is not None:
        where_clauses.append("group_name = ?")
        params.append(group_name)

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    # total count
    cur = conn.cursor()
    count_sql = f"SELECT COUNT(1) FROM reconciliation_results {where_sql}"
    cur.execute(count_sql, tuple(params))
    total = int(cur.fetchone()[0])

    # results with pagination
    query_sql = f"SELECT id, run_id, dataset_id, row_identifier, status, group_name, key_used, difference FROM reconciliation_results {where_sql} ORDER BY id ASC LIMIT ? OFFSET ?"
    final_params = list(params) + [int(limit), int(offset)]
    cur.execute(query_sql, tuple(final_params))
    rows = cur.fetchall()
    results = []
    for r in rows:
        results.append(
            {
                "id": r[0],
                "run_id": r[1],
                "dataset_id": r[2],
                "row_identifier": r[3],
                "status": r[4],
                "group_name": r[5],
                "key_used": r[6],
                "difference": r[7],
            }
        )

    return {"results": results, "total": total}
