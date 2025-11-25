import sqlite3
from pathlib import Path

from app.core.config import settings
from app.core.db import init_db, get_connection
from app.repo import schema
from app.repo.reconciliation_results import save_reconciliation_results, query_reconciliation_results


def test_save_and_query_results(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "test.db"))
    init_db()
    conn = get_connection()
    try:
        schema.create_tables(conn)
        run_id = 1
        results = [
            {"row_identifier": "k1", "status": "Conferem", "group": "Conciliados", "key_name": "keyA", "difference": 0.0},
            {"row_identifier": "k2", "status": "Apenas A", "group": "Não conciliados", "key_name": "keyA", "difference": 10.0},
        ]
        ids = save_reconciliation_results(conn, run_id, results)
        assert len(ids) == 2

        res = query_reconciliation_results(conn, run_id=run_id, limit=10, offset=0)
        assert res["total"] >= 2
        assert any(r["row_identifier"] == "k1" for r in res["results"]) or any(r["row_identifier"] == "k1" for r in res["results"])
    finally:
        conn.close()
