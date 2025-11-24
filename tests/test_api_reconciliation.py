import json
import pandas as pd
from fastapi.testclient import TestClient
from fastapi import FastAPI

from app.api.reconciliation import router
from app.core.config import settings
from app.core.db import init_db, get_connection
from app.repo.schema import create_tables
from app.repo.datasets import create_dataset_record


app = FastAPI()
app.include_router(router)
client = TestClient(app)


def test_reconciliation_run_and_endpoints(tmp_path, monkeypatch):
    # setup storage and db
    monkeypatch.setattr(settings, "STORAGE_PATH", str(tmp_path / "storage"))
    db_file = tmp_path / "test.db"
    monkeypatch.setattr(settings, "DB_PATH", str(db_file))

    init_db()
    conn = get_connection()
    try:
        create_tables(conn)

        # prepare two preprocessed CSV files for base A and B
        storage = tmp_path / "storage"
        pre_dir = storage / "pre"
        pre_dir.mkdir(parents=True, exist_ok=True)

        df_a = pd.DataFrame({"ida": ["K1", "K2"], "value": [100, 50]})
        df_b = pd.DataFrame({"idb": ["K1", "K3"], "value": [100, 20]})

        # save as <dataset_id>.csv names; create dataset records first with storage paths
        # create dummy files to point to
        file_a = storage / "a.csv"
        file_b = storage / "b.csv"
        file_a.parent.mkdir(parents=True, exist_ok=True)
        df_a.to_csv(file_a, index=False)
        df_b.to_csv(file_b, index=False)

        # create dataset records
        id_a = create_dataset_record(conn, "a.csv", "B", "csv", str(file_a))
        id_b = create_dataset_record(conn, "b.csv", "B", "csv", str(file_b))

        # create preprocessed CSVs under storage/pre/<id>.csv
        df_a.to_csv(pre_dir / f"{id_a}.csv", index=False)
        df_b.to_csv(pre_dir / f"{id_b}.csv", index=False)

        # create reconciliation config with a single key mapping ida->idb and value_column 'value'
        payload = {
            "name": "test-recon",
            "base_a_id": id_a,
            "base_b_id": id_b,
            "value_column_a": "value",
            "value_column_b": "value",
            "invert_a": 0,
            "invert_b": 0,
            "threshold": 0.0,
            "keys": [
                {"name": "byKey", "base_a_columns": ["ida"], "base_b_columns": ["idb"]}
            ],
        }

        resp = client.post("/reconciliation-configs", json=payload)
        assert resp.status_code == 200
        cfg = resp.json()
        cfg_id = cfg["reconciliation_config_id"]

        # start run
        run_resp = client.post(f"/reconciliations/configs/{cfg_id}/run")
        assert run_resp.status_code == 200
        run_json = run_resp.json()["run"]
        run_id = run_json["run_id"]

        # get run summary
        summary_resp = client.get(f"/reconciliations/runs/{run_id}")
        assert summary_resp.status_code == 200
        body = summary_resp.json()
        assert "run" in body and "details" in body

        # list results via listing endpoint
        list_resp = client.get(f"/reconciliations/results?run_id={run_id}&limit=10&offset=0")
        assert list_resp.status_code == 200
        list_body = list_resp.json()
        assert "results" in list_body and "total" in list_body

        # export to excel
        export_resp = client.get(f"/reconciliations/runs/{run_id}/export")
        assert export_resp.status_code == 200
        # content-type for xlsx
        assert "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in export_resp.headers.get("content-type", "")

        # labels and health
        labels = client.get("/reconciliations/labels").json()
        assert "statuses" in labels and "groups" in labels
        health = client.get("/reconciliations/health").json()
        assert health.get("status") == "ok"

    finally:
        conn.close()
