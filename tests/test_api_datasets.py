from fastapi.testclient import TestClient
from app.api.datasets import router
from fastapi import FastAPI
import io


app = FastAPI()
app.include_router(router)

client = TestClient(app)


def test_upload_dataset_endpoint(tmp_path, monkeypatch):
    # monkeypatch storage path and DB path to tmp
    from app.core.config import settings
    storage_dir = tmp_path / "storage"
    db_file = tmp_path / "test.db"
    monkeypatch.setattr(settings, "STORAGE_PATH", str(storage_dir))
    monkeypatch.setattr(settings, "DB_PATH", str(db_file))

    # Prepare a small file
    file_content = b"a,b,c\n1,2,3\n"
    files = {"file": ("sample.csv", io.BytesIO(file_content), "text/csv")}
    data = {"base_type": "fiscal", "format": "csv"}

    response = client.post("/datasets", files=files, data=data)
    assert response.status_code == 200
    body = response.json()
    assert "dataset_id" in body
    assert body["name"] == "sample.csv"
    assert body["base_type"] == "fiscal"
    assert body["format"] == "csv"
