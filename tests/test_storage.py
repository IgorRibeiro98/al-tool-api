import tempfile
from pathlib import Path

from app.core.storage import save_file
from app.core.config import settings


def test_save_file_creates_storage_and_writes(tmp_path, monkeypatch):
    storage_dir = tmp_path / "storage"
    monkeypatch.setattr(settings, "STORAGE_PATH", str(storage_dir))

    file_bytes = b"hello world"
    filename = "greeting.txt"

    abs_path = save_file(file_bytes, filename)
    p = Path(abs_path)
    assert p.exists()
    assert p.read_bytes() == file_bytes
    # ensure returned path is absolute
    assert p.is_absolute()
