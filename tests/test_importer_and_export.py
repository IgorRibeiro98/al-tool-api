import pandas as pd
from pathlib import Path

from app.core.storage import save_preprocessed_df, export_to_excel
from app.pipeline.importer import load_preprocessed_dataset_dataframe
from app.core.config import settings


def test_save_and_load_preprocessed(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "STORAGE_PATH", str(tmp_path / "storage"))
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    path = save_preprocessed_df(df, 42)
    loaded = load_preprocessed_dataset_dataframe(42)
    assert list(loaded.columns) == list(df.columns) or "__is_reversal__" in loaded.columns


def test_export_to_excel_creates_file(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "STORAGE_PATH", str(tmp_path / "storage"))
    df = pd.DataFrame({"a": [1], "b": [2]})
    out = export_to_excel(df, tmp_path / "out" / "t.xlsx")
    p = Path(out)
    assert p.exists()
