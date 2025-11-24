"""Simple file storage helper.

Provides a minimal `save_file` function that writes bytes to the
configured `STORAGE_PATH` and returns the absolute path to the saved file.
"""
from __future__ import annotations

from pathlib import Path
from typing import Union, Any

import pandas as pd

from .config import settings


def save_file(file_bytes: bytes, filename: str) -> str:
    """Save `file_bytes` under `settings.STORAGE_PATH/filename`.

    Ensures the storage directory exists. Returns the absolute path
    to the saved file as a string.
    """
    storage_dir = Path(settings.STORAGE_PATH)
    storage_dir.mkdir(parents=True, exist_ok=True)

    file_path = storage_dir / filename
    file_path.write_bytes(file_bytes)

    return str(file_path.resolve())


def _extract_dataset_id(dataset: Any) -> int:
    """Extract an integer dataset id from several possible inputs.

    Accepts an int, a numeric string, a dict with 'id' or 'dataset_id', or
    an object with an `id` attribute.
    """
    if isinstance(dataset, int):
        return dataset
    if isinstance(dataset, str) and dataset.isdigit():
        return int(dataset)
    if isinstance(dataset, dict):
        if "id" in dataset:
            return int(dataset["id"])
        if "dataset_id" in dataset:
            return int(dataset["dataset_id"])
    if hasattr(dataset, "id"):
        try:
            return int(getattr(dataset, "id"))
        except Exception:
            pass
    raise ValueError("`dataset` must be an int or contain an 'id'/'dataset_id' value")


def save_preprocessed_df(df: pd.DataFrame, dataset: Any) -> str:
    """Save a preprocessed DataFrame as CSV under `storage/pre/<dataset_id>.csv`.

    Returns the absolute path to the saved CSV.
    """
    dataset_id = _extract_dataset_id(dataset)

    pre_dir = Path(settings.STORAGE_PATH) / "pre"
    pre_dir.mkdir(parents=True, exist_ok=True)

    file_path = pre_dir / f"{dataset_id}.csv"
    # ensure the __is_reversal__ column is present so downstream users
    # can rely on its existence. Do not modify caller's DataFrame.
    if "__is_reversal__" not in df.columns:
        df_to_save = df.copy()
        df_to_save["__is_reversal__"] = False
    else:
        df_to_save = df

    # write CSV without index by default
    df_to_save.to_csv(file_path, index=False)

    return str(file_path.resolve())


__all__ = ["save_file", "save_preprocessed_df"]


def export_to_excel(df: pd.DataFrame, path: Union[str, Path]) -> str:
    """Export a DataFrame to an Excel file at `path`.

    Ensures the parent directory exists. Returns the absolute path to the
    written file as a string.
    """
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Use pandas to write Excel file (index excluded by default here)
    df.to_excel(file_path, index=False)

    return str(file_path.resolve())


__all__ = ["save_file", "save_preprocessed_df", "export_to_excel"]
