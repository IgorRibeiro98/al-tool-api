import json
import pandas as pd
import numpy as np

from app.pipeline.cancellation import mark_canceled


def test_mark_canceled_basic_and_missing():
    df = pd.DataFrame({
        "ind": ["S", "N", None, "s"],
        "val": [1, 2, 3, 4],
    })

    # basic case: canceled_value 'S'
    out = mark_canceled(df.copy(), "ind", "S")
    assert "__is_canceled__" in out.columns
    assert out["__is_canceled__"].tolist() == [True, False, False, False]

    # missing column -> should create column with all False
    df2 = pd.DataFrame({"a": [1, 2, 3]})
    out2 = mark_canceled(df2.copy(), "missing", "X")
    assert "__is_canceled__" in out2.columns
    assert out2["__is_canceled__"].sum() == 0


def test_mark_canceled_handles_nans_and_types():
    df = pd.DataFrame({
        "ind": ["S", np.nan, "S", 1],
    })

    out = mark_canceled(df.copy(), "ind", "S")
    # NaN should be False
    assert out["__is_canceled__"].tolist() == [True, False, True, False]

    # When comparing against numeric canceled_value, non-equal types should be False
    out2 = mark_canceled(df.copy(), "ind", 1)
    assert out2["__is_canceled__"].tolist() == [False, False, False, True]


def test_run_preprocess_base_b_integration(tmp_path, monkeypatch):
    # Setup temporary storage and DB
    from app.core.config import settings
    from app.core.db import init_db, get_connection
    from app.repo.schema import create_tables
    from app.repo.datasets import create_dataset_record
    from app.repo.cancellation_config import save_cancellation_config
    from app.pipeline.preprocess_base_b import run_preprocess_base_b

    monkeypatch.setattr(settings, "STORAGE_PATH", str(tmp_path / "storage"))
    db_file = tmp_path / "test.db"
    monkeypatch.setattr(settings, "DB_PATH", str(db_file))

    # prepare a sample CSV file to act as dataset
    df = pd.DataFrame({
        "id_a": ["A", "B", "C", "D"],
        "id_b": ["X", "Y", "Z", "W"],
        "indicator": ["S", "N", "S", None],
        "value": [10, 20, 30, 40],
    })

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir(parents=True, exist_ok=True)
    csv_path = storage_dir / "sample.csv"
    df.to_csv(csv_path, index=False)

    # init DB and create tables
    init_db()
    conn = get_connection()
    try:
        create_tables(conn)

        # create dataset record
        dataset_id = create_dataset_record(conn, "sample.csv", "B", "csv", str(csv_path))

        # save cancellation config: indicator column 'indicator', canceled_value 'S', active 'N'
        save_cancellation_config(conn, dataset_id, "indicator", "S", "N")

        # run preprocessing
        summary = run_preprocess_base_b(conn, dataset_id)

        assert summary["total_rows"] == 4
        assert summary["canceled_rows"] == 2
        assert summary["active_rows"] == 2

        # ensure preprocessed file exists and has __is_canceled__ column
        pre_file = storage_dir / "pre" / f"{dataset_id}.csv"
        assert pre_file.exists()
        df_pre = pd.read_csv(pre_file)
        assert "__is_canceled__" in df_pre.columns
        # check counts match
        assert int(df_pre["__is_canceled__"].sum()) == 2

    finally:
        conn.close()
