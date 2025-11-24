import pandas as pd
import numpy as np

from app.pipeline.utils import (
    build_key,
    generate_keys,
    filter_concilable,
    invert_value,
    group_by_key,
    classify,
    find_missing_keys,
    attach_results,
)


def test_build_key_and_generate_keys():
    df = pd.DataFrame({"a": [1, None], "b": ["x", "y"]})
    s0 = build_key(df.iloc[0], ["a", "b"]) 
    assert s0 == "1|x"

    keys = generate_keys(df, ["a", "b"])
    assert isinstance(keys, pd.Series)
    assert keys.tolist() == ["1|x", "|y"]


def test_filter_concilable():
    df = pd.DataFrame({"v": [1, 2, 3], "__is_reversal__": [False, True, 0], "__is_canceled__": [0, 0, 1]})
    out = filter_concilable(df)
    # row 1 is reversal, row 2 canceled -> only row 0 kept
    assert len(out) == 1


def test_invert_value_coercion_and_keyerror():
    df = pd.DataFrame({"val": [1, "2", "x"]})
    out = invert_value(df, "val")
    assert out["val"].iloc[0] == -1
    assert out["val"].iloc[1] == -2
    assert np.isnan(out["val"].iloc[2])

    try:
        invert_value(df, "missing")
        assert False, "Expected KeyError"
    except KeyError:
        pass


def test_group_by_key_and_classify():
    df = pd.DataFrame({"key": ["A", "B", "A"], "amt": [10, 5, 2]})
    totals = group_by_key(df, df["key"], "amt")
    assert totals["A"] == 12.0 and totals["B"] == 5.0

    # classification
    assert classify(0, 0)[0] == "both_zero"
    assert classify(100, 99, threshold=1)[0] == "match"
    assert classify(5, 0)[0] == "only_a"
    assert classify(0, 3)[0] == "only_b"
    assert classify(5, 3)[0] == "mismatch"


def test_find_missing_and_attach_results():
    a = ["k1", "k2"]
    b = ["k2", "k3"]
    onlyA, onlyB = find_missing_keys(a, b)
    assert onlyA == ["k1"] and onlyB == ["k3"]

    df = pd.DataFrame({"k": ["k1", "k2", "k3"], "v": [1, 2, 3]})
    mapping = {"k1": ("match", "matched", 0.0), "k3": {"status": "only_a", "group": "unmatched", "difference": 3.0}}
    out = attach_results(df, mapping, key_column="k", status_col="st", group_col="grp", diff_col="diff")
    assert "st" in out.columns and "grp" in out.columns and "diff" in out.columns
    assert out.loc[0, "st"] == "match"
    assert out.loc[2, "st"] == "only_a"
