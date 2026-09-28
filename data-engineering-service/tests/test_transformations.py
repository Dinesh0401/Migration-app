import pandas as pd
import pytest
from app.transformations.engine import TransformationEngine


def test_rename_transformation():
    engine = TransformationEngine()
    df = pd.DataFrame({"OLD_A": [1, 2], "OLD_B": ["x", "y"]})

    res1 = engine.apply(df, [{"type": "rename", "source": "OLD_A", "target": "new_a"}])
    assert "new_a" in res1.columns
    assert "OLD_A" not in res1.columns
    assert "OLD_B" in res1.columns

    res2 = engine.apply(df, [{"type": "rename", "columns": {"OLD_A": "col_1", "OLD_B": "col_2"}}])
    assert list(res2.columns) == ["col_1", "col_2"]


def test_trim_transformation():
    engine = TransformationEngine()
    df = pd.DataFrame({"NAME": ["  Alice  ", "Bob ", " Charlie"]})
    res = engine.apply(df, [{"type": "trim", "source": "NAME"}])
    assert list(res["NAME"]) == ["Alice", "Bob", "Charlie"]


def test_cast_transformation():
    engine = TransformationEngine()
    df = pd.DataFrame({"AGE": ["25", "30", None], "SCORE": ["95.5", "88.0", "70.2"]})
    res = engine.apply(
        df,
        [
            {"type": "cast", "source": "AGE", "dtype": "int"},
            {"type": "cast", "source": "SCORE", "dtype": "float"},
        ],
    )
    assert res["AGE"].dtype.name == "Int64"
    assert res.loc[0, "AGE"] == 25
    assert res["SCORE"].dtype.name == "float64"
    assert res.loc[0, "SCORE"] == 95.5


def test_default_transformation():
    engine = TransformationEngine()
    df = pd.DataFrame({"STATUS": ["ACTIVE", None, "PENDING"]})
    res = engine.apply(df, [{"type": "default", "source": "STATUS", "value": "UNKNOWN"}])
    assert list(res["STATUS"]) == ["ACTIVE", "UNKNOWN", "PENDING"]


def test_null_handling():
    engine = TransformationEngine()
    df = pd.DataFrame({"ID": [1, 2, 3], "VAL": ["a", None, "c"]})
    res_drop = engine.apply(df, [{"type": "null_handling", "source": "VAL", "strategy": "drop"}])
    assert len(res_drop) == 2
    assert list(res_drop["ID"]) == [1, 3]

    res_fill = engine.apply(df, [{"type": "null_handling", "source": "VAL", "strategy": "fill", "value": "N/A"}])
    assert len(res_fill) == 3
    assert list(res_fill["VAL"]) == ["a", "N/A", "c"]


def test_filter_transformation():
    engine = TransformationEngine()
    df = pd.DataFrame({"ID": [1, 2, 3, 4], "SCORE": [10, 20, 30, 40]})
    res = engine.apply(df, [{"type": "filter", "column": "SCORE", "operator": "gt", "value": 20}])
    assert len(res) == 2
    assert list(res["ID"]) == [3, 4]


def test_value_mapping():
    engine = TransformationEngine()
    df = pd.DataFrame({"GENDER": ["M", "F", "U"]})
    res = engine.apply(
        df,
        [
            {
                "type": "value_mapping",
                "source": "GENDER",
                "mapping": {"M": "Male", "F": "Female"},
                "default": "Other",
            }
        ],
    )
    assert list(res["GENDER"]) == ["Male", "Female", "Other"]


def test_concatenate_transformation():
    engine = TransformationEngine()
    df = pd.DataFrame({"FIRST": ["John", "Jane"], "LAST": ["Doe", "Smith"]})
    res = engine.apply(
        df,
        [
            {
                "type": "concatenate",
                "source_columns": ["FIRST", "LAST"],
                "target_column": "FULL_NAME",
                "separator": " ",
            }
        ],
    )
    assert "FULL_NAME" in res.columns
    assert list(res["FULL_NAME"]) == ["John Doe", "Jane Smith"]


def test_derive_and_split():
    engine = TransformationEngine()
    df = pd.DataFrame({"EMAIL": ["ALICE@EXAMPLE.COM", "BOB@EXAMPLE.COM"]})
    res = engine.apply(df, [{"type": "derive", "source": "EMAIL", "target": "email_clean", "expression": "lowercase"}])
    assert list(res["email_clean"]) == ["alice@example.com", "bob@example.com"]

    res2 = engine.apply(res, [{"type": "split", "source": "email_clean", "target": "username", "separator": "@", "index": 0}])
    assert list(res2["username"]) == ["alice", "bob"]


def test_empty_transformations_returns_copy():
    engine = TransformationEngine()
    df = pd.DataFrame({"A": [1, 2]})
    res = engine.apply(df, [])
    assert res.equals(df)
    assert res is not df


def test_invalid_transformation_raises():
    engine = TransformationEngine()
    df = pd.DataFrame({"A": [1, 2]})
    with pytest.raises(ValueError, match="Unsupported transformation"):
        engine.apply(df, [{"type": "nonexistent_op"}])
