import pandas as pd

from app.transformations.engine import TransformationEngine
from app.validation.validator import validate_dataframe


def test_transformation_engine_supports_core_operations():
    df = pd.DataFrame(
        {
            "FIRST_NAME": [" Dinesh ", "Anita", " "],
            "LAST_NAME": ["S J", "Patel", "Kumar"],
            "SALARY": ["1000", "2500", "abc"],
        }
    )

    engine = TransformationEngine()
    transformed = engine.apply(
        df,
        [
            {"operation": "rename", "source": "FIRST_NAME", "target": "first_name"},
            {"operation": "rename", "source": "LAST_NAME", "target": "last_name"},
            {"operation": "trim", "source": "first_name", "target": "first_name"},
            {"operation": "trim", "source": "last_name", "target": "last_name"},
            {"operation": "concat", "inputs": ["first_name", "last_name"], "target": "full_name", "separator": " "},
            {"operation": "cast", "source": "SALARY", "target": "salary", "dtype": "float"},
        ],
    )

    assert "full_name" in transformed.columns
    assert transformed.loc[0, "full_name"] == "Dinesh S J"
    assert transformed["salary"].dtype.kind in "ifufc"


def test_validator_flags_missing_required_columns_and_duplicates():
    df = pd.DataFrame(
        {
            "employee_id": [1, 1],
            "first_name": ["Alice", "Bob"],
        }
    )

    result = validate_dataframe(
        df,
        required_columns=["employee_id", "first_name", "last_name"],
        primary_key="employee_id",
    )

    assert result["valid"] is False
    assert any("required column" in msg.lower() for msg in result["errors"])
    assert any("duplicate" in msg.lower() for msg in result["errors"])
