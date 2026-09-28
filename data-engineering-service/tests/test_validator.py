import pandas as pd
from app.validation.validator import validate_migration_contract, validate_dataframe


def test_contract_validation_success():
    spec = {
        "source": {"type": "oracle", "schema": "HR"},
        "target": {"type": "postgresql", "schema": "public"},
        "data_migration": {
            "extraction": {"query": "SELECT * FROM HR.JOBS"},
            "loading": {"table": "jobs", "columns": ["job_id", "job_title"]},
        },
        "transformations": [
            {"type": "rename", "source": "JOB_ID", "target": "job_id"},
        ],
    }
    valid, errors = validate_migration_contract(spec)
    assert valid is True
    assert errors == []


def test_contract_validation_missing_source():
    spec = {
        "target": {"type": "postgresql"},
        "data_migration": {"loading": {"table": "target_tbl"}},
    }
    valid, errors = validate_migration_contract(spec)
    assert valid is False
    assert any("source" in err for err in errors)


def test_contract_validation_missing_query_and_table():
    spec = {
        "source": {"type": "oracle"},
        "target": {"type": "postgresql"},
        "data_migration": {"loading": {"table": "target_tbl"}},
    }
    valid, errors = validate_migration_contract(spec)
    assert valid is False
    assert any("extraction query" in err for err in errors)


def test_contract_validation_catches_destructive_ddl_by_default():
    spec = {
        "source": {"type": "oracle", "table": "DEPT"},
        "target": {"type": "postgresql", "table": "dept"},
        "table_management": {
            "statements": ["DROP TABLE dept", "CREATE TABLE dept (id int)"]
        },
    }
    valid, errors = validate_migration_contract(spec)
    assert valid is False
    assert any("Destructive DDL statement rejected" in err for err in errors)


def test_contract_validation_permits_destructive_ddl_when_flagged():
    spec = {
        "source": {"type": "oracle", "table": "DEPT"},
        "target": {"type": "postgresql", "table": "dept"},
        "execution_options": {"allow_destructive_ddl": True},
        "table_management": {
            "statements": ["DROP TABLE dept", "CREATE TABLE dept (id int)"]
        },
    }
    valid, errors = validate_migration_contract(spec)
    assert valid is True
    assert errors == []


def test_contract_validation_always_blocks_drop_database():
    spec = {
        "source": {"type": "oracle", "table": "DEPT"},
        "target": {"type": "postgresql", "table": "dept"},
        "execution_options": {"allow_destructive_ddl": True},
        "table_management": {
            "statements": ["DROP DATABASE master"]
        },
    }
    valid, errors = validate_migration_contract(spec)
    assert valid is False
    assert any("Prohibited DDL statement rejected" in err for err in errors)


def test_contract_validation_invalid_loading_columns():
    spec = {
        "source": {"type": "oracle", "table": "DEPT"},
        "target": {"type": "postgresql", "table": "dept"},
        "data_migration": {
            "loading": {"table": "dept", "columns": "not_a_list"}
        },
    }
    valid, errors = validate_migration_contract(spec)
    assert valid is False
    assert any("loading.columns" in err for err in errors)


def test_dataframe_validation_detects_duplicates():
    df = pd.DataFrame({"id": [1, 2, 2], "val": ["a", "b", "c"]})
    res = validate_dataframe(df, primary_key="id")
    assert res["valid"] is False
    assert any("Duplicate" in err for err in res["errors"])


def test_dataframe_validation_detects_missing_columns():
    df = pd.DataFrame({"id": [1, 2]})
    res = validate_dataframe(df, required_columns=["id", "name"])
    assert res["valid"] is False
    assert any("Missing required" in err for err in res["errors"])


def test_dataframe_validation_passes():
    df = pd.DataFrame({"id": [1, 2], "name": ["Alpha", "Beta"]})
    res = validate_dataframe(df, required_columns=["id", "name"], primary_key="id")
    assert res["valid"] is True
    assert res["status"] == "passed"


def test_contract_validation_teammate_format():
    contract = {
        "provider": "gemini",
        "result": {
            "source": "oracle",
            "target": "postgresql",
            "data_extraction": [
                "SELECT DISTINCT DEPARTMENT_ID, DEPARTMENT_NAME FROM EMPLOYEE WHERE DEPARTMENT_ID IS NOT NULL AND DEPARTMENT_NAME IS NOT NULL;",
                "SELECT EMPLOYEE_ID, EMPLOYEE_NAME, DEPARTMENT_ID FROM EMPLOYEE;",
            ],
            "data_management": [
                "INSERT INTO public.departments (department_id, department_name) VALUES {VALUES_PLACEHOLDER};",
                "INSERT INTO public.employees (employee_id, employee_name, department_id) VALUES {VALUES_PLACEHOLDER};",
            ],
            "placeholder": "{VALUES_PLACEHOLDER}",
        },
    }
    valid, errors = validate_migration_contract(contract)
    assert valid is True
    assert errors == []
