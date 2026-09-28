import pytest
import pandas as pd
from sqlalchemy import text

from app.adapters.registry import default_registry
from app.adapters.postgres_adapter import PostgresTargetAdapter
from app.adapters.oracle_adapter import OracleSourceAdapter
from app.services.migration_service import MigrationService
from app.validation.validator import validate_migration_contract, normalize_contract


def test_ai_json_extraction_section_consumed():
    """
    Verifies that the Data Plane cleanly consumes the AI teammate's JSON response,
    extracts the SELECT queries and INSERT {VALUES_PLACEHOLDER} loading definitions.
    """
    ai_response = {
        "provider": "gemini",
        "result": {
            "source": "oracle",
            "target": "postgresql",
            "data_extraction": [
                "SELECT DISTINCT DEPARTMENT_ID, DEPARTMENT_NAME FROM EMPLOYEE WHERE DEPARTMENT_ID IS NOT NULL AND DEPARTMENT_NAME IS NOT NULL;",
                "SELECT EMPLOYEE_ID, EMPLOYEE_NAME, DEPARTMENT_ID FROM EMPLOYEE;"
            ],
            "data_management": [
                "INSERT INTO public.departments (department_id, department_name) VALUES {VALUES_PLACEHOLDER};",
                "INSERT INTO public.employees (employee_id, employee_name, department_id) VALUES {VALUES_PLACEHOLDER};"
            ],
            "placeholder": "{VALUES_PLACEHOLDER}"
        }
    }

    normalized = normalize_contract(ai_response)
    assert normalized["source"]["type"] == "oracle"
    assert normalized["target"]["type"] == "postgresql"
    assert len(normalized["data_extraction"]) == 2
    assert len(normalized["data_management"]) == 2

    is_valid, errors = validate_migration_contract(ai_response)
    assert is_valid is True, f"Contract validation failed: {errors}"
    assert errors == []


def test_ddl_create_works_in_postgres():
    """
    Verifies that CREATE TABLE executes successfully against PostgreSQL
    without dropping or deleting any existing tables.
    """
    postgres_adapter = default_registry.get_target_adapter("postgresql")
    create_stmt = """
    CREATE TABLE IF NOT EXISTS public.verify_create_pipeline (
        record_id INT,
        record_name VARCHAR(100)
    )
    """
    executed = postgres_adapter.execute_ddl([create_stmt])
    assert len(executed) == 1

    with postgres_adapter.get_engine().connect() as conn:
        res = conn.execute(
            text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'verify_create_pipeline'")
        ).scalar()
        assert res == "verify_create_pipeline"


def test_ddl_alter_works_in_postgres():
    """
    Verifies that ALTER TABLE executes successfully against PostgreSQL
    without dropping or deleting any existing tables.
    """
    postgres_adapter = default_registry.get_target_adapter("postgresql")
    alter_stmt = "ALTER TABLE public.verify_create_pipeline ADD COLUMN IF NOT EXISTS status_flag VARCHAR(20)"

    executed = postgres_adapter.execute_ddl([alter_stmt])
    assert len(executed) == 1

    with postgres_adapter.get_engine().connect() as conn:
        res = conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'verify_create_pipeline' AND column_name = 'status_flag'")
        ).scalar()
        assert res == "status_flag"


def test_drop_and_delete_are_prohibited_and_blocked():
    """
    Verifies that DROP DATABASE and DELETE FROM are strictly blocked and prohibited
    by both the contract validator and the database adapter.
    """
    delete_spec = {
        "source": {"type": "oracle", "table": "DEPT"},
        "target": {"type": "postgresql", "table": "dept"},
        "table_management": [
            "DELETE FROM public.dept WHERE id = 1"
        ],
        "data_migration": {
            "extraction": {"query": "SELECT * FROM DEPT"},
            "loading": {"table": "dept", "columns": ["id"]},
        }
    }
    is_valid, errors = validate_migration_contract(delete_spec)
    assert is_valid is False
    assert any("Prohibited DDL statement rejected" in err for err in errors)

    postgres_adapter = default_registry.get_target_adapter("postgresql")
    with pytest.raises(ValueError, match="Prohibited DDL statement rejected"):
        postgres_adapter.execute_ddl(["DELETE FROM public.verify_create_pipeline WHERE record_id = 1"])

    with pytest.raises(ValueError, match="Prohibited DDL statement rejected"):
        postgres_adapter.execute_ddl(["DROP DATABASE master"])


def test_oracle_select_real_hr_employees():
    """
    Verifies:
    1. Oracle extraction query executes against real Oracle database (HR.EMPLOYEES).
    2. Rows are returned.
    3. Column names are available.
    4. Row count is captured.
    5. Clean error reporting on invalid query.
    """
    oracle_adapter = default_registry.get_source_adapter("oracle")
    health = oracle_adapter.test_connection()
    assert health["connected"] is True, f"Oracle not connected: {health}"

    query = "SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DATE, SALARY, DEPARTMENT_ID FROM HR.EMPLOYEES"
    df = oracle_adapter.extract(query)

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0, "Expected HR.EMPLOYEES to contain rows"
    lower_cols = [c.lower() for c in df.columns]
    assert "employee_id" in lower_cols
    assert "first_name" in lower_cols
    assert "last_name" in lower_cols
    assert "department_id" in lower_cols
    row_count = len(df)
    assert row_count >= 100, f"Expected at least 100 employees in HR schema, got {row_count}"

    with pytest.raises(RuntimeError, match="Oracle extraction failed"):
        oracle_adapter.extract("SELECT * FROM NON_EXISTENT_TABLE_FOR_TEST")


def test_postgres_insert_loading_with_row_counts():
    """
    Verifies that rows are actually inserted into the PostgreSQL target table
    using the bulk parameterized Pandas target_adapter.load (without DELETE or manual SQL string interpolation),
    and that row counts are verified accurately.
    """
    postgres_adapter = default_registry.get_target_adapter("postgresql")

    postgres_adapter.execute_ddl([
        """
        CREATE TABLE IF NOT EXISTS public.pipeline_insert_verify (
            emp_id INT,
            full_name VARCHAR(100),
            dept_id INT
        )
        """
    ])

    initial_count = postgres_adapter.get_row_count("pipeline_insert_verify", "public")

    test_df = pd.DataFrame({
        "emp_id": [201, 202, 203],
        "full_name": ["Alice Developer", "Bob Engineer", "Charlie Architect"],
        "dept_id": [10, 20, 10],
    })

    loaded_count = postgres_adapter.load(
        df=test_df,
        table="pipeline_insert_verify",
        schema="public",
        mode="append",
    )

    assert loaded_count == 3
    final_count = postgres_adapter.get_row_count("pipeline_insert_verify", "public")
    assert final_count == initial_count + 3


def test_end_to_end_ai_json_data_pipeline_execution():
    """
    Full End-to-End Test demonstrating:
    AI JSON -> Data Plane -> Oracle SELECT -> Extract rows -> Pandas -> PostgreSQL INSERT -> Row counts verified.
    No DROP, no DELETE.
    """
    service = MigrationService()

    contract = {
        "provider": "gemini",
        "result": {
            "source": "oracle",
            "target": "postgresql",
            "table_management": [
                "CREATE TABLE IF NOT EXISTS public.ai_e2e_departments (department_id INT, department_name VARCHAR(100));",
                "CREATE TABLE IF NOT EXISTS public.ai_e2e_employees (employee_id INT, first_name VARCHAR(50), last_name VARCHAR(50), department_id INT);"
            ],
            "data_extraction": [
                "SELECT DISTINCT DEPARTMENT_ID, DEPARTMENT_NAME FROM HR.DEPARTMENTS WHERE DEPARTMENT_ID IS NOT NULL AND DEPARTMENT_NAME IS NOT NULL",
                "SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, DEPARTMENT_ID FROM HR.EMPLOYEES WHERE EMPLOYEE_ID IS NOT NULL"
            ],
            "data_management": [
                "INSERT INTO public.ai_e2e_departments (department_id, department_name) VALUES {VALUES_PLACEHOLDER};",
                "INSERT INTO public.ai_e2e_employees (employee_id, first_name, last_name, department_id) VALUES {VALUES_PLACEHOLDER};"
            ],
            "placeholder": "{VALUES_PLACEHOLDER}",
            "summary": "AI migration contract for HR departments and employees"
        }
    }

    result = service.run_migration(contract)

    assert result["status"] == "success", f"Pipeline failed: {result}"
    assert result["source"] == "oracle"
    assert result["target"]["schema"] == "public"
    assert result["total_tables"] == 2
    assert "ai_e2e_departments" in result["tables_migrated"]
    assert "ai_e2e_employees" in result["tables_migrated"]

    assert result["source_rows"] > 100
    assert result["loaded_rows"] == result["source_rows"]
    assert result["target_rows"] >= result["loaded_rows"]
    assert result["failed_rows"] == 0

    assert len(result["results"]) == 2
    dept_res = result["results"][0]
    assert dept_res["target"]["table"] == "ai_e2e_departments"
    assert dept_res["loaded_rows"] >= 20

    emp_res = result["results"][1]
    assert emp_res["target"]["table"] == "ai_e2e_employees"
    assert emp_res["loaded_rows"] >= 100
