import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from app.services.migration_service import MigrationService
from app.adapters.registry import AdapterRegistry
from app.adapters.base import BaseSourceAdapter, BaseTargetAdapter


class MockSourceAdapter(BaseSourceAdapter):
    def __init__(self, data=None):
        self.data = (
            data
            if data is not None
            else pd.DataFrame({"ID": [1, 2, 3], "NAME": ["  Alice  ", "Bob", "Charlie"]})
        )

    def extract(self, query: str) -> pd.DataFrame:
        return self.data.copy()

    def test_connection(self) -> dict:
        return {"connected": True}


class MockTargetAdapter(BaseTargetAdapter):
    def __init__(self, row_count: int | None = None):
        self.loaded_data = None
        self.executed_ddls = []
        self._custom_row_count = row_count

    def execute_ddl(self, statements: list[str], allow_destructive_ddl: bool = False) -> list[str]:
        for stmt in statements:
            stmt_clean = stmt.strip()
            if "DROP DATABASE" in stmt_clean.upper():
                raise ValueError(f"Prohibited DDL: {stmt_clean}")
            if not allow_destructive_ddl and ("DROP TABLE" in stmt_clean.upper() or "TRUNCATE" in stmt_clean.upper()):
                raise ValueError(f"Destructive DDL rejected (allow_destructive_ddl=False): {stmt_clean}")
            self.executed_ddls.append(stmt_clean)
        return statements

    def load(self, df: pd.DataFrame, table: str, schema: str = "public", mode: str = "append") -> int:
        self.loaded_data = df.copy()
        return len(df)

    def get_row_count(self, table: str, schema: str = "public") -> int:
        if self._custom_row_count is not None:
            return self._custom_row_count
        return len(self.loaded_data) if self.loaded_data is not None else 0

    def test_connection(self) -> dict:
        return {"connected": True}


def test_migration_service_full_flow():
    registry = AdapterRegistry()
    mock_src = MockSourceAdapter()
    mock_tgt = MockTargetAdapter()

    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)

    spec = {
        "source": {"type": "oracle", "schema": "CUSTOM_SCHEMA"},
        "target": {"type": "postgresql", "schema": "public"},
        "table_management": {
            "statements": ["CREATE TABLE IF NOT EXISTS my_target (id INT, full_name TEXT)"]
        },
        "data_migration": {
            "extraction": {"query": "SELECT ID, NAME FROM CUSTOM_SCHEMA.SRC_TABLE"},
            "loading": {"table": "my_target", "columns": ["id", "full_name"]},
        },
        "transformations": [
            {"type": "rename", "source": "ID", "target": "id"},
            {"type": "trim", "source": "NAME", "target": "full_name"},
        ],
    }

    result = service.run_migration(spec)

    assert result["status"] == "success"
    assert result["source_rows"] == 3
    assert result["extracted_rows"] == 3
    assert result["transformed_rows"] == 3
    assert result["filtered_rows"] == 0
    assert result["loaded_rows"] == 3
    assert result["target_rows"] == 3
    assert result["failed_rows"] == 0
    assert result["validation"]["status"] == "passed"
    assert len(mock_tgt.executed_ddls) == 1
    assert list(mock_tgt.loaded_data.columns) == ["id", "full_name"]
    assert mock_tgt.loaded_data.loc[0, "full_name"] == "Alice"


def test_loading_columns_strictly_enforced_and_drops_unwanted_columns():
    """
    Verifies requirement #1:
    - extract source DataFrame with extra / intermediate columns
    - apply transformations
    - validate required columns
    - select/reorder exactly loading.columns
    - ensure extra columns in DataFrame are NOT loaded into target
    """
    registry = AdapterRegistry()
    df_with_extras = pd.DataFrame(
        {
            "ID": [10, 20],
            "FIRST_NAME": ["Jane", "John"],
            "LAST_NAME": ["Doe", "Smith"],
            "INTERNAL_SECRET": ["sec1", "sec2"],
            "SCRATCHPAD": [999, 888],
        }
    )
    mock_src = MockSourceAdapter(data=df_with_extras)
    mock_tgt = MockTargetAdapter()

    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)

    spec = {
        "source": {"type": "oracle"},
        "target": {"type": "postgresql"},
        "data_migration": {
            "extraction": {"query": "SELECT * FROM SOURCE_TABLE"},
            "loading": {
                "table": "dest_table",
                "columns": ["id", "full_name"],
            },
        },
        "transformations": [
            {"type": "rename", "source": "ID", "target": "id"},
            {"type": "concat", "inputs": ["FIRST_NAME", "LAST_NAME"], "target": "full_name", "separator": " "},
        ],
    }

    result = service.run_migration(spec)

    assert result["status"] == "success"
    assert result["source_rows"] == 2
    assert result["loaded_rows"] == 2
    assert list(mock_tgt.loaded_data.columns) == ["id", "full_name"]
    assert "INTERNAL_SECRET" not in mock_tgt.loaded_data.columns
    assert "SCRATCHPAD" not in mock_tgt.loaded_data.columns
    assert "FIRST_NAME" not in mock_tgt.loaded_data.columns
    assert "LAST_NAME" not in mock_tgt.loaded_data.columns
    assert mock_tgt.loaded_data.loc[0, "full_name"] == "Jane Doe"


def test_destructive_ddl_blocked_by_default():
    """
    Verifies requirement #2:
    Destructive DDL (DROP TABLE, TRUNCATE, DROP SCHEMA) is blocked by default
    without explicit allow_destructive_ddl=True.
    """
    registry = AdapterRegistry()
    mock_src = MockSourceAdapter()
    mock_tgt = MockTargetAdapter()
    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)

    spec = {
        "source": {"type": "oracle"},
        "target": {"type": "postgresql"},
        "table_management": {
            "statements": [
                "DROP TABLE public.target_table",
                "CREATE TABLE public.target_table (id INT)",
            ]
        },
        "data_migration": {
            "extraction": {"query": "SELECT ID FROM SOURCE_TABLE"},
            "loading": {"table": "target_table", "columns": ["ID"]},
        },
    }

    result = service.run_migration(spec)

    assert result["status"] == "failed"
    assert result["stage"] == "validation"
    assert any("Destructive DDL statement rejected" in err for err in result["errors"])
    assert len(mock_tgt.executed_ddls) == 0


def test_destructive_ddl_allowed_with_explicit_option():
    """
    Verifies requirement #2:
    Destructive DDL is executed when allow_destructive_ddl=True is explicitly passed.
    """
    registry = AdapterRegistry()
    mock_src = MockSourceAdapter()
    mock_tgt = MockTargetAdapter()
    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)

    spec = {
        "source": {"type": "oracle"},
        "target": {"type": "postgresql"},
        "execution_options": {
            "allow_destructive_ddl": True
        },
        "table_management": {
            "statements": [
                "DROP TABLE public.target_table",
                "CREATE TABLE public.target_table (id INT)",
            ]
        },
        "data_migration": {
            "extraction": {"query": "SELECT ID FROM SOURCE_TABLE"},
            "loading": {"table": "target_table", "columns": ["ID"]},
        },
    }

    result = service.run_migration(spec)

    assert result["status"] == "success"
    assert "DROP TABLE public.target_table" in mock_tgt.executed_ddls


def test_drop_database_always_rejected_even_with_option():
    """
    DROP DATABASE must never be allowed.
    """
    registry = AdapterRegistry()
    mock_src = MockSourceAdapter()
    mock_tgt = MockTargetAdapter()
    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)

    spec = {
        "source": {"type": "oracle"},
        "target": {"type": "postgresql"},
        "execution_options": {
            "allow_destructive_ddl": True
        },
        "table_management": {
            "statements": [
                "DROP DATABASE production_db",
            ]
        },
        "data_migration": {
            "extraction": {"query": "SELECT ID FROM SOURCE_TABLE"},
            "loading": {"table": "target_table"},
        },
    }

    result = service.run_migration(spec)

    assert result["status"] == "failed"
    assert result["stage"] == "validation"
    assert any("Prohibited DDL" in err for err in result["errors"])


def test_arbitrary_table_and_columns_migration():
    """
    Verifies requirement #4:
    Works on completely arbitrary tables and schemas (e.g. warehouse_inventory).
    """
    registry = AdapterRegistry()
    inv_df = pd.DataFrame(
        {
            "SKU_CODE": ["SKU-001", "SKU-002", "SKU-003"],
            "BIN_LOC": ["A-1", "B-2", "C-3"],
            "QTY_ON_HAND": [150, 42, 0],
            "UNIT_COST": ["12.50", "99.95", "4.00"],
            "STATUS": ["active", "active", "discontinued"],
        }
    )
    mock_src = MockSourceAdapter(data=inv_df)
    mock_tgt = MockTargetAdapter()
    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)

    spec = {
        "source": {"type": "oracle", "schema": "LOGISTICS"},
        "target": {"type": "postgresql", "schema": "inventory"},
        "data_migration": {
            "extraction": {"query": "SELECT * FROM LOGISTICS.INVENTORY_STOCK"},
            "loading": {
                "table": "stock_items",
                "columns": ["sku", "bin", "qty", "cost", "status"],
            },
        },
        "transformations": [
            {"type": "rename", "source": "SKU_CODE", "target": "sku"},
            {"type": "rename", "source": "BIN_LOC", "target": "bin"},
            {"type": "rename", "source": "QTY_ON_HAND", "target": "qty"},
            {"type": "cast", "source": "UNIT_COST", "target": "cost", "dtype": "float"},
            {"type": "rename", "source": "STATUS", "target": "status"},
        ],
    }

    result = service.run_migration(spec)

    assert result["status"] == "success"
    assert result["target"]["schema"] == "inventory"
    assert result["target"]["table"] == "stock_items"
    assert result["source_rows"] == 3
    assert result["loaded_rows"] == 3
    assert list(mock_tgt.loaded_data.columns) == ["sku", "bin", "qty", "cost", "status"]
    assert mock_tgt.loaded_data.loc[1, "cost"] == 99.95


def test_empty_dataframe_batch_migration():
    """
    Verifies handling of empty source data (0 rows).
    """
    registry = AdapterRegistry()
    empty_df = pd.DataFrame({"ID": pd.Series(dtype="int"), "NAME": pd.Series(dtype="str")})
    mock_src = MockSourceAdapter(data=empty_df)
    mock_tgt = MockTargetAdapter()
    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)

    spec = {
        "source": {"type": "oracle"},
        "target": {"type": "postgresql"},
        "data_migration": {
            "extraction": {"query": "SELECT ID, NAME FROM EMPTY_TABLE"},
            "loading": {"table": "empty_target", "columns": ["ID", "NAME"]},
        },
    }

    result = service.run_migration(spec)

    assert result["status"] == "success"
    assert result["source_rows"] == 0
    assert result["extracted_rows"] == 0
    assert result["transformed_rows"] == 0
    assert result["filtered_rows"] == 0
    assert result["loaded_rows"] == 0
    assert result["target_rows"] == 0


def test_transformation_reducing_row_count():
    """
    Verifies requirement #6:
    Accurate metrics when transformations reduce rows (e.g. filter).
    """
    registry = AdapterRegistry()
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "status": ["active", "inactive", "active", "deleted", "active"],
        }
    )
    mock_src = MockSourceAdapter(data=df)
    mock_tgt = MockTargetAdapter()
    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)

    spec = {
        "source": {"type": "oracle"},
        "target": {"type": "postgresql"},
        "data_migration": {
            "extraction": {"query": "SELECT * FROM ITEMS"},
            "loading": {"table": "active_items", "columns": ["id", "status"]},
        },
        "transformations": [
            {"type": "filter", "column": "status", "operator": "==", "value": "active"}
        ],
    }

    result = service.run_migration(spec)

    assert result["status"] == "success"
    assert result["source_rows"] == 5
    assert result["extracted_rows"] == 5
    assert result["transformed_rows"] == 3
    assert result["filtered_rows"] == 2
    assert result["loaded_rows"] == 3
    assert result["target_rows"] == 3


def test_failed_target_load_reports_failure():
    """
    Verifies error reporting and failure metrics when target loading fails.
    """
    registry = AdapterRegistry()
    mock_src = MockSourceAdapter()
    failing_tgt = MagicMock(spec=BaseTargetAdapter)
    failing_tgt.execute_ddl.return_value = []
    failing_tgt.load.side_effect = RuntimeError("PostgreSQL foreign key constraint violated")

    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: failing_tgt)

    service = MigrationService(registry=registry)

    spec = {
        "source": {"type": "oracle"},
        "target": {"type": "postgresql"},
        "data_migration": {
            "extraction": {"query": "SELECT ID, NAME FROM SRC"},
            "loading": {"table": "fk_target", "columns": ["ID", "NAME"]},
        },
    }

    result = service.run_migration(spec)

    assert result["status"] == "failed"
    assert result["stage"] == "target_loading"
    assert result["source_rows"] == 3
    assert result["loaded_rows"] == 0
    assert result["failed_rows"] == 3
    assert "foreign key constraint violated" in result["message"]


def test_table_management_as_string_list():
    """
    Verifies that table_management provided as a raw list of SQL statements
    is accepted, validated, and executed correctly by the migration service.
    """
    registry = AdapterRegistry()
    mock_src = MockSourceAdapter()
    mock_tgt = MockTargetAdapter()
    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)

    spec = {
        "source": {"type": "oracle"},
        "target": {"type": "postgresql"},
        "table_management": [
            "CREATE TABLE IF NOT EXISTS public.employees (employee_id INTEGER NOT NULL);",
            "ALTER TABLE public.departments ADD COLUMN location VARCHAR(100);",
        ],
        "data_migration": {
            "extraction": {"query": "SELECT ID, NAME FROM SRC"},
            "loading": {"table": "employees", "columns": ["ID", "NAME"]},
        },
    }

    result = service.run_migration(spec)

    assert result["status"] == "success"
    assert len(mock_tgt.executed_ddls) == 2
    assert "CREATE TABLE IF NOT EXISTS public.employees (employee_id INTEGER NOT NULL);" in mock_tgt.executed_ddls


def test_table_management_skipped_when_execute_ddl_false():
    """
    Verifies that when execute_ddl is False, table_management DDL statements
    are safely skipped in the Data Plane (preventing duplicate execution).
    """
    registry = AdapterRegistry()
    mock_src = MockSourceAdapter()
    mock_tgt = MockTargetAdapter()
    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)

    spec = {
        "source": {"type": "oracle"},
        "target": {"type": "postgresql"},
        "execution_options": {
            "execute_ddl": False
        },
        "table_management": [
            "ALTER TABLE public.departments ADD COLUMN location VARCHAR(100);",
        ],
        "data_migration": {
            "extraction": {"query": "SELECT ID, NAME FROM SRC"},
            "loading": {"table": "departments", "columns": ["ID", "NAME"]},
        },
    }

    result = service.run_migration(spec)

    assert result["status"] == "success"
    assert len(mock_tgt.executed_ddls) == 0


def test_teammate_dml_contract_with_values_placeholder_multi_table():
    """
    Verifies that the teammate's contract format (nested result, string source/target,
    multiple data_extraction SELECT queries, and multiple data_management INSERT queries
    with {VALUES_PLACEHOLDER}) validates and executes all tables successfully.
    """
    registry = AdapterRegistry()

    class DynamicMockSourceAdapter(BaseSourceAdapter):
        def extract(self, query: str) -> pd.DataFrame:
            if "DISTINCT" in query:
                return pd.DataFrame({
                    "DEPARTMENT_ID": [10, 20, 30],
                    "DEPARTMENT_NAME": ["Administration", "Marketing", "Purchasing"],
                })
            elif "EMPLOYEE_ID" in query:
                return pd.DataFrame({
                    "EMPLOYEE_ID": [101, 102, 103],
                    "EMPLOYEE_NAME": ["Alice", "Bob", "Charlie"],
                    "DEPARTMENT_ID": [10, 20, 10],
                })
            return pd.DataFrame()

        def test_connection(self):
            return {"connected": True}

    class MultiTargetAdapter(BaseTargetAdapter):
        def __init__(self):
            self.loaded_tables: dict[str, pd.DataFrame] = {}

        def execute_ddl(self, statements, allow_destructive_ddl=False):
            return []

        def load(self, df, table, schema="public", mode="append"):
            self.loaded_tables[table] = df.copy()
            return len(df)

        def get_row_count(self, table, schema="public"):
            return len(self.loaded_tables.get(table, []))

        def test_connection(self):
            return {"connected": True}

    mock_src = DynamicMockSourceAdapter()
    mock_tgt = MultiTargetAdapter()
    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)

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
            "files": [
                {
                    "path": "migration/oracle/data/001_select_employee.sql",
                    "group": "data_extraction",
                    "index": 0,
                    "statement": "SELECT DISTINCT DEPARTMENT_ID, DEPARTMENT_NAME FROM EMPLOYEE WHERE DEPARTMENT_ID IS NOT NULL AND DEPARTMENT_NAME IS NOT NULL;",
                },
                {
                    "path": "migration/oracle/data/002_select_employee.sql",
                    "group": "data_extraction",
                    "index": 1,
                    "statement": "SELECT EMPLOYEE_ID, EMPLOYEE_NAME, DEPARTMENT_ID FROM EMPLOYEE;",
                },
                {
                    "path": "migration/postgres/dml/003_insert_departments.sql",
                    "group": "data_management",
                    "index": 0,
                    "statement": "INSERT INTO public.departments (department_id, department_name) VALUES {VALUES_PLACEHOLDER};",
                },
                {
                    "path": "migration/postgres/dml/004_insert_employees.sql",
                    "group": "data_management",
                    "index": 1,
                    "statement": "INSERT INTO public.employees (employee_id, employee_name, department_id) VALUES {VALUES_PLACEHOLDER};",
                },
            ],
            "placeholder": "{VALUES_PLACEHOLDER}",
            "summary": "Generated 2 Oracle to PostgreSQL table migration pairs.",
        },
        "layout": {
            "extraction": "migration/oracle/data",
            "management": "migration/postgres/dml",
            "manifest": "migration/manifest.json",
        },
    }

    result = service.run_migration(contract)

    assert result["status"] == "success"
    assert result["total_tables"] == 2
    assert "departments" in result["tables_migrated"]
    assert "employees" in result["tables_migrated"]
    assert result["source_rows"] == 6
    assert result["loaded_rows"] == 6

    assert "departments" in mock_tgt.loaded_tables
    assert list(mock_tgt.loaded_tables["departments"].columns) == ["department_id", "department_name"]
    assert len(mock_tgt.loaded_tables["departments"]) == 3

    assert "employees" in mock_tgt.loaded_tables
    assert list(mock_tgt.loaded_tables["employees"].columns) == ["employee_id", "employee_name", "department_id"]
    assert len(mock_tgt.loaded_tables["employees"]) == 3
