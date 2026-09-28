from __future__ import annotations

import pytest
from sqlalchemy import text
from app.adapters.registry import default_registry
from app.services.migration_service import MigrationService
from app.validation.validator import normalize_contract, validate_migration_contract


TEAMMATE_SCHEMA_CONTRACT = {
  "result": {
    "schema_design": {
      "provider": "groq",
      "status": "recommended",
      "summary": "FIRST_NAME set to NOT NULL, keys preserved.",
      "issue_reason": "",
      "target_schema": {
        "tables": [
          {
            "tableName": "EMPLOYEES",
            "columns": [
              {
                "columnName": "EMPLOYEE_ID",
                "dataType": "INTEGER",
                "dataLength": -1,
                "dataPrecision": -1,
                "dataScale": -1,
                "nullable": "N",
                "dataDefault": "",
                "columnId": 1
              },
              {
                "columnName": "FIRST_NAME",
                "dataType": "VARCHAR",
                "dataLength": 50,
                "dataPrecision": -1,
                "dataScale": -1,
                "nullable": "N",
                "dataDefault": "",
                "columnId": 2
              }
            ],
            "primaryKey": {
              "constraintName": "EMPLOYEES_PK",
              "columns": [
                "EMPLOYEE_ID"
              ]
            },
            "foreignKeys": []
          }
        ]
      }
    },
    "table_management": {
      "provider": "groq",
      "source": "oracle",
      "target": "postgresql",
      "table_management": [
        "CREATE TABLE IF NOT EXISTS public.teammate_employees (employee_id INTEGER NOT NULL, first_name VARCHAR(50) NOT NULL);",
        "ALTER TABLE public.teammate_employees DROP CONSTRAINT IF EXISTS teammate_employees_pkey;",
        "ALTER TABLE public.teammate_employees ADD CONSTRAINT teammate_employees_pkey PRIMARY KEY (employee_id);"
      ],
      "plan": {
        "create": [
          "EMPLOYEES"
        ],
        "drop": [],
        "type_mappings": [
          {
            "table": "EMPLOYEES",
            "column": "EMPLOYEE_ID",
            "oracle": "NUMBER",
            "postgresql": "INTEGER"
          },
          {
            "table": "EMPLOYEES",
            "column": "FIRST_NAME",
            "oracle": "VARCHAR2",
            "postgresql": "VARCHAR"
          }
        ]
      },
      "summary": "Created 1 table and dropped 0 tables."
    },
    "files": []
  },
  "layout": {
    "bundle_root": "migration",
    "source_schema": "migration/oracle/schema/source_schema.json",
    "target_schema": "migration/postgres/schema/target_schema.json",
    "ddl": "migration/postgres/ddl",
    "manifest": "migration/manifest.json"
  }
}


TEAMMATE_DATA_CONTRACT = {
    "provider": "groq",
    "result": {
        "source": "oracle",
        "target": "postgresql",
        "data_extraction": [
            "SELECT 10 AS DEPARTMENT_ID, 'Engineering' AS DEPARTMENT_NAME FROM DUAL UNION ALL SELECT 20, 'Sales' FROM DUAL",
            "SELECT 101 AS EMPLOYEE_ID, 'Alice' AS EMPLOYEE_NAME, 10 AS DEPARTMENT_ID FROM DUAL UNION ALL SELECT 102, 'Bob', 20 FROM DUAL"
        ],
        "data_management": [
            "INSERT INTO public.teammate_departments (department_id, department_name) VALUES {VALUES_PLACEHOLDER};",
            "INSERT INTO public.teammate_employees_data (employee_id, employee_name, department_id) VALUES {VALUES_PLACEHOLDER};"
        ],
        "placeholder": "{VALUES_PLACEHOLDER}",
        "summary": "Generated 2 Oracle to PostgreSQL table migration pairs."
    },
    "layout": {
        "extraction": "migration/oracle/data",
        "management": "migration/postgres/dml",
        "manifest": "migration/manifest.json"
    }
}


def test_teammate_schema_contract_validation_and_execution():
    # 1. Validation
    is_valid, errors = validate_migration_contract(TEAMMATE_SCHEMA_CONTRACT)
    assert is_valid, f"Validation failed: {errors}"

    # 2. Execution via MigrationService
    service = MigrationService()
    res = service.run_migration(TEAMMATE_SCHEMA_CONTRACT)

    assert res["status"] == "success"
    assert res["kind"] == "schema"
    assert "teammate_employees" in res["tables_created"]
    assert "teammate_employees" in res["tables_verified"]

    # Verify table in PostgreSQL
    pg_adapter = default_registry.get_target_adapter("postgresql")
    with pg_adapter.get_engine().connect() as conn:
        exists = conn.execute(text(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'teammate_employees'"
        )).scalar()
        assert exists == 1

    # Cleanup
    with pg_adapter.get_engine().begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS public.teammate_employees"))


def test_teammate_multi_table_data_contract_execution():
    pg_adapter = default_registry.get_target_adapter("postgresql")
    with pg_adapter.get_engine().begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.teammate_departments (
            department_id INT PRIMARY KEY,
            department_name VARCHAR(100)
        );
        CREATE TABLE IF NOT EXISTS public.teammate_employees_data (
            employee_id INT PRIMARY KEY,
            employee_name VARCHAR(100),
            department_id INT
        );
        TRUNCATE TABLE public.teammate_departments, public.teammate_employees_data;
        """))

    # 1. Validation
    is_valid, errors = validate_migration_contract(TEAMMATE_DATA_CONTRACT)
    assert is_valid, f"Validation failed: {errors}"

    # 2. Multi-table execution
    service = MigrationService()
    res = service.run_migration(TEAMMATE_DATA_CONTRACT)

    assert res["status"] == "success"
    assert res["total_tables"] == 2
    assert "teammate_departments" in res["tables_migrated"]
    assert "teammate_employees_data" in res["tables_migrated"]
    assert res["reconciliation"]["status"] == "PASS"
    assert res["source_rows"] == 4
    assert res["loaded_rows"] == 4

    # Verify rows in PostgreSQL
    dept_df = pg_adapter.verify_data("teammate_departments", "public")
    assert len(dept_df) == 2
    emp_df = pg_adapter.verify_data("teammate_employees_data", "public")
    assert len(emp_df) == 2

    # Cleanup
    with pg_adapter.get_engine().begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS public.teammate_departments, public.teammate_employees_data"))
