import pytest
import pandas as pd
from sqlalchemy import text

from app.adapters.registry import default_registry
from app.services.migration_service import MigrationService


def test_live_database_connections():
    oracle_adapter = default_registry.get_source_adapter("oracle")
    oracle_health = oracle_adapter.test_connection()
    assert oracle_health["connected"] is True, f"Oracle connection failed: {oracle_health}"

    postgres_adapter = default_registry.get_target_adapter("postgresql")
    postgres_health = postgres_adapter.test_connection()
    assert postgres_health["connected"] is True, f"PostgreSQL connection failed: {postgres_health}"


def test_live_end_to_end_migration():
    """
    Executes a real end-to-end migration from Oracle through Pandas into PostgreSQL.
    Extracts actual row data from Oracle, applies transformations, and loads into PostgreSQL.
    """
    service = MigrationService()
    target_table = "live_e2e_test_table"

    spec = {
        "source": {
            "type": "oracle",
            "schema": "HR",
        },
        "target": {
            "type": "postgresql",
            "schema": "public",
        },
        "execution_options": {
            "allow_destructive_ddl": True,
        },
        "table_management": {
            "statements": [
                f"DROP TABLE IF EXISTS {target_table}",
                f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                    entity_code VARCHAR(50) PRIMARY KEY,
                    display_name VARCHAR(100) NOT NULL
                )
                """,
            ]
        },
        "data_migration": {
            "extraction": {
                "query": "SELECT 'LIVE_E2E' AS CODE, '  Live Test System  ' AS RAW_NAME FROM DUAL"
            },
            "loading": {
                "table": target_table,
                "columns": ["entity_code", "display_name"],
                "mode": "append",
            },
        },
        "transformations": [
            {
                "type": "rename",
                "source": "CODE",
                "target": "entity_code",
            },
            {
                "type": "trim",
                "source": "RAW_NAME",
                "target": "display_name",
            },
        ],
    }

    result = service.run_migration(spec)

    assert result["status"] == "success", f"Migration failed: {result}"
    assert result["source_rows"] == 1
    assert result["transformed_rows"] == 1
    assert result["target_rows"] == 1
    assert result["validation"]["status"] == "passed"

    postgres_adapter = default_registry.get_target_adapter("postgresql")
    with postgres_adapter.get_engine().connect() as conn:
        row = conn.execute(text(f"SELECT entity_code, display_name FROM {target_table}")).fetchone()
        assert row is not None
        assert row[0] == "LIVE_E2E"
        assert row[1] == "Live Test System"

    with postgres_adapter.get_engine().begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {target_table}"))


def test_live_loading_columns_projection_and_safety():
    """
    Live test against Oracle and PostgreSQL verifying:
    1. loading.columns drops unrequested columns extracted from source
    2. Exact target columns are loaded into PostgreSQL
    3. Row count metrics are properly reconciled
    """
    service = MigrationService()
    target_table = "live_projection_test_table"

    postgres_adapter = default_registry.get_target_adapter("postgresql")
    with postgres_adapter.get_engine().begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {target_table}"))
        conn.execute(text(f"""
            CREATE TABLE {target_table} (
                item_id INT PRIMARY KEY,
                item_label VARCHAR(80) NOT NULL
            )
        """))

    try:
        spec = {
            "source": {"type": "oracle"},
            "target": {"type": "postgresql"},
            "data_migration": {
                "extraction": {
                    "query": """
                        SELECT 501 AS ITEM_ID,
                               '  Widget A  ' AS RAW_LABEL,
                               'SECRET_FIELD' AS INTERNAL_SECRET,
                               9999 AS DEBUG_METRIC
                        FROM DUAL
                    """
                },
                "loading": {
                    "table": target_table,
                    "columns": ["item_id", "item_label"],
                    "mode": "append",
                },
            },
            "transformations": [
                {"type": "rename", "source": "ITEM_ID", "target": "item_id"},
                {"type": "trim", "source": "RAW_LABEL", "target": "item_label"},
            ],
        }

        result = service.run_migration(spec)

        assert result["status"] == "success"
        assert result["source_rows"] == 1
        assert result["extracted_rows"] == 1
        assert result["transformed_rows"] == 1
        assert result["loaded_rows"] == 1
        assert result["target_rows"] == 1
        assert result["target"]["columns"] == ["item_id", "item_label"]

        with postgres_adapter.get_engine().connect() as conn:
            row = conn.execute(text(f"SELECT item_id, item_label FROM {target_table}")).fetchone()
            assert row is not None
            assert row[0] == 501
            assert row[1] == "Widget A"
    finally:
        with postgres_adapter.get_engine().begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {target_table}"))
