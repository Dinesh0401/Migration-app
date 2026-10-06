from __future__ import annotations

import logging
from typing import Any
import pandas as pd
from sqlalchemy import text

from app.config import settings
from app.utils.database import postgres_connection, test_postgres_connection

logger = logging.getLogger(__name__)


class PostgresService:
    """
    Generic PostgreSQL Database Service.
    Handles connectivity, schema discovery, table existence, row counting,
    and metadata extraction dynamically without hardcoding tables or schemas.
    """

    def test_connection(self) -> dict[str, Any]:
        """Tests live PostgreSQL connectivity and returns status."""
        return test_postgres_connection()

    def table_exists(self, schema: str, table: str) -> bool:
        """
        Dynamically verifies if a table exists in the PostgreSQL database.
        Checks information_schema.tables case-insensitively.
        """
        schema_clean = schema.strip().lower()
        table_clean = table.strip().lower()

        query = text("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE LOWER(table_schema) = :sch
              AND LOWER(table_name) = :tbl
        """)
        try:
            with postgres_connection() as conn:
                count = conn.execute(query, {"sch": schema_clean, "tbl": table_clean}).scalar()
                if count and count > 0:
                    return True

                # Also check views
                view_query = text("""
                    SELECT COUNT(*)
                    FROM information_schema.views
                    WHERE LOWER(table_schema) = :sch
                      AND LOWER(table_name) = :tbl
                """)
                v_count = conn.execute(view_query, {"sch": schema_clean, "tbl": table_clean}).scalar()
                return bool(v_count and v_count > 0)
        except Exception as exc:
            logger.error(f"Error checking PostgreSQL table existence {schema}.{table}: {exc}")
            raise RuntimeError(f"PostgreSQL table check failed for {schema}.{table}: {str(exc)}") from exc

    def get_row_count(self, schema: str, table: str) -> int:
        """Dynamically counts total rows in the specified PostgreSQL table."""
        schema_clean = schema.strip().lower()
        table_clean = table.strip().lower()

        sql = f'SELECT COUNT(*) FROM "{schema_clean}"."{table_clean}"'
        try:
            with postgres_connection() as conn:
                count = conn.execute(text(sql)).scalar()
                return int(count or 0)
        except Exception as exc:
            logger.error(f"Error executing count on PostgreSQL {schema_clean}.{table_clean}: {exc}")
            raise RuntimeError(f"Failed to count rows in PostgreSQL {schema_clean}.{table_clean}: {str(exc)}") from exc

    def get_columns_metadata(self, schema: str, table: str) -> list[dict[str, Any]]:
        """
        Retrieves column definitions, data types, nullability, and primary key flags
        dynamically from PostgreSQL information_schema.
        """
        schema_clean = schema.strip().lower()
        table_clean = table.strip().lower()

        pk_query = text("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE LOWER(tc.table_schema) = :sch
              AND LOWER(tc.table_name) = :tbl
              AND tc.constraint_type = 'PRIMARY KEY'
        """)

        cols_query = text("""
            SELECT column_name, data_type, character_maximum_length,
                   numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns
            WHERE LOWER(table_schema) = :sch
              AND LOWER(table_name) = :tbl
            ORDER BY ordinal_position
        """)

        try:
            with postgres_connection() as conn:
                pk_rows = conn.execute(pk_query, {"sch": schema_clean, "tbl": table_clean}).fetchall()
                primary_keys = {row[0].lower() for row in pk_rows}

                col_rows = conn.execute(cols_query, {"sch": schema_clean, "tbl": table_clean}).fetchall()
                results = []
                for row in col_rows:
                    col_name = str(row[0])
                    results.append({
                        "name": col_name,
                        "type": str(row[1]),
                        "length": row[2],
                        "precision": row[3],
                        "scale": row[4],
                        "nullable": str(row[5]).upper() == "YES",
                        "primary_key": col_name.lower() in primary_keys,
                    })
                return results
        except Exception as exc:
            logger.error(f"Error fetching PostgreSQL metadata for {schema_clean}.{table_clean}: {exc}")
            raise RuntimeError(f"Failed to fetch PostgreSQL columns metadata: {str(exc)}") from exc

    def get_null_counts(self, schema: str, table: str, columns: list[str]) -> dict[str, int]:
        """Calculates null counts dynamically for given columns in PostgreSQL."""
        if not columns:
            return {}

        schema_clean = schema.strip().lower()
        table_clean = table.strip().lower()

        clauses = [f'SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS "{col}_nulls"' for col in columns]
        sql = f'SELECT {", ".join(clauses)} FROM "{schema_clean}"."{table_clean}"'

        try:
            with postgres_connection() as conn:
                row = conn.execute(text(sql)).fetchone()
                if not row:
                    return {col: 0 for col in columns}
                return {col: int(row[i] or 0) for i, col in enumerate(columns)}
        except Exception as exc:
            logger.error(f"Error calculating null counts for PostgreSQL {schema_clean}.{table_clean}: {exc}")
            raise RuntimeError(f"Failed to calculate PostgreSQL null counts: {str(exc)}") from exc

    def execute_ddl(self, statements: list[str]) -> list[str]:
        """Executes DDL statements against target PostgreSQL database."""
        executed = []
        with postgres_connection() as conn:
            for stmt in statements:
                cleaned = stmt.strip().rstrip(";")
                if cleaned:
                    conn.execute(text(cleaned))
                    executed.append(cleaned)
            conn.commit()
        return executed
