from __future__ import annotations

import logging
from typing import Any
import pandas as pd
from sqlalchemy import text

from app.config import settings
from app.utils.database import oracle_connection, test_oracle_connection

logger = logging.getLogger(__name__)


class OracleService:
    """
    Generic Oracle Database Service.
    Handles connectivity, schema discovery, table existence, row counting,
    and metadata extraction dynamically without hardcoding tables or schemas.
    """

    def test_connection(self) -> dict[str, Any]:
        """Tests live Oracle connectivity and returns status."""
        return test_oracle_connection()

    def table_exists(self, schema: str, table: str) -> bool:
        """
        Dynamically verifies if a table exists in the Oracle database.
        Checks ALL_TABLES and ALL_VIEWS case-insensitively.
        """
        schema_clean = schema.strip().upper()
        table_clean = table.strip().upper()

        query = text("""
            SELECT COUNT(*)
            FROM ALL_TABLES
            WHERE OWNER = :owner AND TABLE_NAME = :tbl
        """)
        try:
            with oracle_connection() as conn:
                count = conn.execute(query, {"owner": schema_clean, "tbl": table_clean}).scalar()
                if count and count > 0:
                    return True

                # Also check ALL_VIEWS in case target entity is a materialized view or view
                view_query = text("""
                    SELECT COUNT(*)
                    FROM ALL_VIEWS
                    WHERE OWNER = :owner AND VIEW_NAME = :tbl
                """)
                v_count = conn.execute(view_query, {"owner": schema_clean, "tbl": table_clean}).scalar()
                return bool(v_count and v_count > 0)
        except Exception as exc:
            logger.error(f"Error checking Oracle table existence {schema}.{table}: {exc}")
            raise RuntimeError(f"Oracle table check failed for {schema}.{table}: {str(exc)}") from exc

    def get_row_count(self, schema: str, table: str) -> int:
        """Dynamically counts total rows in the specified Oracle table."""
        schema_clean = schema.strip().upper()
        table_clean = table.strip().upper()

        sql = f'SELECT COUNT(*) FROM "{schema_clean}"."{table_clean}"'
        try:
            with oracle_connection() as conn:
                count = conn.execute(text(sql)).scalar()
                return int(count or 0)
        except Exception as exc:
            logger.error(f"Error executing count on Oracle {schema_clean}.{table_clean}: {exc}")
            raise RuntimeError(f"Failed to count rows in Oracle {schema_clean}.{table_clean}: {str(exc)}") from exc

    def get_columns_metadata(self, schema: str, table: str) -> list[dict[str, Any]]:
        """
        Retrieves column definitions, data types, nullability, and primary key flags
        dynamically from Oracle data dictionary views.
        """
        schema_clean = schema.strip().upper()
        table_clean = table.strip().upper()

        pk_query = text("""
            SELECT acc.COLUMN_NAME
            FROM ALL_CONSTRAINTS ac
            JOIN ALL_CONS_COLUMNS acc
              ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
             AND ac.OWNER = acc.OWNER
            WHERE ac.OWNER = :owner
              AND ac.TABLE_NAME = :tbl
              AND ac.CONSTRAINT_TYPE = 'P'
        """)

        cols_query = text("""
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :owner AND TABLE_NAME = :tbl
            ORDER BY COLUMN_ID
        """)

        try:
            with oracle_connection() as conn:
                pk_rows = conn.execute(pk_query, {"owner": schema_clean, "tbl": table_clean}).fetchall()
                primary_keys = {row[0] for row in pk_rows}

                col_rows = conn.execute(cols_query, {"owner": schema_clean, "tbl": table_clean}).fetchall()
                results = []
                for row in col_rows:
                    col_name = str(row[0])
                    results.append({
                        "name": col_name,
                        "type": str(row[1]),
                        "length": row[2],
                        "precision": row[3],
                        "scale": row[4],
                        "nullable": row[5] == "Y",
                        "primary_key": col_name in primary_keys,
                    })
                return results
        except Exception as exc:
            logger.error(f"Error fetching Oracle metadata for {schema_clean}.{table_clean}: {exc}")
            raise RuntimeError(f"Failed to fetch Oracle columns metadata: {str(exc)}") from exc

    def get_null_counts(self, schema: str, table: str, columns: list[str]) -> dict[str, int]:
        """Calculates null counts dynamically for given columns."""
        if not columns:
            return {}

        schema_clean = schema.strip().upper()
        table_clean = table.strip().upper()

        # Build dynamic aggregates: SUM(CASE WHEN "COL" IS NULL THEN 1 ELSE 0 END)
        clauses = [f'SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS "{col}_nulls"' for col in columns]
        sql = f'SELECT {", ".join(clauses)} FROM "{schema_clean}"."{table_clean}"'

        try:
            with oracle_connection() as conn:
                row = conn.execute(text(sql)).fetchone()
                if not row:
                    return {col: 0 for col in columns}
                return {col: int(row[i] or 0) for i, col in enumerate(columns)}
        except Exception as exc:
            logger.error(f"Error calculating null counts for Oracle {schema_clean}.{table_clean}: {exc}")
            raise RuntimeError(f"Failed to calculate Oracle null counts: {str(exc)}") from exc

    def extract_dataframe(self, query: str) -> pd.DataFrame:
        """Executes extraction query and returns DataFrame."""
        try:
            with oracle_connection() as conn:
                return pd.read_sql_query(text(query.strip().rstrip(";")), con=conn)
        except Exception as exc:
            logger.error(f"Error extracting data from Oracle: {exc}")
            raise RuntimeError(f"Oracle extraction query failed: {str(exc)}") from exc
