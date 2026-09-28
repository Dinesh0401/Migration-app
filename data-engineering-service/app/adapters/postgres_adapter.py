from __future__ import annotations

import logging
import re
from typing import Any
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.adapters.base import BaseTargetAdapter
from app.config.settings import Settings, settings

logger = logging.getLogger(__name__)

DESTRUCTIVE_PATTERNS = [
    re.compile(r"\bDROP\s+TABLE\b", re.IGNORECASE),
    re.compile(r"\bDROP\s+SCHEMA\b", re.IGNORECASE),
    re.compile(r"\bTRUNCATE(\s+TABLE)?\b", re.IGNORECASE),
]

ALWAYS_BLOCKED_PATTERNS = [
    re.compile(r"\bDROP\s+DATABASE\b", re.IGNORECASE),
    re.compile(r"\bDELETE(\s+FROM)?\b", re.IGNORECASE),
]

class PostgresTargetAdapter(BaseTargetAdapter):
    """
    PostgreSQL target adapter using SQLAlchemy and psycopg2.
    Executes DDL statements and loads Pandas DataFrames into PostgreSQL.
    """

    def __init__(self, config: Settings | None = None) -> None:
        self.config = config or settings
        self._engine: Engine | None = None

    def get_engine(self) -> Engine:
        if self._engine is None:
            url = self.config.get_postgres_url()
            self._engine = create_engine(url, pool_pre_ping=True)
            logger.info("Initialized SQLAlchemy PostgreSQL engine with postgresql+psycopg2 dialect.")
        return self._engine

    def execute_ddl(self, statements: list[str], allow_destructive_ddl: bool = False) -> list[str]:
        if not statements:
            return []

        executed: list[str] = []
        with self.get_engine().begin() as conn:
            for stmt in statements:
                stmt_clean = stmt.strip().rstrip(";")
                if not stmt_clean:
                    continue

                for pattern in ALWAYS_BLOCKED_PATTERNS:
                    if pattern.search(stmt_clean):
                        raise ValueError(f"Prohibited DDL statement rejected: {stmt_clean}")

                if not allow_destructive_ddl:
                    for pattern in DESTRUCTIVE_PATTERNS:
                        if pattern.search(stmt_clean):
                            raise ValueError(
                                f"Destructive DDL statement rejected (allow_destructive_ddl=False): '{stmt_clean}'. "
                                "To permit destructive DDL operations, set allow_destructive_ddl=True in execution_options."
                            )

                logger.info(f"Executing target DDL: {stmt_clean[:60]}...")
                conn.execute(text(stmt_clean))
                executed.append(stmt_clean)

        return executed

    def get_row_count(self, table: str, schema: str = "public") -> int:
        if not table or not table.strip():
            return 0
        clean_table = table.strip().replace('"', "")
        clean_schema = (schema or "public").strip().replace('"', "")
        try:
            with self.get_engine().connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM "{clean_schema}"."{clean_table}"')).scalar()
                return int(result) if result is not None else 0
        except Exception as exc:
            logger.warning(f"Could not retrieve target row count for {clean_schema}.{clean_table}: {exc}")
            return 0

    def verify_data(self, table: str, schema: str = "public", limit: int = 5) -> pd.DataFrame:
        """
        Queries and returns sample rows from the target table for live data verification.
        """
        if not table or not table.strip():
            return pd.DataFrame()
        clean_table = table.strip().replace('"', "")
        clean_schema = (schema or "public").strip().replace('"', "")
        try:
            with self.get_engine().connect() as conn:
                return pd.read_sql_query(
                    text(f'SELECT * FROM "{clean_schema}"."{clean_table}" LIMIT {limit}'),
                    con=conn,
                )
        except Exception as exc:
            logger.warning(f"Could not verify target data for {clean_schema}.{clean_table}: {exc}")
            return pd.DataFrame()

    def load(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = "public",
        mode: str = "append",
    ) -> int:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Expected pandas DataFrame for target loading")

        if not table or not table.strip():
            raise ValueError("Target table name cannot be empty")

        clean_table = table.strip()
        clean_schema = (schema or "public").strip()

        valid_modes = {"append", "replace", "fail"}
        if mode not in valid_modes:
            raise ValueError(f"Unsupported write mode '{mode}'. Expected one of: {valid_modes}")

        row_count = len(df)
        if row_count == 0:
            logger.info(f"DataFrame is empty. 0 rows loaded into {clean_schema}.{clean_table}.")
            return 0

        logger.info(f"Loading {row_count} rows into PostgreSQL target {clean_schema}.{clean_table} (mode={mode}).")

        try:
            with self.get_engine().begin() as conn:
                df.to_sql(
                    name=clean_table,
                    con=conn,
                    schema=clean_schema,
                    if_exists=mode,
                    index=False,
                    method="multi",
                    chunksize=1000,
                )
            logger.info(f"Successfully loaded {row_count} rows into {clean_schema}.{clean_table}.")
            return row_count
        except Exception as exc:
            logger.error(f"PostgreSQL target loading failed: {exc}")
            raise RuntimeError(f"PostgreSQL loading failed for table '{clean_table}': {str(exc)}") from exc

    def test_connection(self) -> dict[str, Any]:
        try:
            with self.get_engine().connect() as conn:
                result = conn.execute(text("SELECT NOW()")).scalar()
                return {
                    "connected": True,
                    "database": "postgresql",
                    "host": self.config.POSTGRES_HOST,
                    "db_name": self.config.POSTGRES_DATABASE,
                    "db_time": str(result),
                }
        except Exception as exc:
            return {
                "connected": False,
                "database": "postgresql",
                "error": str(exc),
            }
