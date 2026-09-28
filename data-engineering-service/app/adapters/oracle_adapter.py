from __future__ import annotations

import logging
from typing import Any
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.adapters.base import BaseSourceAdapter
from app.config.settings import Settings, settings

logger = logging.getLogger(__name__)


class OracleSourceAdapter(BaseSourceAdapter):
    """
    Oracle source adapter using modern SQLAlchemy and python-oracledb (thin mode).
    Executes extraction queries and returns standardized in-memory Pandas DataFrames.
    """

    def __init__(self, config: Settings | None = None) -> None:
        self.config = config or settings
        self._engine: Engine | None = None

    def get_engine(self) -> Engine:
        if self._engine is None:
            url = self.config.get_oracle_url()
            self._engine = create_engine(url, pool_pre_ping=True)
            logger.info("Initialized SQLAlchemy Oracle engine with oracle+oracledb dialect.")
        return self._engine

    def extract(self, query: str) -> pd.DataFrame:
        if not query or not query.strip():
            raise ValueError("Extraction query cannot be empty")

        cleaned_query = query.strip().rstrip(";")
        logger.info("Executing Oracle source extraction query.")
        logger.debug(f"Query: {cleaned_query}")

        try:
            with self.get_engine().connect() as conn:
                df = pd.read_sql_query(text(cleaned_query), con=conn)
                logger.info(f"Extracted {len(df)} rows from Oracle.")
                return df
        except Exception as exc:
            logger.error(f"Oracle extraction failed: {exc}")
            raise RuntimeError(f"Oracle extraction failed: {str(exc)}") from exc

    def test_connection(self) -> dict[str, Any]:
        try:
            with self.get_engine().connect() as conn:
                result = conn.execute(text("SELECT SYSDATE FROM DUAL")).scalar()
                return {
                    "connected": True,
                    "database": "oracle",
                    "host": self.config.ORACLE_HOST,
                    "service": self.config.ORACLE_SERVICE,
                    "db_time": str(result),
                }
        except Exception as exc:
            return {
                "connected": False,
                "database": "oracle",
                "error": str(exc),
            }
