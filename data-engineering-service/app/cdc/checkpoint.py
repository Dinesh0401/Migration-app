from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.adapters.registry import default_registry
from app.adapters.postgres_adapter import PostgresTargetAdapter

logger = logging.getLogger(__name__)


class PostgresCheckpointStore:
    """
    Durable PostgreSQL-backed SCN checkpoint store.
    Stores pipeline_name, last_scn, and updated_at in `migration.cdc_checkpoint`.
    Ensures safe restart and recovery semantics:
    Sequence: capture -> apply PostgreSQL -> save SCN.
    """

    def __init__(self, target_adapter: PostgresTargetAdapter | None = None) -> None:
        self.adapter = target_adapter or default_registry.get_target_adapter("postgresql")
        self._ensure_table_exists()

    def _get_engine(self) -> Engine:
        return self.adapter.get_engine()

    def _ensure_table_exists(self) -> None:
        """Creates the migration schema and cdc_checkpoint table if they do not exist."""
        ddl = """
        CREATE SCHEMA IF NOT EXISTS migration;
        CREATE TABLE IF NOT EXISTS migration.cdc_checkpoint (
            pipeline_name VARCHAR(100) PRIMARY KEY,
            last_scn BIGINT NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            with self._get_engine().begin() as conn:
                conn.execute(text(ddl))
            logger.info("Verified migration.cdc_checkpoint table exists.")
        except Exception as exc:
            logger.error(f"Failed to initialize checkpoint table: {exc}")
            raise

    def get_last_scn(self, pipeline_name: str = "oracle_to_postgres_cdc") -> int | None:
        """Retrieves the last committed SCN for the specified pipeline, or None if no checkpoint."""
        query = text("SELECT last_scn FROM migration.cdc_checkpoint WHERE pipeline_name = :pname")
        with self._get_engine().connect() as conn:
            result = conn.execute(query, {"pname": pipeline_name}).scalar()
            return int(result) if result is not None else None

    def save_scn(self, pipeline_name: str, scn: int) -> None:
        """
        Persists the latest successfully applied SCN.
        Idempotent upsert via ON CONFLICT (pipeline_name) DO UPDATE.
        """
        stmt = text("""
        INSERT INTO migration.cdc_checkpoint (pipeline_name, last_scn, updated_at)
        VALUES (:pname, :scn, CURRENT_TIMESTAMP)
        ON CONFLICT (pipeline_name)
        DO UPDATE SET last_scn = EXCLUDED.last_scn, updated_at = CURRENT_TIMESTAMP;
        """)
        with self._get_engine().begin() as conn:
            conn.execute(stmt, {"pname": pipeline_name, "scn": scn})
        logger.debug(f"[CHECKPOINT] Saved SCN {scn} for pipeline '{pipeline_name}'.")

    def get_checkpoint_info(self, pipeline_name: str = "oracle_to_postgres_cdc") -> dict[str, Any]:
        """Returns metadata for the checkpoint."""
        query = text("""
        SELECT pipeline_name, last_scn, updated_at
        FROM migration.cdc_checkpoint
        WHERE pipeline_name = :pname
        """)
        with self._get_engine().connect() as conn:
            row = conn.execute(query, {"pname": pipeline_name}).mappings().first()
            if row:
                return {
                    "pipeline_name": row["pipeline_name"],
                    "last_scn": int(row["last_scn"]),
                    "updated_at": str(row["updated_at"]),
                }
            return {
                "pipeline_name": pipeline_name,
                "last_scn": None,
                "updated_at": None,
            }

    def reset_checkpoint(self, pipeline_name: str = "oracle_to_postgres_cdc") -> None:
        """Clears the checkpoint for the specified pipeline (used for reset/testing)."""
        stmt = text("DELETE FROM migration.cdc_checkpoint WHERE pipeline_name = :pname")
        with self._get_engine().begin() as conn:
            conn.execute(stmt, {"pname": pipeline_name})
        logger.info(f"Reset checkpoint for pipeline '{pipeline_name}'.")
