from __future__ import annotations

import logging
import time
from typing import Any

from app.cdc.oracle_logminer import OracleLogMinerCDCSource
from app.cdc.consumer import CDCConsumer
from app.cdc.checkpoint import PostgresCheckpointStore
from app.cdc.models import CDCStatus, CDCWindowResult

logger = logging.getLogger(__name__)


class CDCService:
    """
    Orchestration service for Log-Based Change Data Capture (CDC).
    Coordinates source capture (Oracle LogMiner), event normalization,
    in-memory transformation, target writing, and SCN checkpointing.
    """

    def __init__(
        self,
        source: OracleLogMinerCDCSource | None = None,
        consumer: CDCConsumer | None = None,
        checkpoint_store: PostgresCheckpointStore | None = None,
    ) -> None:
        self.source = source or OracleLogMinerCDCSource()
        self.checkpoint_store = checkpoint_store or PostgresCheckpointStore()
        self.consumer = consumer or CDCConsumer(checkpoint_store=self.checkpoint_store)

    def check_readiness(self) -> dict[str, Any]:
        """Validates Oracle LogMiner prerequisites and PostgreSQL target connectivity."""
        oracle_check = self.source.check_prerequisites()
        pg_check = {"connected": False}
        try:
            pg_check = self.checkpoint_store.adapter.test_connection()
        except Exception as exc:
            pg_check = {"connected": False, "error": str(exc)}

        is_ready = oracle_check.get("ready", False) and pg_check.get("connected", False)

        return {
            "status": "ready" if is_ready else "not_ready",
            "oracle": oracle_check,
            "postgresql": pg_check,
            "ready": is_ready,
        }

    def get_status(self, pipeline_name: str = "oracle_to_postgres_cdc") -> dict[str, Any]:
        """Returns the current CDC status, SCN offsets, and replication lag."""
        checkpoint_info = self.checkpoint_store.get_checkpoint_info(pipeline_name)
        last_scn = checkpoint_info.get("last_scn")

        current_ora_scn = None
        try:
            current_ora_scn = self.source.get_current_scn()
        except Exception as exc:
            logger.warning(f"Could not retrieve current Oracle SCN: {exc}")

        lag = None
        if last_scn is not None and current_ora_scn is not None:
            lag = max(0, current_ora_scn - last_scn)

        return CDCStatus(
            pipeline_name=pipeline_name,
            last_checkpoint_scn=last_scn,
            current_oracle_scn=current_ora_scn,
            lag_scn=lag,
            updated_at=checkpoint_info.get("updated_at"),
            is_running=False,
        ).to_dict()

    def get_checkpoint(self, pipeline_name: str = "oracle_to_postgres_cdc") -> dict[str, Any]:
        """Returns checkpoint details for the specified pipeline."""
        return self.checkpoint_store.get_checkpoint_info(pipeline_name)

    def run_batch_window(
        self,
        pipeline_name: str = "oracle_to_postgres_cdc",
        start_scn: int | None = None,
        end_scn: int | None = None,
        table_filter: list[str] | None = None,
        transformations: list[dict[str, Any]] | None = None,
        target_table: str = "employees",
        target_schema: str = "public",
        simulate_failure_at_event: int | None = None,
    ) -> dict[str, Any]:
        """
        Executes a bounded SCN window:
        1. Reads last checkpoint SCN if start_scn not provided.
        2. Queries LogMiner for events in [start_scn, end_scn].
        3. Applies transformations via shared TransformationEngine runtime.
        4. Applies idempotently to PostgreSQL.
        5. Checkpoints SCN.
        """
        if start_scn is None:
            last_scn = self.checkpoint_store.get_last_scn(pipeline_name)
            if last_scn is not None:
                start_scn = last_scn + 1
            else:
                # If no previous checkpoint, default to current SCN
                start_scn = self.source.get_current_scn()
                logger.info(f"No checkpoint found. Starting from current SCN: {start_scn}")

        if end_scn is None:
            end_scn = self.source.get_current_scn()

        logger.info(f"[CDC Batch Window] Processing [{start_scn} -> {end_scn}] for table(s): {table_filter}")

        events = self.source.read_changes(
            start_scn=start_scn,
            end_scn=end_scn,
            table_filter=table_filter,
        )

        result = self.consumer.process_events(
            events=events,
            pipeline_name=pipeline_name,
            target_table=target_table,
            target_schema=target_schema,
            transformations=transformations,
            start_scn=start_scn,
            end_scn=end_scn,
            simulate_failure_at_event=simulate_failure_at_event,
        )

        return result.to_dict()

    def run_continuous(
        self,
        pipeline_name: str = "oracle_to_postgres_cdc",
        poll_interval_seconds: float = 2.0,
        max_iterations: int | None = None,
        table_filter: list[str] | None = None,
        transformations: list[dict[str, Any]] | None = None,
        target_table: str = "employees",
        target_schema: str = "public",
    ) -> dict[str, Any]:
        """
        Continuously mines change events from Oracle redo logs and streams to PostgreSQL.
        Maintains durable SCN checkpoints across iterations.
        """
        logger.info(f"[CDC Continuous] Starting streaming loop for pipeline '{pipeline_name}' (poll={poll_interval_seconds}s)...")
        iteration = 0
        total_captured = 0
        total_applied = 0

        while True:
            iteration += 1
            if max_iterations is not None and iteration > max_iterations:
                logger.info(f"[CDC Continuous] Reached max iterations limit ({max_iterations}). Exiting loop.")
                break

            last_checkpoint = self.checkpoint_store.get_last_scn(pipeline_name)
            current_scn = self.source.get_current_scn()

            start_scn = (last_checkpoint + 1) if last_checkpoint is not None else current_scn

            if start_scn <= current_scn:
                try:
                    res = self.run_batch_window(
                        pipeline_name=pipeline_name,
                        start_scn=start_scn,
                        end_scn=current_scn,
                        table_filter=table_filter,
                        transformations=transformations,
                        target_table=target_table,
                        target_schema=target_schema,
                    )
                    captured = res.get("events_captured", 0)
                    applied = res.get("events_applied", 0)
                    total_captured += captured
                    total_applied += applied

                    if captured > 0:
                        logger.info(f"[CDC Loop #{iteration}] Captured: {captured}, Applied: {applied}, SCN: {res.get('last_checkpoint_scn')}")
                except Exception as exc:
                    logger.error(f"[CDC Loop #{iteration}] Error during CDC window: {exc}")

            if max_iterations is not None and iteration >= max_iterations:
                break

            time.sleep(poll_interval_seconds)

        return {
            "pipeline_name": pipeline_name,
            "iterations_completed": iteration,
            "total_events_captured": total_captured,
            "total_events_applied": total_applied,
            "last_checkpoint_scn": self.checkpoint_store.get_last_scn(pipeline_name),
            "status": "STOPPED",
        }
