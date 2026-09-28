from __future__ import annotations

import logging
import threading
import time
from typing import Any

from app.cdc.base import BaseCDCSource
from app.cdc.consumer import CDCConsumer
from app.cdc.oracle_logminer import OracleLogMinerCDCSource
from app.cdc.checkpoint import PostgresCheckpointStore

logger = logging.getLogger("cdc_worker")


class CDCWorkerManager:
    """
    Manages continuous background polling workers for LogMiner CDC.
    Thread-safe singleton lifecycle:
      - /cdc/start -> spawns background thread polling Oracle -> PostgreSQL -> checkpoint
      - /cdc/stop  -> signals stop event and waits for graceful worker termination
      - /cdc/status -> reports worker health, lag, iterations, and SCN progression
    """

    _instance: CDCWorkerManager | None = None
    _lock = threading.RLock()

    def __new__(cls) -> CDCWorkerManager:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._init_manager()
            return cls._instance

    def _init_manager(self) -> None:
        self.workers: dict[str, dict[str, Any]] = {}
        self.worker_lock = threading.RLock()

    def is_running(self, pipeline_name: str = "oracle_to_postgres_cdc") -> bool:
        with self.worker_lock:
            info = self.workers.get(pipeline_name)
            if not info:
                return False
            thread: threading.Thread | None = info.get("thread")
            stop_event: threading.Event | None = info.get("stop_event")
            return bool(thread and thread.is_alive() and stop_event and not stop_event.is_set())

    def get_stats(self, pipeline_name: str = "oracle_to_postgres_cdc") -> dict[str, Any]:
        with self.worker_lock:
            info = self.workers.get(pipeline_name)
            if not info:
                return {
                    "is_running": False,
                    "iterations": 0,
                    "total_captured": 0,
                    "total_applied": 0,
                    "last_poll_time": None,
                    "last_error": None,
                }
            return {
                "is_running": self.is_running(pipeline_name),
                "started_at": info.get("started_at"),
                "poll_interval_seconds": info.get("poll_interval_seconds"),
                "iterations": info.get("iterations", 0),
                "total_captured": info.get("total_captured", 0),
                "total_applied": info.get("total_applied", 0),
                "last_poll_time": info.get("last_poll_time"),
                "last_error": info.get("last_error"),
            }

    def start_worker(
        self,
        pipeline_name: str = "oracle_to_postgres_cdc",
        poll_interval_seconds: float = 2.0,
        table_filter: list[str] | None = None,
        transformations: list[dict[str, Any]] | None = None,
        target_table: str = "employees",
        target_schema: str = "public",
        safety_overlap_scn: int = 200,
        source: OracleLogMinerCDCSource | None = None,
        consumer: CDCConsumer | None = None,
        checkpoint_store: PostgresCheckpointStore | None = None,
    ) -> dict[str, Any]:
        with self.worker_lock:
            if self.is_running(pipeline_name):
                return {
                    "status": "ALREADY_RUNNING",
                    "pipeline_name": pipeline_name,
                    "is_running": True,
                    "message": f"CDC background worker is already active for pipeline '{pipeline_name}'",
                }

            stop_event = threading.Event()
            info: dict[str, Any] = {
                "pipeline_name": pipeline_name,
                "poll_interval_seconds": poll_interval_seconds,
                "target_table": target_table,
                "target_schema": target_schema,
                "table_filter": table_filter or ["EMPLOYEES"],
                "transformations": transformations or [],
                "safety_overlap_scn": int(safety_overlap_scn),
                "stop_event": stop_event,
                "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "iterations": 0,
                "total_captured": 0,
                "total_applied": 0,
                "last_poll_time": None,
                "last_error": None,
            }

            worker_thread = threading.Thread(
                target=self._worker_loop,
                args=(pipeline_name, info, source, consumer, checkpoint_store),
                name=f"CDCWorker-{pipeline_name}",
                daemon=True,
            )
            info["thread"] = worker_thread
            self.workers[pipeline_name] = info
            worker_thread.start()

            return {
                "status": "STARTED",
                "pipeline_name": pipeline_name,
                "is_running": True,
                "poll_interval_seconds": poll_interval_seconds,
                "target_table": target_table,
                "message": f"CDC background worker started successfully for pipeline '{pipeline_name}'",
            }

    def stop_worker(self, pipeline_name: str = "oracle_to_postgres_cdc", timeout: float = 5.0) -> dict[str, Any]:
        with self.worker_lock:
            info = self.workers.get(pipeline_name)
            if not info or not self.is_running(pipeline_name):
                return {
                    "status": "NOT_RUNNING",
                    "pipeline_name": pipeline_name,
                    "is_running": False,
                    "message": f"No active CDC background worker found for pipeline '{pipeline_name}'",
                }

            stop_event: threading.Event = info["stop_event"]
            thread: threading.Thread = info["thread"]
            stop_event.set()

        # Wait for worker thread outside the lock
        thread.join(timeout=timeout)

        return {
            "status": "STOPPED",
            "pipeline_name": pipeline_name,
            "is_running": False,
            "iterations_completed": info.get("iterations", 0),
            "total_events_applied": info.get("total_applied", 0),
            "message": f"CDC background worker stopped successfully for pipeline '{pipeline_name}'",
        }

    def _worker_loop(
        self,
        pipeline_name: str,
        info: dict[str, Any],
        source: OracleLogMinerCDCSource | None,
        consumer: CDCConsumer | None,
        checkpoint_store: PostgresCheckpointStore | None,
    ) -> None:
        src = source or OracleLogMinerCDCSource()
        chk = checkpoint_store or PostgresCheckpointStore()
        csm = consumer or CDCConsumer(checkpoint_store=chk)

        stop_event: threading.Event = info["stop_event"]
        poll_interval: float = info["poll_interval_seconds"]
        target_table: str = info["target_table"]
        target_schema: str = info["target_schema"]
        table_filter: list[str] = info["table_filter"]
        transformations: list[dict[str, Any]] = info["transformations"]

        print(f"\n=======================================================", flush=True)
        print(f" [CDC] Background worker STARTED for pipeline '{pipeline_name}'", flush=True)
        print(f" [CDC] Polling Oracle every {poll_interval}s for table(s): {table_filter}", flush=True)
        print(f" [CDC] Target: {target_schema}.{target_table}", flush=True)
        print(f"=======================================================\n", flush=True)
        logger.info(f"[CDC Worker] Worker thread started for '{pipeline_name}'")

        # Establish baseline checkpoint if none exists
        last_checkpoint = chk.get_last_scn(pipeline_name)
        if last_checkpoint is None:
            try:
                init_scn = src.get_current_scn()
                chk.save_scn(pipeline_name, init_scn)
                print(f"[CDC] Initial baseline SCN set to {init_scn}", flush=True)
            except Exception as exc:
                logger.error(f"[CDC Worker] Could not fetch initial SCN: {exc}")

        while not stop_event.is_set():
            try:
                self._poll_cycle(pipeline_name, info, src, csm, chk)
                info["iterations"] = info.get("iterations", 0) + 1
                info["last_poll_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as exc:
                info["last_error"] = str(exc)
                logger.error(f"[CDC Worker Error] {exc}", exc_info=True)
                print(f"[CDC Worker Error] {exc}", flush=True)

            stop_event.wait(poll_interval)

        print(f"\n[CDC] Background worker STOPPED for pipeline '{pipeline_name}'.\n", flush=True)
        logger.info(f"[CDC Worker] Worker thread ended for '{pipeline_name}'")

    def _poll_cycle(
        self,
        pipeline_name: str,
        info: dict[str, Any],
        src: BaseCDCSource,
        csm: CDCConsumer,
        chk: PostgresCheckpointStore,
    ) -> bool:
        """
        Executes a single polling iteration for the pipeline.
        Returns True if a window was processed (idle or with events), False if skipped (no new SCN).
        Raises exceptions on mining or consumer failures (checkpoint will NOT advance).
        """
        table_filter: list[str] = info.get("table_filter", ["EMPLOYEES"])
        target_table: str = info.get("target_table", "employees")
        target_schema: str = info.get("target_schema", "public")
        transformations: list[dict[str, Any]] = info.get("transformations", [])

        checkpoint_before = chk.get_last_scn(pipeline_name)
        current_scn = src.get_current_scn()

        if checkpoint_before is None or current_scn <= checkpoint_before:
            return False

        # FIX 1 & FIX 2: Inspect active transactions early to adjust lookback window and fail closed
        oldest_active_scn = None
        active_tx_check_failed = False
        if hasattr(src, "get_oldest_active_transaction_scn"):
            try:
                res = src.get_oldest_active_transaction_scn()
                if isinstance(res, (int, float)):
                    oldest_active_scn = int(res)
            except Exception as exc:
                active_tx_check_failed = True
                logger.error(
                    f"[CDC Worker] Active-transaction safety check failed on V$TRANSACTION: {exc}. "
                    f"Checkpoint advancement will be halted for safety on this cycle."
                )

        safety_overlap = int(info.get("safety_overlap_scn", 0))

        # FIX 2: Dynamic start_scn lookback:
        # If an active transaction exists, ensure LogMiner looks back far enough
        # to include its START_SCN so it is completely reconstructed upon commit.
        if oldest_active_scn is not None:
            start_scn = max(
                1,
                min(
                    checkpoint_before - safety_overlap + 1,
                    oldest_active_scn,
                ),
            )
        else:
            start_scn = max(1, checkpoint_before - safety_overlap + 1)

        end_scn = current_scn

        # 2. Mine changes from source.
        # If LogMiner encounters an error (e.g. ORA-01291, DB connection loss),
        # an exception is raised here and propagates out. Checkpoint is NOT updated.
        events = src.read_changes(
            start_scn=start_scn,
            end_scn=end_scn,
            table_filter=table_filter,
        )

        raw_rows_count = getattr(src, "last_raw_rows_count", len(events))

        # 3. Deduplicate events against checkpoint_before:
        # Filter out events whose commit_scn / scn was already applied in a previous window.
        new_events = [
            ev for ev in events
            if (getattr(ev, "commit_scn", None) or ev.scn) > checkpoint_before
        ]
        normalized_count = len(new_events)

        # ISSUE 1: Fail closed across BOTH empty and non-empty events
        if active_tx_check_failed:
            logger.warning(
                f"[CDC Worker] Active-transaction check on V$TRANSACTION failed for pipeline '{pipeline_name}'. "
                f"Halting poll cycle for safety: {len(new_events)} event(s) will NOT be applied and "
                f"checkpoint remains held at {checkpoint_before}."
            )
            return True

        if new_events:
            print(f"\n-------------------------------------------------------", flush=True)
            for ev in new_events:
                op = str(getattr(ev.operation, "value", ev.operation)).upper()
                c_scn = getattr(ev, "commit_scn", None) or ev.scn
                print(f"[CDC] {op} detected", flush=True)
                print(f"[CDC] SCN: {ev.scn} | COMMIT_SCN: {c_scn} | Table: {ev.source_table} | PK: {ev.primary_key}", flush=True)

            # 4. Process events and apply to PostgreSQL.
            # If target apply fails, CDCConsumer raises RuntimeError.
            # Checkpoint is NOT updated for failed events.
            result = csm.process_events(
                events=new_events,
                pipeline_name=pipeline_name,
                target_table=target_table,
                target_schema=target_schema,
                transformations=transformations,
                start_scn=start_scn,
                end_scn=end_scn,
            )

            print(f"[CDC] Apply -> PostgreSQL: {result.events_applied} row(s) applied ({result.inserts} ins, {result.updates} upd, {result.deletes} del)", flush=True)
            print(f"[CDC] Checkpoint updated: SCN={result.last_checkpoint_scn}", flush=True)
            print(f"-------------------------------------------------------\n", flush=True)

            info["total_captured"] = info.get("total_captured", 0) + len(new_events)
            info["total_applied"] = info.get("total_applied", 0) + result.events_applied
        else:
            # 5. No new committed events found in this window.
            if oldest_active_scn is not None:
                # An uncommitted transaction started at oldest_active_scn.
                # Safe boundary is oldest_active_scn - 1.
                safe_end_scn = min(end_scn, oldest_active_scn - 1)
                if safe_end_scn > checkpoint_before:
                    chk.save_scn(pipeline_name, safe_end_scn)
                else:
                    logger.debug(
                        f"[CDC Worker] In-flight transaction open at SCN {oldest_active_scn}. Checkpoint held at {checkpoint_before}."
                    )
            else:
                # Genuinely zero in-flight transactions in the database.
                # Advance checkpoint to end_scn so scan window remains bounded.
                chk.save_scn(pipeline_name, end_scn)

        checkpoint_after = chk.get_last_scn(pipeline_name)

        # 4. Structured logging showing all required metrics
        logger.info(
            "[CDC Worker Poll] pipeline='%s' | window=[%s -> %s] | raw_rows=%s | normalized_events=%s | checkpoint: %s -> %s",
            pipeline_name,
            start_scn,
            end_scn,
            raw_rows_count,
            normalized_count,
            checkpoint_before,
            checkpoint_after,
        )
        print(
            f"[CDC Poll] window=[{start_scn} -> {end_scn}] | raw_rows={raw_rows_count} | events={normalized_count} | checkpoint: {checkpoint_before} -> {checkpoint_after}",
            flush=True,
        )

        return True


default_worker_manager = CDCWorkerManager()
