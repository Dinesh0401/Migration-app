from __future__ import annotations

import logging
import threading
import time
from typing import Any

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
                last_checkpoint = chk.get_last_scn(pipeline_name)
                current_scn = src.get_current_scn()

                if last_checkpoint is not None and current_scn > last_checkpoint:
                    start_scn = last_checkpoint + 1
                    end_scn = current_scn

                    events = src.read_changes(
                        start_scn=start_scn,
                        end_scn=end_scn,
                        table_filter=table_filter,
                    )

                    if events:
                        print(f"\n-------------------------------------------------------", flush=True)
                        for ev in events:
                            op = str(getattr(ev.operation, "value", ev.operation)).upper()
                            print(f"[CDC] {op} detected", flush=True)
                            print(f"[CDC] SCN: {ev.scn} | Table: {ev.source_table} | PK: {ev.primary_key}", flush=True)

                        result = csm.process_events(
                            events=events,
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

                        info["total_captured"] += len(events)
                        info["total_applied"] += result.events_applied
                    else:
                        # Advance checkpoint to current_scn to keep scan window bounded
                        chk.save_scn(pipeline_name, current_scn)

                info["iterations"] += 1
                info["last_poll_time"] = time.strftime("%Y-%m-%d %H:%M:%S")

            except Exception as exc:
                info["last_error"] = str(exc)
                logger.error(f"[CDC Worker Error] {exc}", exc_info=True)
                print(f"[CDC Worker Error] {exc}", flush=True)

            stop_event.wait(poll_interval)

        print(f"\n[CDC] Background worker STOPPED for pipeline '{pipeline_name}'.\n", flush=True)
        logger.info(f"[CDC Worker] Worker thread ended for '{pipeline_name}'")


default_worker_manager = CDCWorkerManager()
