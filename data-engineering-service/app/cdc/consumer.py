from __future__ import annotations

import logging
from typing import Any, Callable
import pandas as pd

from app.cdc.models import CDCEvent, CDCOperation, CDCWindowResult
from app.cdc.checkpoint import PostgresCheckpointStore
from app.cdc.writer import PostgresCDCWriter
from app.transformations.engine import TransformationEngine

logger = logging.getLogger(__name__)


class CDCConsumer:
    """
    Consumer pipeline for normalized CDC events.
    Applies the shared TransformationEngine runtime on individual events,
    delegates idempotent application to PostgresCDCWriter,
    and updates the durable checkpoint ONLY AFTER successful target application.
    """

    def __init__(
        self,
        writer: PostgresCDCWriter | None = None,
        checkpoint_store: PostgresCheckpointStore | None = None,
        transform_engine: TransformationEngine | None = None,
    ) -> None:
        self.writer = writer or PostgresCDCWriter()
        self.checkpoint_store = checkpoint_store or PostgresCheckpointStore()
        self.transform_engine = transform_engine or TransformationEngine()

    def transform_event_payload(
        self,
        payload: dict[str, Any],
        transformations: list[dict[str, Any]] | None,
    ) -> dict[str, Any]:
        """
        Adapts a single CDC event dictionary to the shared TransformationEngine
        by passing a 1-row DataFrame through engine.apply(), returning the transformed dict.
        """
        if not transformations or not payload:
            return payload

        # Convert 1 row to DataFrame
        row_df = pd.DataFrame([payload])
        transformed_df = self.transform_engine.apply(row_df, transformations)

        # Convert back to dict
        if len(transformed_df) > 0:
            return transformed_df.iloc[0].to_dict()
        return payload

    def process_events(
        self,
        events: list[CDCEvent],
        pipeline_name: str,
        target_table: str,
        target_schema: str = "public",
        transformations: list[dict[str, Any]] | None = None,
        start_scn: int = 0,
        end_scn: int = 0,
        on_event_hook: Callable[[CDCEvent, str], None] | None = None,
        simulate_failure_at_event: int | None = None,
    ) -> CDCWindowResult:
        """
        Processes a sequence of SCN-ordered CDCEvents.
        Guarantees:
        Sequence: capture -> validate -> transform -> apply PostgreSQL -> checkpoint SCN.
        Never checkpoints before successful apply.
        """
        result = CDCWindowResult(
            pipeline_name=pipeline_name,
            start_scn=start_scn,
            end_scn=end_scn,
            events_captured=len(events),
        )

        # Group events by transaction to preserve transactional atomicity for checkpoints.
        # Key: transaction_id (XID) when available, else commit_scn (or scn) as fallback.
        # Do NOT assume adjacent events always belong to the same transaction.
        # Preserves transaction arrival order and internal event sequence.
        tx_dict: dict[str, tuple[int, list[CDCEvent]]] = {}
        for event in events:
            tx_watermark = event.commit_scn or event.scn
            tx_key = str(event.transaction_id).strip() if event.transaction_id else (
                f"commit_{event.commit_scn}" if event.commit_scn else f"scn_{event.scn}"
            )
            if tx_key in tx_dict:
                cur_commit_scn, cur_events = tx_dict[tx_key]
                cur_events.append(event)
                tx_dict[tx_key] = (max(cur_commit_scn, tx_watermark), cur_events)
            else:
                tx_dict[tx_key] = (tx_watermark, [event])

        global_idx = 0
        for tx_key, (tx_commit_scn, tx_events) in tx_dict.items():
            # We must apply ALL events belonging to this transaction inside ONE atomic PostgreSQL transaction
            tx_audit_entries: list[dict[str, Any]] = []
            tx_inserts = 0
            tx_updates = 0
            tx_deletes = 0

            try:
                with self.writer.get_engine().begin() as pg_conn:
                    for event in tx_events:
                        global_idx += 1
                        op = str(event.operation).upper()

                        # Optional hook for failure simulation testing
                        if simulate_failure_at_event is not None and global_idx == simulate_failure_at_event:
                            error_msg = f"Simulated writer failure at event #{global_idx} (SCN={event.scn})"
                            logger.error(f"[TEST RECOVERY] {error_msg}")
                            raise RuntimeError(error_msg)

                        transformed_data: dict[str, Any] | None = None

                        # Apply transformation runtime
                        if op in (CDCOperation.INSERT.value, CDCOperation.UPDATE.value):
                            raw_payload = event.after or {}
                            transformed_data = self.transform_event_payload(raw_payload, transformations)
                            logger.debug(f"[TRANSFORM] SCN={event.scn} OP={op} raw={raw_payload} transformed={transformed_data}")

                        # Apply to PostgreSQL target on the atomic pg_conn
                        apply_success = self.writer.apply_event(
                            event=event,
                            target_table=target_table,
                            target_schema=target_schema,
                            transformed_row=transformed_data,
                            conn=pg_conn,
                        )

                        if not apply_success:
                            raise RuntimeError(f"Target apply returned False for SCN={event.scn}")

                        if op == CDCOperation.INSERT.value:
                            tx_inserts += 1
                        elif op == CDCOperation.UPDATE.value:
                            tx_updates += 1
                        elif op == CDCOperation.DELETE.value:
                            tx_deletes += 1

                        tx_audit_entries.append({
                            "scn": event.scn,
                            "commit_scn": tx_commit_scn,
                            "operation": op,
                            "primary_key": event.primary_key,
                            "applied": True,
                        })

                        if on_event_hook:
                            on_event_hook(event, "applied")

                        print(f"[CDC] SCN={event.scn} COMMIT_SCN={tx_commit_scn} OP={op} PK={event.primary_key} -> [TRANSFORM] -> [TARGET] {op} applied")

                # PostgreSQL transaction committed successfully!
            except Exception as exc:
                # PostgreSQL transaction rolled back automatically!
                result.events_failed += len(tx_events)
                result.status = "FAILED"
                result.error = f"Transaction '{tx_key}' target apply failed: {exc}"
                logger.error(f"[-] Transaction '{tx_key}' apply failed: {exc}. Target rolled back. Checkpoint NOT advanced.")
                raise RuntimeError(result.error) from exc

            # Accumulate successfully committed stats
            result.events_applied += len(tx_events)
            result.inserts += tx_inserts
            result.updates += tx_updates
            result.deletes += tx_deletes
            result.events.extend(tx_audit_entries)

            # CRITICAL RULE: Advance checkpoint ONLY AFTER PostgreSQL COMMIT succeeds
            self.checkpoint_store.save_scn(pipeline_name, tx_commit_scn)
            result.last_checkpoint_scn = tx_commit_scn
            logger.debug(f"[CHECKPOINT] Transaction '{tx_key}' completed ({len(tx_events)} events). Checkpoint SCN={tx_commit_scn}")
            print(f"[CDC] Transaction '{tx_key}' complete -> [CHECKPOINT] SCN={tx_commit_scn}", flush=True)

        return result
