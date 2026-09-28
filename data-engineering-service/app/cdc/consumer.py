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

        for idx, event in enumerate(events, start=1):
            op = str(event.operation).upper()

            # Optional hook for failure simulation testing
            if simulate_failure_at_event is not None and idx == simulate_failure_at_event:
                error_msg = f"Simulated writer failure at event #{idx} (SCN={event.scn})"
                logger.error(f"[TEST RECOVERY] {error_msg}")
                result.events_failed += 1
                result.status = "FAILED"
                result.error = error_msg
                raise RuntimeError(error_msg)

            transformed_data: dict[str, Any] | None = None

            # Apply transformation runtime
            if op in (CDCOperation.INSERT.value, CDCOperation.UPDATE.value):
                raw_payload = event.after or {}
                transformed_data = self.transform_event_payload(raw_payload, transformations)
                logger.debug(f"[TRANSFORM] SCN={event.scn} OP={op} raw={raw_payload} transformed={transformed_data}")

            # Apply to PostgreSQL target
            try:
                apply_success = self.writer.apply_event(
                    event=event,
                    target_table=target_table,
                    target_schema=target_schema,
                    transformed_row=transformed_data,
                )
            except Exception as exc:
                result.events_failed += 1
                result.status = "FAILED"
                result.error = f"Target apply failed at SCN={event.scn}: {exc}"
                logger.error(f"[-] Target apply failed at SCN={event.scn}: {exc}. Checkpoint NOT advanced.")
                raise RuntimeError(result.error) from exc

            if apply_success:
                result.events_applied += 1
                if op == CDCOperation.INSERT.value:
                    result.inserts += 1
                elif op == CDCOperation.UPDATE.value:
                    result.updates += 1
                elif op == CDCOperation.DELETE.value:
                    result.deletes += 1

                # CRITICAL RULE: Advance checkpoint ONLY AFTER successful apply
                self.checkpoint_store.save_scn(pipeline_name, event.scn)
                result.last_checkpoint_scn = event.scn

                result.events.append({
                    "scn": event.scn,
                    "operation": op,
                    "primary_key": event.primary_key,
                    "applied": True,
                })

                if on_event_hook:
                    on_event_hook(event, "applied")

                print(f"[CDC] SCN={event.scn} OP={op} PK={event.primary_key} -> [TRANSFORM] -> [TARGET] {op} applied -> [CHECKPOINT] SCN={event.scn}")

        return result
