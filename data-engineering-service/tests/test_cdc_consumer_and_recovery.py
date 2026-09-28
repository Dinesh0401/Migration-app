from __future__ import annotations

import pytest
from sqlalchemy import text
from app.adapters.registry import default_registry
from app.cdc.consumer import CDCConsumer
from app.cdc.checkpoint import PostgresCheckpointStore
from app.cdc.writer import PostgresCDCWriter
from app.cdc.models import CDCEvent, CDCOperation
from app.transformations.engine import TransformationEngine


def test_cdc_consumer_transformation_reuse_and_recovery():
    adapter = default_registry.get_target_adapter("postgresql")
    engine = adapter.get_engine()
    table = "cdc_consumer_test_table"
    pipeline_name = "test_consumer_recovery_pipe"

    with engine.begin() as conn:
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS public.{table} (
            emp_id INT PRIMARY KEY,
            full_name VARCHAR(100),
            salary NUMERIC(10, 2)
        );
        TRUNCATE TABLE public.{table};
        """))

    checkpoint_store = PostgresCheckpointStore()
    checkpoint_store.reset_checkpoint(pipeline_name)

    writer = PostgresCDCWriter(target_adapter=adapter)
    transform_engine = TransformationEngine()
    consumer = CDCConsumer(
        writer=writer,
        checkpoint_store=checkpoint_store,
        transform_engine=transform_engine,
    )

    transformations = [
        {"type": "normalize_columns", "case": "lower"},
        {"type": "rename", "columns": {"id": "emp_id", "name": "full_name"}},
        {"type": "normalize", "source": "full_name", "case": "upper", "strip": True},
    ]

    events = [
        CDCEvent(
            scn=2001,
            operation=CDCOperation.INSERT,
            source_schema="HR",
            source_table="EMPLOYEES",
            primary_key={"ID": 10},
            after={"ID": 10, "NAME": "  john doe  ", "SALARY": 3000.0},
        ),
        CDCEvent(
            scn=2002,
            operation=CDCOperation.INSERT,
            source_schema="HR",
            source_table="EMPLOYEES",
            primary_key={"ID": 20},
            after={"ID": 20, "NAME": "  jane smith  ", "SALARY": 4500.0},
        ),
    ]

    # --- SIMULATE FAILURE AT EVENT #2 ---
    # Event 1 will succeed and checkpoint SCN 2001.
    # Event 2 will fail before checkpointing SCN 2002.
    with pytest.raises(RuntimeError, match="Simulated writer failure"):
        consumer.process_events(
            events=events,
            pipeline_name=pipeline_name,
            target_table=table,
            target_schema="public",
            transformations=transformations,
            simulate_failure_at_event=2,
        )

    # Verify that checkpoint is SCN 2001 (event #1), and NOT advanced to 2002!
    ckpt_after_fail = checkpoint_store.get_last_scn(pipeline_name)
    assert ckpt_after_fail == 2001, "Checkpoint must NOT advance when event application fails!"

    # Verify target has only 1 row (from event 1)
    assert adapter.get_row_count(table) == 1
    sample = adapter.verify_data(table, "public")
    assert sample.iloc[0]["full_name"] == "JOHN DOE"

    # --- RESTART CONSUMER / RECOVERY ---
    # Recovery: Filter events that have SCN > last_checkpoint (SCN 2001) -> only event 2 is retried
    pending_events = [e for e in events if e.scn > ckpt_after_fail]
    assert len(pending_events) == 1
    assert pending_events[0].scn == 2002

    recovery_result = consumer.process_events(
        events=pending_events,
        pipeline_name=pipeline_name,
        target_table=table,
        target_schema="public",
        transformations=transformations,
    )

    assert recovery_result.status == "PASS"
    assert recovery_result.events_applied == 1
    assert recovery_result.last_checkpoint_scn == 2002

    # Checkpoint is now safely updated to SCN 2002!
    assert checkpoint_store.get_last_scn(pipeline_name) == 2002
    assert adapter.get_row_count(table) == 2

    # Cleanup
    checkpoint_store.reset_checkpoint(pipeline_name)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE public.{table}"))
