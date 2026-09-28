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


def test_cdc_consumer_multi_event_transaction_atomic_checkpoint():
    """
    Transaction-level Checkpoint Safety Test:
    - Transaction has 3 DML events sharing COMMIT_SCN=1000 (and transaction_id='TX_MULTI_1000').
    - Event 1 succeeds.
    - Event 2 fails (via simulate_failure_at_event=2).
    - Checkpoint must NOT advance to 1000! Checkpoint remains at previous SCN (900).
    - Retry reprocesses all 3 events.
    - After all 3 events succeed, checkpoint advances to 1000.
    """
    adapter = default_registry.get_target_adapter("postgresql")
    engine = adapter.get_engine()
    table = "cdc_multi_event_test_table"
    pipeline_name = "test_multi_event_tx_pipe"

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
    checkpoint_store.save_scn(pipeline_name, 900)

    writer = PostgresCDCWriter(target_adapter=adapter)
    consumer = CDCConsumer(writer=writer, checkpoint_store=checkpoint_store)

    events = [
        CDCEvent(
            scn=950,
            commit_scn=1000,
            transaction_id="TX_MULTI_1000",
            operation=CDCOperation.INSERT,
            source_schema="HR",
            source_table="EMPLOYEES",
            primary_key={"emp_id": 1},
            after={"emp_id": 1, "full_name": "Alice", "salary": 5000.0},
        ),
        CDCEvent(
            scn=960,
            commit_scn=1000,
            transaction_id="TX_MULTI_1000",
            operation=CDCOperation.INSERT,
            source_schema="HR",
            source_table="EMPLOYEES",
            primary_key={"emp_id": 2},
            after={"emp_id": 2, "full_name": "Bob", "salary": 6000.0},
        ),
        CDCEvent(
            scn=970,
            commit_scn=1000,
            transaction_id="TX_MULTI_1000",
            operation=CDCOperation.INSERT,
            source_schema="HR",
            source_table="EMPLOYEES",
            primary_key={"emp_id": 3},
            after={"emp_id": 3, "full_name": "Charlie", "salary": 7000.0},
        ),
    ]

    # Attempt 1: Event 2 fails
    with pytest.raises(RuntimeError, match="Simulated writer failure"):
        consumer.process_events(
            events=events,
            pipeline_name=pipeline_name,
            target_table=table,
            target_schema="public",
            simulate_failure_at_event=2,
        )

    # Checkpoint must NOT become 1000! It must remain at 900
    ckpt_after_fail = checkpoint_store.get_last_scn(pipeline_name)
    assert ckpt_after_fail == 900, f"Expected checkpoint 900, got {ckpt_after_fail}"

    # TRANSACTION ATOMICITY VERIFICATION:
    # Event 1 was executed, but because Event 2 failed, the entire transaction rolled back in PostgreSQL.
    # PostgreSQL must have NONE of the transaction's partial writes (0 rows).
    assert adapter.get_row_count(table) == 0, (
        f"Transaction atomicity failure: Expected 0 rows due to rollback, but found {adapter.get_row_count(table)}"
    )

    # Attempt 2: Retry with all required events
    recovery_result = consumer.process_events(
        events=events,
        pipeline_name=pipeline_name,
        target_table=table,
        target_schema="public",
    )

    assert recovery_result.status == "PASS"
    assert recovery_result.events_applied == 3
    assert recovery_result.last_checkpoint_scn == 1000

    # After all 3 succeed, checkpoint becomes 1000
    assert checkpoint_store.get_last_scn(pipeline_name) == 1000
    assert adapter.get_row_count(table) == 3

    # Cleanup
    checkpoint_store.reset_checkpoint(pipeline_name)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE public.{table}"))


def test_cdc_consumer_interleaved_multi_transaction_grouping_and_atomicity():
    """
    Regression Test for Issue 2:
    Interleaved-looking events from two separate transactions:
    - Tx A (COMMIT_SCN=1000): Event A1, Event A2
    - Tx B (COMMIT_SCN=2000): Event B1, Event B2
    Input sequence: [A1, B1, A2, B2] (interleaved).

    Verifies:
    1. Events belonging to Tx A stay together in one transaction.
    2. Events belonging to Tx B stay together in one transaction.
    3. Tx A commits -> checkpoint advances to 1000.
    4. If Tx B fails at Event B2:
       - Tx B rolls back completely (Event B1 partial write is undone).
       - Checkpoint stays at 1000 (does NOT advance to 2000).
       - Target table contains only rows from Tx A.
    5. On retry of Tx B:
       - Tx B commits.
       - Checkpoint advances to 2000.
       - Target table contains all rows from both transactions.
    """
    adapter = default_registry.get_target_adapter("postgresql")
    engine = adapter.get_engine()
    table = "cdc_interleaved_tx_test_table"
    pipeline_name = "test_interleaved_tx_pipe"

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
    checkpoint_store.save_scn(pipeline_name, 500)

    writer = PostgresCDCWriter(target_adapter=adapter)
    consumer = CDCConsumer(writer=writer, checkpoint_store=checkpoint_store)

    event_a1 = CDCEvent(
        scn=910,
        commit_scn=1000,
        transaction_id="TX_A",
        operation=CDCOperation.INSERT,
        source_schema="HR",
        source_table="EMPLOYEES",
        primary_key={"emp_id": 1},
        after={"emp_id": 1, "full_name": "Alice", "salary": 5000.0},
    )
    event_b1 = CDCEvent(
        scn=920,
        commit_scn=2000,
        transaction_id="TX_B",
        operation=CDCOperation.INSERT,
        source_schema="HR",
        source_table="EMPLOYEES",
        primary_key={"emp_id": 2},
        after={"emp_id": 2, "full_name": "Bob", "salary": 6000.0},
    )
    event_a2 = CDCEvent(
        scn=930,
        commit_scn=1000,
        transaction_id="TX_A",
        operation=CDCOperation.INSERT,
        source_schema="HR",
        source_table="EMPLOYEES",
        primary_key={"emp_id": 3},
        after={"emp_id": 3, "full_name": "Charlie", "salary": 7000.0},
    )
    event_b2 = CDCEvent(
        scn=940,
        commit_scn=2000,
        transaction_id="TX_B",
        operation=CDCOperation.INSERT,
        source_schema="HR",
        source_table="EMPLOYEES",
        primary_key={"emp_id": 4},
        after={"emp_id": 4, "full_name": "Dave", "salary": 8000.0},
    )

    # Interleaved order of arrival
    interleaved_events = [event_a1, event_b1, event_a2, event_b2]

    # Attempt 1: Tx A succeeds (events 1 & 2 in global order of Tx A),
    # but Tx B fails at event #4 (Event B2)
    with pytest.raises(RuntimeError, match="Simulated writer failure"):
        consumer.process_events(
            events=interleaved_events,
            pipeline_name=pipeline_name,
            target_table=table,
            target_schema="public",
            simulate_failure_at_event=4,
        )

    # Assertions for Attempt 1:
    # 1. Tx A succeeded: Checkpoint advanced to 1000
    assert checkpoint_store.get_last_scn(pipeline_name) == 1000
    # 2. Tx B failed and rolled back: Rows 2 and 4 do NOT exist. Only rows 1 and 3 exist!
    assert adapter.get_row_count(table) == 2
    with engine.connect() as conn:
        ids = conn.execute(text(f"SELECT emp_id FROM public.{table} ORDER BY emp_id")).scalars().all()
        assert list(ids) == [1, 3], f"Expected only Tx A rows [1, 3], but found {ids}"

    # Attempt 2: Re-process remaining events (or full batch) without simulated failure
    recovery_result = consumer.process_events(
        events=[event_b1, event_b2],
        pipeline_name=pipeline_name,
        target_table=table,
        target_schema="public",
    )
    assert recovery_result.status == "PASS"

    # Assertions for Attempt 2:
    # 1. Tx B succeeded: Checkpoint advanced to 2000
    assert checkpoint_store.get_last_scn(pipeline_name) == 2000
    # 2. All 4 rows now exist in PostgreSQL
    assert adapter.get_row_count(table) == 4
    with engine.connect() as conn:
        ids = conn.execute(text(f"SELECT emp_id FROM public.{table} ORDER BY emp_id")).scalars().all()
        assert list(ids) == [1, 2, 3, 4]

    # Cleanup
    checkpoint_store.reset_checkpoint(pipeline_name)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE public.{table}"))
