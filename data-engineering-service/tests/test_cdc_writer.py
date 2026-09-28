from __future__ import annotations

from sqlalchemy import text
from app.adapters.registry import default_registry
from app.cdc.writer import PostgresCDCWriter
from app.cdc.models import CDCEvent, CDCOperation


def test_postgres_cdc_writer_idempotency():
    adapter = default_registry.get_target_adapter("postgresql")
    engine = adapter.get_engine()
    table = "cdc_writer_test_table"

    with engine.begin() as conn:
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS public.{table} (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            val NUMERIC(10, 2)
        );
        TRUNCATE TABLE public.{table};
        """))

    writer = PostgresCDCWriter(target_adapter=adapter)

    # 1. INSERT event
    event1 = CDCEvent(
        scn=1001,
        operation=CDCOperation.INSERT,
        source_schema="HR",
        source_table="TEST",
        primary_key={"ID": 1},
        after={"ID": 1, "NAME": "Initial", "VAL": 100.0},
    )
    applied1 = writer.apply_event(event1, target_table=table, target_schema="public")
    assert applied1 is True
    assert adapter.get_row_count(table) == 1

    # 2. Duplicate INSERT event (Idempotent test: should not fail, acts as upsert)
    applied_dup = writer.apply_event(event1, target_table=table, target_schema="public")
    assert applied_dup is True
    assert adapter.get_row_count(table) == 1

    # 3. UPDATE event
    event2 = CDCEvent(
        scn=1002,
        operation=CDCOperation.UPDATE,
        source_schema="HR",
        source_table="TEST",
        primary_key={"ID": 1},
        before={"ID": 1, "NAME": "Initial", "VAL": 100.0},
        after={"ID": 1, "NAME": "Updated", "VAL": 250.0},
    )
    applied2 = writer.apply_event(event2, target_table=table, target_schema="public")
    assert applied2 is True
    rows = adapter.verify_data(table, "public")
    assert rows.iloc[0]["name"] == "Updated"
    assert float(rows.iloc[0]["val"]) == 250.0

    # 4. DELETE event
    event3 = CDCEvent(
        scn=1003,
        operation=CDCOperation.DELETE,
        source_schema="HR",
        source_table="TEST",
        primary_key={"ID": 1},
        before={"ID": 1, "NAME": "Updated", "VAL": 250.0},
    )
    applied3 = writer.apply_event(event3, target_table=table, target_schema="public")
    assert applied3 is True
    assert adapter.get_row_count(table) == 0

    # 5. Duplicate DELETE event (Idempotent delete test)
    applied_del_dup = writer.apply_event(event3, target_table=table, target_schema="public")
    assert applied_del_dup is True
    assert adapter.get_row_count(table) == 0

    # Cleanup
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE public.{table}"))
