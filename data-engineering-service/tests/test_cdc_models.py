from __future__ import annotations

from app.cdc.models import CDCEvent, CDCOperation, CDCStatus, CDCWindowResult


def test_cdc_event_creation_and_serialization():
    event = CDCEvent(
        scn=1051,
        operation=CDCOperation.INSERT,
        source_schema="HR",
        source_table="EMPLOYEES",
        primary_key={"EMPLOYEE_ID": 101},
        before=None,
        after={"EMPLOYEE_ID": 101, "FIRST_NAME": "John", "SALARY": 5000},
        timestamp="2026-09-28 10:00:00",
        transaction_id="0x0012.01",
    )

    d = event.to_dict()
    assert d["scn"] == 1051
    assert d["operation"] == "INSERT"
    assert d["source_schema"] == "HR"
    assert d["source_table"] == "EMPLOYEES"
    assert d["primary_key"] == {"EMPLOYEE_ID": 101}
    assert d["after"]["FIRST_NAME"] == "John"
    assert d["before"] is None

    reconstructed = CDCEvent.from_dict(d)
    assert reconstructed.scn == 1051
    assert reconstructed.operation == "INSERT"
    assert reconstructed.primary_key == {"EMPLOYEE_ID": 101}


def test_cdc_window_result():
    res = CDCWindowResult(
        pipeline_name="test_pipeline",
        start_scn=1000,
        end_scn=1050,
        events_captured=3,
        events_applied=3,
        inserts=1,
        updates=1,
        deletes=1,
        last_checkpoint_scn=1050,
    )
    d = res.to_dict()
    assert d["events_captured"] == 3
    assert d["inserts"] == 1
    assert d["updates"] == 1
    assert d["deletes"] == 1
    assert d["status"] == "PASS"


def test_cdc_status_dataclass():
    status = CDCStatus(
        pipeline_name="demo",
        last_checkpoint_scn=500,
        current_oracle_scn=520,
        lag_scn=20,
        updated_at="2026-09-28 12:00:00",
    )
    d = status.to_dict()
    assert d["lag_scn"] == 20
    assert d["pipeline_name"] == "demo"
