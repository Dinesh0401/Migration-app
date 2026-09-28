from __future__ import annotations

import time
from unittest.mock import MagicMock
from app.cdc.worker import CDCWorkerManager


def test_cdc_worker_lifecycle():
    manager = CDCWorkerManager()
    pipe_name = "test_unit_worker_pipe"

    # Mock source, consumer, checkpoint store
    mock_source = MagicMock()
    mock_source.get_current_scn.return_value = 1000
    mock_source.read_changes.return_value = []

    mock_checkpoint = MagicMock()
    mock_checkpoint.get_last_scn.return_value = 999

    mock_consumer = MagicMock()

    # 1. Start worker
    res_start = manager.start_worker(
        pipeline_name=pipe_name,
        poll_interval_seconds=0.1,
        source=mock_source,
        consumer=mock_consumer,
        checkpoint_store=mock_checkpoint,
    )
    assert res_start["status"] == "STARTED"
    assert res_start["is_running"] is True
    assert manager.is_running(pipe_name) is True

    # 2. Second start returns ALREADY_RUNNING
    res_dup = manager.start_worker(pipeline_name=pipe_name)
    assert res_dup["status"] == "ALREADY_RUNNING"
    assert res_dup["is_running"] is True

    # 3. Check stats
    time.sleep(0.25)
    stats = manager.get_stats(pipe_name)
    assert stats["is_running"] is True
    assert stats["iterations"] >= 1

    # 4. Stop worker
    res_stop = manager.stop_worker(pipeline_name=pipe_name)
    assert res_stop["status"] == "STOPPED"
    assert res_stop["is_running"] is False
    assert manager.is_running(pipe_name) is False

    # 5. Stop non-running worker
    res_stop_idle = manager.stop_worker(pipeline_name="non_existent_pipe")
    assert res_stop_idle["status"] == "NOT_RUNNING"


def test_cdc_worker_events_found_checkpoint_advances():
    """Requirement 10a: events found -> apply -> checkpoint advances."""
    import pytest
    from app.cdc.models import CDCEvent, CDCOperation, CDCWindowResult

    manager = CDCWorkerManager()
    pipe_name = "test_pipe_events_found"
    info = {"table_filter": ["EMPLOYEES"], "target_table": "employees", "target_schema": "public"}

    mock_source = MagicMock()
    mock_source.get_current_scn.return_value = 1010
    test_event = CDCEvent(
        scn=1005,
        operation=CDCOperation.UPDATE,
        source_schema="SYSTEM",
        source_table="EMPLOYEES",
        primary_key={"EMPLOYEE_ID": 101},
        after={"EMPLOYEE_ID": 101, "SALARY": 6000},
    )
    mock_source.read_changes.return_value = [test_event]
    mock_source.last_raw_rows_count = 1

    mock_checkpoint = MagicMock()
    mock_checkpoint.get_last_scn.side_effect = [1000, 1005]

    mock_consumer = MagicMock()
    mock_consumer.process_events.return_value = CDCWindowResult(
        pipeline_name=pipe_name,
        start_scn=1001,
        end_scn=1010,
        events_captured=1,
        events_applied=1,
        last_checkpoint_scn=1005,
    )

    processed = manager._poll_cycle(pipe_name, info, mock_source, mock_consumer, mock_checkpoint)
    assert processed is True
    mock_source.read_changes.assert_called_once_with(start_scn=1001, end_scn=1010, table_filter=["EMPLOYEES"])
    mock_consumer.process_events.assert_called_once()
    assert info.get("total_captured") == 1
    assert info.get("total_applied") == 1


def test_cdc_worker_empty_window_checkpoint_advances_safely():
    """Requirement 10b: successful empty mining window -> checkpoint can advance safely."""
    manager = CDCWorkerManager()
    pipe_name = "test_pipe_empty_window"
    info = {"table_filter": ["EMPLOYEES"]}

    mock_source = MagicMock()
    mock_source.get_current_scn.return_value = 1020
    mock_source.read_changes.return_value = []
    mock_source.last_raw_rows_count = 0

    mock_checkpoint = MagicMock()
    mock_checkpoint.get_last_scn.side_effect = [1000, 1020]

    mock_consumer = MagicMock()

    processed = manager._poll_cycle(pipe_name, info, mock_source, mock_consumer, mock_checkpoint)
    assert processed is True
    mock_source.read_changes.assert_called_once_with(start_scn=1001, end_scn=1020, table_filter=["EMPLOYEES"])
    mock_consumer.process_events.assert_not_called()
    # Checkpoint safely advances to end_scn
    mock_checkpoint.save_scn.assert_called_once_with(pipe_name, 1020)


def test_cdc_worker_mining_exception_checkpoint_not_advanced():
    """Requirement 10c: mining exception -> checkpoint does NOT advance."""
    import pytest

    manager = CDCWorkerManager()
    pipe_name = "test_pipe_mining_exception"
    info = {"table_filter": ["EMPLOYEES"]}

    mock_source = MagicMock()
    mock_source.get_current_scn.return_value = 1030
    mock_source.read_changes.side_effect = RuntimeError("Oracle LogMiner ORA-01291: missing logfile")

    mock_checkpoint = MagicMock()
    mock_checkpoint.get_last_scn.return_value = 1000

    mock_consumer = MagicMock()

    with pytest.raises(RuntimeError, match="missing logfile"):
        manager._poll_cycle(pipe_name, info, mock_source, mock_consumer, mock_checkpoint)

    # Checkpoint MUST NOT advance!
    mock_checkpoint.save_scn.assert_not_called()
    mock_consumer.process_events.assert_not_called()


def test_cdc_worker_apply_failure_checkpoint_not_advanced():
    """Requirement 10d: PostgreSQL apply failure -> checkpoint does NOT advance."""
    import pytest
    from app.cdc.models import CDCEvent, CDCOperation

    manager = CDCWorkerManager()
    pipe_name = "test_pipe_apply_failure"
    info = {"table_filter": ["EMPLOYEES"]}

    mock_source = MagicMock()
    mock_source.get_current_scn.return_value = 1040
    test_event = CDCEvent(
        scn=1035,
        operation=CDCOperation.INSERT,
        source_schema="SYSTEM",
        source_table="EMPLOYEES",
        primary_key={"EMPLOYEE_ID": 102},
        after={"EMPLOYEE_ID": 102, "SALARY": 7000},
    )
    mock_source.read_changes.return_value = [test_event]

    mock_checkpoint = MagicMock()
    mock_checkpoint.get_last_scn.return_value = 1000

    mock_consumer = MagicMock()
    mock_consumer.process_events.side_effect = RuntimeError("PostgreSQL Target Apply Failed: connection refused")

    with pytest.raises(RuntimeError, match="Target Apply Failed"):
        manager._poll_cycle(pipe_name, info, mock_source, mock_consumer, mock_checkpoint)

    # Checkpoint MUST NOT advance!
    mock_checkpoint.save_scn.assert_not_called()


def test_cdc_worker_uncommitted_transaction_boundary_race_regression():
    """
    Regression Test:
    Reproduces transaction-boundary race with COMMITTED_DATA_ONLY:
    - SCN 36080831: DML occurs (UPDATE EMPLOYEES salary 6100 -> 6200).
    - SCN 36080850: Polling window executes while transaction is still open.
      LogMiner returns 0 rows because commit has not occurred yet.
      Active transaction exists in Oracle at START_SCN = 36080831.
      Worker MUST NOT advance checkpoint past 36080830!
    - SCN 36080879: Transaction commits.
    - SCN 36080900: Next polling window runs with safety overlap window.
      LogMiner returns the committed transaction (scn=36080831, commit_scn=36080879).
      Worker applies event and advances checkpoint to COMMIT_SCN 36080879!
    """
    from app.cdc.models import CDCEvent, CDCOperation, CDCWindowResult

    manager = CDCWorkerManager()
    pipe_name = "test_pipe_tx_boundary_race"
    info = {
        "table_filter": ["EMPLOYEES"],
        "target_table": "employees",
        "target_schema": "public",
        "safety_overlap_scn": 200,
    }

    mock_source = MagicMock()
    mock_checkpoint = MagicMock()
    mock_consumer = MagicMock()

    # --- Window 1: Transaction in-flight (uncommitted) ---
    # Checkpoint is at 36080800, current SCN is 36080850.
    mock_checkpoint.get_last_scn.side_effect = [36080800, 36080800]
    mock_source.get_current_scn.return_value = 36080850
    # LogMiner returns 0 committed rows
    mock_source.read_changes.return_value = []
    # V$TRANSACTION reports an active transaction that started at 36080831
    mock_source.get_oldest_active_transaction_scn.return_value = 36080831

    processed_w1 = manager._poll_cycle(pipe_name, info, mock_source, mock_consumer, mock_checkpoint)
    assert processed_w1 is True

    # Checkpoint must NOT advance past the in-flight transaction start SCN!
    # safe_end_scn = 36080831 - 1 = 36080830 > 36080800
    mock_checkpoint.save_scn.assert_called_once_with(pipe_name, 36080830)
    mock_consumer.process_events.assert_not_called()

    # --- Window 2: Transaction commits at 36080879 ---
    mock_checkpoint.reset_mock()
    mock_source.reset_mock()
    mock_consumer.reset_mock()

    # Checkpoint before Window 2 is 36080830
    mock_checkpoint.get_last_scn.side_effect = [36080830, 36080879]
    mock_source.get_current_scn.return_value = 36080900
    # No more active transactions
    mock_source.get_oldest_active_transaction_scn.return_value = None

    committed_event = CDCEvent(
        scn=36080831,
        commit_scn=36080879,
        operation=CDCOperation.UPDATE,
        source_schema="SYSTEM",
        source_table="EMPLOYEES",
        primary_key={"EMPLOYEE_ID": 101},
        before={"EMPLOYEE_ID": 101, "SALARY": 6100},
        after={"EMPLOYEE_ID": 101, "SALARY": 6200},
    )
    # LogMiner now returns the committed transaction!
    mock_source.read_changes.return_value = [committed_event]
    mock_consumer.process_events.return_value = CDCWindowResult(
        pipeline_name=pipe_name,
        start_scn=36080631,
        end_scn=36080900,
        events_captured=1,
        events_applied=1,
        last_checkpoint_scn=36080879,
    )

    processed_w2 = manager._poll_cycle(pipe_name, info, mock_source, mock_consumer, mock_checkpoint)
    assert processed_w2 is True

    # Verify that the overlap window was used (looking back before 36080831)
    call_kwargs = mock_source.read_changes.call_args[1]
    assert call_kwargs["start_scn"] <= 36080831, "start_scn must look back before the DML SCN!"
    assert call_kwargs["end_scn"] >= 36080879, "end_scn must cover the commit SCN!"

    # Verify consumer processed the event and applied 6200
    mock_consumer.process_events.assert_called_once()
    assert info.get("total_captured") == 1
    assert info.get("total_applied") == 1


def test_cdc_worker_v_transaction_error_fails_closed():
    """
    FIX 1 Test:
    When querying V$TRANSACTION raises an exception (e.g. permission or network error),
    the worker must fail closed:
    1. Poll cycle must NOT advance the checkpoint.
    2. Checkpoint remains at checkpoint_before.
    3. The worker does not crash and can continue polling safely on next cycle.
    4. When V$TRANSACTION recovers and events appear, they are processed with no data skipped.
    """
    from app.cdc.models import CDCEvent, CDCOperation, CDCWindowResult

    manager = CDCWorkerManager()
    pipe_name = "test_pipe_fail_closed"
    info = {
        "table_filter": ["EMPLOYEES"],
        "target_table": "employees",
        "target_schema": "public",
        "safety_overlap_scn": 200,
    }

    mock_source = MagicMock()
    mock_checkpoint = MagicMock()
    mock_consumer = MagicMock()

    # --- Cycle 1: V$TRANSACTION lookup fails ---
    mock_checkpoint.get_last_scn.side_effect = [1000, 1000]
    mock_source.get_current_scn.return_value = 1050
    mock_source.get_oldest_active_transaction_scn.side_effect = RuntimeError("ORA-01031: insufficient privileges")
    mock_source.read_changes.return_value = []

    res = manager._poll_cycle(pipe_name, info, mock_source, mock_consumer, mock_checkpoint)
    assert res is True
    # Checkpoint MUST NOT advance!
    mock_checkpoint.save_scn.assert_not_called()
    mock_consumer.process_events.assert_not_called()

    # --- Cycle 2: V$TRANSACTION recovers and event commits ---
    mock_checkpoint.reset_mock()
    mock_source.reset_mock()
    mock_consumer.reset_mock()

    mock_checkpoint.get_last_scn.side_effect = [1000, 1060]
    mock_source.get_current_scn.return_value = 1100
    mock_source.get_oldest_active_transaction_scn.side_effect = None
    mock_source.get_oldest_active_transaction_scn.return_value = None

    committed_event = CDCEvent(
        scn=1040,
        commit_scn=1060,
        operation=CDCOperation.INSERT,
        source_schema="SYSTEM",
        source_table="EMPLOYEES",
        primary_key={"EMPLOYEE_ID": 102},
        after={"EMPLOYEE_ID": 102, "SALARY": 5000},
    )
    mock_source.read_changes.return_value = [committed_event]
    mock_consumer.process_events.return_value = CDCWindowResult(
        pipeline_name=pipe_name,
        start_scn=801,
        end_scn=1100,
        events_captured=1,
        events_applied=1,
        last_checkpoint_scn=1060,
    )

    res2 = manager._poll_cycle(pipe_name, info, mock_source, mock_consumer, mock_checkpoint)
    assert res2 is True
    # Event applied and processed without any loss
    mock_consumer.process_events.assert_called_once()
    assert mock_source.read_changes.call_args[1]["start_scn"] <= 1040


def test_cdc_worker_dynamic_start_scn_lookback_for_long_running_tx():
    """
    FIX 2 Test:
    A long-running transaction started at SCN 10,000.
    Current checkpoint is 10,500.
    Standard safety overlap is 200 (which would only reach 10,301).
    With dynamic lookback:
    1. Worker queries oldest active transaction SCN -> returns 10,000.
    2. start_scn must dynamically reach 10,000 (min(10,301, 10,000) = 10,000).
    3. Transaction commits at 10,600.
    4. LogMiner reads with start_scn <= 10,000, successfully discovering the transaction's DML.
    """
    from app.cdc.models import CDCEvent, CDCOperation, CDCWindowResult

    manager = CDCWorkerManager()
    pipe_name = "test_pipe_dynamic_lookback"
    info = {
        "table_filter": ["EMPLOYEES"],
        "target_table": "employees",
        "target_schema": "public",
        "safety_overlap_scn": 200,
    }

    mock_source = MagicMock()
    mock_checkpoint = MagicMock()
    mock_consumer = MagicMock()

    # Checkpoint is 10,500, current SCN is 10,650
    mock_checkpoint.get_last_scn.side_effect = [10500, 10600]
    mock_source.get_current_scn.return_value = 10650
    # Oldest active transaction started at 10,000 (500 SCNs before checkpoint!)
    mock_source.get_oldest_active_transaction_scn.return_value = 10000

    long_running_event = CDCEvent(
        scn=10000,
        commit_scn=10600,
        operation=CDCOperation.UPDATE,
        source_schema="SYSTEM",
        source_table="EMPLOYEES",
        primary_key={"EMPLOYEE_ID": 101},
        before={"EMPLOYEE_ID": 101, "SALARY": 6000},
        after={"EMPLOYEE_ID": 101, "SALARY": 7000},
    )
    mock_source.read_changes.return_value = [long_running_event]
    mock_consumer.process_events.return_value = CDCWindowResult(
        pipeline_name=pipe_name,
        start_scn=10000,
        end_scn=10650,
        events_captured=1,
        events_applied=1,
        last_checkpoint_scn=10600,
    )

    res = manager._poll_cycle(pipe_name, info, mock_source, mock_consumer, mock_checkpoint)
    assert res is True

    # Assert LogMiner start_scn reached 10,000 or earlier
    call_kwargs = mock_source.read_changes.call_args[1]
    assert call_kwargs["start_scn"] <= 10000, f"start_scn was {call_kwargs['start_scn']}, expected <= 10000"
    assert call_kwargs["end_scn"] == 10650
    mock_consumer.process_events.assert_called_once()


def test_cdc_worker_v_transaction_error_with_non_empty_events_fails_closed():
    """
    Regression Test for Issue 1:
    When V$TRANSACTION lookup fails, even if LogMiner read_changes returns non-empty events:
    - active_tx_check_failed must trigger
    - consumer.process_events() must NOT be called
    - checkpoint must NOT advance (remains at checkpoint_before)
    - poll cycle returns True cleanly, allowing next cycle to retry safely.
    """
    from app.cdc.models import CDCEvent, CDCOperation

    manager = CDCWorkerManager()
    pipe_name = "test_pipe_fail_closed_non_empty"
    info = {
        "table_filter": ["EMPLOYEES"],
        "target_table": "employees",
        "target_schema": "public",
        "safety_overlap_scn": 200,
    }

    mock_source = MagicMock()
    mock_checkpoint = MagicMock()
    mock_consumer = MagicMock()

    mock_checkpoint.get_last_scn.return_value = 5000
    mock_source.get_current_scn.return_value = 5100
    # V$TRANSACTION raises an error
    mock_source.get_oldest_active_transaction_scn.side_effect = RuntimeError("ORA-01033: ORACLE initialization or shutdown in progress")

    # Mined events are non-empty
    event1 = CDCEvent(
        scn=5050,
        commit_scn=5080,
        operation=CDCOperation.INSERT,
        source_schema="SYSTEM",
        source_table="EMPLOYEES",
        primary_key={"EMPLOYEE_ID": 105},
        after={"EMPLOYEE_ID": 105, "SALARY": 4000},
    )
    mock_source.read_changes.return_value = [event1]

    res = manager._poll_cycle(pipe_name, info, mock_source, mock_consumer, mock_checkpoint)
    assert res is True

    # Critical assertions:
    # 1. Consumer was NEVER called
    mock_consumer.process_events.assert_not_called()
    # 2. Checkpoint NEVER advanced
    mock_checkpoint.save_scn.assert_not_called()
