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
