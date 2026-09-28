from __future__ import annotations

import pytest
from app.cdc.checkpoint import PostgresCheckpointStore


def test_checkpoint_store_lifecycle():
    store = PostgresCheckpointStore()
    pipeline_name = "test_unit_pipeline"

    # Reset
    store.reset_checkpoint(pipeline_name)
    assert store.get_last_scn(pipeline_name) is None

    # Save SCN 1000
    store.save_scn(pipeline_name, 1000)
    assert store.get_last_scn(pipeline_name) == 1000

    info = store.get_checkpoint_info(pipeline_name)
    assert info["pipeline_name"] == pipeline_name
    assert info["last_scn"] == 1000
    assert info["updated_at"] is not None

    # Advance SCN 1050 (Idempotent update)
    store.save_scn(pipeline_name, 1050)
    assert store.get_last_scn(pipeline_name) == 1050

    # Cleanup
    store.reset_checkpoint(pipeline_name)
    assert store.get_last_scn(pipeline_name) is None
