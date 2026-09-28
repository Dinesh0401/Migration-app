from __future__ import annotations

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_cdc_check_endpoint():
    response = client.get("/cdc/check")
    assert response.status_code == 200
    data = response.json()
    assert "ready" in data
    assert "oracle" in data
    assert "postgresql" in data


def test_cdc_status_endpoint():
    response = client.get("/cdc/status?pipeline_name=test_pipe")
    assert response.status_code == 200
    data = response.json()
    assert data["pipeline_name"] == "test_pipe"
    assert "last_checkpoint_scn" in data
    assert "current_oracle_scn" in data


def test_cdc_checkpoint_endpoint():
    response = client.get("/cdc/checkpoint?pipeline_name=test_pipe")
    assert response.status_code == 200
    data = response.json()
    assert data["pipeline_name"] == "test_pipe"
    assert "last_scn" in data


def test_cdc_worker_lifecycle_endpoints(monkeypatch):
    from unittest.mock import MagicMock
    from app.services.cdc_service import CDCService

    mock_start = MagicMock(return_value={"status": "STARTED", "pipeline_name": "test_pipe", "is_running": True})
    mock_stop = MagicMock(return_value={"status": "STOPPED", "pipeline_name": "test_pipe", "is_running": False})
    mock_run = MagicMock(return_value={"status": "PASS", "applied_events": 0})

    monkeypatch.setattr(CDCService, "start_worker", mock_start)
    monkeypatch.setattr(CDCService, "stop_worker", mock_stop)
    monkeypatch.setattr(CDCService, "run_batch_window", mock_run)

    # Test /cdc/start with JSON body
    res_start = client.post("/cdc/start", json={"pipeline_name": "test_pipe"})
    assert res_start.status_code == 200
    assert res_start.json()["status"] == "STARTED"
    assert res_start.json()["is_running"] is True

    # Test /cdc/start without body
    res_start_empty = client.post("/cdc/start")
    assert res_start_empty.status_code == 200
    assert res_start_empty.json()["status"] == "STARTED"

    # Test /cdc/stop with JSON body
    res_stop = client.post("/cdc/stop", json={"pipeline_name": "test_pipe"})
    assert res_stop.status_code == 200
    assert res_stop.json()["status"] == "STOPPED"
    assert res_stop.json()["is_running"] is False

    # Test /cdc/stop without body
    res_stop_empty = client.post("/cdc/stop")
    assert res_stop_empty.status_code == 200
    assert res_stop_empty.json()["status"] == "STOPPED"

    # Test /cdc/run batch mode
    res_run = client.post("/cdc/run", json={"mode": "batch", "pipeline_name": "test_pipe"})
    assert res_run.status_code == 200
    assert res_run.json()["status"] == "PASS"

    # Test /cdc/run continuous mode delegates to start_worker
    res_run_cont = client.post("/cdc/run", json={"mode": "continuous", "pipeline_name": "test_pipe"})
    assert res_run_cont.status_code == 200
    assert res_run_cont.json()["status"] == "STARTED"


