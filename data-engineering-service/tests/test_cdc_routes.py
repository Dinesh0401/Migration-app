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
