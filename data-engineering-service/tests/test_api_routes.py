import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from app.main import app

client = TestClient(app)


def test_health_check_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "data-engineering-service"
    assert "adapters" in data
    assert "connections" in data
    assert "oracle" in data["adapters"]["sources"]
    assert "postgresql" in data["adapters"]["targets"]


def test_api_run_migration_validation_failure():
    response = client.post("/migration/run", json={"invalid": "payload"})
    assert response.status_code == 422
    data = response.json()
    assert data["status"] == "failed"
    assert data["stage"] == "validation"
    assert len(data["errors"]) > 0


def test_api_run_migration_success():
    spec = {
        "source": {"type": "oracle", "schema": "TEST_SCHEMA"},
        "target": {"type": "postgresql", "schema": "public"},
        "data_migration": {
            "extraction": {"query": "SELECT ID, NAME FROM TEST_SCHEMA.TEST_TABLE"},
            "loading": {"table": "test_target", "columns": ["id", "name"]},
        },
        "transformations": [
            {"type": "rename", "source": "ID", "target": "id"},
            {"type": "rename", "source": "NAME", "target": "name"},
        ],
    }

    mock_result = {
        "status": "success",
        "source": "oracle",
        "target": "postgresql",
        "source_rows": 5,
        "transformed_rows": 5,
        "target_rows": 5,
        "failed_rows": 0,
        "validation": {"status": "passed", "warnings": []},
        "target": {"schema": "public", "table": "test_target"},
    }

    with patch("app.services.migration_service.MigrationService.run_migration", return_value=mock_result):
        response = client.post("/migration/run", json=spec)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["target_rows"] == 5


def test_api_validate_endpoint():
    spec = {
        "source": {"type": "oracle", "schema": "HR"},
        "target": {"type": "postgresql", "schema": "public"},
        "data_migration": {
            "extraction": {"query": "SELECT * FROM EMPLOYEES"},
            "loading": {"table": "employees", "columns": ["id"]},
        },
    }
    response = client.post("/migration/validate", json=spec)
    assert response.status_code == 200
    assert response.json()["valid"] is True


def test_api_transform_endpoint():
    payload = {
        "data": [
            {"NAME": "  john doe  ", "SALARY": 5000},
            {"NAME": "  jane smith  ", "SALARY": 6000},
        ],
        "transformations": [
            {"type": "normalize_columns", "case": "lower"},
            {"type": "normalize", "source": "name", "target": "name", "case": "title", "strip": True},
        ],
    }
    response = client.post("/migration/transform", json=payload)
    assert response.status_code == 200
    res = response.json()
    assert res["status"] == "success"
    assert res["transformed_rows"] == 2
    assert res["sample"][0]["name"] == "John Doe"

