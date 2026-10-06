from __future__ import annotations

import json
from unittest.mock import MagicMock, patch
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.migration import DatabaseEntity, MigrationRequest, MigrationResponse
from app.models.validation import ValidationRequest, ValidationResponse
from app.services.dvt_service import DVTService
from app.services.seatunnel_service import SeaTunnelService
from app.services.validation_service import ValidationService

client = TestClient(app)


# ---------------------------------------------------------------------------
# 1. Health Endpoints Tests
# ---------------------------------------------------------------------------
def test_get_health():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["service"] == "data-engineering-service"


def test_get_health_databases_both_connected():
    with patch("app.services.oracle_service.OracleService.test_connection", return_value={"status": "connected", "database": "oracle"}), \
         patch("app.services.postgres_service.PostgresService.test_connection", return_value={"status": "connected", "database": "postgresql"}):
        response = client.get("/health/databases")
        assert response.status_code == 200
        data = response.json()
        assert data["oracle"]["status"] == "connected"
        assert data["postgresql"]["status"] == "connected"


def test_get_health_databases_one_failed():
    with patch("app.services.oracle_service.OracleService.test_connection", return_value={"status": "connected", "database": "oracle"}), \
         patch("app.services.postgres_service.PostgresService.test_connection", return_value={"status": "failed", "error": "connection refused"}):
        response = client.get("/health/databases")
        assert response.status_code == 200
        data = response.json()
        assert data["oracle"]["status"] == "connected"
        assert data["postgresql"]["status"] == "failed"
        assert "connection refused" in data["postgresql"]["error"]


# ---------------------------------------------------------------------------
# 2. Migration Endpoints Tests
# ---------------------------------------------------------------------------
def test_post_migration_run_generic():
    payload = {
        "source": {
            "type": "oracle",
            "schema": "HR",
            "table": "EMPLOYEES",
        },
        "target": {
            "type": "postgresql",
            "schema": "migration",
            "table": "employees",
        },
    }

    mock_resp = MigrationResponse(
        migration_id="mig_test_001",
        status="completed",
        source="oracle.HR.EMPLOYEES",
        target="postgresql.migration.employees",
        duration_seconds=1.2,
    )

    with patch.object(SeaTunnelService, "execute_migration", return_value=mock_resp):
        response = client.post("/migration/run", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["migration_id"] == "mig_test_001"
        assert data["status"] == "completed"
        assert data["source"] == "oracle.HR.EMPLOYEES"
        assert data["target"] == "postgresql.migration.employees"


def test_post_migration_run_failure():
    payload = {
        "source": {
            "type": "oracle",
            "schema": "HR",
            "table": "INVALID_TABLE",
        },
        "target": {
            "type": "postgresql",
            "schema": "migration",
            "table": "invalid_table",
        },
    }

    mock_resp = MigrationResponse(
        migration_id="mig_test_fail",
        status="failed",
        source="oracle.HR.INVALID_TABLE",
        target="postgresql.migration.invalid_table",
        error="SeaTunnel process failed",
        exit_code=1,
    )

    with patch.object(SeaTunnelService, "execute_migration", return_value=mock_resp):
        response = client.post("/migration/run", json=payload)
        assert response.status_code == 400
        data = response.json()
        assert data["status"] == "failed"
        assert data["error"] == "SeaTunnel process failed"
        assert data["exit_code"] == 1


def test_get_migration_by_id():
    SeaTunnelService._migrations["mig_test_hist"] = {
        "migration_id": "mig_test_hist",
        "status": "completed",
        "source": "oracle.HR.DEPARTMENTS",
        "target": "postgresql.migration.departments",
    }

    response = client.get("/migration/mig_test_hist")
    assert response.status_code == 200
    data = response.json()
    assert data["migration_id"] == "mig_test_hist"

    # Non-existent ID
    response_404 = client.get("/migration/non_existent_id")
    assert response_404.status_code == 404


# ---------------------------------------------------------------------------
# 3. Validation Endpoints Tests
# ---------------------------------------------------------------------------
def test_post_validation_run():
    payload = {
        "source": {
            "type": "oracle",
            "schema": "HR",
            "table": "EMPLOYEES",
        },
        "target": {
            "type": "postgresql",
            "schema": "migration",
            "table": "employees",
        },
        "checks": ["row_count", "schema", "null_count"],
    }

    mock_resp = ValidationResponse(
        validation_id="val_test_001",
        status="passed",
        source="oracle.HR.EMPLOYEES",
        target="postgresql.migration.employees",
        checks={
            "connection": {"status": "passed"},
            "table_existence": {"status": "passed"},
            "row_count": {"check": "row_count", "source": 107, "target": 107, "status": "passed"},
            "schema": {"check": "schema", "status": "passed", "columns": {"matched": 11, "missing": 0, "extra": 0}},
        },
        summary={"passed_checks": 4, "failed_checks": 0, "overall": "passed"},
    )

    with patch.object(ValidationService, "run_validation", return_value=mock_resp):
        response = client.post("/validation/run", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["validation_id"] == "val_test_001"
        assert data["status"] == "passed"
        assert data["checks"]["row_count"]["source"] == 107
        assert data["checks"]["row_count"]["target"] == 107


def test_get_validation_by_id():
    ValidationService._validations["val_test_hist"] = {
        "validation_id": "val_test_hist",
        "status": "passed",
        "source": "oracle.HR.EMPLOYEES",
        "target": "postgresql.migration.employees",
        "checks": {},
        "summary": {},
    }

    response = client.get("/validation/val_test_hist")
    assert response.status_code == 200
    data = response.json()
    assert data["validation_id"] == "val_test_hist"

    # Non-existent ID
    response_404 = client.get("/validation/missing_val_id")
    assert response_404.status_code == 404


# ---------------------------------------------------------------------------
# 4. Combined Migration & Validation Endpoint (/migration-and-validate)
# ---------------------------------------------------------------------------
def test_migration_and_validate_success():
    payload = {
        "source": {
            "type": "oracle",
            "schema": "HR",
            "table": "EMPLOYEES",
        },
        "target": {
            "type": "postgresql",
            "schema": "migration",
            "table": "employees",
        },
    }

    mock_mig = MigrationResponse(
        migration_id="mig_comb_001",
        status="completed",
        source="oracle.HR.EMPLOYEES",
        target="postgresql.migration.employees",
        duration_seconds=1.5,
    )

    mock_val = ValidationResponse(
        validation_id="val_comb_001",
        status="passed",
        source="oracle.HR.EMPLOYEES",
        target="postgresql.migration.employees",
        checks={
            "row_count": {"source": 107, "target": 107, "status": "passed"},
            "schema": {"status": "passed", "columns": {"matched": 11}},
        },
        summary={"passed_checks": 2, "failed_checks": 0, "overall": "passed"},
    )

    with patch("app.services.oracle_service.OracleService.test_connection", return_value={"status": "connected"}), \
         patch("app.services.postgres_service.PostgresService.test_connection", return_value={"status": "connected"}), \
         patch.object(SeaTunnelService, "execute_migration", return_value=mock_mig), \
         patch.object(ValidationService, "run_validation", return_value=mock_val):

        response = client.post("/migration-and-validate", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["overall_status"] == "completed"
        assert data["migration"]["status"] == "completed"
        assert data["validation"]["status"] == "passed"
        assert data["validation"]["row_count"]["source"] == 107


def test_migration_and_validate_stop_on_connection_error():
    payload = {
        "source": {"type": "oracle", "schema": "HR", "table": "EMPLOYEES"},
        "target": {"type": "postgresql", "schema": "migration", "table": "employees"},
    }

    with patch("app.services.oracle_service.OracleService.test_connection", return_value={"status": "failed", "error": "timeout"}), \
         patch("app.services.postgres_service.PostgresService.test_connection", return_value={"status": "connected"}), \
         patch.object(SeaTunnelService, "execute_migration") as mock_exec:

        response = client.post("/migration-and-validate", json=payload)
        assert response.status_code == 503
        data = response.json()
        assert data["overall_status"] == "migration_failed"
        assert "Oracle connection failed" in data["migration"]["error"]
        # SeaTunnel must NOT be called when connection check fails!
        mock_exec.assert_not_called()


def test_migration_and_validate_stop_on_seatunnel_failure():
    payload = {
        "source": {"type": "oracle", "schema": "HR", "table": "EMPLOYEES"},
        "target": {"type": "postgresql", "schema": "migration", "table": "employees"},
    }

    mock_fail_mig = MigrationResponse(
        migration_id="mig_comb_fail",
        status="failed",
        source="oracle.HR.EMPLOYEES",
        target="postgresql.migration.employees",
        error="SeaTunnel process failed",
        exit_code=1,
    )

    with patch("app.services.oracle_service.OracleService.test_connection", return_value={"status": "connected"}), \
         patch("app.services.postgres_service.PostgresService.test_connection", return_value={"status": "connected"}), \
         patch.object(SeaTunnelService, "execute_migration", return_value=mock_fail_mig), \
         patch.object(ValidationService, "run_validation") as mock_val:

        response = client.post("/migration-and-validate", json=payload)
        assert response.status_code == 400
        data = response.json()
        assert data["overall_status"] == "migration_failed"
        assert data["migration"]["status"] == "failed"
        # Validation must NOT be executed when SeaTunnel fails!
        mock_val.assert_not_called()


# ---------------------------------------------------------------------------
# 5. DVT Service Tests
# ---------------------------------------------------------------------------
def test_dvt_service_intermediate_json_and_cli():
    src = DatabaseEntity(type="oracle", schema="HR", table="EMPLOYEES")
    tgt = DatabaseEntity(type="postgresql", schema="migration", table="employees")

    dvt_svc = DVTService()
    internal_rep = dvt_svc.generate_internal_representation(src, tgt)

    assert internal_rep["source"]["database"] == "oracle"
    assert internal_rep["source"]["schema"] == "HR"
    assert internal_rep["source"]["table"] == "EMPLOYEES"
    assert internal_rep["target"]["database"] == "postgresql"
    assert internal_rep["target"]["schema"] == "migration"
    assert internal_rep["target"]["table"] == "employees"

    cli_cmds = dvt_svc.generate_dvt_cli_commands(internal_rep, dry_run=True)
    assert len(cli_cmds) >= 2
    assert any("validate schema" in cmd for cmd in cli_cmds)
    assert any("validate row" in cmd for cmd in cli_cmds)
    assert any("HR.EMPLOYEES=migration.employees" in cmd for cmd in cli_cmds)


# ---------------------------------------------------------------------------
# 6. CLI Support Tests
# ---------------------------------------------------------------------------
def test_cli_health():
    from app.cli import cmd_health
    with patch("app.services.oracle_service.OracleService.test_connection", return_value={"status": "connected"}), \
         patch("app.services.postgres_service.PostgresService.test_connection", return_value={"status": "connected"}):
        exit_code = cmd_health()
        assert exit_code == 0
