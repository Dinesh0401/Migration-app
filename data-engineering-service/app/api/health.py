from __future__ import annotations

import logging
from typing import Any
from fastapi import APIRouter
from app.adapters.registry import default_registry
from app.services.oracle_service import OracleService
from app.services.postgres_service import PostgresService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["health"])


@router.get("/health", summary="Basic Service Health Check")
def get_service_health() -> dict[str, Any]:
    """Returns basic service health status along with registered adapters and connections."""
    return {
        "status": "ok",
        "service": "data-engineering-service",
        "adapters": {
            "sources": default_registry.get_supported_sources(),
            "targets": default_registry.get_supported_targets(),
        },
        "connections": {
            "oracle": {"status": "configured"},
            "postgresql": {"status": "configured"},
        },
    }


@router.get("/health/databases", summary="Database Connectivity Health Check")
def get_database_health() -> dict[str, Any]:
    """
    Tests live connectivity to Oracle and PostgreSQL databases.
    Reports connection status without exposing passwords or sensitive tokens.
    """
    oracle_svc = OracleService()
    postgres_svc = PostgresService()

    ora_res = oracle_svc.test_connection()
    pg_res = postgres_svc.test_connection()

    oracle_status = {"status": ora_res.get("status", "failed")}
    if ora_res.get("status") != "connected":
        oracle_status["error"] = ora_res.get("error", "Oracle connection failed")

    postgres_status = {"status": pg_res.get("status", "failed")}
    if pg_res.get("status") != "connected":
        postgres_status["error"] = pg_res.get("error", "PostgreSQL connection failed")

    return {
        "oracle": oracle_status,
        "postgresql": postgres_status,
    }
