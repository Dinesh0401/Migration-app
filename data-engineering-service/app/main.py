from __future__ import annotations

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes.migration import router as migration_router
from app.routes.cdc import router as cdc_router
from app.adapters.registry import default_registry
from app.config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

app = FastAPI(
    title="Data Engineering Migration Engine",
    description="Reusable data migration and transformation execution plane for Oracle to PostgreSQL and future engines.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(migration_router)
app.include_router(cdc_router)


@app.get("/health")
def health_check() -> dict:
    """Health check endpoint showing service status and database connectivity."""
    oracle_health = {"connected": False}
    postgres_health = {"connected": False}

    try:
        oracle_adapter = default_registry.get_source_adapter("oracle")
        oracle_health = oracle_adapter.test_connection()
    except Exception as exc:
        oracle_health = {"connected": False, "error": str(exc)}

    try:
        postgres_adapter = default_registry.get_target_adapter("postgresql")
        postgres_health = postgres_adapter.test_connection()
    except Exception as exc:
        postgres_health = {"connected": False, "error": str(exc)}

    all_connected = oracle_health.get("connected", False) and postgres_health.get("connected", False)

    return {
        "status": "ok" if all_connected else "partial",
        "service": "data-engineering-service",
        "adapters": {
            "sources": default_registry.get_supported_sources(),
            "targets": default_registry.get_supported_targets(),
        },
        "connections": {
            "oracle": oracle_health,
            "postgresql": postgres_health,
        },
        "endpoints": settings.get_safe_summary(),
    }
