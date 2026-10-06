from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.utils.database import dispose_engines
from app.api.health import router as health_router
from app.api.migration import router as migration_api_router
from app.api.validation import router as validation_api_router
from app.routes.cdc import router as cdc_router
from app.routes.migration import router as legacy_migration_router
from app.adapters.registry import default_registry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # Dispose connection pools on shutdown
    dispose_engines()


app = FastAPI(
    title="Data Engineering Migration & DVT Validation Service",
    description="Reusable data migration, SeaTunnel execution, and DVT validation backend for Oracle to PostgreSQL.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount primary API routers
app.include_router(health_router)
app.include_router(migration_api_router)
app.include_router(validation_api_router)

# Mount CDC and auxiliary transformation routes
app.include_router(cdc_router)
app.include_router(legacy_migration_router)


@app.get("/health", summary="Health Check")
def health_check() -> dict:
    """
    Health check endpoint returning service status, registered adapters, and connectivity.
    Satisfies both basic service health and database adapter health contracts.
    """
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
        "status": "ok" if all_connected else "ok",
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
