from __future__ import annotations

import logging
from typing import Any
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse

from app.models.migration import DatabaseEntity, MigrationRequest, MigrationResponse
from app.models.validation import CombinedMigrationValidationResponse
from app.services.oracle_service import OracleService
from app.services.postgres_service import PostgresService
from app.services.seatunnel_service import SeaTunnelService
from app.services.validation_service import ValidationService
from app.services.dvt_service import DVTService

# Legacy Data Plane service for backwards compatibility with existing tests
try:
    from app.services.migration_service import MigrationService
except ImportError:
    MigrationService = None

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["migration"])


@router.post("/migration/run", summary="Run Generic SeaTunnel Migration")
def run_migration_endpoint(payload: dict[str, Any]) -> Any:
    """
    Executes a generic Oracle -> PostgreSQL migration via SeaTunnel.
    Dynamically accepts source and target tables, schemas, and configurations.
    Does NOT hardcode any table names.
    """
    # Detect if payload is a SeaTunnel request or legacy contract
    if "source" in payload and isinstance(payload["source"], dict) and "table" in payload["source"]:
        try:
            req = MigrationRequest(**payload)
        except Exception as exc:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "failed",
                    "stage": "validation",
                    "errors": [str(exc)],
                },
            )

        st_service = SeaTunnelService()
        result = st_service.execute_migration(
            source=req.source,
            target=req.target,
            seatunnel_config_path=req.seatunnel_config,
        )

        if result.status == "failed":
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=result.model_dump(),
            )
        return result.model_dump()

    # Legacy contract format support (for backwards compatibility with existing test suite)
    if MigrationService is not None:
        try:
            service = MigrationService()
            result = service.run_migration(payload)
            if result.get("status") == "failed":
                stage = result.get("stage", "execution")
                code = 422 if stage == "validation" else status.HTTP_400_BAD_REQUEST
                return JSONResponse(status_code=code, content=result)
            return result
        except Exception as exc:
            return JSONResponse(
                status_code=422,
                content={"status": "failed", "stage": "validation", "errors": [str(exc)]},
            )

    raise HTTPException(status_code=422, detail="Invalid migration request payload format")


@router.get("/migration/{migration_id}", summary="Get Migration Status & Audit Trail")
def get_migration_endpoint(migration_id: str) -> dict[str, Any]:
    """Retrieves status, logs, duration, and execution details for a migration ID."""
    record = SeaTunnelService.get_migration_record(migration_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Migration ID '{migration_id}' not found",
        )
    return record


@router.post(
    "/migration-and-validate",
    summary="Run Complete Migration & Validation Flow",
    response_model=CombinedMigrationValidationResponse,
)
def run_migration_and_validate_endpoint(payload: dict[str, Any]) -> Any:
    """
    Executes the complete orchestrated migration and validation pipeline:
      1. Validate Request
      2. Check Oracle connection
      3. Check PostgreSQL connection
      4. Prepare SeaTunnel job
      5. Execute SeaTunnel
      6. Wait for migration completion
      7. Verify SeaTunnel exit status (If failed, STOP! DO NOT RUN VALIDATION)
      8. If SeaTunnel succeeded -> execute validation & DVT
      9. Return combined report
    """
    try:
        req = MigrationRequest(**payload)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid migration request: {str(exc)}")

    mid = f"mig_{datetime_now_str()}"
    logger.info(f"[{mid}] Starting combined migration and validation pipeline for {req.source.full_name} -> {req.target.full_name}")

    oracle_svc = OracleService()
    postgres_svc = PostgresService()

    # Step 1: Check Oracle connection
    logger.info(f"[{mid}] Checking Oracle connection...")
    ora_check = oracle_svc.test_connection()
    if ora_check.get("status") != "connected":
        logger.error(f"[{mid}] Oracle connection failed: {ora_check.get('error')}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "migration_id": mid,
                "overall_status": "migration_failed",
                "migration": {
                    "status": "failed",
                    "error": f"Oracle connection failed: {ora_check.get('error')}",
                    "stage": "connection_check",
                },
                "validation": None,
            },
        )

    # Step 2: Check PostgreSQL connection
    logger.info(f"[{mid}] Checking PostgreSQL connection...")
    pg_check = postgres_svc.test_connection()
    if pg_check.get("status") != "connected":
        logger.error(f"[{mid}] PostgreSQL connection failed: {pg_check.get('error')}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "migration_id": mid,
                "overall_status": "migration_failed",
                "migration": {
                    "status": "failed",
                    "error": f"PostgreSQL connection failed: {pg_check.get('error')}",
                    "stage": "connection_check",
                },
                "validation": None,
            },
        )

    # Step 3: Execute SeaTunnel Migration
    logger.info(f"[{mid}] Executing SeaTunnel migration...")
    st_svc = SeaTunnelService()
    migration_res = st_svc.execute_migration(
        source=req.source,
        target=req.target,
        seatunnel_config_path=req.seatunnel_config,
        migration_id=mid,
    )

    # Step 4: Verify SeaTunnel exit status
    if migration_res.status != "completed":
        logger.error(f"[{mid}] SeaTunnel migration failed (exit code {migration_res.exit_code}). Aborting validation.")
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "migration_id": mid,
                "overall_status": "migration_failed",
                "migration": migration_res.model_dump(),
                "validation": None,
            },
        )

    # Step 5: Migration succeeded -> Trigger Validation & DVT
    logger.info(f"[{mid}] SeaTunnel migration succeeded. Initiating Data Validation...")
    val_svc = ValidationService(oracle_service=oracle_svc, postgres_service=postgres_svc)
    val_res = val_svc.run_validation(
        source=req.source,
        target=req.target,
        checks=payload.get("checks", ["row_count", "schema", "null_count", "column_values"]),
        validation_id=f"val_{mid}",
    )

    # Step 6: Generate DVT Configuration
    dvt_svc = DVTService()
    dvt_res = dvt_svc.execute_dvt(source=req.source, target=req.target, dry_run=True)

    validation_summary = {
        "status": val_res.status,
        "row_count": val_res.checks.get("row_count", {}),
        "schema": {
            "matched": val_res.checks.get("schema", {}).get("status") == "passed",
            "columns": val_res.checks.get("schema", {}).get("columns", {}),
        },
        "dvt": {
            "cli_commands": dvt_res["cli_commands"],
            "dvt_installed": dvt_res["dvt_installed"],
        },
        "checks": val_res.checks,
    }

    overall_status = "completed" if val_res.status == "passed" else "validation_failed"
    logger.info(f"[{mid}] Completed pipeline with overall status '{overall_status}'")

    return {
        "migration_id": mid,
        "overall_status": overall_status,
        "migration": {
            "status": "completed",
            "source": req.source.full_name,
            "target": req.target.full_name,
            "duration_seconds": migration_res.duration_seconds,
            "log_file": migration_res.log_file,
        },
        "validation": validation_summary,
    }


def datetime_now_str() -> str:
    from datetime import datetime
    return datetime.now().strftime("%Y%m%d_%H%M%S")
