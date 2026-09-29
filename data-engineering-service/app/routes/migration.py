from __future__ import annotations

import logging
from typing import Any
import pandas as pd
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse

from app.services.migration_service import MigrationService
from app.transformations.engine import TransformationEngine
from app.validation.validator import validate_migration_contract, normalize_contract

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["migration"])


@router.post("/migration/validate", summary="Validate Migration Contract")
def validate_contract_endpoint(spec: dict[str, Any]) -> dict[str, Any]:
    """
    Validates an AI JSON migration contract schema, checking DDL safety and structural integrity.
    """
    try:
        norm = normalize_contract(spec)
        is_valid, errors = validate_migration_contract(norm)
        return {
            "status": "valid" if is_valid else "invalid",
            "valid": is_valid,
            "errors": errors,
        }
    except Exception as exc:
        logger.error(f"Error validating contract: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"status": "invalid", "valid": False, "errors": [str(exc)]},
        )


@router.post("/migration/schema", summary="Execute Schema DDL (CREATE / ALTER / TRUNCATE)")
def execute_schema_ddl(spec: dict[str, Any]) -> dict[str, Any]:
    """
    Executes table management DDL (CREATE, ALTER, TRUNCATE) on the target database.
    """
    try:
        service = MigrationService()
        if "kind" not in spec:
            spec = dict(spec)
            spec["kind"] = "schema"
        return service.run_migration(spec)
    except Exception as exc:
        logger.error(f"Error executing schema DDL: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "failed", "error": str(exc)},
        )


@router.post("/migration/transform", summary="Preview In-Memory Transformations")
def preview_transformations(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Previews in-memory transformations (case normalization, trim, casting, renaming, etc.)
    on sample data records without writing to any database.
    """
    try:
        data = payload.get("data", [])
        transformations = payload.get("transformations", [])

        if not data:
            return {"status": "success", "transformed_rows": 0, "sample": []}

        df = pd.DataFrame(data)
        engine = TransformationEngine()
        transformed_df = engine.apply(df, transformations)

        return {
            "status": "success",
            "original_rows": len(df),
            "transformed_rows": len(transformed_df),
            "columns": list(transformed_df.columns),
            "sample": transformed_df.to_dict(orient="records"),
        }
    except Exception as exc:
        logger.error(f"Error previewing transformations: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"status": "failed", "error": str(exc)},
        )


@router.post("/migration/run", summary="Run Complete Migration Pipeline")
def run_migration(spec: dict[str, Any]) -> dict[str, Any]:
    """
    Executes the complete Data Plane pipeline:
      1. Target Table Management (CREATE / ALTER / TRUNCATE DDL)
      2. Source Data Extraction (SELECT query from Oracle)
      3. In-Memory Transformation (Pandas engine apply)
      4. Target Loading (INSERT into PostgreSQL)
      5. Reconciliation & Integrity Verification
    """
    try:
        service = MigrationService()
        result = service.run_migration(spec)

        if result.get("status") == "failed":
            stage = result.get("stage", "execution")
            if stage == "validation":
                status_code = 422
            else:
                status_code = status.HTTP_400_BAD_REQUEST

            return JSONResponse(status_code=status_code, content=result)

        return result

    except Exception as exc:
        logger.error(f"Unexpected unhandled error in run_migration route: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "failed",
                "stage": "route_handler",
                "message": "Internal server error occurred during migration execution",
                "errors": [str(exc)],
            },
        )

