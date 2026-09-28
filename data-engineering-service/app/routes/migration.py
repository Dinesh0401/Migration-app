from __future__ import annotations

import logging
from typing import Any
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse

from app.services.migration_service import MigrationService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["migration"])


@router.post("/migration/run")
def run_migration(spec: dict[str, Any]) -> dict[str, Any]:
    """
    Executes a generic data migration according to the provided migration contract.
    Extracts actual row data from source, applies Pandas transformations, validates,
    and loads into the target database.
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
