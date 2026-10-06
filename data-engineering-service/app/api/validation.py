from __future__ import annotations

import logging
from typing import Any
from fastapi import APIRouter, HTTPException, status

from app.models.validation import ValidationRequest, ValidationResponse
from app.services.validation_service import ValidationService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["validation"])


@router.post("/validation/run", summary="Run Layered Data Validation", response_model=ValidationResponse)
def run_validation_endpoint(request: ValidationRequest) -> Any:
    """
    Executes layered data validation between Oracle source and PostgreSQL target:
      - Connection health
      - Table existence
      - Row count parity
      - Schema definition parity (column names, types, primary keys)
      - Null counts & column value parity
    """
    val_service = ValidationService()
    try:
        result = val_service.run_validation(
            source=request.source,
            target=request.target,
            checks=request.checks,
        )
        return result
    except Exception as exc:
        logger.error(f"Validation execution failed: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Validation execution error: {str(exc)}",
        )


@router.get("/validation/{validation_id}", summary="Get Validation Report")
def get_validation_endpoint(validation_id: str) -> dict[str, Any]:
    """Retrieves an existing validation report by validation ID."""
    record = ValidationService.get_validation_record(validation_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Validation report ID '{validation_id}' not found",
        )
    return record
