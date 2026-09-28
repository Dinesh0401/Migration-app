from __future__ import annotations

import logging
from typing import Any
from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import JSONResponse

from app.services.cdc_service import CDCService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["cdc"])


@router.get("/cdc/check")
def check_cdc_readiness() -> dict[str, Any]:
    """
    Validates Oracle LogMiner prerequisites and PostgreSQL target readiness.
    Inspects version, container, DBMS_LOGMNR package, privileges,
    supplemental logging, and redo log status.
    """
    try:
        service = CDCService()
        return service.check_readiness()
    except Exception as exc:
        logger.error(f"Error checking CDC readiness: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "error": str(exc)},
        )


@router.get("/cdc/status")
def get_cdc_status(
    pipeline_name: str = Query("oracle_to_postgres_cdc", description="Pipeline identifier")
) -> dict[str, Any]:
    """
    Retrieves current CDC operational status, SCN checkpoint offsets, and replication lag.
    """
    try:
        service = CDCService()
        return service.get_status(pipeline_name=pipeline_name)
    except Exception as exc:
        logger.error(f"Error fetching CDC status: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "error": str(exc)},
        )


@router.get("/cdc/checkpoint")
def get_cdc_checkpoint(
    pipeline_name: str = Query("oracle_to_postgres_cdc", description="Pipeline identifier")
) -> dict[str, Any]:
    """
    Retrieves durable SCN checkpoint from PostgreSQL migration.cdc_checkpoint table.
    """
    try:
        service = CDCService()
        return service.get_checkpoint(pipeline_name=pipeline_name)
    except Exception as exc:
        logger.error(f"Error fetching CDC checkpoint: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "error": str(exc)},
        )


@router.post("/cdc/run")
def run_cdc(spec: dict[str, Any]) -> dict[str, Any]:
    """
    Executes a CDC window or starts continuous streaming according to the provided specification.
    """
    try:
        service = CDCService()
        mode = str(spec.get("mode", "batch")).lower()
        pipeline_name = spec.get("pipeline_name", "oracle_to_postgres_cdc")
        target_table = spec.get("target_table", "employees")
        target_schema = spec.get("target_schema", "public")
        table_filter = spec.get("table_filter") or (
            [spec["source_table"]] if spec.get("source_table") else None
        )
        transformations = spec.get("transformations", [])

        if mode == "continuous":
            poll_interval = float(spec.get("poll_interval_seconds", 2.0))
            max_iterations = spec.get("max_iterations")
            if max_iterations is not None:
                max_iterations = int(max_iterations)
            return service.run_continuous(
                pipeline_name=pipeline_name,
                poll_interval_seconds=poll_interval,
                max_iterations=max_iterations,
                table_filter=table_filter,
                transformations=transformations,
                target_table=target_table,
                target_schema=target_schema,
            )
        else:
            start_scn = spec.get("start_scn")
            end_scn = spec.get("end_scn")
            if start_scn is not None:
                start_scn = int(start_scn)
            if end_scn is not None:
                end_scn = int(end_scn)

            return service.run_batch_window(
                pipeline_name=pipeline_name,
                start_scn=start_scn,
                end_scn=end_scn,
                table_filter=table_filter,
                transformations=transformations,
                target_table=target_table,
                target_schema=target_schema,
            )
    except Exception as exc:
        logger.error(f"Error running CDC: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "failed", "error": str(exc)},
        )
