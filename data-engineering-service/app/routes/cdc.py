from __future__ import annotations

import logging
from typing import Any
from fastapi import APIRouter, Body, HTTPException, Query, status
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


@router.post("/cdc/start")
def start_cdc(spec: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    """
    Starts a continuous background CDC worker thread for Oracle LogMiner.
    Continuously polls Oracle redo logs, applies DML to PostgreSQL, and advances durable SCN checkpoint.
    """
    try:
        service = CDCService()
        if spec is None:
            spec = {}
        pipeline_name = spec.get("pipeline_name", "oracle_to_postgres_cdc")
        poll_interval = float(spec.get("poll_interval_seconds", 2.0))
        target_table = spec.get("target_table", "employees")
        target_schema = spec.get("target_schema", "public")
        table_filter = spec.get("table_filter") or (
            [spec["source_table"]] if spec.get("source_table") else ["EMPLOYEES"]
        )
        transformations = spec.get("transformations", [])

        return service.start_worker(
            pipeline_name=pipeline_name,
            poll_interval_seconds=poll_interval,
            table_filter=table_filter,
            transformations=transformations,
            target_table=target_table,
            target_schema=target_schema,
        )
    except Exception as exc:
        logger.error(f"Error starting CDC worker: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "error": str(exc)},
        )


@router.post("/cdc/stop")
def stop_cdc(spec: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    """
    Gracefully stops the active continuous background CDC worker thread.
    """
    try:
        service = CDCService()
        if spec is None:
            spec = {}
        pipeline_name = spec.get("pipeline_name", "oracle_to_postgres_cdc")
        return service.stop_worker(pipeline_name=pipeline_name)
    except Exception as exc:
        logger.error(f"Error stopping CDC worker: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "error": str(exc)},
        )


@router.post("/cdc/run")
def run_cdc(spec: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    """
    Executes a CDC window synchronously, or starts the continuous background worker if mode='continuous'.
    """
    try:
        service = CDCService()
        if spec is None:
            spec = {}
        mode = str(spec.get("mode", "batch")).lower()
        pipeline_name = spec.get("pipeline_name", "oracle_to_postgres_cdc")
        target_table = spec.get("target_table", "employees")
        target_schema = spec.get("target_schema", "public")
        table_filter = spec.get("table_filter") or (
            [spec["source_table"]] if spec.get("source_table") else None
        )
        transformations = spec.get("transformations", [])

        if mode in ("continuous", "start"):
            poll_interval = float(spec.get("poll_interval_seconds", 2.0))
            return service.start_worker(
                pipeline_name=pipeline_name,
                poll_interval_seconds=poll_interval,
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

