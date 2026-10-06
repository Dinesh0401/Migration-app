from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any
import pandas as pd

from app.config import settings
from app.models.migration import DatabaseEntity, MigrationResponse

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOGS_DIR = BASE_DIR / "logs" / "migrations"
CONFIGS_DIR = BASE_DIR / "generated" / "seatunnel"


class SeaTunnelService:
    """
    Executes and orchestrates SeaTunnel migration jobs between Oracle and PostgreSQL.
    Dynamically generates HOCON job configurations from settings without hardcoding.
    Captures stdout/stderr and records full audit trails for every migration ID.
    """

    # In-memory registry for migration history: migration_id -> migration details
    _migrations: dict[str, dict[str, Any]] = {}

    def __init__(self) -> None:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        CONFIGS_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_migration_record(cls, migration_id: str) -> dict[str, Any] | None:
        """Retrieves a historical migration execution record by migration_id."""
        return cls._migrations.get(migration_id)

    def generate_seatunnel_config(
        self,
        source: DatabaseEntity,
        target: DatabaseEntity,
        query: str | None = None,
    ) -> Path:
        """
        Dynamically generates a SeaTunnel HOCON configuration file.
        Uses connection details from settings without hardcoding credentials in source code.
        """
        src_table = f'"{source.schema_name}"."{source.table}"'
        select_query = query or f"SELECT * FROM {src_table}"

        # Standard JDBC URLs from settings
        oracle_jdbc_url = f"jdbc:oracle:thin:@//{settings.oracle_host}:{settings.oracle_port}/{settings.oracle_service}"
        postgres_jdbc_url = f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_database}"

        config_content = f"""# SeaTunnel Zeta Batch Migration Configuration
# Generated dynamically by Data Engineering Migration Service
# Timestamp: {datetime.now().isoformat()}

env {{
  parallelism = 1
  job.mode = "BATCH"
}}

source {{
  Jdbc {{
    driver = "oracle.jdbc.OracleDriver"
    url = "{oracle_jdbc_url}"
    user = "{settings.oracle_user}"
    password = "{settings.oracle_password}"
    query = "{select_query}"
    result_table_name = "src_table"
  }}
}}

transform {{
}}

sink {{
  Jdbc {{
    source_table_name = "src_table"
    driver = "org.postgresql.Driver"
    url = "{postgres_jdbc_url}"
    user = "{settings.postgres_user}"
    password = "{settings.postgres_password}"
    generate_sink_sql = true
    database = "{settings.postgres_database}"
    table = "{target.schema_name}.{target.table}"
  }}
}}
"""
        config_path = CONFIGS_DIR / f"seatunnel_{source.schema_name}_{source.table}_to_{target.table}.conf"
        config_path.write_text(config_content, encoding="utf-8")
        logger.info(f"Generated dynamic SeaTunnel config at {config_path}")
        return config_path

    def _find_seatunnel_binary(self) -> str | None:
        """Finds SeaTunnel CLI executable from settings, environment, or system PATH."""
        if settings.SEATUNNEL_BIN and Path(settings.SEATUNNEL_BIN).exists():
            return settings.SEATUNNEL_BIN

        if settings.SEATUNNEL_HOME:
            home = Path(settings.SEATUNNEL_HOME)
            for candidate in ["seatunnel.cmd", "seatunnel.bat", "seatunnel.sh", "seatunnel"]:
                p = home / "bin" / candidate
                if p.exists():
                    return str(p)

        # Look in PATH
        for cmd in ["seatunnel.cmd", "seatunnel.bat", "seatunnel.sh", "seatunnel"]:
            which_res = shutil.which(cmd)
            if which_res:
                return which_res

        return None

    def execute_migration(
        self,
        source: DatabaseEntity,
        target: DatabaseEntity,
        seatunnel_config_path: str | None = None,
        migration_id: str | None = None,
    ) -> MigrationResponse:
        """
        Executes SeaTunnel migration for source -> target.
        Captures exit code, stdout, and stderr into dedicated log file.
        """
        mid = migration_id or f"mig_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        log_file = LOGS_DIR / f"{mid}.log"
        start_time = time.time()

        # Step 1: Resolve or generate SeaTunnel configuration
        if seatunnel_config_path and Path(seatunnel_config_path).exists():
            config_path = Path(seatunnel_config_path)
            logger.info(f"[{mid}] Using provided SeaTunnel config: {config_path}")
        else:
            config_path = self.generate_seatunnel_config(source, target)
            logger.info(f"[{mid}] Generated dynamic SeaTunnel config: {config_path}")

        # Record migration start
        record = {
            "migration_id": mid,
            "status": "running",
            "source": source.full_name,
            "target": target.full_name,
            "config_path": str(config_path),
            "log_file": str(log_file),
            "started_at": datetime.now().isoformat(),
        }
        self._migrations[mid] = record

        # Step 2: Check for SeaTunnel binary
        st_bin = self._find_seatunnel_binary()
        log_lines: list[str] = [
            f"=== SEATUNNEL MIGRATION AUDIT LOG ===",
            f"Migration ID : {mid}",
            f"Timestamp    : {datetime.now().isoformat()}",
            f"Source       : {source.full_name}",
            f"Target       : {target.full_name}",
            f"Config File  : {config_path}",
            f"Executable   : {st_bin or 'Native Execution Engine (Data Plane fallback)'}",
            "--------------------------------------------------",
        ]

        if st_bin:
            # Live SeaTunnel Subprocess Execution
            logger.info(f"[{mid}] Executing SeaTunnel binary: {st_bin} --config {config_path}")
            cmd = [st_bin, "--config", str(config_path)]
            try:
                proc = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=600,
                )
                log_lines.append("[STDOUT]:")
                log_lines.append(proc.stdout)
                log_lines.append("[STDERR]:")
                log_lines.append(proc.stderr)
                log_lines.append(f"Exit Code: {proc.returncode}")

                exit_code = proc.returncode
                duration = round(time.time() - start_time, 2)

                if exit_code == 0:
                    status = "completed"
                    error_msg = None
                else:
                    status = "failed"
                    error_msg = f"SeaTunnel process exited with code {exit_code}: {proc.stderr[:300]}"
            except Exception as exc:
                exit_code = 1
                status = "failed"
                duration = round(time.time() - start_time, 2)
                error_msg = f"Failed to execute SeaTunnel process: {str(exc)}"
                log_lines.append(f"[EXCEPTION]: {error_msg}")
        else:
            # Fallback to direct Python-based Data Plane execution when SeaTunnel binary is absent
            logger.info(f"[{mid}] SeaTunnel binary not found on PATH; utilizing Data Plane migration runner.")
            log_lines.append("SeaTunnel CLI binary not present in environment.")
            log_lines.append("Invoking native Data Plane execution engine to process migration...")

            try:
                from app.services.oracle_service import OracleService
                from app.services.postgres_service import PostgresService
                from app.utils.database import postgres_connection
                from sqlalchemy import text

                oracle_svc = OracleService()
                postgres_svc = PostgresService()

                # Extract from Oracle
                select_sql = f'SELECT * FROM "{source.schema_name.upper()}"."{source.table.upper()}"'
                df = oracle_svc.extract_dataframe(select_sql)
                rows_count = len(df)
                log_lines.append(f"Extracted {rows_count} rows from Oracle {source.schema_name}.{source.table}")

                # Ensure target table exists or create target table structure if needed
                cols_meta = oracle_svc.get_columns_metadata(source.schema_name, source.table)
                pk_cols = [c["name"].lower() for c in cols_meta if c.get("primary_key")]

                # Map to basic PostgreSQL types
                type_map = {
                    "NUMBER": "NUMERIC",
                    "VARCHAR2": "VARCHAR",
                    "CHAR": "CHAR",
                    "DATE": "TIMESTAMP",
                    "TIMESTAMP": "TIMESTAMP",
                    "CLOB": "TEXT",
                    "BLOB": "BYTEA",
                }

                col_defs = []
                for c in cols_meta:
                    t = type_map.get(c["type"].upper().split("(")[0], "VARCHAR")
                    if t == "VARCHAR" and c.get("length"):
                        t = f"VARCHAR({c['length']})"
                    col_defs.append(f'"{c["name"].lower()}" {t}')

                pk_clause = f", PRIMARY KEY ({', '.join(pk_cols)})" if pk_cols else ""
                create_sql = f'CREATE TABLE IF NOT EXISTS "{target.schema_name.lower()}"."{target.table.lower()}" ({", ".join(col_defs)}{pk_clause})'
                postgres_svc.execute_ddl([create_sql])

                # Insert into PostgreSQL using parameterized batch insert
                if not df.empty:
                    cols_target = [f'"{c.lower()}"' for c in df.columns]
                    val_placeholders = [f":{c.lower()}" for c in df.columns]
                    insert_sql = text(
                        f'INSERT INTO "{target.schema_name.lower()}"."{target.table.lower()}" ({", ".join(cols_target)}) '
                        f'VALUES ({", ".join(val_placeholders)}) '
                        f'ON CONFLICT DO NOTHING'
                    )
                    records = [{col.lower(): (None if pd.isna(val) else val) for col, val in row.items()} for row in df.to_dict(orient="records")]

                    with postgres_connection() as conn:
                        conn.execute(insert_sql, records)
                        conn.commit()

                log_lines.append(f"Loaded {rows_count} rows into PostgreSQL {target.schema_name}.{target.table}")
                status = "completed"
                exit_code = 0
                error_msg = None
                duration = round(time.time() - start_time, 2)
            except Exception as exc:
                exit_code = 1
                status = "failed"
                duration = round(time.time() - start_time, 2)
                error_msg = f"Data Plane migration failed: {str(exc)}"
                log_lines.append(f"[ERROR]: {error_msg}")

        # Write log file
        log_file.write_text("\n".join(log_lines), encoding="utf-8")

        response = MigrationResponse(
            migration_id=mid,
            status=status,
            source=source.full_name,
            target=target.full_name,
            error=error_msg,
            exit_code=exit_code,
            duration_seconds=duration,
            log_file=str(log_file),
            details={
                "config_path": str(config_path),
                "binary": st_bin or "native",
            },
        )

        record.update({
            "status": status,
            "error": error_msg,
            "exit_code": exit_code,
            "duration_seconds": duration,
            "completed_at": datetime.now().isoformat(),
        })

        logger.info(f"[{mid}] Migration finished with status '{status}' in {duration}s")
        return response
