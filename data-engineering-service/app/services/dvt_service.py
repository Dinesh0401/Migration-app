from __future__ import annotations

import json
import logging
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from app.config import settings
from app.models.migration import DatabaseEntity

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DVT_DIR = BASE_DIR / "generated" / "dvt"


class DVTService:
    """
    Google Cloud Data Validation Tool (DVT) Configuration & Execution Service.
    Converts migration requests into an internal normalized JSON intermediate representation,
    and then converts it dynamically into native DVT YAML configs and CLI commands.
    """

    def __init__(self) -> None:
        DVT_DIR.mkdir(parents=True, exist_ok=True)

    def generate_internal_representation(
        self,
        source: DatabaseEntity,
        target: DatabaseEntity,
        validations: dict[str, bool] | None = None,
    ) -> dict[str, Any]:
        """
        Generates the internal normalized JSON intermediate representation.
        Acts as the decoupled abstraction layer between the migration request and DVT CLI.
        """
        v_flags = validations or {
            "row_count": True,
            "schema": True,
            "data": True,
        }

        return {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "service": "data-engineering-service",
            },
            "source": {
                "database": source.type.lower(),
                "schema": source.schema_name,
                "table": source.table,
            },
            "target": {
                "database": target.type.lower(),
                "schema": target.schema_name,
                "table": target.table,
            },
            "validation": v_flags,
        }

    def generate_dvt_cli_commands(self, internal_rep: dict[str, Any], dry_run: bool = True) -> list[str]:
        """
        Translates the internal JSON representation into concrete Data Validation Tool CLI commands.
        """
        src = internal_rep["source"]
        tgt = internal_rep["target"]
        flags = internal_rep.get("validation", {})

        src_tbl = f"{src['schema']}.{src['table']}"
        tgt_tbl = f"{tgt['schema']}.{tgt['table']}"
        tbl_mapping = f"{src_tbl}={tgt_tbl}"

        dry_run_flag = " --dry-run" if dry_run else ""
        commands: list[str] = []

        if flags.get("schema", True):
            commands.append(
                f"data-validation validate schema -sc oracle_source -tc postgresql_target -tbls {tbl_mapping}{dry_run_flag}"
            )

        if flags.get("row_count", True):
            commands.append(
                f"data-validation validate row -sc oracle_source -tc postgresql_target -tbls {tbl_mapping}{dry_run_flag}"
            )

        if flags.get("data", False):
            commands.append(
                f"data-validation validate column -sc oracle_source -tc postgresql_target -tbls {tbl_mapping}{dry_run_flag}"
            )

        return commands

    def generate_dvt_yaml_configs(self, internal_rep: dict[str, Any]) -> dict[str, Path]:
        """
        Generates native DVT YAML configurations for `data-validation configs run`.
        """
        src = internal_rep["source"]
        tgt = internal_rep["target"]
        tbl_key = f"{src['schema']}_{src['table']}"

        schema_cfg = {
            "schema_validation": {
                "source_conn_name": "oracle_source",
                "target_conn_name": "postgresql_target",
                "schema_name": src["schema"],
                "table_name": src["table"],
                "target_schema_name": tgt["schema"],
                "target_table_name": tgt["table"],
            }
        }
        schema_path = DVT_DIR / f"schema_{tbl_key}.json"
        schema_path.write_text(json.dumps(schema_cfg, indent=2), encoding="utf-8")

        row_cfg = {
            "data_validation": {
                "source_conn_name": "oracle_source",
                "target_conn_name": "postgresql_target",
                "schema_name": src["schema"],
                "table_name": src["table"],
                "target_schema_name": tgt["schema"],
                "target_table_name": tgt["table"],
                "validation_type": "Row",
            }
        }
        row_path = DVT_DIR / f"row_{tbl_key}.json"
        row_path.write_text(json.dumps(row_cfg, indent=2), encoding="utf-8")

        return {
            "schema_config": schema_path,
            "row_config": row_path,
        }

    def execute_dvt(
        self,
        source: DatabaseEntity,
        target: DatabaseEntity,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """
        Executes Google Cloud DVT or returns dry-run validation execution plans.
        """
        internal_json = self.generate_internal_representation(source, target)
        cli_cmds = self.generate_dvt_cli_commands(internal_json, dry_run=dry_run)
        yaml_paths = self.generate_dvt_yaml_configs(internal_json)

        dvt_bin = shutil.which("data-validation")
        results: list[dict[str, Any]] = []

        if dvt_bin:
            logger.info(f"Executing DVT CLI ({dvt_bin}) for {source.relation_name} -> {target.relation_name}")
            for cmd in cli_cmds:
                try:
                    proc = subprocess.run(
                        cmd,
                        shell=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        timeout=120,
                    )
                    results.append({
                        "command": cmd,
                        "exit_code": proc.returncode,
                        "status": "passed" if proc.returncode == 0 else "failed",
                        "output": proc.stdout[:500],
                        "error": proc.stderr[:500] if proc.returncode != 0 else None,
                    })
                except Exception as exc:
                    results.append({
                        "command": cmd,
                        "status": "failed",
                        "error": str(exc),
                    })
        else:
            logger.info("data-validation CLI not installed in current environment; generating dry-run plan.")
            for cmd in cli_cmds:
                results.append({
                    "command": cmd,
                    "mode": "dry-run",
                    "status": "ready",
                    "note": "Ready for execution with data-validation CLI tool",
                })

        return {
            "internal_representation": internal_json,
            "cli_commands": cli_cmds,
            "config_files": {k: str(v) for k, v in yaml_paths.items()},
            "results": results,
            "dvt_installed": bool(dvt_bin),
        }
