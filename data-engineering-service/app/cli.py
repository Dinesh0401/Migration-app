#!/usr/bin/env python3
"""
app.cli — Command-Line Interface for Data Migration & DVT Validation Service.

Provides CLI testing and automation using the exact same service layer as FastAPI:
  - python -m app.cli health
  - python -m app.cli migrate --source HR.EMPLOYEES --target migration.employees
  - python -m app.cli validate --source HR.EMPLOYEES --target migration.employees
  - python -m app.cli migrate-and-validate --source HR.EMPLOYEES --target migration.employees
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Tuple

from app.models.migration import DatabaseEntity
from app.services.oracle_service import OracleService
from app.services.postgres_service import PostgresService
from app.services.seatunnel_service import SeaTunnelService
from app.services.validation_service import ValidationService
from app.services.dvt_service import DVTService


def parse_relation(rel_str: str, default_type: str) -> DatabaseEntity:
    """Parses 'SCHEMA.TABLE' or 'table' into DatabaseEntity."""
    cleaned = rel_str.replace('"', '').replace("'", "").strip()
    parts = cleaned.split('.')
    if len(parts) >= 2:
        return DatabaseEntity(type=default_type, schema=parts[0], table=parts[1])
    return DatabaseEntity(type=default_type, schema="public", table=parts[0])


def cmd_health() -> int:
    """Tests live Oracle and PostgreSQL connectivity."""
    print("=" * 60)
    print("DATA ENGINEERING SERVICE - DATABASE HEALTH CHECK")
    print("=" * 60)
    ora = OracleService().test_connection()
    pg = PostgresService().test_connection()

    print(f"Oracle     : Status = {ora.get('status').upper()} (Host: {ora.get('host', 'N/A')}, Service: {ora.get('service', 'N/A')})")
    if ora.get("error"):
        print(f"             Error  : {ora.get('error')}")

    print(f"PostgreSQL : Status = {pg.get('status').upper()} (Host: {pg.get('host', 'N/A')}, DB: {pg.get('database_name', 'N/A')})")
    if pg.get("error"):
        print(f"             Error  : {pg.get('error')}")

    print("=" * 60)
    return 0 if (ora.get("status") == "connected" and pg.get("status") == "connected") else 1


def cmd_migrate(source_str: str, target_str: str, config_path: str | None = None) -> int:
    """Executes SeaTunnel migration."""
    src = parse_relation(source_str, "oracle")
    tgt = parse_relation(target_str, "postgresql")

    print(f"[+] Starting SeaTunnel Migration: {src.full_name} -> {tgt.full_name}")
    st_svc = SeaTunnelService()
    res = st_svc.execute_migration(source=src, target=tgt, seatunnel_config_path=config_path)

    print(f"Migration ID : {res.migration_id}")
    print(f"Status       : {res.status.upper()}")
    print(f"Duration     : {res.duration_seconds}s")
    if res.log_file:
        print(f"Log File     : {res.log_file}")
    if res.error:
        print(f"Error        : {res.error}")

    return 0 if res.status == "completed" else 1


def cmd_validate(source_str: str, target_str: str) -> int:
    """Executes layered data validation and DVT generation."""
    src = parse_relation(source_str, "oracle")
    tgt = parse_relation(target_str, "postgresql")

    print(f"[+] Starting Layered Validation & DVT: {src.full_name} vs {tgt.full_name}")
    val_svc = ValidationService()
    val_res = val_svc.run_validation(source=src, target=tgt)

    dvt_svc = DVTService()
    dvt_res = dvt_svc.execute_dvt(source=src, target=tgt, dry_run=True)

    print(f"Validation ID : {val_res.validation_id}")
    print(f"Overall Status: {val_res.status.upper()}")
    print("-" * 60)
    print("Checks Summary:")
    for chk_name, chk_val in val_res.checks.items():
        st = chk_val.get("status", "unknown") if isinstance(chk_val, dict) else "done"
        print(f"  - {chk_name:<16}: {str(st).upper()}")

    print("-" * 60)
    print("DVT CLI Commands (Dry-Run):")
    for cmd in dvt_res["cli_commands"]:
        print(f"  $ {cmd}")

    return 0 if val_res.status == "passed" else 1


def cmd_migrate_and_validate(source_str: str, target_str: str) -> int:
    """Executes complete pipeline: health -> SeaTunnel migration -> validation."""
    print("==================================================")
    print("MIGRATION & VALIDATION PIPELINE EXECUTION")
    print("==================================================")
    src = parse_relation(source_str, "oracle")
    tgt = parse_relation(target_str, "postgresql")

    # Step 1: Health check
    ora = OracleService().test_connection()
    pg = PostgresService().test_connection()
    if ora.get("status") != "connected" or pg.get("status") != "connected":
        print(f"[-] Pre-check failed: Oracle ({ora.get('status')}), Postgres ({pg.get('status')})")
        return 1

    # Step 2: Migrate
    mig_code = cmd_migrate(source_str, target_str)
    if mig_code != 0:
        print("[-] Migration failed. Aborting validation.")
        return mig_code

    # Step 3: Validate
    print("\n[+] Migration completed. Triggering validation...")
    val_code = cmd_validate(source_str, target_str)
    return val_code


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m app.cli",
        description="Data Engineering Migration and DVT Validation CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Health
    subparsers.add_parser("health", help="Check database connectivity")

    # Migrate
    mig_p = subparsers.add_parser("migrate", help="Execute SeaTunnel migration")
    mig_p.add_argument("--source", "-s", required=True, help="Oracle source table (e.g. HR.EMPLOYEES)")
    mig_p.add_argument("--target", "-t", required=True, help="PostgreSQL target table (e.g. migration.employees)")
    mig_p.add_argument("--config", "-c", required=False, help="Optional SeaTunnel config path")

    # Validate
    val_p = subparsers.add_parser("validate", help="Execute Data Validation")
    val_p.add_argument("--source", "-s", required=True, help="Oracle source table (e.g. HR.EMPLOYEES)")
    val_p.add_argument("--target", "-t", required=True, help="PostgreSQL target table (e.g. migration.employees)")

    # Migrate and Validate
    mav_p = subparsers.add_parser("migrate-and-validate", help="Execute full Migration and Validation pipeline")
    mav_p.add_argument("--source", "-s", required=True, help="Oracle source table (e.g. HR.EMPLOYEES)")
    mav_p.add_argument("--target", "-t", required=True, help="PostgreSQL target table (e.g. migration.employees)")

    args = parser.parse_args()

    if args.command == "health":
        sys.exit(cmd_health())
    elif args.command == "migrate":
        sys.exit(cmd_migrate(args.source, args.target, getattr(args, "config", None)))
    elif args.command == "validate":
        sys.exit(cmd_validate(args.source, args.target))
    elif args.command == "migrate-and-validate":
        sys.exit(cmd_migrate_and_validate(args.source, args.target))


if __name__ == "__main__":
    main()
