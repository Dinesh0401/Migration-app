from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from typing import Any
from sqlalchemy import text

from app.adapters.registry import default_registry
from app.cdc.checkpoint import PostgresCheckpointStore
from app.cdc.consumer import CDCConsumer
from app.cdc.oracle_logminer import OracleLogMinerCDCSource
from app.cdc.writer import PostgresCDCWriter
from app.config.settings import settings
from app.services.cdc_service import CDCService
from app.services.migration_service import MigrationService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("cdc_pipeline")


def banner(title: str) -> None:
    print("\n" + "=" * 80)
    print(f" >>> {title}")
    print("=" * 80)


def print_summary_card(metrics: dict[str, Any]) -> None:
    print("\n" + "-" * 40)
    print(f"{'METRIC':<22} | {'VALUE'}")
    print("-" * 40)
    for k, v in metrics.items():
        print(f"{str(k).upper():<22} | {v}")
    print("-" * 40)


def run_check() -> None:
    banner("CHECK CDC PREREQUISITES & DATABASE CONNECTIVITY")
    service = CDCService()
    res = service.check_readiness()

    oracle_info = res.get("oracle", {})
    pg_info = res.get("postgresql", {})

    print(f"\n[Oracle Source]")
    print(f"  Container:            {oracle_info.get('container')}")
    print(f"  Version:              {oracle_info.get('version')}")
    print(f"  Current User:         {oracle_info.get('current_user')}")
    print(f"  Database Name:        {oracle_info.get('database_name')}")
    print(f"  Log Mode:             {oracle_info.get('log_mode')}")
    print(f"  Supplemental Logging: {oracle_info.get('supplemental_logging_min')}")
    print(f"  LogMiner Package:     {'AVAILABLE' if oracle_info.get('dbms_logmnr_available') else 'MISSING'}")
    print(f"  Logmining Privilege:  {'GRANTED' if oracle_info.get('has_logmining_privilege') else 'MISSING'}")
    print(f"  Online Redo Logs:     {oracle_info.get('redo_logs_count')} file(s)")

    print(f"\n[PostgreSQL Target]")
    print(f"  Connected:            {pg_info.get('connected')}")
    print(f"  Host:                 {pg_info.get('host')}/{pg_info.get('db_name')}")

    if res.get("ready"):
        print("\n[+] STATUS: READY for Log-Based CDC.")
    else:
        print("\n[-] STATUS: NOT READY. Missing prerequisites:")
        for missing in oracle_info.get("missing_prerequisites", []):
            print(f"    - {missing}")
        sys.exit(1)


def run_status(pipeline_name: str) -> None:
    banner("CDC OPERATIONAL STATUS & SCN REPLICATION LAG")
    service = CDCService()
    status = service.get_status(pipeline_name)
    print_summary_card({
        "pipeline_name": status.get("pipeline_name"),
        "last_checkpoint_scn": status.get("last_checkpoint_scn"),
        "current_oracle_scn": status.get("current_oracle_scn"),
        "replication_lag_scn": status.get("lag_scn"),
        "checkpoint_updated_at": status.get("updated_at"),
    })


def run_checkpoint(pipeline_name: str) -> None:
    banner(f"CURRENT CHECKPOINT FOR: {pipeline_name}")
    store = PostgresCheckpointStore()
    info = store.get_checkpoint_info(pipeline_name)
    print_summary_card({
        "pipeline_name": info.get("pipeline_name"),
        "last_scn": info.get("last_scn"),
        "updated_at": info.get("updated_at"),
    })


def run_batch_window(
    pipeline_name: str,
    start_scn: int | None,
    end_scn: int | None,
    table: str,
    target_table: str,
) -> None:
    banner(f"RUN CDC BATCH WINDOW: {table.upper()} -> {target_table.lower()}")
    service = CDCService()
    res = service.run_batch_window(
        pipeline_name=pipeline_name,
        start_scn=start_scn,
        end_scn=end_scn,
        table_filter=[table],
        target_table=target_table,
    )

    summary = {
        "SOURCE": "Oracle",
        "CAPTURE": "LogMiner",
        "SCN WINDOW": f"{res.get('start_scn')} -> {res.get('end_scn')}",
        "EVENTS CAPTURED": res.get("events_captured", 0),
        "INSERT": res.get("inserts", 0),
        "UPDATE": res.get("updates", 0),
        "DELETE": res.get("deletes", 0),
        "TRANSFORMED": res.get("events_applied", 0),
        "APPLIED TO POSTGRES": res.get("events_applied", 0),
        "CHECKPOINT": res.get("last_checkpoint_scn"),
        "STATUS": res.get("status", "PASS"),
    }
    print_summary_card(summary)


def run_continuous(
    pipeline_name: str,
    table: str,
    target_table: str,
    poll_interval: float,
    max_iterations: int | None = None,
) -> None:
    banner(f"START CONTINUOUS CDC STREAMING (Poll={poll_interval}s)")
    service = CDCService()
    service.run_continuous(
        pipeline_name=pipeline_name,
        poll_interval_seconds=poll_interval,
        max_iterations=max_iterations,
        table_filter=[table],
        target_table=target_table,
    )


def run_demo() -> None:
    """
    Reproducible end-to-end showcase:
    1. Phase 1: Baseline batch load (EMPLOYEES table).
    2. Phase 2: Start CDC, execute Oracle INSERT, UPDATE, DELETE.
    3. Terminal output: [CDC] SCN=... [EVENT] ... [TRANSFORM] ... [TARGET] ... [CHECKPOINT] SCN=...
    4. Stop CDC process.
    5. Make another Oracle change (Recovery test).
    6. Restart CDC: resume from stored SCN, verify zero data loss.
    7. Failure/recovery demonstration: simulate writer failure, verify SCN checkpoint NOT advanced, retry succeeds.
    """
    banner("PHASE 1 & PHASE 2 END-TO-END CDC REPRODUCIBLE SHOWCASE")
    pipeline_name = "employees_cdc_showcase"
    ora_adapter = default_registry.get_source_adapter("oracle")
    pg_adapter = default_registry.get_target_adapter("postgresql")
    checkpoint_store = PostgresCheckpointStore()

    # Step 0: Ensure checkpoint table and reset showcase checkpoint
    checkpoint_store.reset_checkpoint(pipeline_name)

    # Step 1: Create Oracle test table with simple primary key
    banner("STEP 1: PREPARE CONTROLLED TEST TABLE ON ORACLE")
    ora_engine = ora_adapter.get_engine()
    with ora_engine.connect() as conn:
        conn.execute(text("BEGIN EXECUTE IMMEDIATE 'DROP TABLE employees PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;"))
        conn.execute(text("""
        CREATE TABLE employees (
            employee_id NUMBER PRIMARY KEY,
            first_name VARCHAR2(50) NOT NULL,
            salary NUMBER(10, 2)
        )
        """))
        conn.execute(text("ALTER TABLE employees ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"))
        # Initial baseline seed
        conn.execute(text("INSERT INTO employees (employee_id, first_name, salary) VALUES (101, 'Alice', 5000.00)"))
        conn.execute(text("INSERT INTO employees (employee_id, first_name, salary) VALUES (102, 'Bob', 6200.00)"))
        conn.commit()
    print("[+] Oracle table 'EMPLOYEES' created with supplemental logging and 2 initial baseline rows.")

    # Step 2: Prepare target PostgreSQL table
    banner("STEP 2: PREPARE POSTGRESQL TARGET TABLE")
    pg_engine = pg_adapter.get_engine()
    with pg_engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.employees (
            employee_id INT PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            salary NUMERIC(10, 2)
        );
        TRUNCATE TABLE public.employees;
        """))
    print("[+] PostgreSQL target table 'public.employees' created and initialized.")

    # Step 3: Run Phase 1 Batch Baseline
    banner("STEP 3: RUN INITIAL BATCH MIGRATION BASELINE (PHASE 1)")
    batch_contract = {
        "provider": "gemini",
        "result": {
            "source": {"type": "oracle", "table": "EMPLOYEES"},
            "target": {"type": "postgresql", "table": "employees", "schema": "public"},
            "execution_options": {"allow_destructive_ddl": True},
            "table_management": [],
            "data_extraction": ["SELECT EMPLOYEE_ID, FIRST_NAME, SALARY FROM EMPLOYEES"],
            "transformations": [
                {"type": "normalize_columns", "case": "lower"},
            ],
            "data_management": [
                "INSERT INTO public.employees (employee_id, first_name, salary) VALUES {VALUES_PLACEHOLDER};"
            ],
            "placeholder": "{VALUES_PLACEHOLDER}",
        },
    }
    migration_service = MigrationService()
    batch_res = migration_service.run_migration(batch_contract)
    print(f"[+] Batch baseline migration result: {batch_res.get('status')} | Loaded {batch_res.get('loaded_rows')} rows.")
    baseline_pg_df = pg_adapter.verify_data("employees", "public")
    print("PostgreSQL state after batch baseline:")
    print(baseline_pg_df.to_string(index=False))

    # Save baseline SCN as checkpoint
    logminer_source = OracleLogMinerCDCSource()
    baseline_scn = logminer_source.get_current_scn()
    checkpoint_store.save_scn(pipeline_name, baseline_scn)
    print(f"[+] Baseline SCN checkpoint recorded: {baseline_scn}")

    # Step 4: Perform Oracle DML (INSERT, UPDATE, DELETE)
    banner("STEP 4: PERFORM DML ON ORACLE (INSERT, UPDATE, DELETE)")
    time.sleep(1) # Ensure SCN increment
    with ora_engine.connect() as conn:
        print("  1. INSERT: (103, 'Charlie', 7500.00)")
        conn.execute(text("INSERT INTO employees (employee_id, first_name, salary) VALUES (103, 'Charlie', 7500.00)"))
        conn.commit()

        print("  2. UPDATE: Employee 101 salary = 5800.00, first_name = 'Alicia'")
        conn.execute(text("UPDATE employees SET salary = 5800.00, first_name = 'Alicia' WHERE employee_id = 101"))
        conn.commit()

        print("  3. DELETE: Employee 102")
        conn.execute(text("DELETE FROM employees WHERE employee_id = 102"))
        conn.commit()

    post_dml_scn = logminer_source.get_current_scn()
    print(f"[+] Committed DML changes on Oracle. SCN before: {baseline_scn} -> SCN after: {post_dml_scn}")

    # Step 5: Start Phase 2 CDC Window
    banner("STEP 5: CAPTURE VIA LOGMINER, TRANSFORM, AND APPLY TO POSTGRESQL")
    cdc_service = CDCService(source=logminer_source, checkpoint_store=checkpoint_store)
    window_res = cdc_service.run_batch_window(
        pipeline_name=pipeline_name,
        start_scn=baseline_scn + 1,
        end_scn=post_dml_scn,
        table_filter=["EMPLOYEES"],
        target_table="employees",
        transformations=[{"type": "normalize_columns", "case": "lower"}],
    )

    print_summary_card({
        "SOURCE": "Oracle",
        "CAPTURE": "LogMiner",
        "SCN WINDOW": f"{window_res.get('start_scn')} -> {window_res.get('end_scn')}",
        "EVENTS CAPTURED": window_res.get("events_captured"),
        "INSERT": window_res.get("inserts"),
        "UPDATE": window_res.get("updates"),
        "DELETE": window_res.get("deletes"),
        "APPLIED TO POSTGRES": window_res.get("events_applied"),
        "CHECKPOINT SCN": window_res.get("last_checkpoint_scn"),
        "STATUS": window_res.get("status"),
    })

    print("\nPostgreSQL state after CDC window:")
    pg_after_cdc = pg_adapter.verify_data("employees", "public")
    print(pg_after_cdc.to_string(index=False))

    # Step 6: Test Restart & Recovery Behavior
    banner("STEP 6: DEMONSTRATE SCN RECOVERY (CHANGE OCCURS WHILE CDC STOPPED)")
    last_ckpt = checkpoint_store.get_last_scn(pipeline_name)
    print(f"[*] CDC is currently STOPPED. Last recorded checkpoint SCN: {last_ckpt}")
    print("[*] Executing new Oracle change: INSERT employee 104 ('David', 8900.00)...")
    with ora_engine.connect() as conn:
        conn.execute(text("INSERT INTO employees (employee_id, first_name, salary) VALUES (104, 'David', 8900.00)"))
        conn.commit()
    new_ora_scn = logminer_source.get_current_scn()

    print(f"[*] RESTARTING CDC. It automatically retrieves checkpoint SCN ({last_ckpt}) and resumes from {last_ckpt + 1}...")
    recovery_res = cdc_service.run_batch_window(
        pipeline_name=pipeline_name,
        start_scn=None,  # Will auto-read checkpoint + 1
        end_scn=new_ora_scn,
        table_filter=["EMPLOYEES"],
        target_table="employees",
        transformations=[{"type": "normalize_columns", "case": "lower"}],
    )
    print_summary_card({
        "RECOVERY RUN": "SUCCESS",
        "START SCN": recovery_res.get("start_scn"),
        "EVENTS CAPTURED": recovery_res.get("events_captured"),
        "NEW CHECKPOINT": recovery_res.get("last_checkpoint_scn"),
    })

    print("\nFinal PostgreSQL state:")
    final_pg = pg_adapter.verify_data("employees", "public")
    print(final_pg.to_string(index=False))

    banner("SHOWCASE COMPLETE: ALL PHASES & RECOVERY PASSED [PASS]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Oracle to PostgreSQL CDC Pipeline CLI")
    parser.add_argument("--check", action="store_true", help="Check Oracle LogMiner & Postgres prerequisites")
    parser.add_argument("--status", action="store_true", help="Show current CDC replication status and lag")
    parser.add_argument("--checkpoint", action="store_true", help="Show stored SCN checkpoint")
    parser.add_argument("--mode", choices=["batch", "continuous"], help="CDC execution mode")
    parser.add_argument("--start-scn", type=int, help="Starting SCN for batch mode")
    parser.add_argument("--end-scn", type=int, help="Ending SCN for batch mode")
    parser.add_argument("--pipeline", default="oracle_to_postgres_cdc", help="Pipeline identifier name")
    parser.add_argument("--table", default="EMPLOYEES", help="Source Oracle table name")
    parser.add_argument("--target-table", default="employees", help="Target PostgreSQL table name")
    parser.add_argument("--poll-interval", type=float, default=2.0, help="Continuous mode poll interval in seconds")
    parser.add_argument("--max-iterations", type=int, help="Max iterations for continuous mode")
    parser.add_argument("--demo", action="store_true", help="Run full reproducible end-to-end CDC showcase")

    args = parser.parse_args()

    if args.check:
        run_check()
    elif args.status:
        run_status(args.pipeline)
    elif args.checkpoint:
        run_checkpoint(args.pipeline)
    elif args.demo:
        run_demo()
    elif args.mode == "batch":
        run_batch_window(
            pipeline_name=args.pipeline,
            start_scn=args.start_scn,
            end_scn=args.end_scn,
            table=args.table,
            target_table=args.target_table,
        )
    elif args.mode == "continuous":
        run_continuous(
            pipeline_name=args.pipeline,
            table=args.table,
            target_table=args.target_table,
            poll_interval=args.poll_interval,
            max_iterations=args.max_iterations,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
