"""
Standalone Read-Only Diagnostic Tool for Oracle LogMiner CDC.

Reuses existing LogMiner configuration, connection, and redo-log discovery logic
from app.config.settings without modifying any production pipeline code,
worker state, PostgreSQL schema, or Oracle database configuration.
"""
from __future__ import annotations

import argparse
import sys
from typing import Any
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

# Reuses existing application configuration
from app.config.settings import settings


def run_diagnostic(
    start_scn: int | None = None,
    end_scn: int | None = None,
    table_name: str | None = "EMPLOYEES",
    show_all: bool = False,
) -> None:
    """
    Executes a read-only LogMiner mining session and inspects V$LOGMNR_CONTENTS.
    """
    cdb_url = settings.get_oracle_cdb_url()
    engine = create_engine(cdb_url, poolclass=NullPool)

    print("=" * 75)
    print(" [LOGMINER READ-ONLY DIAGNOSTIC]")
    print("=" * 75)
    print(f"Connection URL (masked): oracle+oracledb://{settings.ORACLE_USER}:***@{settings.ORACLE_HOST}:{settings.ORACLE_PORT}/?service_name={settings.ORACLE_CDB_SERVICE}")

    with engine.connect() as conn:
        # 1. Print current Oracle SCN
        cur_scn = conn.execute(text("SELECT CURRENT_SCN FROM V$DATABASE")).scalar()
        print(f"Current Oracle SCN: {cur_scn}")

        # 2. Inspect and print redo log sequences and files from V$LOG / V$LOGFILE
        log_rows = conn.execute(text("""
            SELECT l.GROUP#, l.SEQUENCE#, l.STATUS, l.FIRST_CHANGE#, l.NEXT_CHANGE#, lf.MEMBER
            FROM V$LOG l
            JOIN V$LOGFILE lf ON l.GROUP# = lf.GROUP#
            WHERE lf.TYPE = 'ONLINE'
            ORDER BY l.GROUP#
        """)).mappings().all()

        print("\nOnline Redo Logs in V$LOG / V$LOGFILE:")
        current_first_change = None
        for lr in log_rows:
            group_no = lr.get("GROUP#") or lr.get("group#")
            seq_no = lr.get("SEQUENCE#") or lr.get("sequence#")
            status = lr.get("STATUS") or lr.get("status")
            f_change = lr.get("FIRST_CHANGE#") or lr.get("first_change#")
            n_change = lr.get("NEXT_CHANGE#") or lr.get("next_change#")
            member = lr.get("MEMBER") or lr.get("member")

            is_curr = " [CURRENT]" if status == "CURRENT" else ""
            print(f"  Group {group_no} | Seq {seq_no} | Status: {status}{is_curr} | SCN: [{f_change} -> {n_change}]")
            print(f"    File: {member}")
            if status == "CURRENT" and f_change is not None:
                current_first_change = int(f_change)

        # Determine requested SCN range
        if end_scn is None:
            end_scn = int(cur_scn)
        if start_scn is None:
            # Default to the first SCN of the current online redo log
            start_scn = current_first_change if current_first_change is not None else int(cur_scn) - 10000

        print(f"\nRequested SCN Range: [{start_scn} -> {end_scn}]")
        print(f"Filter Table: {table_name if not show_all else 'ALL TABLES'}")
        print("-" * 75)

        # 3. Discover online redo log files (same logic as OracleLogMinerCDCSource)
        log_files = conn.execute(text("SELECT MEMBER FROM V$LOGFILE WHERE TYPE = 'ONLINE'")).scalars().all()
        if not log_files:
            print("ERROR: No ONLINE redo logs found in V$LOGFILE.")
            return

        # 4. Clean up any stale session
        try:
            conn.execute(text("BEGIN DBMS_LOGMNR.END_LOGMNR; EXCEPTION WHEN OTHERS THEN NULL; END;"))
        except Exception:
            pass

        # 5. Add online redo log files
        first = True
        for lf in log_files:
            opt = "DBMS_LOGMNR.NEW" if first else "DBMS_LOGMNR.ADDFILE"
            first = False
            conn.execute(text(f"BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '{lf}', OPTIONS => {opt}); END;"))

        # 6. Start LogMiner
        start_sql = f"""
        BEGIN
            DBMS_LOGMNR.START_LOGMNR(
                STARTSCN => {start_scn},
                ENDSCN   => {end_scn},
                OPTIONS  => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY
            );
        END;
        """
        conn.execute(text(start_sql))
        print("LogMiner session started successfully.")

        try:
            # 7. Check columns present in V$LOGMNR_CONTENTS
            cols = set(conn.execute(text("SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = 'V_$LOGMNR_CONTENTS'")).scalars().all())
            extra_cols = []
            if "SRC_CON_NAME" in cols:
                extra_cols.append("SRC_CON_NAME")
            if "SRC_CON_ID" in cols:
                extra_cols.append("SRC_CON_ID")
            if "START_SCN" in cols:
                extra_cols.append("START_SCN")
            if "COMMIT_SCN" in cols:
                extra_cols.append("COMMIT_SCN")

            extra_cols_str = (", " + ", ".join(extra_cols)) if extra_cols else ""

            # Base query
            query = f"""
            SELECT SCN, OPERATION, SEG_OWNER, TABLE_NAME, SQL_REDO{extra_cols_str}
            FROM V$LOGMNR_CONTENTS
            WHERE OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
            ORDER BY SCN, RS_ID, SSN
            """

            all_rows = conn.execute(text(query)).mappings().all()
            print(f"Total raw DML rows in range [{start_scn} -> {end_scn}]: {len(all_rows)}")

            # Separate target table matches vs others
            target_matches = []
            other_rows = []

            target_tbl_clean = (table_name or "EMPLOYEES").upper().strip()
            for r in all_rows:
                tbl = str(r.get("TABLE_NAME") or r.get("table_name") or "").upper()
                if tbl == target_tbl_clean:
                    target_matches.append(r)
                else:
                    other_rows.append(r)

            print(f"\nTarget Table ({target_tbl_clean}) Rows Found: {len(target_matches)}")
            print("-" * 75)
            for idx, r in enumerate(target_matches, 1):
                scn = r.get("SCN") or r.get("scn")
                op = r.get("OPERATION") or r.get("operation")
                owner = r.get("SEG_OWNER") or r.get("seg_owner")
                tbl = r.get("TABLE_NAME") or r.get("table_name")
                con_name = r.get("SRC_CON_NAME") or r.get("src_con_name") or "N/A"
                con_id = r.get("SRC_CON_ID") or r.get("src_con_id") or "N/A"
                start_s = r.get("START_SCN") or r.get("start_scn") or "N/A"
                commit_s = r.get("COMMIT_SCN") or r.get("commit_scn") or "N/A"
                sql = r.get("SQL_REDO") or r.get("sql_redo") or ""

                print(f"[{idx}] SCN: {scn} | START_SCN: {start_s} | COMMIT_SCN: {commit_s}")
                print(f"    Container: {con_name} (ID: {con_id}) | Owner: {owner} | Table: {tbl} | Op: {op}")
                print(f"    SQL_REDO: {sql}")
                print()

            if show_all and other_rows:
                print(f"\nOther DML Rows in Range (showing first 15 of {len(other_rows)}):")
                print("-" * 75)
                for idx, r in enumerate(other_rows[:15], 1):
                    scn = r.get("SCN") or r.get("scn")
                    op = r.get("OPERATION") or r.get("operation")
                    owner = r.get("SEG_OWNER") or r.get("seg_owner")
                    tbl = r.get("TABLE_NAME") or r.get("table_name")
                    con_name = r.get("SRC_CON_NAME") or r.get("src_con_name") or "N/A"
                    print(f"  [{idx}] SCN={scn} | OP={op} | CON={con_name} | {owner}.{tbl}")

        finally:
            # 8. Cleanly end LogMiner session
            try:
                conn.execute(text("BEGIN DBMS_LOGMNR.END_LOGMNR; EXCEPTION WHEN OTHERS THEN NULL; END;"))
                print("-" * 75)
                print("LogMiner session closed cleanly.")
            except Exception as exc:
                print(f"Warning ending LogMiner session: {exc}")

    print("=" * 75)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read-only diagnostic tool for Oracle LogMiner CDC.")
    parser.add_argument("--start-scn", type=int, default=None, help="Start SCN for LogMiner mining window")
    parser.add_argument("--end-scn", type=int, default=None, help="End SCN for LogMiner mining window")
    parser.add_argument("--table", type=str, default="EMPLOYEES", help="Table name to inspect (default: EMPLOYEES)")
    parser.add_argument("--all", action="store_true", help="Also display non-target DML rows in SCN range")
    args = parser.parse_args()

    run_diagnostic(
        start_scn=args.start_scn,
        end_scn=args.end_scn,
        table_name=args.table,
        show_all=args.all,
    )
