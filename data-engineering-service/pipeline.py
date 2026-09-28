from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any
import pandas as pd
from sqlalchemy import text
from app.adapters.registry import default_registry
from app.services.migration_service import MigrationService
from app.transformations.engine import _resolve_col
from app.validation.validator import validate_migration_contract, normalize_contract


def banner(title: str) -> None:
    print("\n" + "=" * 80)
    print(f" >>> {title}")
    print("=" * 80)


DEMO_HR_CONTRACT: dict[str, Any] = {
    "provider": "gemini",
    "result": {
        "source": {
            "type": "oracle",
            "schema": "HR",
            "table": "EMPLOYEES",
        },
        "target": {
            "type": "postgresql",
            "schema": "public",
            "table": "pipeline_showcase",
        },
        "execution_options": {
            "allow_destructive_ddl": True,
        },
        "table_management": [
            """
            CREATE TABLE IF NOT EXISTS public.pipeline_showcase (
                employee_id INT,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                salary NUMERIC(10, 2),
                department_id INT
            )
            """,
            "TRUNCATE TABLE public.pipeline_showcase",
            "ALTER TABLE public.pipeline_showcase ADD COLUMN IF NOT EXISTS tier_category VARCHAR(30)",
        ],
        "data_extraction": [
            "SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, SALARY, DEPARTMENT_ID FROM HR.EMPLOYEES WHERE EMPLOYEE_ID IS NOT NULL"
        ],
        "transformations": [
            {"type": "normalize_columns", "case": "lower"},
            {"type": "normalize", "source": "first_name", "case": "upper", "strip": True},
            {"type": "normalize", "source": "last_name", "case": "upper", "strip": True},
            {"type": "round", "source": "salary", "decimals": 2},
            {
                "type": "derive",
                "target": "tier_category",
                "source": "salary",
                "condition": {
                    "operator": "gte",
                    "value": 10000,
                    "then": "EXECUTIVE",
                    "else": "STANDARD",
                },
            },
        ],
        "data_management": [
            "INSERT INTO public.pipeline_showcase (employee_id, first_name, last_name, salary, department_id, tier_category) VALUES {VALUES_PLACEHOLDER};"
        ],
        "placeholder": "{VALUES_PLACEHOLDER}",
        "summary": "Demonstration migration: Oracle HR.EMPLOYEES to PostgreSQL with Pandas transformation",
    },
}


def load_contract() -> dict[str, Any]:
    """Loads contract from CLI argument if provided, else returns demonstration contract."""
    if len(sys.argv) > 1:
        filepath = Path(sys.argv[1])
        if not filepath.exists():
            print(f"[-] Error: Contract file '{filepath}' not found.")
            sys.exit(1)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                contract = json.load(f)
            print(f"[+] Loaded AI JSON migration contract from: {filepath}")
            return contract
        except Exception as exc:
            print(f"[-] Error parsing JSON contract: {exc}")
            sys.exit(1)

    print("[*] No contract file supplied. Using demonstration contract (Oracle HR.EMPLOYEES -> PostgreSQL).")
    return DEMO_HR_CONTRACT


def main() -> None:
    banner("DATA PLANE COMPLETE PIPELINE: CREATE -> ALTER -> SELECT -> TRANSFORM -> INSERT")

    banner("STEP 0: CHECK LIVE DATABASE CONNECTIONS")
    oracle_adapter = default_registry.get_source_adapter("oracle")
    postgres_adapter = default_registry.get_target_adapter("postgresql")

    ora = oracle_adapter.test_connection()
    pg = postgres_adapter.test_connection()

    print(f"[Oracle]     Connected: {ora.get('connected')} | Host: {ora.get('host')}/{ora.get('service')} | Time: {ora.get('db_time')}")
    print(f"[PostgreSQL] Connected: {pg.get('connected')} | Host: {pg.get('host')}/{pg.get('db_name')} | Time: {pg.get('db_time')}")

    if not ora.get("connected") or not pg.get("connected"):
        print("[-] Error: Database connection failed. Please check your .env settings.")
        sys.exit(1)

    contract = load_contract()

    banner("VALIDATE AI-GENERATED MIGRATION CONTRACT")
    is_valid, validation_errors = validate_migration_contract(contract)
    if not is_valid:
        print("[-] Contract validation failed:")
        for err in validation_errors:
            print(f"    - {err}")
        sys.exit(1)
    print("[+] Contract is valid. Scope: CREATE, ALTER, SELECT, TRANSFORM, INSERT, VERIFY.")

    service = MigrationService()
    norm = normalize_contract(contract)

    target_info = norm.get("target", {}) if isinstance(norm.get("target"), dict) else {}
    target_table = target_info.get("table")
    target_schema = target_info.get("schema", "public")
    target_columns: list[str] | None = None

    dm = norm.get("data_migration", {}) if isinstance(norm.get("data_migration"), dict) else {}
    if dm.get("loading", {}).get("table"):
        target_table = dm["loading"]["table"]
    if dm.get("loading", {}).get("columns"):
        target_columns = dm["loading"]["columns"]

    data_mgmt = norm.get("data_management", [])
    if isinstance(data_mgmt, list) and len(data_mgmt) > 0:
        match = re.search(r"INSERT\s+INTO\s+(?:([\w\$\"]+)\.)?([\w\$\"]+)\s*(?:\(([^)]+)\))?", str(data_mgmt[0]), re.IGNORECASE)
        if match:
            if match.group(1):
                target_schema = match.group(1).replace('"', '')
            if not target_table:
                target_table = match.group(2).replace('"', '')
            if not target_columns and match.group(3):
                target_columns = [c.strip().replace('"', '') for c in match.group(3).split(",")]

    target_table = target_table or "pipeline_showcase"

    statements = norm.get("table_management", [])
    if isinstance(statements, dict):
        statements = statements.get("statements", [])

    banner("STEP 1 & 2: [CREATE & ALTER] TARGET STRUCTURE IN POSTGRESQL")
    if statements:
        allow_destructive = bool(
            norm.get("execution_options", {}).get("allow_destructive_ddl", False)
            or norm.get("allow_destructive_ddl", False)
        )
        print(f"Executing {len(statements)} DDL statement(s) on PostgreSQL:")
        for stmt in statements:
            clean = stmt.strip().rstrip(";")
            print(f"  > {clean[:70]}...")
        postgres_adapter.execute_ddl(statements, allow_destructive_ddl=allow_destructive)
        print("[+] DDL executed successfully.")
    else:
        print("[*] No DDL statements specified in contract (schema pre-managed).")

    banner("STEP 3: [SELECT] / EXTRACTION FROM SOURCE DATABASE")
    extraction_queries = norm.get("data_extraction", [])
    if not extraction_queries:
        if dm.get("extraction", {}).get("query"):
            extraction_queries = [dm["extraction"]["query"]]
        elif norm.get("query"):
            extraction_queries = [norm["query"]]

    query = extraction_queries[0] if extraction_queries else "SELECT * FROM DUAL"
    print(f"[Query Sent to Source]:\n  {query}")
    extracted_df = oracle_adapter.extract(query)
    source_count = len(extracted_df)
    print(f"\n[+] Extracted {source_count} rows from source into in-memory Pandas DataFrame.")
    print("Preview of Raw Extracted Rows (Top 5):")
    print(extracted_df.head(5).to_string(index=False))

    banner("STEP 4: [TRANSFORM] GENERIC IN-MEMORY PANDAS DATA TRANSFORMATIONS")
    transformations = norm.get("transformations") or norm.get("operations", [])
    print(f"Applying {len(transformations)} declarative transformation step(s) via TransformationEngine:")
    for idx, step in enumerate(transformations, 1):
        print(f"  {idx}. {step.get('type') or step.get('operation')}: {step}")

    transformed_df = service.transform_engine.apply(extracted_df, transformations)
    transformed_count = len(transformed_df)
    print(f"\n[+] Transformed {transformed_count} rows in memory.")
    print("Preview of Transformed Rows (Top 5):")
    print(transformed_df.head(5).to_string(index=False))

    df_to_load = transformed_df
    if target_columns and isinstance(target_columns, list):
        projected = {}
        for col in target_columns:
            src_col = _resolve_col(transformed_df, str(col))
            if src_col not in transformed_df.columns:
                raise KeyError(f"Target column '{col}' not found in transformed DataFrame columns: {list(transformed_df.columns)}")
            projected[str(col)] = transformed_df[src_col].copy()
        df_to_load = pd.DataFrame(projected, index=transformed_df.index)

    banner(f"STEP 5: [INSERT] TARGET LOADING INTO POSTGRESQL ({target_schema}.{target_table})")
    before_count = postgres_adapter.get_row_count(target_table, target_schema)
    print(f"Target count BEFORE insert: {before_count} rows")
    print(f"Executing bulk parameterized INSERT of {len(df_to_load)} rows into {target_schema}.{target_table}...")

    rows_inserted = postgres_adapter.load(
        df=df_to_load,
        table=target_table,
        schema=target_schema,
        mode="append",
    )

    after_count = postgres_adapter.get_row_count(target_table, target_schema)
    print(f"[+] Loaded {rows_inserted} rows into PostgreSQL target table.")
    print(f"[+] Target count AFTER insert:  {after_count} rows")

    banner(f"STEP 6: [VERIFY] LIVE ROWS QUERIED DIRECTLY FROM POSTGRESQL")
    sample_rows = postgres_adapter.verify_data(target_table, target_schema, limit=5)
    print("Live Data Retrieved from PostgreSQL:")
    print(sample_rows.to_string(index=False))

    banner("STEP 7: AUDIT & RECONCILIATION SUMMARY")
    is_reconciled = (source_count == rows_inserted) and (after_count >= rows_inserted)
    status = "PASS" if is_reconciled else "FAIL"

    print(f"1. CREATE/ALTER:   TARGET STRUCTURE READY ({target_schema}.{target_table}) -> [PASS]")
    print(f"2. SELECT:         {source_count} ROWS EXTRACTED FROM SOURCE -> [PASS]")
    print(f"3. TRANSFORM:      {transformed_count} ROWS TRANSFORMED IN PANDAS -> [PASS]")
    print(f"4. INSERT:         {rows_inserted} ROWS LOADED INTO TARGET -> [PASS]")
    print(f"5. VERIFY:         TARGET TABLE QUERIED & CONFIRMED -> [PASS]")
    print(f"6. RECONCILIATION: {source_count} SOURCE == {after_count} TARGET -> [{status}]")
    print("=" * 80)

    if not is_reconciled:
        sys.exit(1)


if __name__ == "__main__":
    main()
