
import sys
import json
import argparse
import pandas as pd
from sqlalchemy import text

from app.adapters.registry import default_registry

DATASETS = {
    "jobs": {
        "title": "Job Positions (HR.JOBS)",
        "source_query": "SELECT JOB_ID, JOB_TITLE, MIN_SALARY, MAX_SALARY FROM HR.JOBS",
        "target_table": "demo_jobs_showcase",
        "target_schema": "public",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS public.demo_jobs_showcase (
                job_id VARCHAR(10) PRIMARY KEY,
                job_title VARCHAR(50) NOT NULL,
                min_salary NUMERIC(10, 2),
                max_salary NUMERIC(10, 2)
            )
        """,
        "alter_sql": "ALTER TABLE public.demo_jobs_showcase ADD COLUMN IF NOT EXISTS verified_flag VARCHAR(10)",
        "columns": ["job_id", "job_title", "min_salary", "max_salary"],
    },
    "countries": {
        "title": "Global Countries (HR.COUNTRIES)",
        "source_query": "SELECT COUNTRY_ID, COUNTRY_NAME, REGION_ID FROM HR.COUNTRIES",
        "target_table": "demo_countries_showcase",
        "target_schema": "public",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS public.demo_countries_showcase (
                country_id CHAR(2) PRIMARY KEY,
                country_name VARCHAR(50) NOT NULL,
                region_id INT
            )
        """,
        "alter_sql": "ALTER TABLE public.demo_countries_showcase ADD COLUMN IF NOT EXISTS verified_flag VARCHAR(10)",
        "columns": ["country_id", "country_name", "region_id"],
    },
    "locations": {
        "title": "Office Locations (HR.LOCATIONS)",
        "source_query": "SELECT LOCATION_ID, STREET_ADDRESS, POSTAL_CODE, CITY, STATE_PROVINCE, COUNTRY_ID FROM HR.LOCATIONS",
        "target_table": "demo_locations_showcase",
        "target_schema": "public",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS public.demo_locations_showcase (
                location_id INT PRIMARY KEY,
                street_address VARCHAR(60),
                postal_code VARCHAR(15),
                city VARCHAR(40) NOT NULL,
                state_province VARCHAR(30),
                country_id CHAR(2)
            )
        """,
        "alter_sql": "ALTER TABLE public.demo_locations_showcase ADD COLUMN IF NOT EXISTS verified_flag VARCHAR(10)",
        "columns": ["location_id", "street_address", "postal_code", "city", "state_province", "country_id"],
    },
    "departments": {
        "title": "Company Departments (HR.DEPARTMENTS)",
        "source_query": "SELECT DEPARTMENT_ID, DEPARTMENT_NAME, MANAGER_ID, LOCATION_ID FROM HR.DEPARTMENTS",
        "target_table": "demo_departments_showcase",
        "target_schema": "public",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS public.demo_departments_showcase (
                department_id INT PRIMARY KEY,
                department_name VARCHAR(50) NOT NULL,
                manager_id INT,
                location_id INT
            )
        """,
        "alter_sql": "ALTER TABLE public.demo_departments_showcase ADD COLUMN IF NOT EXISTS verified_flag VARCHAR(10)",
        "columns": ["department_id", "department_name", "manager_id", "location_id"],
    },
    "employees": {
        "title": "Company Employees (HR.EMPLOYEES)",
        "source_query": "SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, SALARY, DEPARTMENT_ID FROM HR.EMPLOYEES WHERE EMPLOYEE_ID IS NOT NULL",
        "target_table": "demo_employees_showcase",
        "target_schema": "public",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS public.demo_employees_showcase (
                employee_id INT PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                salary NUMERIC(10, 2),
                department_id INT
            )
        """,
        "alter_sql": "ALTER TABLE public.demo_employees_showcase ADD COLUMN IF NOT EXISTS verified_flag VARCHAR(10)",
        "columns": ["employee_id", "first_name", "last_name", "salary", "department_id"],
    },
}


def print_banner(title: str):
    print("\n" + "=" * 80)
    print(f" >>> {title}")
    print("=" * 80)


def run_dataset_test(dataset_key: str):
    dataset = DATASETS.get(dataset_key.lower())
    if not dataset:
        print(f"[-] Unknown dataset '{dataset_key}'. Choose from: {list(DATASETS.keys())}")
        sys.exit(1)

    print_banner(f"TESTING DATASET: {dataset['title'].upper()}")
    print(f"Source Oracle Query:  {dataset['source_query']}")
    print(f"Target Postgres Table: {dataset['target_schema']}.{dataset['target_table']}")

    oracle_adapter = default_registry.get_source_adapter("oracle")
    postgres_adapter = default_registry.get_target_adapter("postgresql")

    schema_name = dataset["target_schema"]
    table_name = dataset["target_table"]

    # Step 1: CREATE TABLE
    print_banner(f"STEP 1: CREATE TABLE IN POSTGRESQL ({schema_name}.{table_name})")
    print("[SQL]:", dataset["create_sql"].strip())
    postgres_adapter.execute_ddl([dataset["create_sql"]])

    with postgres_adapter.get_engine().connect() as conn:
        cols = conn.execute(text(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """)).fetchall()

    print("[+] Table Created Successfully. Columns:")
    for c in cols:
        print(f"    - {c[0]} ({c[1]}, nullable: {c[2]})")

    # Clean state
    print(f"\nEnsuring clean demo state on {schema_name}.{table_name}...")
    postgres_adapter.execute_ddl([f"TRUNCATE TABLE {schema_name}.{table_name}"], allow_destructive_ddl=True)
    print(f"[+] Initial row count is guaranteed 0.")

    # Step 2: ALTER TABLE
    print_banner("STEP 2: ALTER TABLE IN POSTGRESQL (ADD COLUMN)")
    print("[SQL]:", dataset["alter_sql"])
    postgres_adapter.execute_ddl([dataset["alter_sql"]])
    print("[+] Column 'verified_flag' successfully added.")

    # Step 3: SELECT from Oracle
    print_banner(f"STEP 3: SELECT / SOURCE EXTRACTION (ORACLE: {dataset_key.upper()})")
    print("[SQL]:", dataset["source_query"])
    extracted_df = oracle_adapter.extract(dataset["source_query"])
    source_count = len(extracted_df)
    print(f"[+] Rows extracted from Oracle: {source_count} row(s)")
    print(f"[+] DataFrame Columns: {list(extracted_df.columns)}")
    print("\nPreview Top 5 Extracted Rows:")
    print(extracted_df.head(5).to_string(index=False))

    # Step 4: INSERT into PostgreSQL
    print_banner(f"STEP 4: INSERT / DATA LOADING INTO POSTGRESQL ({schema_name}.{table_name})")
    df_to_load = extracted_df.copy()
    df_to_load.columns = [c.lower() for c in df_to_load.columns]
    df_to_load["verified_flag"] = "PASS"

    before_count = postgres_adapter.get_row_count(table_name, schema_name)
    print(f"Target count BEFORE insert: {before_count} rows")
    print(f"Executing bulk parameterized insert of {len(df_to_load)} rows...")

    rows_inserted = postgres_adapter.load(
        df=df_to_load,
        table=table_name,
        schema=schema_name,
        mode="append"
    )

    after_count = postgres_adapter.get_row_count(table_name, schema_name)
    print(f"[+] Target count AFTER insert:  {after_count} rows (inserted: {rows_inserted})")

    # Step 5: VERIFY rows in PostgreSQL
    print_banner(f"STEP 5: VERIFY ROWS DIRECTLY IN POSTGRESQL ({schema_name}.{table_name})")
    with postgres_adapter.get_engine().connect() as conn:
        sample_rows = pd.read_sql(text(f"SELECT * FROM {schema_name}.{table_name} LIMIT 5"), conn)
    print("Live Rows Queried from PostgreSQL:")
    print(sample_rows.to_string(index=False))

    # Step 6: Reconciliation
    print_banner(f"RECONCILIATION SUMMARY FOR DATASET: {dataset_key.upper()}")
    is_match = (source_count == after_count)
    status = "PASS" if is_match else "FAIL"

    print(f"DATASET TESTED: {dataset['title']}")
    print(f"SOURCE ROWS:    {source_count}")
    print(f"TARGET ROWS:    {after_count}")
    print(f"RECONCILIATION: {status}")
    print("=" * 80)

    if not is_match:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Test Data Plane with various Oracle datasets.")
    parser.add_argument(
        "--dataset",
        choices=list(DATASETS.keys()),
        default="jobs",
        help="Dataset to test (default: jobs). Options: jobs, countries, locations, departments, employees"
    )
    args = parser.parse_args()
    run_dataset_test(args.dataset)


if __name__ == "__main__":
    main()
