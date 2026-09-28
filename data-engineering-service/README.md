# Data Engineering Service (Data Plane Pipeline)

A robust, dataset-independent database migration and data transformation pipeline. Built to dynamically consume AI-generated JSON migration contracts and execute automated extraction, in-memory Pandas transformations, target DDL management, bulk parameterized loading, live verification, and source-to-target reconciliation.

---

## Architecture Overview

```text
                  AI-Generated JSON Contract
                              ↓
                  Contract Validator (`validator.py`)
                    [CREATE, ALTER, SELECT, TRANSFORM, INSERT, VERIFY]
                              ↓
                  Migration Service (`migration_service.py`)
                              ↓
        ┌─────────────────────┴─────────────────────┐
        ↓                                           ↓
  Source Adapter                             Target Adapter
  (`oracle_adapter.py`)                    (`postgres_adapter.py`)
        ↓                                           ↓
  Extract Raw Query                          Execute Target DDL
        ↓                                    (CREATE / ALTER)
  Source DataFrame                                  │
        └─────────────────┬─────────────────────────┘
                          ↓
              Transformation Engine (`engine.py`)
              [normalize, trim, cast, round, filter,
               value_mapping, derive, merge, split]
                          ↓
                 Transformed DataFrame
                          ↓
              Target Column Projection
                          ↓
              Bulk Parameterized Loading
               (`postgres_adapter.load`)
                          ↓
                 PostgreSQL Target Table
                          ↓
              Live Target Verification
                          ↓
            Source vs. Target Reconciliation
```

---

## Key Features

- **Generic & Dataset-Independent**: Operates on arbitrary database schemas and tables. No table names (e.g. `EMPLOYEES`) or column names (e.g. `FIRST_NAME`, `SALARY`) are hardcoded into the core pipeline.
- **Dynamic AI JSON Contract Consumption**: Seamlessly executes AI-generated migration specifications with extraction queries, declarative Pandas transformation rules, and `{VALUES_PLACEHOLDER}` insertion statements.
- **In-Memory Pandas Transformation Engine**: Provides reusable, declarative transformation operations:
  - Column renaming (`rename`, `columns` map, case conversion)
  - String cleaning and normalization (`trim`, `normalize`)
  - Type casting (`cast` to int, float, string, boolean, datetime)
  - Numeric rounding and precision handling (`round`)
  - Null handling and default value injection (`null_handling`, `default`)
  - Row filtering (`filter` with eq, ne, gt, lt, gte, lte, in, not_in, is_null)
  - Categorical mapping (`value_mapping`)
  - Conditional and mathematical column derivation (`derive`)
  - Field splitting and merging (`split`, `concatenate`)
- **Strict DDL & Query Safety**: Contract validator enforces permitted operations (`CREATE`, `ALTER`, `SELECT`, `TRANSFORM`, `INSERT`, `VERIFY`). Automatically blocks prohibited operations (`DROP DATABASE`, `DELETE FROM`) and guards destructive DDL (`DROP TABLE`, `TRUNCATE`).
- **Target Column Projection**: Safely projects and aligns source data against target columns, preventing schema errors on loading.
- **SQL Injection Prevention**: Uses SQLAlchemy parameterized bulk insertions (`method="multi"`) rather than unsafe raw SQL string concatenation.
- **Dual Interface**:
  - **CLI Demonstration (`pipeline.py`)**: End-to-end visual execution with banners, live verification, and audit metrics.
  - **FastAPI Service (`app/main.py`)**: REST API exposing `POST /migration/run` and `GET /health` with interactive Swagger UI docs.

---

## Project Structure

```text
data-engineering-service/
│
├── app/
│   ├── adapters/
│   │   ├── base.py              # Abstract base classes for source and target adapters
│   │   ├── oracle_adapter.py    # Oracle source extraction adapter (SQLAlchemy + python-oracledb)
│   │   ├── postgres_adapter.py  # PostgreSQL target adapter (SQLAlchemy + psycopg2)
│   │   ├── registry.py          # Dynamic adapter registry for source/target resolution
│   │   └── __init__.py
│   │
│   ├── config/
│   │   ├── settings.py          # Environment settings with masked credentials
│   │   └── __init__.py
│   │
│   ├── routes/
│   │   ├── migration.py         # FastAPI migration route handler
│   │   └── __init__.py
│   │
│   ├── services/
│   │   ├── migration_service.py # Generic pipeline orchestration & reconciliation
│   │   └── __init__.py
│   │
│   ├── transformations/
│   │   ├── engine.py            # Generic, declarative in-memory Pandas transformation engine
│   │   └── __init__.py
│   │
│   ├── validation/
│   │   ├── validator.py         # AI contract validation and DataFrame integrity checks
│   │   └── __init__.py
│   │
│   ├── main.py                  # FastAPI application entrypoint
│   └── __init__.py
│
├── tests/
│   ├── sample_contract.json     # Custom sample AI migration contract
│   ├── test_adapters.py         # Adapter registry and database tests
│   ├── test_ai_teammate_pipeline.py # AI contract parsing and full pipeline integration tests
│   ├── test_api_routes.py       # FastAPI route handler tests
│   ├── test_custom_dataset.py   # Multi-dataset independence tests (e-commerce, IoT)
│   ├── test_e2e_live_migration.py # Live end-to-end database migration tests
│   ├── test_migration_pipeline.py # Core pipeline validation tests
│   ├── test_migration_service.py # Unit tests for MigrationService orchestrator
│   ├── test_transformations.py  # Comprehensive transformation engine tests
│   └── test_validator.py        # Contract validation and safety rule tests
│
├── pipeline.py                  # CLI demonstration entrypoint (accepts dynamic contracts)
├── requirements.txt             # Python dependencies
├── .env.example                 # Example configuration template (credentials masked)
├── .gitignore                   # Git ignore patterns (.env, cache, tests)
└── README.md                    # Project documentation
```

---

## Setup & Configuration

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Copy `.env.example` to `.env` and fill in your database credentials:

```bash
cp .env.example .env
```

Ensure `.env` contains:

```env
# Oracle Source Database Configuration
ORACLE_USER=your_oracle_user
ORACLE_PASSWORD=your_oracle_password
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE=FREEPDB1

# PostgreSQL Target Database Configuration
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_target_database

# Service Configuration
DATA_ENGINEERING_PORT=8000
APP_ENV=development
```

> **Security Note:** Never commit `.env` or expose database passwords. `.env` is tracked in `.gitignore`.

---

## Usage

### 1. CLI Demonstration (`pipeline.py`)

Run the default demonstration (Oracle `HR.EMPLOYEES` -> PostgreSQL `public.pipeline_showcase`):

```bash
python pipeline.py
```

Run with an arbitrary AI-generated JSON migration contract:

```bash
python pipeline.py tests/sample_contract.json
```

The CLI demonstration prints progress across all 7 stages:
1. `CREATE`: Target structure setup
2. `ALTER`: Target table alteration
3. `SELECT`: Source extraction into Pandas DataFrame
4. `TRANSFORM`: Declarative transformations applied
5. `INSERT`: Parameterized bulk loading into PostgreSQL
6. `VERIFY`: Live query against PostgreSQL
7. `RECONCILIATION`: Source row count vs. target row count audit

---

### 2. FastAPI Web Service

Start the FastAPI application:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

- **Interactive Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Health Check Endpoint**: `GET /health`
- **Migration Endpoint**: `POST /migration/run`

Example Request Body:

```json
{
  "source": {
    "type": "oracle",
    "schema": "HR",
    "table": "DEPARTMENTS"
  },
  "target": {
    "type": "postgresql",
    "schema": "public",
    "table": "departments"
  },
  "table_management": [
    "CREATE TABLE IF NOT EXISTS public.departments (department_id INT PRIMARY KEY, department_name VARCHAR(100));"
  ],
  "data_extraction": [
    "SELECT DEPARTMENT_ID, DEPARTMENT_NAME FROM HR.DEPARTMENTS WHERE DEPARTMENT_ID IS NOT NULL"
  ],
  "transformations": [
    {"type": "normalize_columns", "case": "lower"},
    {"type": "normalize", "source": "department_name", "case": "title", "strip": true}
  ],
  "data_management": [
    "INSERT INTO public.departments (department_id, department_name) VALUES {VALUES_PLACEHOLDER};"
  ],
  "placeholder": "{VALUES_PLACEHOLDER}"
}
```

---

---

## Phase 2: Log-Based CDC (Change Data Capture)

The Data Plane features two unified execution modes sharing the same core adapters and transformation runtime:

```text
CONTROL PLANE: AI Generates Schema / Migration Contracts (Groq / Gemini)
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                         DATA PLANE                          │
│                                                             │
│   PHASE 1: BATCH BASELINE        PHASE 2: ONGOING CDC       │
│   Full Oracle SELECT             Oracle Redo Logs           │
│           │                              │                  │
│           ▼                              ▼                  │
│    Pandas DataFrame               Oracle LogMiner           │
│           │                              │                  │
│           │                              ▼                  │
│           │                     Normalized CDCEvent         │
│           │                              │                  │
│           └──────────────┬───────────────┘                  │
│                          ▼                                  │
│         Shared TransformationEngine Runtime                 │
│                          │                                  │
│                          ▼                                  │
│                PostgreSQL CDC Writer                        │
│             (Idempotent Upsert & Delete)                    │
│                          │                                  │
│                          ▼                                  │
│               Durable SCN Checkpoint Store                  │
│               (migration.cdc_checkpoint)                    │
└─────────────────────────────────────────────────────────────┘
```

### CDC Event Schema

```json
{
  "scn": 33982757,
  "operation": "INSERT",
  "source_schema": "SYSTEM",
  "source_table": "EMPLOYEES",
  "primary_key": {
    "EMPLOYEE_ID": 103
  },
  "before": null,
  "after": {
    "EMPLOYEE_ID": 103,
    "FIRST_NAME": "Charlie",
    "SALARY": 7500.0
  },
  "timestamp": "2026-09-28 12:03:00",
  "transaction_id": "0x000a.001.00000012",
  "raw_sql": "insert into \"SYSTEM\".\"EMPLOYEES\"(\"EMPLOYEE_ID\",\"FIRST_NAME\",\"SALARY\") values ('103','Charlie','7500');"
}
```

- **UPDATE Events**: Preserves both `before` (from WHERE clause with supplemental logging) and `after` (from SET clause).
- **DELETE Events**: Preserves `before` (containing deleted row identity), with `after: null`.
- **SCN Ordering**: Preserves commit SCN for strictly ordered apply and recovery.

### SCN Checkpoint & Recovery Guarantee

The sequence is strictly enforced:
$$\text{Source Redo} \longrightarrow \text{LogMiner} \longrightarrow \text{Normalize} \longrightarrow \text{Transform} \longrightarrow \text{PostgreSQL Apply} \longrightarrow \text{Save SCN Checkpoint}$$

**Critical Rule**: The checkpoint SCN is committed to PostgreSQL (`migration.cdc_checkpoint`) **ONLY AFTER** the change is successfully written to the target database. If writer failure occurs, the checkpoint is **NOT** advanced, ensuring zero data loss upon consumer restart.

### Supported AI Teammate Contract Formats

The Contract Normalizer automatically accepts:
1. **Schema / Table-Management Contract**:
   ```json
   {
     "result": {
       "schema_design": { "target_schema": { "tables": [...] } },
       "table_management": {
         "source": "oracle",
         "target": "postgresql",
         "table_management": [
           "CREATE TABLE IF NOT EXISTS public.employees (...);",
           "ALTER TABLE public.employees ADD CONSTRAINT employees_pkey PRIMARY KEY (...);"
         ]
       }
     }
   }
   ```
2. **Multi-Table Data Migration Contract**:
   ```json
   {
     "provider": "groq",
     "result": {
       "source": "oracle",
       "target": "postgresql",
       "data_extraction": [
         "SELECT DEPARTMENT_ID, DEPARTMENT_NAME FROM EMPLOYEE;",
         "SELECT EMPLOYEE_ID, EMPLOYEE_NAME, DEPARTMENT_ID FROM EMPLOYEE;"
       ],
       "data_management": [
         "INSERT INTO public.departments (...) VALUES {VALUES_PLACEHOLDER};",
         "INSERT INTO public.employees (...) VALUES {VALUES_PLACEHOLDER};"
       ],
       "placeholder": "{VALUES_PLACEHOLDER}"
     }
   }
   ```

---

## CLI Demonstration Commands

### 1. Batch Migration Pipeline
```bash
# Run baseline batch migration
python pipeline.py tests/ai_contract.json
```

### 2. Log-Based CDC Pipeline
```bash
# Check Oracle LogMiner & PostgreSQL prerequisites
python cdc_pipeline.py --check

# Check current replication status and lag
python cdc_pipeline.py --status

# Inspect durable PostgreSQL SCN checkpoint
python cdc_pipeline.py --checkpoint

# Run a bounded SCN batch window
python cdc_pipeline.py --mode batch --start-scn 33980000 --end-scn 33980500

# Run continuous CDC streaming (polling interval = 2.0s)
python cdc_pipeline.py --mode continuous --poll-interval 2.0

# Run the complete, reproducible end-to-end showcase
python cdc_pipeline.py --demo
```

### 3. FastAPI Web Service
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Endpoints:
- `GET /health`: System & database health
- `POST /migration/run`: Run batch or schema migration contract
- `GET /cdc/check`: Verify CDC prerequisites
- `GET /cdc/status`: Current CDC status and SCN replication lag
- `GET /cdc/checkpoint`: Current PostgreSQL SCN checkpoint
- `POST /cdc/run`: Trigger CDC batch window or streaming mode

---

## Running the Test Suite

```bash
# Run all 73 automated tests (unit, integration, contracts, and CDC recovery)
python -m pytest tests/ -v
```
