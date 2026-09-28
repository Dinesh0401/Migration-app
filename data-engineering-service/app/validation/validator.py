from __future__ import annotations

import re
from typing import Any
import pandas as pd


def _resolve_col(df: pd.DataFrame, col_name: str) -> str:
    if col_name in df.columns:
        return col_name
    col_lower = str(col_name).strip().lower()
    for c in df.columns:
        if str(c).strip().lower() == col_lower:
            return c
    return col_name


def normalize_contract(spec: dict[str, Any]) -> dict[str, Any]:
    """
    Normalizes a contract spec to ensure compatibility with different AI contract formats:
    1. Unwraps nested 'result' dictionaries (e.g. {"provider": "gemini|groq", "result": {...}}).
    2. Fallbacks source/target from table_management or schema_design if missing at root.
    3. Converts string source/target (e.g. "source": "oracle") into dicts (e.g. {"type": "oracle"}).
    4. Extracts nested DDL statements list from table_management (e.g. table_management.table_management).
    5. Identifies contract kind: 'schema' (DDL only) vs 'data' (data extraction & loading).
    """
    if not isinstance(spec, dict):
        return spec

    normalized = dict(spec)
    if "result" in normalized and isinstance(normalized["result"], dict):
        result_payload = normalized.pop("result")
        for k, v in result_payload.items():
            if k not in normalized or not normalized[k]:
                normalized[k] = v

    # Fallback source/target from table_management or schema_design
    tm = normalized.get("table_management")
    if isinstance(tm, dict):
        if not normalized.get("source") and tm.get("source"):
            normalized["source"] = tm.get("source")
        if not normalized.get("target") and tm.get("target"):
            normalized["target"] = tm.get("target")
        # Extract statements list if nested under table_management or statements key
        if "table_management" in tm and isinstance(tm["table_management"], list):
            normalized["table_management"] = tm["table_management"]
        elif "statements" in tm and isinstance(tm["statements"], list):
            normalized["table_management"] = tm["statements"]

    sd = normalized.get("schema_design")
    if isinstance(sd, dict):
        if not normalized.get("source") and sd.get("source"):
            normalized["source"] = sd.get("source")
        if not normalized.get("target") and sd.get("target"):
            normalized["target"] = sd.get("target")

    if isinstance(normalized.get("source"), str):
        normalized["source"] = {"type": normalized["source"]}

    if isinstance(normalized.get("target"), str):
        normalized["target"] = {"type": normalized["target"]}

    # Detect contract kind
    has_extraction = bool(
        normalized.get("data_extraction")
        or normalized.get("query")
        or (isinstance(normalized.get("data_migration"), dict) and normalized["data_migration"].get("extraction"))
    )
    has_table_mgmt = bool(normalized.get("table_management"))

    if has_table_mgmt and not has_extraction:
        normalized["kind"] = "schema"
    else:
        normalized["kind"] = normalized.get("kind", "data")

    return normalized


def validate_migration_contract(spec: dict[str, Any]) -> tuple[bool, list[str]]:
    """
    Validates the migration instruction contract before any database execution.
    Returns (is_valid, list_of_error_messages).
    Enforces scope: CREATE, ALTER, SELECT, TRANSFORM, INSERT, VERIFY.
    Supports both Schema Contracts (DDL) and Data Migration Contracts.
    Rejects unsupported, prohibited (DROP DATABASE, DELETE), or dangerous operations.
    """
    errors: list[str] = []

    if not isinstance(spec, dict):
        return False, ["Migration contract must be a JSON object."]

    spec = normalize_contract(spec)
    contract_kind = spec.get("kind", "data")

    source = spec.get("source")
    if not source or not isinstance(source, dict):
        errors.append("Contract must include a 'source' object.")
    else:
        source_type = source.get("type") or source.get("database")
        if not source_type:
            errors.append("Source must specify 'type' or 'database' (e.g. 'oracle').")

    target = spec.get("target")
    if not target or not isinstance(target, dict):
        errors.append("Contract must include a 'target' object.")
    else:
        target_type = target.get("type") or target.get("database")
        if not target_type:
            errors.append("Target must specify 'type' or 'database' (e.g. 'postgresql').")

    if contract_kind != "schema":
        data_migration = spec.get("data_migration", {})
        extraction = data_migration.get("extraction", {}) if isinstance(data_migration, dict) else {}
        query = extraction.get("query") if isinstance(extraction, dict) else None
        source_table = source.get("table") if isinstance(source, dict) else None
        direct_query = spec.get("query") or (source.get("query") if isinstance(source, dict) else None)
        data_extraction = spec.get("data_extraction")
        if isinstance(data_extraction, list) and len(data_extraction) > 0 and isinstance(data_extraction[0], str):
            direct_query = direct_query or data_extraction[0]

        all_queries: list[str] = []
        if query:
            all_queries.append(query)
        if direct_query and direct_query != query:
            all_queries.append(direct_query)
        if isinstance(data_extraction, list):
            for q in data_extraction:
                if isinstance(q, str) and q not in all_queries:
                    all_queries.append(q)

        if not (all_queries or source_table):
            errors.append("Contract must specify an extraction query in 'data_migration.extraction.query', 'data_extraction', or a source 'table'.")

        for q in all_queries:
            q_clean = q.strip().rstrip(";")
            if re.search(r"\b(DELETE|DROP|TRUNCATE|UPDATE|INSERT)\b", q_clean, re.IGNORECASE):
                errors.append(f"Extraction query must be read-only (SELECT). Prohibited operation detected: '{q_clean}'")

        loading = data_migration.get("loading", {}) if isinstance(data_migration, dict) else {}
        target_table = loading.get("table") if isinstance(loading, dict) else None
        if not target_table:
            target_table = target.get("table") if isinstance(target, dict) else None

        data_management = spec.get("data_management")
        if not target_table and isinstance(data_management, list) and len(data_management) > 0:
            match = re.search(r"INSERT\s+INTO\s+(?:[\w\$\"]+\.)?([\w\$\"]+)", str(data_management[0]), re.IGNORECASE)
            if match:
                target_table = match.group(1).replace('"', '')

        if not target_table:
            errors.append("Contract must specify a target table in 'data_migration.loading.table', 'target.table', or 'data_management'.")

        loading_columns = loading.get("columns") if isinstance(loading, dict) else None
        if loading_columns is not None:
            if not isinstance(loading_columns, list):
                errors.append("'data_migration.loading.columns' must be a list of column names.")
            elif not all(isinstance(c, str) and c.strip() for c in loading_columns):
                errors.append("Each element in 'data_migration.loading.columns' must be a non-empty string.")

        if isinstance(data_management, list):
            for stmt in data_management:
                if not isinstance(stmt, str):
                    continue
                stmt_clean = stmt.strip().rstrip(";")
                if re.search(r"\b(DELETE|DROP\s+DATABASE|DROP\s+TABLE|TRUNCATE)\b", stmt_clean, re.IGNORECASE):
                    errors.append(f"Prohibited statement in data_management: '{stmt_clean}'")
    else:
        # Schema-only contract must have table_management
        if not spec.get("table_management"):
            errors.append("Schema contract must specify 'table_management' with DDL statement(s).")

    transformations = spec.get("transformations") or spec.get("operations")
    if transformations is not None:
        if not isinstance(transformations, list):
            errors.append("'transformations' must be a list of transformation objects.")
        else:
            for i, step in enumerate(transformations):
                if not isinstance(step, dict):
                    errors.append(f"Transformation step #{i+1} must be an object.")
                else:
                    op = step.get("type") or step.get("operation")
                    if not op:
                        errors.append(f"Transformation step #{i+1} missing 'type' or 'operation'.")

    table_mgmt = spec.get("table_management")
    if table_mgmt is not None:
        if isinstance(table_mgmt, list):
            statements = table_mgmt
        elif isinstance(table_mgmt, dict):
            statements = table_mgmt.get("statements")
        else:
            errors.append("'table_management' must be a list of SQL strings or an object containing 'statements'.")
            statements = None

        if statements is not None:
            if not isinstance(statements, list):
                errors.append("'table_management.statements' must be a list of SQL strings.")
            else:
                exec_opts = spec.get("execution_options", {}) if isinstance(spec.get("execution_options"), dict) else {}
                allow_destructive = bool(
                    exec_opts.get("allow_destructive_ddl", False)
                    or spec.get("allow_destructive_ddl", False)
                )
                for stmt in statements:
                    if not isinstance(stmt, str):
                        continue
                    stmt_clean = stmt.strip().rstrip(";")
                    if re.search(r"\b(DROP\s+DATABASE|DELETE(\s+FROM)?)\b", stmt_clean, re.IGNORECASE):
                        errors.append(f"Prohibited DDL statement rejected: '{stmt_clean}'")
                    elif not allow_destructive and re.search(r"\b(DROP\s+TABLE|DROP\s+SCHEMA|TRUNCATE)\b", stmt_clean, re.IGNORECASE):
                        errors.append(
                            f"Destructive DDL statement rejected (allow_destructive_ddl=False): '{stmt_clean}'. "
                            "Set allow_destructive_ddl=True in execution_options to permit destructive operations."
                        )

    return len(errors) == 0, errors


def validate_dataframe(
    df: pd.DataFrame,
    required_columns: list[str] | None = None,
    primary_key: str | list[str] | None = None,
    expected_dtypes: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Validates an in-memory Pandas DataFrame prior to loading into the target database.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if df is None:
        return {
            "valid": False,
            "status": "failed",
            "rows": 0,
            "errors": ["DataFrame is null"],
            "warnings": [],
        }

    row_count = len(df)

    if required_columns:
        resolved_cols = [_resolve_col(df, c) for c in required_columns]
        missing_columns = [required_columns[i] for i, c in enumerate(resolved_cols) if c not in df.columns]
        if missing_columns:
            errors.append(f"Missing required column(s): {missing_columns}. DataFrame columns: {list(df.columns)}")

    if primary_key:
        pk_cols = [primary_key] if isinstance(primary_key, str) else list(primary_key)
        resolved_pk_cols = [_resolve_col(df, c) for c in pk_cols]
        existing_pk_cols = [c for c in resolved_pk_cols if c in df.columns]
        if existing_pk_cols and len(existing_pk_cols) == len(pk_cols):
            duplicates = df.duplicated(subset=existing_pk_cols).sum()
            if duplicates:
                errors.append(f"Duplicate primary key values found on {existing_pk_cols}: {duplicates} duplicate row(s)")
            null_pks = df[existing_pk_cols].isna().any(axis=1).sum()
            if null_pks:
                errors.append(f"Primary key column(s) {existing_pk_cols} contain {null_pks} null value(s)")

    if expected_dtypes:
        for col, expected_type in expected_dtypes.items():
            resolved = _resolve_col(df, col)
            if resolved in df.columns:
                actual = str(df[resolved].dtype)
                if expected_type in ("numeric", "int", "float") and not pd.api.types.is_numeric_dtype(df[resolved]):
                    errors.append(f"Column '{col}' expected {expected_type} but found {actual}")
                elif expected_type == "string" and not pd.api.types.is_string_dtype(df[resolved]):
                    warnings.append(f"Column '{col}' expected string but found {actual}")

    is_valid = len(errors) == 0
    return {
        "valid": is_valid,
        "status": "passed" if is_valid else "failed",
        "rows": row_count,
        "errors": errors,
        "warnings": warnings,
    }
