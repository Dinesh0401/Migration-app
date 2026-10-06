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


def validate_records(
    records: list[dict[str, Any]] | list[tuple[Any, ...]],
    column_names: list[str] | None = None,
    required_columns: list[str] | None = None,
    primary_key: str | list[str] | None = None,
    expected_dtypes: dict[str, str] | None = None,
    allow_empty: bool = True,
) -> dict[str, Any]:
    """
    Validates in-memory records (lists of dicts or tuples) prior to loading into target database.
    Implemented in Pure Python with ZERO dependency on Pandas.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if records is None:
        return {
            "valid": False,
            "status": "failed",
            "rows": 0,
            "errors": ["Records collection is None."],
            "warnings": [],
        }

    total_rows = len(records)
    if total_rows == 0:
        if not allow_empty:
            errors.append("Dataset is empty; zero records found.")
        return {
            "valid": len(errors) == 0,
            "status": "passed" if len(errors) == 0 else "failed",
            "rows": 0,
            "errors": errors,
            "warnings": warnings,
        }

    # Normalize records into list of dictionaries
    dict_records: list[dict[str, Any]] = []
    first_record = records[0]

    if isinstance(first_record, dict):
        dict_records = records  # type: ignore
        detected_columns = list(first_record.keys())
    elif isinstance(first_record, (tuple, list)):
        if not column_names:
            errors.append("column_names must be provided when records are given as tuples or lists.")
            return {
                "valid": False,
                "status": "failed",
                "rows": total_rows,
                "errors": errors,
                "warnings": warnings,
            }
        detected_columns = list(column_names)
        dict_records = [dict(zip(column_names, row)) for row in records]
    else:
        errors.append(f"Unsupported record format: {type(first_record).__name__}. Expected dict or tuple.")
        return {
            "valid": False,
            "status": "failed",
            "rows": total_rows,
            "errors": errors,
            "warnings": warnings,
        }

    col_map = {c.lower(): c for c in detected_columns}

    if required_columns:
        for req_col in required_columns:
            if req_col.lower() not in col_map:
                errors.append(f"Missing required column: '{req_col}'. Available columns: {detected_columns}")

    if primary_key:
        pk_cols = [primary_key] if isinstance(primary_key, str) else list(primary_key)
        resolved_pks: list[str] = []
        for pk in pk_cols:
            if pk.lower() in col_map:
                resolved_pks.append(col_map[pk.lower()])
            else:
                errors.append(f"Primary key column '{pk}' not found in record columns: {detected_columns}")

        if len(resolved_pks) == len(pk_cols):
            seen_pk_values: set[tuple[Any, ...]] = set()
            null_pk_count = 0
            duplicate_pk_count = 0

            for row_idx, row in enumerate(dict_records):
                pk_tuple = tuple(row.get(col) for col in resolved_pks)
                if any(val is None for val in pk_tuple):
                    null_pk_count += 1
                    if null_pk_count <= 5:
                        errors.append(f"Primary key column(s) {resolved_pks} contain NULL value at row #{row_idx + 1}: {pk_tuple}")
                else:
                    if pk_tuple in seen_pk_values:
                        duplicate_pk_count += 1
                        if duplicate_pk_count <= 5:
                            errors.append(f"Duplicate primary key value detected on {resolved_pks} at row #{row_idx + 1}: {pk_tuple}")
                    else:
                        seen_pk_values.add(pk_tuple)

            if null_pk_count > 5:
                errors.append(f"... and {null_pk_count - 5} more NULL primary key occurrences.")
            if duplicate_pk_count > 5:
                errors.append(f"... and {duplicate_pk_count - 5} more duplicate primary key occurrences.")

    if expected_dtypes:
        for col_name, expected_type in expected_dtypes.items():
            if col_name.lower() in col_map:
                actual_col = col_map[col_name.lower()]
                norm_type = expected_type.strip().lower()

                type_mismatches = 0
                for row_idx, row in enumerate(dict_records[:100]):
                    val = row.get(actual_col)
                    if val is None:
                        continue

                    valid_type = True
                    if norm_type in ("int", "integer"):
                        if isinstance(val, bool) or not isinstance(val, int):
                            try:
                                int(str(val))
                            except (ValueError, TypeError):
                                valid_type = False
                    elif norm_type in ("float", "numeric", "decimal", "double"):
                        if isinstance(val, bool) or not isinstance(val, (int, float)):
                            try:
                                float(str(val))
                            except (ValueError, TypeError):
                                valid_type = False
                    elif norm_type in ("str", "string", "varchar", "text"):
                        if not isinstance(val, str):
                            valid_type = False

                    if not valid_type:
                        type_mismatches += 1
                        if type_mismatches <= 3:
                            errors.append(
                                f"Type mismatch on column '{col_name}': expected {expected_type}, "
                                f"found value '{val}' (type {type(val).__name__}) at row #{row_idx + 1}."
                            )

    is_valid = len(errors) == 0
    return {
        "valid": is_valid,
        "status": "passed" if is_valid else "failed",
        "rows": total_rows,
        "errors": errors,
        "warnings": warnings,
    }


def validate_dataframe(
    df: Any,
    required_columns: list[str] | None = None,
    primary_key: str | list[str] | None = None,
    expected_dtypes: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Backward-compatibility wrapper delegating to pure Python validate_records.
    """
    if df is None:
        return {
            "valid": False,
            "status": "failed",
            "rows": 0,
            "errors": ["DataFrame is null"],
            "warnings": [],
        }

    records = df.to_dict(orient="records") if hasattr(df, "to_dict") else list(df)
    return validate_records(
        records=records,
        required_columns=required_columns,
        primary_key=primary_key,
        expected_dtypes=expected_dtypes,
    )
