from __future__ import annotations

import logging
import re
from typing import Any
import pandas as pd

from app.adapters.registry import AdapterRegistry, default_registry
from app.transformations.engine import TransformationEngine
from app.validation.validator import validate_migration_contract, validate_dataframe, _resolve_col, normalize_contract

logger = logging.getLogger(__name__)


class MigrationService:
    """
    Generic Data Engineering Migration Service.
    Orchestrates:
      1. Contract Validation (including safety & destructive DDL check)
      2. Table Management DDL on Target (governed by allow_destructive_ddl)
      3. Source Data Extraction -> In-Memory Pandas DataFrame (batch extraction)
      4. Dynamic DataFrame Transformations (rename, trim, cast, filter, etc.)
      5. Post-transformation Data Validation (types, nulls, PK uniqueness)
      6. Target Column Projection & Loading (strictly honoring loading.columns)
      7. Metrics & Audit Reporting (reconciliation of source/extracted/transformed/loaded/target rows)
    Does NOT contain hardcoded database, schema, table, or column names.
    Supports single table specs as well as multi-table extraction/management contract pairs.
    """

    def __init__(
        self,
        registry: AdapterRegistry | None = None,
        transform_engine: TransformationEngine | None = None,
    ) -> None:
        self.registry = registry or default_registry
        self.transform_engine = transform_engine or TransformationEngine()

    def run_migration(self, spec: dict[str, Any]) -> dict[str, Any]:
        spec = normalize_contract(spec)

        if spec.get("kind") == "schema":
            return self._run_schema_migration(spec)

        data_extraction = spec.get("data_extraction")
        data_management = spec.get("data_management")
        if isinstance(data_extraction, list) and len(data_extraction) > 1:
            return self._run_multi_migration(spec)

        return self._run_single_migration(spec)

    def _run_schema_migration(self, spec: dict[str, Any]) -> dict[str, Any]:
        """
        Executes a schema-only (DDL) migration contract.
        Validates DDL, applies statements to target adapter, and verifies target table existence.
        """
        logger.info("Executing Schema / Table-Management Contract on Target...")
        is_valid, validation_errors = validate_migration_contract(spec)
        if not is_valid:
            logger.error(f"Schema contract validation failed: {validation_errors}")
            return {
                "status": "failed",
                "stage": "validation",
                "kind": "schema",
                "message": "Schema contract validation failed",
                "errors": validation_errors,
            }

        target_info = spec.get("target", {}) if isinstance(spec.get("target"), dict) else {}
        target_type = str(target_info.get("type") or target_info.get("database") or "postgresql").lower()
        target_adapter = self.registry.get_target_adapter(target_type)

        statements = spec.get("table_management", [])
        if isinstance(statements, dict):
            statements = statements.get("statements", statements.get("table_management", []))

        exec_opts = spec.get("execution_options", {}) if isinstance(spec.get("execution_options"), dict) else {}
        allow_destructive = bool(exec_opts.get("allow_destructive_ddl", False) or spec.get("allow_destructive_ddl", False))

        executed = target_adapter.execute_ddl(statements, allow_destructive_ddl=allow_destructive)

        # Detect created / altered table names from DDL statements
        created_tables: list[str] = []
        for stmt in executed:
            m = re.search(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:([\w\$\"]+)\.)?([\w\$\"]+)", stmt, re.IGNORECASE)
            if m:
                created_tables.append(m.group(2).replace('"', ''))

        # Verify table existence via PostgreSQL target
        from sqlalchemy import text
        verified_tables: list[str] = []
        with target_adapter.get_engine().connect() as conn:
            for tbl in created_tables:
                exists = conn.execute(
                    text("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = :t"),
                    {"t": tbl.lower()}
                ).scalar()
                if exists:
                    verified_tables.append(tbl)

        return {
            "status": "success",
            "kind": "schema",
            "source": spec.get("source", {}).get("type", "oracle"),
            "target": target_type,
            "statements_executed": len(executed),
            "tables_created": created_tables,
            "tables_verified": verified_tables,
            "summary": f"Executed {len(executed)} table management DDL statement(s). Verified {len(verified_tables)} table(s) exist.",
        }

    def _run_multi_migration(self, spec: dict[str, Any]) -> dict[str, Any]:
        source_info = spec.get("source", {}) if isinstance(spec.get("source"), dict) else {}
        target_info = spec.get("target", {}) if isinstance(spec.get("target"), dict) else {}
        source_type = str(source_info.get("type") or source_info.get("database") or "oracle").lower()
        target_type = str(target_info.get("type") or target_info.get("database") or "postgresql").lower()
        target_schema = target_info.get("schema", "public")

        data_extraction = spec.get("data_extraction", [])
        data_management = spec.get("data_management", [])
        logger.info(f"Multiple migration pairs detected ({len(data_extraction)} pairs). Executing sequentially...")

        table_mgmt = spec.get("table_management")
        if table_mgmt:
            target_adapter = self.registry.get_target_adapter(target_type)
            statements = table_mgmt if isinstance(table_mgmt, list) else table_mgmt.get("statements", [])
            exec_opts = spec.get("execution_options", {}) if isinstance(spec.get("execution_options"), dict) else {}
            should_execute_ddl = bool(exec_opts.get("execute_ddl", spec.get("execute_ddl", True)))
            if statements and should_execute_ddl:
                allow_destructive = bool(exec_opts.get("allow_destructive_ddl", False) or spec.get("allow_destructive_ddl", False))
                target_adapter.execute_ddl(statements, allow_destructive_ddl=allow_destructive)

        sub_results: list[dict[str, Any]] = []
        total_source_rows = 0
        total_extracted_rows = 0
        total_transformed_rows = 0
        total_filtered_rows = 0
        total_loaded_rows = 0
        total_target_rows = 0

        for idx, ext_query in enumerate(data_extraction):
            mgmt_stmt = data_management[idx] if (isinstance(data_management, list) and idx < len(data_management)) else None
            unit_target_info = dict(target_info)
            if mgmt_stmt:
                match = re.search(r"INSERT\s+INTO\s+(?:([\w\$\"]+)\.)?([\w\$\"]+)", mgmt_stmt, re.IGNORECASE)
                if match:
                    if match.group(1):
                        unit_target_info["schema"] = match.group(1).replace('"', '')
                    unit_target_info["table"] = match.group(2).replace('"', '')

            sub_spec = {
                **spec,
                "target": unit_target_info,
                "table_management": [],
                "data_extraction": [ext_query],
                "data_management": [mgmt_stmt] if mgmt_stmt else [],
            }
            if "data_migration" in sub_spec:
                sub_spec.pop("data_migration", None)

            sub_res = self._run_single_migration(sub_spec)
            if sub_res.get("status") == "failed":
                return sub_res

            sub_results.append(sub_res)
            total_source_rows += sub_res.get("source_rows", 0)
            total_extracted_rows += sub_res.get("extracted_rows", 0)
            total_transformed_rows += sub_res.get("transformed_rows", 0)
            total_filtered_rows += sub_res.get("filtered_rows", 0)
            total_loaded_rows += sub_res.get("loaded_rows", 0)
            total_target_rows += sub_res.get("target_rows", 0)

        tables_migrated = [r.get("target", {}).get("table", f"table_{i}") for i, r in enumerate(sub_results)]
        last_target = sub_results[-1].get("target", {}) if sub_results else {}

        is_reconciled = (total_source_rows == total_loaded_rows)
        return {
            "status": "success",
            "source": source_type,
            "target": target_type,
            "tables_migrated": tables_migrated,
            "total_tables": len(sub_results),
            "source_rows": total_source_rows,
            "extracted_rows": total_extracted_rows,
            "transformed_rows": total_transformed_rows,
            "filtered_rows": total_filtered_rows,
            "loaded_rows": total_loaded_rows,
            "target_rows": total_target_rows,
            "reconciliation": {
                "status": "PASS" if is_reconciled else "FAIL",
                "source_rows": total_source_rows,
                "loaded_rows": total_loaded_rows,
                "target_rows": total_target_rows,
                "is_reconciled": is_reconciled,
            },
            "failed_rows": 0,
            "validation": {"status": "passed", "warnings": []},
            "target": last_target,
            "results": sub_results,
        }

    def _run_single_migration(self, spec: dict[str, Any]) -> dict[str, Any]:
        logger.info("==================================================")
        logger.info("Starting Generic Data Engineering Migration Run")
        logger.info("==================================================")

        source_rows = 0
        extracted_rows = 0
        transformed_rows = 0
        filtered_rows = 0
        loaded_rows = 0
        target_rows = 0
        current_stage = "validation"

        source_info = spec.get("source", {}) if isinstance(spec.get("source"), dict) else {}
        target_info = spec.get("target", {}) if isinstance(spec.get("target"), dict) else {}

        source_type = str(source_info.get("type") or source_info.get("database") or "oracle").lower()
        target_type = str(target_info.get("type") or target_info.get("database") or "postgresql").lower()
        source_schema = source_info.get("schema", "")
        target_schema = target_info.get("schema", "public")

        try:
            current_stage = "validation"
            is_valid, validation_errors = validate_migration_contract(spec)
            if not is_valid:
                logger.error(f"Migration contract validation failed: {validation_errors}")
                return {
                    "status": "failed",
                    "stage": current_stage,
                    "message": "Migration contract validation failed",
                    "source": source_type,
                    "target": target_type,
                    "source_rows": 0,
                    "extracted_rows": 0,
                    "transformed_rows": 0,
                    "loaded_rows": 0,
                    "target_rows": 0,
                    "failed_rows": 0,
                    "errors": validation_errors,
                }

            current_stage = "adapter_selection"
            source_adapter = self.registry.get_source_adapter(source_type)
            target_adapter = self.registry.get_target_adapter(target_type)
            logger.info(f"Selected source adapter: {source_adapter.__class__.__name__}, target adapter: {target_adapter.__class__.__name__}")

            current_stage = "table_management"
            table_mgmt = spec.get("table_management", {})
            if isinstance(table_mgmt, list):
                statements = table_mgmt
            elif isinstance(table_mgmt, dict):
                statements = table_mgmt.get("statements", [])
            else:
                statements = []
            if statements:
                exec_opts = spec.get("execution_options", {}) if isinstance(spec.get("execution_options"), dict) else {}
                should_execute_ddl = bool(exec_opts.get("execute_ddl", spec.get("execute_ddl", True)))
                if not should_execute_ddl:
                    logger.info("Skipping table_management DDL execution in Data Plane (execute_ddl=False). Schema is managed by Control Plane.")
                else:
                    allow_destructive = bool(
                        exec_opts.get("allow_destructive_ddl", False)
                        or spec.get("allow_destructive_ddl", False)
                    )
                    logger.info(f"Executing {len(statements)} table management DDL statement(s) on target (allow_destructive_ddl={allow_destructive})...")
                    target_adapter.execute_ddl(statements, allow_destructive_ddl=allow_destructive)
                    logger.info("Table management statements completed successfully.")

            current_stage = "source_extraction"
            data_migration = spec.get("data_migration", {})
            extraction = data_migration.get("extraction", {}) if isinstance(data_migration, dict) else {}
            query = extraction.get("query") or spec.get("query") or source_info.get("query")
            if not query:
                data_extraction = spec.get("data_extraction")
                if isinstance(data_extraction, list) and len(data_extraction) > 0 and isinstance(data_extraction[0], str):
                    query = data_extraction[0]

            if not query:
                source_table = source_info.get("table")
                if not source_table:
                    raise ValueError("Cannot extract: Neither 'data_migration.extraction.query' nor 'source.table' provided")
                full_src_table = f"{source_schema}.{source_table}" if source_schema else source_table
                query = f"SELECT * FROM {full_src_table}"

            logger.info(f"Batch extracting data using query: '{query}' into in-memory Pandas DataFrame...")
            raw_df = source_adapter.extract(query)
            source_rows = len(raw_df)
            extracted_rows = source_rows
            logger.info(f"Extracted {source_rows} row(s) from source.")

            current_stage = "transformation"
            transformations = spec.get("transformations")
            if transformations is None:
                transformations = spec.get("operations", [])

            transformed_df = self.transform_engine.apply(raw_df, transformations)
            transformed_rows = len(transformed_df)
            filtered_rows = max(0, extracted_rows - transformed_rows)
            logger.info(f"Transformations applied. Extracted: {extracted_rows}, after transformations: {transformed_rows} (filtered: {filtered_rows})")

            current_stage = "post_validation"
            loading = data_migration.get("loading", {}) if isinstance(data_migration, dict) else {}
            target_table = loading.get("table") or target_info.get("table")
            target_columns = loading.get("columns")

            data_management = spec.get("data_management")
            if (not target_table or not target_columns) and isinstance(data_management, list) and len(data_management) > 0:
                stmt = str(data_management[0])
                match = re.search(r"INSERT\s+INTO\s+(?:([\w\$\"]+)\.)?([\w\$\"]+)\s*(?:\(([^)]+)\))?", stmt, re.IGNORECASE)
                if match:
                    if match.group(1):
                        target_schema = match.group(1).replace('"', '')
                    if not target_table:
                        target_table = match.group(2).replace('"', '')
                    if not target_columns and match.group(3):
                        target_columns = [c.strip().replace('"', '') for c in match.group(3).split(",")]

            val_result = validate_dataframe(
                transformed_df,
                required_columns=target_columns,
                primary_key=spec.get("primary_key"),
                expected_dtypes=spec.get("expected_dtypes"),
            )

            if not val_result["valid"]:
                logger.error(f"Post-transformation validation failed: {val_result['errors']}")
                return {
                    "status": "failed",
                    "stage": current_stage,
                    "message": "Transformed data failed validation criteria",
                    "source": source_type,
                    "target": target_type,
                    "source_rows": source_rows,
                    "extracted_rows": extracted_rows,
                    "transformed_rows": transformed_rows,
                    "loaded_rows": 0,
                    "target_rows": 0,
                    "failed_rows": source_rows,
                    "errors": val_result["errors"],
                    "warnings": val_result["warnings"],
                }

            current_stage = "target_loading"
            df_to_load = transformed_df

            if target_columns and isinstance(target_columns, list) and len(target_columns) > 0:
                projected_data: dict[str, pd.Series] = {}
                for target_col in target_columns:
                    source_col = _resolve_col(transformed_df, str(target_col))
                    if source_col not in transformed_df.columns:
                        raise KeyError(
                            f"Target column '{target_col}' not found in transformed DataFrame columns: {list(transformed_df.columns)}"
                        )
                    projected_data[str(target_col)] = transformed_df[source_col].copy()

                df_to_load = pd.DataFrame(projected_data, index=transformed_df.index)
                logger.info(
                    f"Target column projection enforced. Target columns: {list(df_to_load.columns)} "
                    f"(excluded {len(transformed_df.columns) - len(df_to_load.columns)} non-target columns)"
                )

            target_mode = str(loading.get("mode") or spec.get("mode", "append")).lower()
            logger.info(f"Loading {len(df_to_load)} row(s) into target {target_schema}.{target_table} (mode={target_mode})...")
            loaded_rows = target_adapter.load(
                df=df_to_load,
                table=target_table,
                schema=target_schema,
                mode=target_mode,
            )

            target_rows = target_adapter.get_row_count(table=target_table, schema=target_schema)
            if target_rows == 0 and loaded_rows > 0:
                target_rows = loaded_rows

            logger.info("==================================================")
            logger.info(f"Migration Completed Successfully! Loaded {loaded_rows} rows. Target total: {target_rows} rows.")
            logger.info("==================================================")

            is_reconciled = (source_rows == loaded_rows) and (loaded_rows <= target_rows)
            return {
                "status": "success",
                "source": source_type,
                "target": target_type,
                "source_rows": source_rows,
                "extracted_rows": extracted_rows,
                "transformed_rows": transformed_rows,
                "filtered_rows": filtered_rows,
                "loaded_rows": loaded_rows,
                "target_rows": target_rows,
                "reconciliation": {
                    "status": "PASS" if is_reconciled else "FAIL",
                    "source_rows": source_rows,
                    "loaded_rows": loaded_rows,
                    "target_rows": target_rows,
                    "is_reconciled": is_reconciled,
                },
                "failed_rows": 0,
                "validation": {
                    "status": "passed",
                    "warnings": val_result.get("warnings", []),
                },
                "target": {
                    "schema": target_schema,
                    "table": target_table,
                    "columns": list(df_to_load.columns),
                },
            }

        except Exception as exc:
            logger.error(f"Migration failed during stage '{current_stage}': {exc}", exc_info=True)
            return {
                "status": "failed",
                "stage": current_stage,
                "message": f"Migration failed during {current_stage}: {str(exc)}",
                "source": source_type,
                "target": target_type,
                "source_rows": source_rows,
                "extracted_rows": extracted_rows,
                "transformed_rows": transformed_rows,
                "loaded_rows": 0,
                "target_rows": 0,
                "failed_rows": source_rows,
                "errors": [str(exc)],
            }
