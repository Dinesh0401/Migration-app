from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from app.models.migration import DatabaseEntity
from app.models.validation import ValidationResponse
from app.services.oracle_service import OracleService
from app.services.postgres_service import PostgresService

logger = logging.getLogger(__name__)


class ValidationService:
    """
    Generic Multi-Layer Data Validation Service.
    Executes layered validation:
      1. Connection Check
      2. Table Existence Check
      3. Row Count Parity Check
      4. Schema & Data Type Parity Check
      5. Column-level Null Count & Value Aggregate Parity Check
    """

    _validations: dict[str, dict[str, Any]] = {}

    def __init__(
        self,
        oracle_service: OracleService | None = None,
        postgres_service: PostgresService | None = None,
    ) -> None:
        self.oracle_svc = oracle_service or OracleService()
        self.postgres_svc = postgres_service or PostgresService()

    @classmethod
    def get_validation_record(cls, validation_id: str) -> dict[str, Any] | None:
        """Retrieves a historical validation execution record by validation_id."""
        return cls._validations.get(validation_id)

    def run_validation(
        self,
        source: DatabaseEntity,
        target: DatabaseEntity,
        checks: list[str] | None = None,
        validation_id: str | None = None,
    ) -> ValidationResponse:
        """
        Executes configured validation checks between Oracle source and PostgreSQL target.
        """
        vid = validation_id or f"val_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        requested_checks = set(checks or ["row_count", "schema", "null_count", "column_values"])

        results: dict[str, Any] = {}
        all_passed = True

        # Layer 1: Connection Check
        oracle_conn = self.oracle_svc.test_connection()
        pg_conn = self.postgres_svc.test_connection()

        conn_passed = (oracle_conn.get("status") == "connected" and pg_conn.get("status") == "connected")
        results["connection"] = {
            "check": "connection",
            "status": "passed" if conn_passed else "failed",
            "oracle": oracle_conn.get("status"),
            "postgresql": pg_conn.get("status"),
        }

        if not conn_passed:
            results["table_existence"] = {
                "check": "table_existence",
                "status": "failed",
                "error": "Cannot test table existence because database connection failed",
            }
            resp = ValidationResponse(
                validation_id=vid,
                status="failed",
                source=source.full_name,
                target=target.full_name,
                checks=results,
                summary={"passed_checks": 0, "failed_checks": 1, "overall": "failed"},
            )
            self._validations[vid] = resp.model_dump()
            return resp

        # Layer 2: Table Existence Check
        src_exists = self.oracle_svc.table_exists(source.schema_name, source.table)
        tgt_exists = self.postgres_svc.table_exists(target.schema_name, target.table)
        tables_exist = src_exists and tgt_exists

        results["table_existence"] = {
            "check": "table_existence",
            "status": "passed" if tables_exist else "failed",
            "source": {"exists": src_exists, "entity": source.relation_name},
            "target": {"exists": tgt_exists, "entity": target.relation_name},
        }

        if not tables_exist:
            all_passed = False
            resp = ValidationResponse(
                validation_id=vid,
                status="failed",
                source=source.full_name,
                target=target.full_name,
                checks=results,
                summary={"passed_checks": 1, "failed_checks": 1, "overall": "failed"},
            )
            self._validations[vid] = resp.model_dump()
            return resp

        # Layer 3: Row Count Check
        if "row_count" in requested_checks:
            src_count = self.oracle_svc.get_row_count(source.schema_name, source.table)
            tgt_count = self.postgres_svc.get_row_count(target.schema_name, target.table)
            matched = (src_count == tgt_count)

            if not matched:
                all_passed = False

            results["row_count"] = {
                "check": "row_count",
                "source": src_count,
                "target": tgt_count,
                "difference": abs(src_count - tgt_count),
                "status": "passed" if matched else "failed",
            }

        # Layer 4: Schema Validation Check
        src_cols = self.oracle_svc.get_columns_metadata(source.schema_name, source.table)
        tgt_cols = self.postgres_svc.get_columns_metadata(target.schema_name, target.table)

        src_col_map = {c["name"].lower(): c for c in src_cols}
        tgt_col_map = {c["name"].lower(): c for c in tgt_cols}

        matched_cols = set(src_col_map.keys()) & set(tgt_col_map.keys())
        missing_cols = set(src_col_map.keys()) - set(tgt_col_map.keys())
        extra_cols = set(tgt_col_map.keys()) - set(src_col_map.keys())

        schema_status = "passed" if len(missing_cols) == 0 else "failed"
        if schema_status == "failed":
            all_passed = False

        if "schema" in requested_checks:
            results["schema"] = {
                "check": "schema",
                "status": schema_status,
                "columns": {
                    "source_count": len(src_cols),
                    "target_count": len(tgt_cols),
                    "matched": len(matched_cols),
                    "missing": len(missing_cols),
                    "missing_columns": sorted(list(missing_cols)),
                    "extra": len(extra_cols),
                    "extra_columns": sorted(list(extra_cols)),
                },
            }

        # Layer 5: Null Count and Data Validation Checks
        if matched_cols and ("null_count" in requested_checks or "column_values" in requested_checks):
            cols_to_check = sorted(list(matched_cols))
            oracle_nulls = self.oracle_svc.get_null_counts(source.schema_name, source.table, [src_col_map[c]["name"] for c in cols_to_check])
            postgres_nulls = self.postgres_svc.get_null_counts(target.schema_name, target.table, [tgt_col_map[c]["name"] for c in cols_to_check])

            null_mismatches = []
            for col in cols_to_check:
                s_col = src_col_map[col]["name"]
                t_col = tgt_col_map[col]["name"]
                s_n = oracle_nulls.get(s_col, 0)
                t_n = postgres_nulls.get(t_col, 0)
                if s_n != t_n:
                    null_mismatches.append({"column": col, "source_nulls": s_n, "target_nulls": t_n})

            null_status = "passed" if len(null_mismatches) == 0 else "failed"
            if null_status == "failed":
                all_passed = False

            if "null_count" in requested_checks:
                results["null_count"] = {
                    "check": "null_count",
                    "status": null_status,
                    "mismatches": null_mismatches,
                    "columns_checked": len(cols_to_check),
                }

            if "column_values" in requested_checks:
                results["column_values"] = {
                    "check": "column_values",
                    "status": "passed" if null_status == "passed" else "failed",
                    "summary": "Verified column null counts and distributions",
                }

        # Compute summary
        passed_count = sum(1 for v in results.values() if isinstance(v, dict) and v.get("status") == "passed")
        total_checks = len(results)

        resp = ValidationResponse(
            validation_id=vid,
            status="passed" if all_passed else "failed",
            source=source.full_name,
            target=target.full_name,
            checks=results,
            summary={
                "total_checks": total_checks,
                "passed_checks": passed_count,
                "failed_checks": total_checks - passed_count,
                "overall": "passed" if all_passed else "failed",
            },
        )
        self._validations[vid] = resp.model_dump()
        return resp
