from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field
from app.models.migration import DatabaseEntity


class ValidationRequest(BaseModel):
    """Request model for dynamic data validation execution."""
    source: DatabaseEntity
    target: DatabaseEntity
    checks: list[str] = Field(
        default=["row_count", "schema", "null_count", "column_values"],
        description="List of validation checks to run: row_count, schema, null_count, column_values",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "source": {
                    "type": "oracle",
                    "schema": "HR",
                    "table": "EMPLOYEES",
                },
                "target": {
                    "type": "postgresql",
                    "schema": "migration",
                    "table": "employees",
                },
                "checks": [
                    "row_count",
                    "schema",
                    "null_count",
                    "column_values",
                ],
            }
        }
    }


class RowCountCheckResult(BaseModel):
    check: str = "row_count"
    source: int
    target: int
    status: str = Field(..., description="'passed' or 'failed'")
    difference: int = 0


class SchemaCheckResult(BaseModel):
    check: str = "schema"
    status: str = Field(..., description="'passed' or 'failed'")
    columns: dict[str, Any] = Field(
        ...,
        description="Matched, missing, extra column count and details",
        json_schema_extra={"example": {"matched": 11, "missing": 0, "extra": 0}},
    )


class DataCheckResult(BaseModel):
    check: str
    status: str = Field(..., description="'passed' or 'failed'")
    details: dict[str, Any] = Field(default_factory=dict)


class ValidationResponse(BaseModel):
    validation_id: str
    status: str = Field(..., description="'passed' or 'failed'")
    source: str
    target: str
    checks: dict[str, Any]
    summary: dict[str, Any]


class CombinedMigrationValidationResponse(BaseModel):
    migration_id: str
    migration: dict[str, Any]
    validation: dict[str, Any] | None = None
    overall_status: str = Field(
        ...,
        description="'completed', 'migration_failed', or 'validation_failed'",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "migration_id": "mig_20261005_001",
                "overall_status": "completed",
                "migration": {
                    "status": "completed",
                    "source": "oracle.HR.EMPLOYEES",
                    "target": "postgresql.migration.employees",
                },
                "validation": {
                    "status": "passed",
                    "row_count": {
                        "source": 107,
                        "target": 107,
                        "matched": True,
                    },
                    "schema": {
                        "matched": True,
                    },
                },
            }
        }
    }
