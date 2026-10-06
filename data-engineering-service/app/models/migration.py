from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class DatabaseEntity(BaseModel):
    """Represents a database entity (type, schema, table) dynamically."""
    type: str = Field(..., description="Database engine type: oracle, postgresql, etc.")
    schema_name: str = Field(..., alias="schema", description="Schema or database namespace name.")
    table: str = Field(..., description="Table name.")

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "type": "oracle",
                "schema": "HR",
                "table": "EMPLOYEES",
            }
        },
    }

    @property
    def full_name(self) -> str:
        return f"{self.type.lower()}.{self.schema_name}.{self.table}"

    @property
    def relation_name(self) -> str:
        return f"{self.schema_name}.{self.table}"


class MigrationRequest(BaseModel):
    """Request model for dynamic SeaTunnel migration execution."""
    source: DatabaseEntity
    target: DatabaseEntity
    seatunnel_config: str | None = Field(default=None, description="Optional custom seatunnel.conf path")
    options: dict[str, Any] = Field(default_factory=dict, description="Runtime migration flags or overrides")

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
                "seatunnel_config": "path/to/seatunnel.conf",
            }
        }
    }


class MigrationResponse(BaseModel):
    """Response model for migration execution status."""
    migration_id: str
    status: str = Field(..., description="'completed' or 'failed'")
    source: str
    target: str
    error: str | None = None
    exit_code: int | None = None
    rows_migrated: int | None = None
    duration_seconds: float | None = None
    log_file: str | None = None
    details: dict[str, Any] | None = None
