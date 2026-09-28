from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any


class CDCOperation(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    DDL = "DDL"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"
    OTHER = "OTHER"


@dataclass
class CDCEvent:
    """
    Normalized internal Change Data Capture event representation.
    Decoupled from specific capture transport (LogMiner, Debezium, etc.).
    """
    scn: int
    operation: CDCOperation | str
    source_schema: str
    source_table: str
    primary_key: dict[str, Any] = field(default_factory=dict)
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    timestamp: str | None = None
    transaction_id: str | None = None
    raw_sql: str | None = None
    sequence: int = 0

    def __post_init__(self) -> None:
        if isinstance(self.operation, CDCOperation):
            self.operation = self.operation.value
        else:
            self.operation = str(self.operation).upper()

    def to_dict(self) -> dict[str, Any]:
        return {
            "scn": self.scn,
            "operation": self.operation,
            "source_schema": self.source_schema,
            "source_table": self.source_table,
            "primary_key": self.primary_key,
            "before": self.before,
            "after": self.after,
            "timestamp": self.timestamp,
            "transaction_id": self.transaction_id,
            "raw_sql": self.raw_sql,
            "sequence": self.sequence,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CDCEvent:
        return cls(
            scn=int(data["scn"]),
            operation=data["operation"],
            source_schema=data.get("source_schema", ""),
            source_table=data.get("source_table", ""),
            primary_key=data.get("primary_key", {}) or {},
            before=data.get("before"),
            after=data.get("after"),
            timestamp=data.get("timestamp"),
            transaction_id=data.get("transaction_id"),
            raw_sql=data.get("raw_sql"),
            sequence=int(data.get("sequence", 0)),
        )


@dataclass
class CDCWindowResult:
    """
    Metrics and audit summary for a CDC processing window.
    """
    pipeline_name: str
    start_scn: int
    end_scn: int
    events_captured: int = 0
    events_applied: int = 0
    events_failed: int = 0
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    last_checkpoint_scn: int | None = None
    status: str = "PASS"
    error: str | None = None
    events: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "start_scn": self.start_scn,
            "end_scn": self.end_scn,
            "events_captured": self.events_captured,
            "events_applied": self.events_applied,
            "events_failed": self.events_failed,
            "inserts": self.inserts,
            "updates": self.updates,
            "deletes": self.deletes,
            "last_checkpoint_scn": self.last_checkpoint_scn,
            "status": self.status,
            "error": self.error,
            "events": self.events,
        }


@dataclass
class CDCStatus:
    pipeline_name: str
    last_checkpoint_scn: int | None
    current_oracle_scn: int | None
    lag_scn: int | None
    updated_at: str | None
    is_running: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
