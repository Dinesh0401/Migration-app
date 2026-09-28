from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
from app.cdc.models import CDCEvent


class BaseCDCSource(ABC):
    """
    Abstract interface for Change Data Capture sources.
    Allows alternative CDC backends (e.g. Debezium, Kafka, Polling) to be plugged
    into the Data Plane without altering downstream transformation or writer logic.
    """

    @abstractmethod
    def get_current_scn(self) -> int:
        """Returns the current System Change Number (SCN) or offset from the source."""
        pass

    @abstractmethod
    def check_prerequisites(self) -> dict[str, Any]:
        """
        Validates whether all source prerequisites (permissions, supplemental logging,
        redo log availability) are met.
        """
        pass

    @abstractmethod
    def read_changes(
        self,
        start_scn: int,
        end_scn: int | None = None,
        table_filter: list[str] | None = None,
    ) -> list[CDCEvent]:
        """
        Reads committed DML changes within the SCN window [start_scn, end_scn].
        Returns normalized CDCEvents ordered by SCN.
        """
        pass

    def start_capture(self, start_scn: int | None = None) -> None:
        """Optional lifecycle hook called before reading stream."""
        pass

    def stop_capture(self) -> None:
        """Optional lifecycle hook called to release resources."""
        pass
