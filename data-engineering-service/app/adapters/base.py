from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
import pandas as pd


class BaseSourceAdapter(ABC):
    """
    Abstract base class for all source database adapters (e.g., Oracle, MySQL).
    Defines the contract for extracting data into standard in-memory Pandas DataFrames.
    """

    @abstractmethod
    def extract(self, query: str) -> pd.DataFrame:
        """
        Executes an extraction query on the source database and returns
        the result as a standardized in-memory Pandas DataFrame.
        """
        pass

    @abstractmethod
    def test_connection(self) -> dict[str, Any]:
        """
        Tests connectivity to the source database and returns diagnostic information.
        """
        pass


class BaseTargetAdapter(ABC):
    """
    Abstract base class for all target database adapters (e.g., PostgreSQL).
    Defines the contract for executing DDL, loading DataFrames, and verifying counts.
    """

    @abstractmethod
    def execute_ddl(self, statements: list[str], allow_destructive_ddl: bool = False) -> list[str]:
        """
        Executes DDL statements (e.g., CREATE, ALTER) on the target database.
        Raises ValueError if destructive statements are detected without permission.
        """
        pass

    @abstractmethod
    def load(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = "public",
        mode: str = "append",
    ) -> int:
        """
        Loads a Pandas DataFrame into the target table using parameterized bulk loading.
        Returns the number of rows loaded.
        """
        pass

    @abstractmethod
    def get_row_count(self, table: str, schema: str = "public") -> int:
        """
        Queries and returns the current row count of the specified target table.
        """
        pass

    def verify_data(self, table: str, schema: str = "public", limit: int = 5) -> pd.DataFrame:
        """
        Queries and returns sample rows from the target table for live data verification.
        """
        return pd.DataFrame()

    @abstractmethod
    def test_connection(self) -> dict[str, Any]:
        """
        Tests connectivity to the target database and returns diagnostic information.
        """
        pass
