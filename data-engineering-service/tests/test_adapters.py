import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from app.adapters.base import BaseSourceAdapter, BaseTargetAdapter
from app.adapters.registry import AdapterRegistry
from app.adapters.oracle_adapter import OracleSourceAdapter
from app.adapters.postgres_adapter import PostgresTargetAdapter


def test_adapter_registry_resolution():
    registry = AdapterRegistry()
    registry.register_source("oracle", OracleSourceAdapter)
    registry.register_target("postgresql", PostgresTargetAdapter)

    src = registry.get_source_adapter("oracle")
    assert isinstance(src, OracleSourceAdapter)

    tgt = registry.get_target_adapter("postgresql")
    assert isinstance(tgt, PostgresTargetAdapter)

    tgt_alias = registry.get_target_adapter("postgres")
    assert isinstance(tgt_alias, PostgresTargetAdapter)

    tgt_pg = registry.get_target_adapter("pg")
    assert isinstance(tgt_pg, PostgresTargetAdapter)


def test_adapter_registry_unsupported():
    registry = AdapterRegistry()
    with pytest.raises(ValueError, match="Unsupported source"):
        registry.get_source_adapter("unsupported_db")

    with pytest.raises(ValueError, match="Unsupported target"):
        registry.get_target_adapter("unsupported_db")


def test_oracle_adapter_mocked_extract():
    adapter = OracleSourceAdapter()
    mock_df = pd.DataFrame({"ID": [101, 102], "NAME": ["A", "B"]})

    with patch("pandas.read_sql_query", return_value=mock_df):
        with patch.object(adapter, "get_engine"):
            res = adapter.extract("SELECT * FROM TEST")
            assert len(res) == 2
            assert list(res["ID"]) == [101, 102]


def test_postgres_adapter_mocked_load():
    adapter = PostgresTargetAdapter()
    df = pd.DataFrame({"id": [1, 2], "val": ["x", "y"]})

    with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
        with patch.object(adapter, "get_engine"):
            loaded = adapter.load(df, table="test_table", schema="public", mode="append")
            assert loaded == 2
            mock_to_sql.assert_called_once()


def test_postgres_adapter_ddl_safety_checks():
    adapter = PostgresTargetAdapter()
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    with patch.object(adapter, "get_engine", return_value=mock_engine):
        executed = adapter.execute_ddl(["CREATE TABLE test (id INT)"])
        assert len(executed) == 1

        with pytest.raises(ValueError, match="Destructive DDL statement rejected"):
            adapter.execute_ddl(["DROP TABLE test"], allow_destructive_ddl=False)

        with pytest.raises(ValueError, match="Destructive DDL statement rejected"):
            adapter.execute_ddl(["TRUNCATE TABLE test"], allow_destructive_ddl=False)

        with pytest.raises(ValueError, match="Destructive DDL statement rejected"):
            adapter.execute_ddl(["DROP SCHEMA test CASCADE"], allow_destructive_ddl=False)

        executed_dest = adapter.execute_ddl(["DROP TABLE test"], allow_destructive_ddl=True)
        assert len(executed_dest) == 1

        with pytest.raises(ValueError, match="Prohibited DDL statement rejected"):
            adapter.execute_ddl(["DROP DATABASE test_db"], allow_destructive_ddl=True)


def test_postgres_adapter_get_row_count():
    adapter = PostgresTargetAdapter()
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = 42
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    with patch.object(adapter, "get_engine", return_value=mock_engine):
        count = adapter.get_row_count("my_table", "public")
        assert count == 42
