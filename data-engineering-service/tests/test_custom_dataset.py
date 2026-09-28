"""
Tests demonstrating dataset-independence of the Data Plane pipeline.
Verifies that the migration engine, validator, and transformation engine
seamlessly execute migrations on non-HR datasets and varied AI contracts.
"""
from __future__ import annotations

import pandas as pd
import pytest

from app.adapters.registry import AdapterRegistry
from app.adapters.base import BaseSourceAdapter, BaseTargetAdapter
from app.services.migration_service import MigrationService
from app.validation.validator import validate_migration_contract


class MockCatalogSourceAdapter(BaseSourceAdapter):
    """Simulates a generic product catalog source database."""

    def __init__(self) -> None:
        self.products = pd.DataFrame({
            "SKU_CODE": ["PRD-001", "PRD-002", "PRD-003", "PRD-004"],
            "RAW_TITLE": ["  Wireless Noise-Canceling Headphones  ", "Mechanical Keyboard ", " 4K Monitor", "USB-C Cable"],
            "BASE_PRICE": [299.999, 129.50, 499.00, 19.95],
            "STOCK_QTY": [15, 0, 5, 120],
            "CATEGORY_TAG": ["electronics", "electronics", "peripherals", "accessories"],
        })

    def extract(self, query: str) -> pd.DataFrame:
        return self.products.copy()

    def test_connection(self) -> dict:
        return {"connected": True, "database": "catalog_db"}


class MockWarehouseTargetAdapter(BaseTargetAdapter):
    """Simulates a generic data warehouse target database."""

    def __init__(self) -> None:
        self.executed_ddl: list[str] = []
        self.loaded_records: pd.DataFrame = pd.DataFrame()

    def execute_ddl(self, statements: list[str], allow_destructive_ddl: bool = False) -> list[str]:
        for stmt in statements:
            self.executed_ddl.append(stmt.strip())
        return statements

    def load(self, df: pd.DataFrame, table: str, schema: str = "public", mode: str = "append") -> int:
        self.loaded_records = df.copy()
        return len(df)

    def get_row_count(self, table: str, schema: str = "public") -> int:
        return len(self.loaded_records)

    def verify_data(self, table: str, schema: str = "public", limit: int = 5) -> pd.DataFrame:
        return self.loaded_records.head(limit)

    def test_connection(self) -> dict:
        return {"connected": True, "database": "warehouse_db"}


def test_ecommerce_custom_dataset_pipeline():
    """
    Validates the end-to-end execution of a non-HR e-commerce dataset:
    SKU catalog -> generic transformations -> warehouse target loading & reconciliation.
    """
    ai_contract = {
        "source": {"type": "oracle", "schema": "INVENTORY", "table": "PRODUCTS"},
        "target": {"type": "postgresql", "schema": "analytics", "table": "dim_products"},
        "execution_options": {"allow_destructive_ddl": True},
        "table_management": [
            """
            CREATE TABLE IF NOT EXISTS analytics.dim_products (
                sku VARCHAR(30) PRIMARY KEY,
                product_name VARCHAR(150),
                price NUMERIC(10, 2),
                stock INT,
                availability VARCHAR(20)
            )
            """,
            "ALTER TABLE analytics.dim_products ADD COLUMN IF NOT EXISTS inventory_status VARCHAR(20)",
        ],
        "data_extraction": [
            "SELECT SKU_CODE, RAW_TITLE, BASE_PRICE, STOCK_QTY, CATEGORY_TAG FROM INVENTORY.PRODUCTS WHERE STOCK_QTY >= 0"
        ],
        "transformations": [
            {"type": "rename", "source": "SKU_CODE", "target": "sku"},
            {"type": "normalize", "source": "RAW_TITLE", "target": "product_name", "strip": True, "case": "title"},
            {"type": "round", "source": "BASE_PRICE", "target": "price", "decimals": 2},
            {"type": "cast", "source": "STOCK_QTY", "target": "stock", "dtype": "int"},
            {
                "type": "derive",
                "target": "availability",
                "source": "STOCK_QTY",
                "condition": {
                    "operator": "gt",
                    "value": 0,
                    "then": "IN_STOCK",
                    "else": "OUT_OF_STOCK",
                },
            },
        ],
        "data_management": [
            "INSERT INTO analytics.dim_products (sku, product_name, price, stock, availability) VALUES {VALUES_PLACEHOLDER};"
        ],
        "placeholder": "{VALUES_PLACEHOLDER}",
    }

    is_valid, errors = validate_migration_contract(ai_contract)
    assert is_valid is True, f"Contract validation failed: {errors}"

    registry = AdapterRegistry()
    mock_src = MockCatalogSourceAdapter()
    mock_tgt = MockWarehouseTargetAdapter()
    registry.register_source("oracle", lambda: mock_src)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)
    result = service.run_migration(ai_contract)

    assert result["status"] == "success", f"Pipeline failed: {result}"
    assert result["source_rows"] == 4
    assert result["extracted_rows"] == 4
    assert result["transformed_rows"] == 4
    assert result["loaded_rows"] == 4
    assert result["target_rows"] == 4
    assert result["reconciliation"]["is_reconciled"] is True
    assert result["reconciliation"]["status"] == "PASS"

    loaded = mock_tgt.loaded_records
    assert list(loaded.columns) == ["sku", "product_name", "price", "stock", "availability"]
    assert loaded.loc[0, "sku"] == "PRD-001"
    assert loaded.loc[0, "product_name"] == "Wireless Noise-Canceling Headphones"
    assert loaded.loc[0, "price"] == 300.0
    assert loaded.loc[0, "stock"] == 15
    assert loaded.loc[0, "availability"] == "IN_STOCK"
    assert loaded.loc[1, "availability"] == "OUT_OF_STOCK"


def test_sensor_telemetry_custom_dataset():
    """
    Validates IoT/sensor telemetry dataset with filtering, calculations, and column projection.
    """
    telemetry_data = pd.DataFrame({
        "DEVICE_ID": ["SN-101", "SN-102", "SN-103", "SN-104"],
        "CELSIUS_TEMP": [22.5, 85.0, 19.8, -5.0],
        "VOLTAGE": [3.31, 3.28, 2.95, 3.30],
        "BATTERY_PCT": [95, 80, 12, 100],
    })

    ai_contract = {
        "source": {"type": "oracle", "schema": "IOT", "table": "SENSOR_READINGS"},
        "target": {"type": "postgresql", "schema": "telemetry", "table": "alerts"},
        "data_migration": {
            "extraction": {"query": "SELECT * FROM IOT.SENSOR_READINGS"},
            "loading": {"table": "alerts", "columns": ["device_id", "temp_f", "alert_level"]},
            "mode": "append",
        },
        "transformations": [
            {"type": "normalize_columns", "case": "lower"},
            {"type": "filter", "column": "celsius_temp", "operator": "gte", "value": 0},
            {"type": "derive", "source": "celsius_temp", "target": "c_scaled", "expression": "multiply", "value": 1.8},
            {"type": "derive", "source": "c_scaled", "target": "temp_f", "expression": "add", "value": 32.0},
            {"type": "round", "source": "temp_f", "decimals": 1},
            {
                "type": "derive",
                "source": "celsius_temp",
                "target": "alert_level",
                "condition": {
                    "operator": "gt",
                    "value": 50.0,
                    "then": "CRITICAL_HEAT",
                    "else": "NORMAL",
                },
            },
        ],
    }

    is_valid, errors = validate_migration_contract(ai_contract)
    assert is_valid is True

    class MockTelemetrySource(BaseSourceAdapter):
        def extract(self, query: str) -> pd.DataFrame:
            return telemetry_data.copy()
        def test_connection(self) -> dict:
            return {"connected": True}

    mock_tgt = MockWarehouseTargetAdapter()
    registry = AdapterRegistry()
    registry.register_source("oracle", MockTelemetrySource)
    registry.register_target("postgresql", lambda: mock_tgt)

    service = MigrationService(registry=registry)
    result = service.run_migration(ai_contract)

    assert result["status"] == "success"
    assert result["source_rows"] == 4
    assert result["extracted_rows"] == 4
    assert result["transformed_rows"] == 3
    assert result["filtered_rows"] == 1
    assert result["loaded_rows"] == 3

    loaded = mock_tgt.loaded_records
    assert list(loaded.columns) == ["device_id", "temp_f", "alert_level"]
    sn102 = loaded[loaded["device_id"] == "SN-102"].iloc[0]
    assert sn102["alert_level"] == "CRITICAL_HEAT"
    assert sn102["temp_f"] == 185.0
