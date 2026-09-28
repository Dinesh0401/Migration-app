from __future__ import annotations

from typing import Type
from app.adapters.base import BaseSourceAdapter, BaseTargetAdapter
from app.adapters.oracle_adapter import OracleSourceAdapter
from app.adapters.postgres_adapter import PostgresTargetAdapter


class AdapterRegistry:
    """
    Central registry for source and target database adapters.
    Allows dynamic adapter resolution based on database type from the migration contract.
    Enables future adapters (e.g. MySQL, MongoDB) to be registered without rewriting the engine.
    """

    def __init__(self) -> None:
        self._source_adapters: dict[str, Type[BaseSourceAdapter]] = {}
        self._target_adapters: dict[str, Type[BaseTargetAdapter]] = {}

    @staticmethod
    def _normalize_name(name: str) -> str:
        return name.strip().lower() if name else ""

    def register_source(self, db_type: str, adapter_class: Type[BaseSourceAdapter]) -> None:
        key = self._normalize_name(db_type)
        self._source_adapters[key] = adapter_class

    def register_target(self, db_type: str, adapter_class: Type[BaseTargetAdapter]) -> None:
        key = self._normalize_name(db_type)
        self._target_adapters[key] = adapter_class

    def get_source_adapter(self, db_type: str) -> BaseSourceAdapter:
        key = self._normalize_name(db_type)
        if key in ("pg", "postgres"):
            key = "postgresql"

        adapter_class = self._source_adapters.get(key)
        if not adapter_class:
            supported = list(self._source_adapters.keys())
            raise ValueError(f"Unsupported source database type: '{db_type}'. Supported sources: {supported}")

        return adapter_class()

    def get_target_adapter(self, db_type: str) -> BaseTargetAdapter:
        key = self._normalize_name(db_type)
        if key in ("pg", "postgres"):
            key = "postgresql"

        adapter_class = self._target_adapters.get(key)
        if not adapter_class:
            supported = list(self._target_adapters.keys())
            raise ValueError(f"Unsupported target database type: '{db_type}'. Supported targets: {supported}")

        return adapter_class()

    def get_supported_sources(self) -> list[str]:
        return list(self._source_adapters.keys())

    def get_supported_targets(self) -> list[str]:
        return list(self._target_adapters.keys())


default_registry = AdapterRegistry()
default_registry.register_source("oracle", OracleSourceAdapter)
default_registry.register_target("postgresql", PostgresTargetAdapter)
