from __future__ import annotations

import logging
from typing import Any
from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from app.adapters.registry import default_registry
from app.adapters.postgres_adapter import PostgresTargetAdapter
from app.cdc.models import CDCEvent, CDCOperation

logger = logging.getLogger(__name__)


class PostgresCDCWriter:
    """
    Idempotent PostgreSQL writer for normalized CDC events.
    Applies INSERT, UPDATE, and DELETE operations using primary keys.
    Supports PostgreSQL UPSERT (ON CONFLICT DO UPDATE) for idempotent delivery.

    ARCHITECTURAL LIMITATION NOTE:
    CDC replay is fully idempotent ONLY when target tables have a stable primary key.
    Without a primary key:
      - INSERT replay may duplicate rows (fallback is raw INSERT without ON CONFLICT).
      - DELETE cannot reliably identify target rows (raises ValueError).
    This is an inherent relational CDC constraint; target tables must define a primary key
    for lossless, idempotent replay.
    """

    def __init__(self, target_adapter: PostgresTargetAdapter | None = None) -> None:
        self.adapter = target_adapter or default_registry.get_target_adapter("postgresql")
        self._target_columns_cache: dict[str, list[str]] = {}
        self._pk_columns_cache: dict[str, list[str]] = {}

    def get_engine(self) -> Engine:
        return self.adapter.get_engine()

    def get_table_columns(self, table: str, schema: str = "public") -> list[str]:
        """Inspects and caches target table column names from PostgreSQL information_schema."""
        cache_key = f"{schema}.{table}".lower()
        if cache_key in self._target_columns_cache:
            return self._target_columns_cache[cache_key]

        query = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
        """)
        with self.get_engine().connect() as conn:
            cols = conn.execute(query, {"schema": schema.lower(), "table": table.lower()}).scalars().all()
            col_list = [str(c) for c in cols]
            self._target_columns_cache[cache_key] = col_list
            return col_list

    def get_primary_key_columns(self, table: str, schema: str = "public") -> list[str]:
        """Inspects and caches primary key columns for target table."""
        cache_key = f"{schema}.{table}".lower()
        if cache_key in self._pk_columns_cache:
            return self._pk_columns_cache[cache_key]

        query = text("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = :schema
          AND tc.table_name = :table
        ORDER BY kcu.ordinal_position
        """)
        with self.get_engine().connect() as conn:
            pks = conn.execute(query, {"schema": schema.lower(), "table": table.lower()}).scalars().all()
            pk_list = [str(c) for c in pks]
            self._pk_columns_cache[cache_key] = pk_list
            return pk_list

    def _map_payload_to_target(
        self,
        payload: dict[str, Any],
        target_columns: list[str],
    ) -> dict[str, Any]:
        """
        Maps source dictionary keys to target column names (case-insensitively).
        """
        mapped: dict[str, Any] = {}
        payload_lower = {k.lower(): v for k, v in payload.items()}
        for col in target_columns:
            if col.lower() in payload_lower:
                mapped[col] = payload_lower[col.lower()]
        return mapped

    def apply_event(
        self,
        event: CDCEvent,
        target_table: str,
        target_schema: str = "public",
        transformed_row: dict[str, Any] | None = None,
        conn: Connection | None = None,
    ) -> bool:
        """
        Applies a single CDCEvent to PostgreSQL.
        Returns True on successful apply, raises exception on failure.
        Accepts optional transaction connection `conn` for atomic multi-event transactions.
        """
        op = str(event.operation).upper()
        if op == CDCOperation.DDL.value:
            logger.info(f"[TARGET] Skipping DDL event at SCN {event.scn}")
            return True

        target_columns = self.get_table_columns(target_table, target_schema)
        pk_columns = self.get_primary_key_columns(target_table, target_schema)

        if not pk_columns and event.primary_key:
            # Fall back to primary key specified in event
            pk_columns = [k.lower() for k in event.primary_key.keys()]

        # Determine payload
        payload = transformed_row if transformed_row is not None else (event.after or event.before or {})
        mapped_data = self._map_payload_to_target(payload, target_columns)

        if op in (CDCOperation.INSERT.value, CDCOperation.UPDATE.value):
            return self._apply_upsert(target_table, target_schema, mapped_data, pk_columns, conn=conn)
        elif op == CDCOperation.DELETE.value:
            return self._apply_delete(target_table, target_schema, mapped_data, event.primary_key, pk_columns, conn=conn)
        else:
            logger.warning(f"[TARGET] Unsupported CDC operation '{op}' at SCN {event.scn}")
            return False

    def _apply_upsert(
        self,
        table: str,
        schema: str,
        data: dict[str, Any],
        pk_columns: list[str],
        conn: Connection | None = None,
    ) -> bool:
        if not data:
            logger.warning(f"[TARGET] Empty data payload for upsert into {schema}.{table}")
            return False

        cols = list(data.keys())
        col_names = ", ".join([f'"{c}"' for c in cols])
        param_names = ", ".join([f":{c}" for c in cols])

        matched_pks = [pk for pk in pk_columns if pk in cols]

        if matched_pks:
            pk_names = ", ".join([f'"{pk}"' for pk in matched_pks])
            update_assignments = ", ".join(
                [f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in matched_pks]
            )
            if update_assignments:
                sql = f"""
                INSERT INTO "{schema}"."{table}" ({col_names})
                VALUES ({param_names})
                ON CONFLICT ({pk_names})
                DO UPDATE SET {update_assignments};
                """
            else:
                # All columns are primary key
                sql = f"""
                INSERT INTO "{schema}"."{table}" ({col_names})
                VALUES ({param_names})
                ON CONFLICT ({pk_names}) DO NOTHING;
                """
        else:
            # Fallback when no PK detected on target:
            # NOTE: Without a primary key, ON CONFLICT cannot be constructed.
            # Replay of an INSERT event will append a duplicate row.
            sql = f"""
            INSERT INTO "{schema}"."{table}" ({col_names})
            VALUES ({param_names});
            """

        if conn is not None:
            conn.execute(text(sql), data)
        else:
            with self.get_engine().begin() as c:
                c.execute(text(sql), data)
        return True

    def _apply_delete(
        self,
        table: str,
        schema: str,
        data: dict[str, Any],
        event_pk: dict[str, Any],
        pk_columns: list[str],
        conn: Connection | None = None,
    ) -> bool:
        # Build where condition from primary key
        where_parts: list[str] = []
        params: dict[str, Any] = {}

        # Look in pk_columns first
        for pk in pk_columns:
            val = None
            if pk in data:
                val = data[pk]
            else:
                for k, v in event_pk.items():
                    if k.lower() == pk.lower():
                        val = v
                        break
            if val is not None:
                where_parts.append(f'"{pk}" = :{pk}')
                params[pk] = val

        # Fallback to event_pk if no where parts
        if not where_parts and event_pk:
            for k, v in event_pk.items():
                clean_k = k.lower()
                where_parts.append(f'"{clean_k}" = :{clean_k}')
                params[clean_k] = v

        if not where_parts:
            # NOTE: Without a primary key, individual rows cannot be reliably identified for deletion.
            raise ValueError(f"Cannot execute DELETE on {schema}.{table}: No primary key identified.")

        where_sql = " AND ".join(where_parts)
        sql = f'DELETE FROM "{schema}"."{table}" WHERE {where_sql};'

        if conn is not None:
            conn.execute(text(sql), params)
        else:
            with self.get_engine().begin() as c:
                c.execute(text(sql), params)
        return True
