from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection

from app.config import settings

logger = logging.getLogger(__name__)

# Singletons for connection engines across requests
_oracle_engine: Engine | None = None
_postgres_engine: Engine | None = None


def get_oracle_engine() -> Engine:
    """
    Returns a pooled, reusable SQLAlchemy Oracle engine.
    Uses settings.oracle_* properties without hardcoding.
    """
    global _oracle_engine
    if _oracle_engine is None:
        url = settings.get_oracle_url()
        _oracle_engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        logger.info(
            f"Initialized pooled Oracle engine for {settings.oracle_user}@{settings.oracle_host}:{settings.oracle_port}/{settings.oracle_service}"
        )
    return _oracle_engine


def get_postgres_engine() -> Engine:
    """
    Returns a pooled, reusable SQLAlchemy PostgreSQL engine.
    Uses settings.postgres_* properties without hardcoding.
    """
    global _postgres_engine
    if _postgres_engine is None:
        url = settings.get_postgres_url()
        _postgres_engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        logger.info(
            f"Initialized pooled PostgreSQL engine for {settings.postgres_user}@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_database}"
        )
    return _postgres_engine


@contextmanager
def oracle_connection() -> Generator[Connection, None, None]:
    """Provides a managed Oracle connection with automatic release back to pool."""
    engine = get_oracle_engine()
    with engine.connect() as conn:
        yield conn


@contextmanager
def postgres_connection() -> Generator[Connection, None, None]:
    """Provides a managed PostgreSQL connection with automatic release back to pool."""
    engine = get_postgres_engine()
    with engine.connect() as conn:
        yield conn


def test_oracle_connection() -> dict:
    """Tests live Oracle connectivity without exposing credentials."""
    try:
        with oracle_connection() as conn:
            db_time = conn.execute(text("SELECT SYSDATE FROM DUAL")).scalar()
            return {
                "status": "connected",
                "database": "oracle",
                "host": settings.oracle_host,
                "service": settings.oracle_service,
                "db_time": str(db_time),
            }
    except Exception as exc:
        return {
            "status": "failed",
            "database": "oracle",
            "error": str(exc),
        }


def test_postgres_connection() -> dict:
    """Tests live PostgreSQL connectivity without exposing credentials."""
    try:
        with postgres_connection() as conn:
            db_version = conn.execute(text("SELECT version()")).scalar()
            return {
                "status": "connected",
                "database": "postgresql",
                "host": settings.postgres_host,
                "database_name": settings.postgres_database,
                "version": str(db_version).split(",")[0] if db_version else "",
            }
    except Exception as exc:
        return {
            "status": "failed",
            "database": "postgresql",
            "error": str(exc),
        }


def dispose_engines() -> None:
    """Closes all connection pools gracefully on application shutdown."""
    global _oracle_engine, _postgres_engine
    if _oracle_engine is not None:
        _oracle_engine.dispose()
        _oracle_engine = None
        logger.info("Disposed Oracle engine pool.")
    if _postgres_engine is not None:
        _postgres_engine.dispose()
        _postgres_engine = None
        logger.info("Disposed PostgreSQL engine pool.")
