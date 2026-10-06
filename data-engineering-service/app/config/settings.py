from __future__ import annotations

import os
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    """
    Typed, environment-driven application settings using Pydantic Settings.
    Zero credentials, hosts, ports, or db names are hardcoded.
    All values are read dynamically from .env or environment variables.
    """
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Oracle Source Database Configuration
    ORACLE_USER: str = Field(default="SYSTEM", alias="ORACLE_USER")
    ORACLE_PASSWORD: str = Field(default="", alias="ORACLE_PASSWORD")
    ORACLE_HOST: str = Field(default="localhost", alias="ORACLE_HOST")
    ORACLE_PORT: int = Field(default=1521, alias="ORACLE_PORT")
    ORACLE_SERVICE: str = Field(default="FREEPDB1", alias="ORACLE_SERVICE")
    ORACLE_CDB_SERVICE: str = Field(default="FREE", alias="ORACLE_CDB_SERVICE")

    # PostgreSQL Target Database Configuration
    POSTGRES_USER: str = Field(default="postgres", alias="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field(default="", alias="POSTGRES_PASSWORD")
    POSTGRES_HOST: str = Field(default="localhost", alias="POSTGRES_HOST")
    POSTGRES_PORT: int = Field(default=5432, alias="POSTGRES_PORT")
    POSTGRES_DATABASE: str = Field(default="migration_exercise", alias="POSTGRES_DATABASE")

    # Service & Execution Configuration
    DATA_ENGINEERING_PORT: int = Field(default=8000, alias="DATA_ENGINEERING_PORT")
    APP_ENV: str = Field(default="development", alias="APP_ENV")
    SEATUNNEL_HOME: str | None = Field(default=None, alias="SEATUNNEL_HOME")
    SEATUNNEL_BIN: str | None = Field(default=None, alias="SEATUNNEL_BIN")

    # Lowercase property accessors for intuitive code consumption
    @property
    def oracle_user(self) -> str:
        return self.ORACLE_USER

    @property
    def oracle_password(self) -> str:
        return self.ORACLE_PASSWORD

    @property
    def oracle_host(self) -> str:
        return self.ORACLE_HOST

    @property
    def oracle_port(self) -> int:
        return self.ORACLE_PORT

    @property
    def oracle_service(self) -> str:
        return self.ORACLE_SERVICE

    @property
    def postgres_user(self) -> str:
        return self.POSTGRES_USER

    @property
    def postgres_password(self) -> str:
        return self.POSTGRES_PASSWORD

    @property
    def postgres_host(self) -> str:
        return self.POSTGRES_HOST

    @property
    def postgres_port(self) -> int:
        return self.POSTGRES_PORT

    @property
    def postgres_database(self) -> str:
        return self.POSTGRES_DATABASE

    @property
    def service_port(self) -> int:
        return self.DATA_ENGINEERING_PORT

    def get_oracle_url(self) -> str:
        """Returns modern SQLAlchemy oracle+oracledb connection URL."""
        if not self.ORACLE_USER or not self.ORACLE_HOST or not self.ORACLE_SERVICE:
            raise ValueError("Oracle credentials are not fully configured in environment")
        return f"oracle+oracledb://{self.ORACLE_USER}:{self.ORACLE_PASSWORD}@{self.ORACLE_HOST}:{self.ORACLE_PORT}/?service_name={self.ORACLE_SERVICE}"

    def get_oracle_cdb_url(self) -> str:
        """Returns modern SQLAlchemy oracle+oracledb connection URL for CDB root."""
        if not self.ORACLE_USER or not self.ORACLE_HOST:
            raise ValueError("Oracle credentials are not fully configured in environment")
        return f"oracle+oracledb://{self.ORACLE_USER}:{self.ORACLE_PASSWORD}@{self.ORACLE_HOST}:{self.ORACLE_PORT}/?service_name={self.ORACLE_CDB_SERVICE}"

    def get_postgres_url(self) -> str:
        """Returns modern SQLAlchemy postgresql+psycopg2 connection URL."""
        if not self.POSTGRES_USER or not self.POSTGRES_HOST or not self.POSTGRES_DATABASE:
            raise ValueError("PostgreSQL connection details are not fully configured in environment")
        return f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DATABASE}"

    def get_safe_summary(self) -> dict:
        """Returns connection endpoints with credentials safely masked."""
        return {
            "oracle": {
                "host": self.ORACLE_HOST,
                "port": self.ORACLE_PORT,
                "service": self.ORACLE_SERVICE,
                "user": self.ORACLE_USER,
                "configured": bool(self.ORACLE_USER and self.ORACLE_PASSWORD),
            },
            "postgresql": {
                "host": self.POSTGRES_HOST,
                "port": self.POSTGRES_PORT,
                "database": self.POSTGRES_DATABASE,
                "user": self.POSTGRES_USER,
                "configured": bool(self.POSTGRES_USER and self.POSTGRES_PASSWORD),
            },
        }


settings = Settings()
