import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

CURRENT_DIR = Path(__file__).resolve().parent.parent.parent
PARENT_DIR = CURRENT_DIR.parent

if (CURRENT_DIR / ".env").exists():
    load_dotenv(CURRENT_DIR / ".env")
elif (PARENT_DIR / ".env").exists():
    load_dotenv(PARENT_DIR / ".env")
else:
    load_dotenv()


@dataclass(frozen=True)
class Settings:
    SERVICE_PORT: int = int(os.getenv("DATA_ENGINEERING_PORT", "8000"))
    APP_ENV: str = os.getenv("APP_ENV", "development")

    ORACLE_USER: str = os.getenv("ORACLE_USER", "")
    ORACLE_PASSWORD: str = os.getenv("ORACLE_PASSWORD", "")
    ORACLE_HOST: str = os.getenv("ORACLE_HOST", "localhost")
    ORACLE_PORT: str = os.getenv("ORACLE_PORT", "1521")
    ORACLE_SERVICE: str = os.getenv("ORACLE_SERVICE") or os.getenv("ORACLE_SERVICE_NAME") or "FREEPDB1"
    ORACLE_CDB_SERVICE: str = os.getenv("ORACLE_CDB_SERVICE") or "FREE"

    POSTGRES_USER: str = os.getenv("POSTGRES_USER") or os.getenv("PG_USER") or "postgres"
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD") or os.getenv("PG_PASSWORD") or ""
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST") or os.getenv("PG_HOST") or "localhost"
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT") or os.getenv("PG_PORT") or "5432"
    POSTGRES_DATABASE: str = (
        os.getenv("POSTGRES_DATABASE")
        or os.getenv("POSTGRES_DB")
        or os.getenv("PG_DATABASE")
        or "migration_exercise"
    )

    def get_oracle_url(self) -> str:
        """Returns modern SQLAlchemy oracle+oracledb connection URL."""
        if not all([self.ORACLE_USER, self.ORACLE_PASSWORD, self.ORACLE_HOST, self.ORACLE_SERVICE]):
            raise ValueError("Oracle credentials are not fully configured in environment")
        return f"oracle+oracledb://{self.ORACLE_USER}:{self.ORACLE_PASSWORD}@{self.ORACLE_HOST}:{self.ORACLE_PORT}/?service_name={self.ORACLE_SERVICE}"

    def get_oracle_cdb_url(self) -> str:
        """Returns modern SQLAlchemy oracle+oracledb connection URL for CDB root (for LogMiner)."""
        if not all([self.ORACLE_USER, self.ORACLE_PASSWORD, self.ORACLE_HOST]):
            raise ValueError("Oracle credentials are not fully configured in environment")
        cdb_service = self.ORACLE_CDB_SERVICE or "FREE"
        return f"oracle+oracledb://{self.ORACLE_USER}:{self.ORACLE_PASSWORD}@{self.ORACLE_HOST}:{self.ORACLE_PORT}/?service_name={cdb_service}"

    def get_postgres_url(self) -> str:
        """Returns modern SQLAlchemy postgresql+psycopg2 connection URL."""
        if not all([self.POSTGRES_USER, self.POSTGRES_HOST, self.POSTGRES_DATABASE]):
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
