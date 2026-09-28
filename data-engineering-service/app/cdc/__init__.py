"""
CDC (Change Data Capture) package for log-based streaming migration.
Extracts row-level changes from Oracle redo logs via LogMiner,
normalizes to standard CDCEvent models, transforms using the shared
TransformationEngine runtime, and applies idempotently to PostgreSQL.
"""
from app.cdc.models import CDCEvent, CDCOperation, CDCStatus, CDCWindowResult
from app.cdc.base import BaseCDCSource
from app.cdc.checkpoint import PostgresCheckpointStore
from app.cdc.normalizer import LogMinerNormalizer
from app.cdc.oracle_logminer import OracleLogMinerCDCSource
from app.cdc.writer import PostgresCDCWriter
from app.cdc.consumer import CDCConsumer

__all__ = [
    "CDCEvent",
    "CDCOperation",
    "CDCStatus",
    "CDCWindowResult",
    "BaseCDCSource",
    "PostgresCheckpointStore",
    "LogMinerNormalizer",
    "OracleLogMinerCDCSource",
    "PostgresCDCWriter",
    "CDCConsumer",
]
