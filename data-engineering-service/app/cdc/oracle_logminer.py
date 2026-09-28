from __future__ import annotations

import logging
from typing import Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.cdc.base import BaseCDCSource
from app.cdc.models import CDCEvent
from app.cdc.normalizer import LogMinerNormalizer
from app.config.settings import Settings, settings

logger = logging.getLogger(__name__)


class OracleLogMinerCDCSource(BaseCDCSource):
    """
    Oracle LogMiner CDC Source.
    Mines committed DML changes directly from Oracle redo logs using DBMS_LOGMNR.
    Handles multitenant CDB/PDB container awareness, online redo log discovery,
    and event normalization.
    """

    def __init__(
        self,
        config: Settings | None = None,
        normalizer: LogMinerNormalizer | None = None,
    ) -> None:
        self.config = config or settings
        self.normalizer = normalizer or LogMinerNormalizer()
        self._engine: Engine | None = None
        self._cdb_engine: Engine | None = None

    def get_source_engine(self) -> Engine:
        """Standard Oracle connection for metadata and current SCN."""
        if self._engine is None:
            url = self.config.get_oracle_url()
            self._engine = create_engine(url, pool_pre_ping=True)
        return self._engine

    def get_logminer_engine(self) -> Engine:
        """
        Engine for LogMiner operations.
        In Oracle multitenant, online redo logs belong to CDB$ROOT.
        If ORACLE_CDB_SERVICE is available, connects to CDB root;
        otherwise falls back to source URL.
        """
        if self._cdb_engine is None:
            try:
                cdb_url = self.config.get_oracle_cdb_url()
                self._cdb_engine = create_engine(cdb_url, pool_pre_ping=True)
            except Exception as exc:
                logger.warning(f"Could not initialize CDB engine ({exc}), using standard engine.")
                self._cdb_engine = self.get_source_engine()
        return self._cdb_engine

    def get_current_scn(self) -> int:
        """Retrieves the current SCN from Oracle."""
        with self.get_source_engine().connect() as conn:
            scn = conn.execute(text("SELECT CURRENT_SCN FROM V$DATABASE")).scalar()
            if scn is None:
                raise RuntimeError("Failed to retrieve CURRENT_SCN from Oracle V$DATABASE.")
            return int(scn)

    def check_prerequisites(self) -> dict[str, Any]:
        """
        Inspects live Oracle instance and reports readiness for LogMiner CDC.
        Verifies version, container, DBMS_LOGMNR package, privileges,
        supplemental logging, and redo logs.
        """
        report: dict[str, Any] = {
            "ready": False,
            "version": None,
            "container": None,
            "current_user": None,
            "database_name": None,
            "log_mode": None,
            "supplemental_logging_min": None,
            "has_logmining_privilege": False,
            "dbms_logmnr_available": False,
            "redo_logs_count": 0,
            "missing_prerequisites": [],
        }

        # Check source connection and basic metadata
        try:
            with self.get_source_engine().connect() as conn:
                report["version"] = conn.execute(text("SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1")).scalar()
                report["container"] = conn.execute(text("SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM DUAL")).scalar()
                report["current_user"] = conn.execute(text("SELECT USER FROM DUAL")).scalar()

                db_row = conn.execute(text("SELECT NAME, LOG_MODE, SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE")).mappings().one_or_none()
                if db_row:
                    report["database_name"] = db_row.get("name")
                    report["log_mode"] = db_row.get("log_mode")
                    report["supplemental_logging_min"] = db_row.get("supplemental_log_data_min")

                privs = set(conn.execute(text("SELECT PRIVILEGE FROM SESSION_PRIVS")).scalars().all())
                report["has_logmining_privilege"] = "LOGMINING" in privs or report["current_user"] == "SYSTEM"

                # Check DBMS_LOGMNR package
                pkg_count = conn.execute(
                    text("SELECT COUNT(*) FROM ALL_OBJECTS WHERE OBJECT_NAME = 'DBMS_LOGMNR' AND STATUS = 'VALID'")
                ).scalar() or 0
                report["dbms_logmnr_available"] = pkg_count > 0

                # Check online redo logs
                redo_count = conn.execute(text("SELECT COUNT(*) FROM V$LOGFILE WHERE TYPE = 'ONLINE'")).scalar() or 0
                report["redo_logs_count"] = int(redo_count)

        except Exception as exc:
            report["missing_prerequisites"].append(f"Source connection / metadata query failed: {exc}")
            return report

        # Validate conditions
        if not report["dbms_logmnr_available"]:
            report["missing_prerequisites"].append("DBMS_LOGMNR package is not installed or invalid.")

        if report.get("supplemental_logging_min") != "YES":
            report["missing_prerequisites"].append(
                "Minimal supplemental logging is NOT enabled. Run: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;"
            )

        if report["redo_logs_count"] == 0:
            report["missing_prerequisites"].append("No ONLINE redo logs accessible via V$LOGFILE.")

        if not report["has_logmining_privilege"]:
            report["missing_prerequisites"].append("Configured user lacks LOGMINING privilege.")

        report["ready"] = len(report["missing_prerequisites"]) == 0
        return report

    def read_changes(
        self,
        start_scn: int,
        end_scn: int | None = None,
        table_filter: list[str] | None = None,
    ) -> list[CDCEvent]:
        """
        Executes LogMiner mining session for the given SCN interval [start_scn, end_scn].
        Returns normalized CDCEvents.
        """
        if end_scn is None:
            end_scn = self.get_current_scn()

        if start_scn > end_scn:
            logger.info(f"start_scn ({start_scn}) > end_scn ({end_scn}). No new SCN range to process.")
            return []

        logger.info(f"[LogMiner] Reading changes in SCN window [{start_scn} -> {end_scn}]")
        engine = self.get_logminer_engine()
        events: list[CDCEvent] = []

        with engine.connect() as conn:
            # 1. Fetch online redo log files
            log_files = conn.execute(text("SELECT MEMBER FROM V$LOGFILE WHERE TYPE = 'ONLINE'")).scalars().all()
            if not log_files:
                raise RuntimeError("No ONLINE redo logs found in V$LOGFILE for LogMiner.")

            # 2. Reset any stale LogMiner session
            try:
                conn.execute(text("BEGIN DBMS_LOGMNR.END_LOGMNR; EXCEPTION WHEN OTHERS THEN NULL; END;"))
            except Exception:
                pass

            # 3. Add online redo log files
            first = True
            for lf in log_files:
                opt = "DBMS_LOGMNR.NEW" if first else "DBMS_LOGMNR.ADDFILE"
                first = False
                conn.execute(text(f"BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '{lf}', OPTIONS => {opt}); END;"))

            # 4. Start LogMiner
            start_sql = f"""
            BEGIN
                DBMS_LOGMNR.START_LOGMNR(
                    STARTSCN => {start_scn},
                    ENDSCN   => {end_scn},
                    OPTIONS  => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY
                );
            END;
            """
            try:
                conn.execute(text(start_sql))
                logger.info(f"[LogMiner] Started session for SCN {start_scn} -> {end_scn}")

                # 5. Query V$LOGMNR_CONTENTS
                cols = set(conn.execute(text("SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = 'V_$LOGMNR_CONTENTS'")).scalars().all())

                # TEMPORARY DIAGNOSTIC:
                # Do not filter SRC_CON_NAME yet.
                # We first want to see whether LogMiner is capturing
                # any DML from the redo logs at all.
                src_con_col = ", SRC_CON_NAME" if "SRC_CON_NAME" in cols else ""

                query = f"""
                SELECT SCN, TIMESTAMP, OPERATION, SEG_OWNER, TABLE_NAME, SQL_REDO, RS_ID, SSN, XID, ROW_ID{src_con_col}
                FROM V$LOGMNR_CONTENTS
                WHERE OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
                ORDER BY SCN, RS_ID, SSN
                """

                rows = conn.execute(text(query)).mappings().all()

                if rows:
                    for row in rows[:10]:
                        logger.info(
                            "[LogMiner DEBUG] SCN=%s OP=%s OWNER=%s TABLE=%s CON=%s SQL=%s",
                            row.get("SCN"),
                            row.get("OPERATION"),
                            row.get("SEG_OWNER"),
                            row.get("TABLE_NAME"),
                            row.get("SRC_CON_NAME"),
                            row.get("SQL_REDO"),
                        )
                else:
                    logger.info("[LogMiner DEBUG] No DML rows returned from V$LOGMNR_CONTENTS")

                logger.info(f"[LogMiner] Extracted {len(rows)} raw DML change records from redo logs.")

                for idx, row in enumerate(rows, start=1):
                    tbl = str(row.get("TABLE_NAME") or "").upper()
                    if table_filter and tbl not in [t.upper().strip() for t in table_filter]:
                        continue
                    event = self.normalizer.normalize_row(dict(row))
                    if event:
                        event.sequence = idx
                        events.append(event)

            finally:
                try:
                    conn.execute(text("BEGIN DBMS_LOGMNR.END_LOGMNR; EXCEPTION WHEN OTHERS THEN NULL; END;"))
                    logger.debug("[LogMiner] Ended mining session.")
                except Exception as exc:
                    logger.warning(f"Error ending LogMiner session: {exc}")

        return events
