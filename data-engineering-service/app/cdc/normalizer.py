from __future__ import annotations

import logging
import re
from typing import Any
from app.cdc.models import CDCEvent, CDCOperation

logger = logging.getLogger(__name__)


def split_sql_tokens(text: str, delimiter: str) -> list[str]:
    """
    Splits text by delimiter while respecting quoted string literals and parenthesized expressions.
    """
    tokens = []
    current = []
    in_quotes = False
    quote_char = None
    paren_depth = 0

    delim_clean = delimiter.strip()
    i = 0
    n = len(text)

    while i < n:
        ch = text[i]
        if ch in ("'", '"') and not in_quotes:
            in_quotes = True
            quote_char = ch
            current.append(ch)
        elif ch == quote_char and in_quotes:
            # Check for SQL escaped quote ''
            if i + 1 < n and text[i + 1] == quote_char:
                current.append(ch)
                current.append(ch)
                i += 1
            else:
                in_quotes = False
                quote_char = None
                current.append(ch)
        elif not in_quotes and ch == "(":
            paren_depth += 1
            current.append(ch)
        elif not in_quotes and ch == ")":
            paren_depth = max(0, paren_depth - 1)
            current.append(ch)
        elif not in_quotes and paren_depth == 0:
            if delim_clean == ",":
                if ch == ",":
                    tokens.append("".join(current).strip())
                    current = []
                    i += 1
                    continue
            else:
                word_match = re.match(r"^\s+" + re.escape(delim_clean) + r"\s+", text[i:], re.IGNORECASE)
                if word_match:
                    tokens.append("".join(current).strip())
                    current = []
                    i += len(word_match.group(0))
                    continue
            current.append(ch)
        else:
            current.append(ch)
        i += 1

    if current:
        token = "".join(current).strip()
        if token.endswith(";"):
            token = token[:-1].strip()
        if token:
            tokens.append(token)
    return tokens


def parse_sql_value(val: str) -> Any:
    """Parses a SQL token into appropriate Python scalar type."""
    val = val.strip()
    if val.upper() in ("NULL", "NULL;"):
        return None
    if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
        inner = val[1:-1].replace("''", "'")
        if re.fullmatch(r"-?\d+", inner):
            try:
                return int(inner)
            except ValueError:
                pass
        elif re.fullmatch(r"-?\d+\.\d+", inner):
            try:
                return float(inner)
            except ValueError:
                pass
        return inner

    date_match = re.search(r"'(.*?)'", val)
    if ("TO_DATE" in val.upper() or "TIMESTAMP" in val.upper()) and date_match:
        return date_match.group(1)
    try:
        if "." in val:
            return float(val)
        return int(val)
    except ValueError:
        return val


def parse_assignments(text_block: str, delimiter: str = ",") -> dict[str, Any]:
    items = split_sql_tokens(text_block, delimiter)
    res = {}
    for item in items:
        if "=" in item:
            k, v = item.split("=", 1)
            col = k.strip().strip('"').strip("'")
            val = parse_sql_value(v.strip())
            res[col] = val
        elif " IS NULL" in item.upper():
            k = re.sub(r"\s+IS\s+NULL", "", item, flags=re.IGNORECASE)
            col = k.strip().strip('"').strip("'")
            res[col] = None
    return res


def parse_sql_redo_insert(sql: str) -> dict[str, Any]:
    m = re.search(r"insert\s+into\s+.*?\((.*?)\)\s+values\s*\((.*)\)\s*;?", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return {}
    cols_str, vals_str = m.group(1), m.group(2)
    cols = [c.strip().strip('"').strip("'") for c in split_sql_tokens(cols_str, ",")]
    vals = [parse_sql_value(v.strip()) for v in split_sql_tokens(vals_str, ",")]
    return dict(zip(cols, vals))


def parse_sql_redo_update(sql: str) -> tuple[dict[str, Any], dict[str, Any]]:
    m = re.search(r"update\s+.*?set\s+(.*?)\s+where\s+(.*)\s*;?", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return {}, {}
    set_str, where_str = m.group(1), m.group(2)
    set_pairs = parse_assignments(set_str, delimiter=",")
    where_pairs = parse_assignments(where_str, delimiter="and")
    # Exclude Oracle internal ROWID pseudo-column
    where_pairs = {k: v for k, v in where_pairs.items() if k.upper() != "ROWID"}

    before = dict(where_pairs)
    after = dict(where_pairs)
    after.update(set_pairs)
    return before, after


def parse_sql_redo_delete(sql: str) -> dict[str, Any]:
    m = re.search(r"delete\s+from\s+.*?where\s+(.*)\s*;?", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return {}
    where_str = m.group(1)
    where_pairs = parse_assignments(where_str, delimiter="and")
    return {k: v for k, v in where_pairs.items() if k.upper() != "ROWID"}


class LogMinerNormalizer:
    """
    Normalizes raw Oracle LogMiner rows into standard CDCEvent instances.
    """

    def normalize_row(
        self,
        row: dict[str, Any],
        pk_columns: list[str] | None = None,
    ) -> CDCEvent | None:
        raw_op = str(row.get("operation") or row.get("OPERATION") or "").upper().strip()
        scn = int(row.get("scn") or row.get("SCN") or 0)
        schema = str(row.get("seg_owner") or row.get("SEG_OWNER") or "").strip()
        table = str(row.get("table_name") or row.get("TABLE_NAME") or "").strip()
        sql_redo = str(row.get("sql_redo") or row.get("SQL_REDO") or "").strip()
        tx_id = str(row.get("xid") or row.get("XID") or row.get("rs_id") or "")
        ts = str(row.get("timestamp") or row.get("TIMESTAMP") or "")

        if raw_op not in ("INSERT", "UPDATE", "DELETE", "DDL"):
            logger.debug(f"Ignoring non-DML operation '{raw_op}' at SCN {scn}")
            return None

        before: dict[str, Any] | None = None
        after: dict[str, Any] | None = None

        if raw_op == "INSERT":
            op = CDCOperation.INSERT
            after = parse_sql_redo_insert(sql_redo)
        elif raw_op == "UPDATE":
            op = CDCOperation.UPDATE
            before, after = parse_sql_redo_update(sql_redo)
        elif raw_op == "DELETE":
            op = CDCOperation.DELETE
            before = parse_sql_redo_delete(sql_redo)
        elif raw_op == "DDL":
            op = CDCOperation.DDL
        else:
            op = CDCOperation.OTHER

        # Determine primary key dict
        primary_key: dict[str, Any] = {}
        data_for_pk = after if after is not None else (before or {})

        if pk_columns:
            for pk in pk_columns:
                pk_clean = pk.strip()
                # Case-insensitive match in data_for_pk
                matched_val = None
                for k, v in data_for_pk.items():
                    if k.lower() == pk_clean.lower():
                        matched_val = v
                        primary_key[k] = v
                        break
                if matched_val is None and pk_clean in data_for_pk:
                    primary_key[pk_clean] = data_for_pk[pk_clean]
        else:
            # Heuristic fallback: columns ending in _ID or named ID
            for k, v in data_for_pk.items():
                if k.upper() == "ID" or k.upper().endswith("_ID"):
                    primary_key[k] = v

            # If still empty and there are columns, use the first column
            if not primary_key and data_for_pk:
                first_k = next(iter(data_for_pk))
                primary_key[first_k] = data_for_pk[first_k]

        return CDCEvent(
            scn=scn,
            operation=op,
            source_schema=schema,
            source_table=table,
            primary_key=primary_key,
            before=before,
            after=after,
            timestamp=ts,
            transaction_id=tx_id,
            raw_sql=sql_redo,
        )
