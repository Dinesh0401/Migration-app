from __future__ import annotations

from app.cdc.normalizer import (
    LogMinerNormalizer,
    parse_sql_redo_insert,
    parse_sql_redo_update,
    parse_sql_redo_delete,
    split_sql_tokens,
)
from app.cdc.models import CDCOperation


def test_split_sql_tokens():
    raw = "'101', 'O''Connor', 5000, NULL"
    tokens = split_sql_tokens(raw, ",")
    assert len(tokens) == 4
    assert tokens[0] == "'101'"
    assert tokens[1] == "'O''Connor'"
    assert tokens[2] == "5000"
    assert tokens[3] == "NULL"


def test_parse_sql_redo_insert():
    sql = 'insert into "HR"."EMPLOYEES"("EMPLOYEE_ID","FIRST_NAME","SALARY") values (\'101\',\'Alice\',\'5000\');'
    parsed = parse_sql_redo_insert(sql)
    assert parsed == {"EMPLOYEE_ID": 101, "FIRST_NAME": "Alice", "SALARY": 5000}


def test_parse_sql_redo_update():
    sql = (
        'update "HR"."EMPLOYEES" set "SALARY" = \'5500\', "FIRST_NAME" = \'Alicia\' '
        'where "EMPLOYEE_ID" = \'101\' and "FIRST_NAME" = \'Alice\' and "SALARY" = \'5000\' '
        'and ROWID = \'AAASJ5AAAAAAJYJAAA\';'
    )
    before, after = parse_sql_redo_update(sql)
    assert before == {"EMPLOYEE_ID": 101, "FIRST_NAME": "Alice", "SALARY": 5000}
    assert after == {"EMPLOYEE_ID": 101, "FIRST_NAME": "Alicia", "SALARY": 5500}
    assert "ROWID" not in before
    assert "ROWID" not in after


def test_parse_sql_redo_delete():
    sql = (
        'delete from "HR"."EMPLOYEES" where "EMPLOYEE_ID" = \'101\' '
        'and "FIRST_NAME" = \'Alicia\' and "SALARY" = \'5500\' and ROWID = \'AAASJ5AAAAAAJYJAAA\';'
    )
    before = parse_sql_redo_delete(sql)
    assert before == {"EMPLOYEE_ID": 101, "FIRST_NAME": "Alicia", "SALARY": 5500}
    assert "ROWID" not in before


def test_normalizer_row():
    normalizer = LogMinerNormalizer()

    # Test INSERT
    row_ins = {
        "SCN": 1051,
        "OPERATION": "INSERT",
        "SEG_OWNER": "HR",
        "TABLE_NAME": "EMPLOYEES",
        "SQL_REDO": 'insert into "HR"."EMPLOYEES"("EMPLOYEE_ID","FIRST_NAME") values (\'101\',\'Bob\');',
        "TIMESTAMP": "2026-09-28 10:00:00",
    }
    event_ins = normalizer.normalize_row(row_ins, pk_columns=["EMPLOYEE_ID"])
    assert event_ins is not None
    assert event_ins.scn == 1051
    assert event_ins.operation == CDCOperation.INSERT
    assert event_ins.primary_key == {"EMPLOYEE_ID": 101}
    assert event_ins.after == {"EMPLOYEE_ID": 101, "FIRST_NAME": "Bob"}
    assert event_ins.before is None

    # Test UPDATE
    row_upd = {
        "SCN": 1052,
        "OPERATION": "UPDATE",
        "SEG_OWNER": "HR",
        "TABLE_NAME": "EMPLOYEES",
        "SQL_REDO": 'update "HR"."EMPLOYEES" set "FIRST_NAME" = \'Bobby\' where "EMPLOYEE_ID" = \'101\' and "FIRST_NAME" = \'Bob\' and ROWID = \'xyz\';',
        "TIMESTAMP": "2026-09-28 10:01:00",
    }
    event_upd = normalizer.normalize_row(row_upd)
    assert event_upd is not None
    assert event_upd.operation == CDCOperation.UPDATE
    assert event_upd.before == {"EMPLOYEE_ID": 101, "FIRST_NAME": "Bob"}
    assert event_upd.after == {"EMPLOYEE_ID": 101, "FIRST_NAME": "Bobby"}
    assert event_upd.primary_key == {"EMPLOYEE_ID": 101}

    # Test DELETE
    row_del = {
        "SCN": 1053,
        "OPERATION": "DELETE",
        "SEG_OWNER": "HR",
        "TABLE_NAME": "EMPLOYEES",
        "SQL_REDO": 'delete from "HR"."EMPLOYEES" where "EMPLOYEE_ID" = \'101\' and ROWID = \'xyz\';',
        "TIMESTAMP": "2026-09-28 10:02:00",
    }
    event_del = normalizer.normalize_row(row_del)
    assert event_del is not None
    assert event_del.operation == CDCOperation.DELETE
    assert event_del.after is None
    assert event_del.before == {"EMPLOYEE_ID": 101}
    assert event_del.primary_key == {"EMPLOYEE_ID": 101}
