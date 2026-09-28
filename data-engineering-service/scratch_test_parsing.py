from app.cdc.normalizer import parse_sql_value, parse_sql_redo_insert, parse_sql_redo_update, parse_sql_redo_delete

# Test parse_sql_value
print("=== parse_sql_value ===")
tests = [
    ("'101'", int),
    ("'5000'", int),
    ("'5500.50'", float),
    ("'Alice'", str),
    ("NULL", type(None)),
    ("'O''Connor'", str),
    ("5000", int),
    ("5500.50", float),
    ("TO_DATE('2026-01-01', 'YYYY-MM-DD')", str),
]
for val, expected_type in tests:
    result = parse_sql_value(val)
    actual_type = type(result)
    status = "PASS" if isinstance(result, expected_type) else "FAIL"
    print(f"  [{status}] parse_sql_value({val!r}) => {result!r} (type={actual_type.__name__}, expected={expected_type.__name__})")

# Test INSERT parsing
print("\n=== INSERT parsing ===")
sql = """insert into "SYSTEM"."EMPLOYEES"("EMPLOYEE_ID","FIRST_NAME","SALARY") values ('101','Alice','5000');"""
parsed = parse_sql_redo_insert(sql)
print(f"  Parsed: {parsed}")
for k, v in parsed.items():
    print(f"    {k}: {v!r} (type={type(v).__name__})")

# Test with numeric salary
sql2 = """insert into "SYSTEM"."EMPLOYEES"("EMPLOYEE_ID","FIRST_NAME","SALARY") values ('103','Charlie','7500.50');"""
parsed2 = parse_sql_redo_insert(sql2)
print(f"  Parsed (decimal): {parsed2}")
for k, v in parsed2.items():
    print(f"    {k}: {v!r} (type={type(v).__name__})")

# Test UPDATE parsing
print("\n=== UPDATE parsing ===")
sql_upd = """update "SYSTEM"."EMPLOYEES" set "SALARY" = '5800.00', "FIRST_NAME" = 'Alicia' where "EMPLOYEE_ID" = '101' and "FIRST_NAME" = 'Alice' and "SALARY" = '5000' and ROWID = 'AAASiVAABAAAbhCAAA';"""
before, after = parse_sql_redo_update(sql_upd)
print(f"  Before: {before}")
print(f"  After:  {after}")
for k, v in after.items():
    print(f"    {k}: {v!r} (type={type(v).__name__})")

# Test DELETE parsing
print("\n=== DELETE parsing ===")
sql_del = """delete from "SYSTEM"."EMPLOYEES" where "EMPLOYEE_ID" = '102' and "FIRST_NAME" = 'Bob' and "SALARY" = '6200.00' and ROWID = 'AAASiVAABAAAbhCAAB';"""
before_del = parse_sql_redo_delete(sql_del)
print(f"  Before: {before_del}")
for k, v in before_del.items():
    print(f"    {k}: {v!r} (type={type(v).__name__})")
