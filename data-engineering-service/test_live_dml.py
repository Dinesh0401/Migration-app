import time
from sqlalchemy import text
from app.adapters.registry import default_registry

oracle_adapter = default_registry.get_source_adapter("oracle")
engine = oracle_adapter.get_engine()

# 1. Clean up old test row if any
with engine.begin() as conn:
    conn.execute(text("DELETE FROM EMPLOYEES WHERE EMPLOYEE_ID = 9876"))
time.sleep(2.5)

# 2. INSERT
print("[Oracle] Executing INSERT for EMPLOYEE_ID=9876...")
with engine.begin() as conn:
    conn.execute(text("""
        INSERT INTO EMPLOYEES (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DATE, JOB_ID, SALARY, DEPARTMENT_ID)
        VALUES (9876, 'Alice', 'LiveStream', 'alice.stream@company.com', TO_DATE('2026-09-28', 'YYYY-MM-DD'), 'IT_PROG', 8500, 60)
    """))
print("[Oracle] INSERT committed!")

time.sleep(3.5)

# 3. UPDATE
print("[Oracle] Executing UPDATE for EMPLOYEE_ID=9876...")
with engine.begin() as conn:
    conn.execute(text("UPDATE EMPLOYEES SET SALARY = 9500, LAST_NAME = 'LiveUpdated' WHERE EMPLOYEE_ID = 9876"))
print("[Oracle] UPDATE committed!")

time.sleep(3.5)

# 4. DELETE
print("[Oracle] Executing DELETE for EMPLOYEE_ID=9876...")
with engine.begin() as conn:
    conn.execute(text("DELETE FROM EMPLOYEES WHERE EMPLOYEE_ID = 9876"))
print("[Oracle] DELETE committed!")
