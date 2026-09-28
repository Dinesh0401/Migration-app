# Oracle + PostgreSQL App

Simple project using:

- React + TypeScript + Vite
- Node.js + Express + TypeScript
- Oracle via `oracledb`
- PostgreSQL via `pg`

## Project structure

```text
oracle-postgres-app/
├── backend/
└── frontend/
```

## Database test data

Optional sample SQL is included in `database/oracle.sql` and `database/postgres.sql`. Run each script in the corresponding database if you want a `STUDENTS`/`students` table to appear in the UI.

## 1. Backend

```bash
cd backend
npm install
```

Create `.env` from `.env.example`:

```bash
copy .env.example .env
```

Set your real Oracle and PostgreSQL credentials.

Then run:

```bash
npm run dev
```

Backend:

```text
http://localhost:5000
```

Connection health:

```text
http://localhost:5000/api/health
```

The Oracle driver uses node-oracledb Thin mode by default, so an Oracle Client installation is not required for basic connections when using a supported Oracle Database version. Advanced Oracle functionality may require Thick mode.

## 2. Frontend

Open another terminal:

```bash
cd frontend
npm install
npm run dev
```

Frontend:

```text
http://localhost:5173
```

## What the app does

1. Tests Oracle connection.
2. Tests PostgreSQL connection.
3. Lists Oracle user tables.
4. Lists PostgreSQL public tables.
5. Lets you select a table and load up to 50 rows.

## Oracle connection example

```env
ORACLE_USER=system
ORACLE_PASSWORD=your_password
ORACLE_CONNECT_STRING=localhost:1521/XEPDB1
```

Change `XEPDB1` to your Oracle service name when necessary.

## PostgreSQL connection example

```env
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=your_password
PG_DATABASE=postgres
```

You can also use:

```env
PG_CONNECTION_STRING=postgresql://postgres:password@localhost:5432/postgres
```
