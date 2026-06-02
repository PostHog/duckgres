# Iceberg PostgreSQL Compatibility Contract

Duckgres lets PostgreSQL clients connect, introspect, query, and write against an
Iceberg catalog (via Lakekeeper) executed by DuckDB. This is **not** full
PostgreSQL emulation. It is a documented, stable subset: supported operations
behave like PostgreSQL, and unsupported semantics return predictable
PostgreSQL-shaped errors or safe command tags rather than raw DuckDB/Flight
failures.

pgwire compatibility does **not** imply PostgreSQL storage, transaction, DDL, or
extension compatibility. The execution engine is DuckDB and the table format is
Iceberg; neither enforces PostgreSQL constraints, indexes, sequences, MVCC, or
the privilege model.

## How it works

A session has two separate identities:

- **Client database**: the PostgreSQL-visible database from the startup packet.
  `current_database()`, `pg_database`, and `information_schema.*.table_catalog`
  expose this value.
- **Physical catalog**: the DuckDB catalog selected for execution (`ducklake` or
  `iceberg`). This is resolved from the per-user default catalog, exact catalog
  selector, and attached worker catalogs.

The transpiler selects a typed **backend profile** per backend
(`transpiler/backend/profile.go`):

| Capability | memory | ducklake | iceberg |
|---|:--:|:--:|:--:|
| Strip table/column constraints (PK/UNIQUE/FK/CHECK/EXCLUDE) | – | ✓ | ✓ |
| Rewrite SERIAL → integer | – | ✓ | ✓ |
| Strip volatile/GENERATED defaults | – | ✓ | ✓ |
| No-op DDL (CREATE/DROP INDEX, VACUUM, ANALYZE, REINDEX, CLUSTER, GRANT/REVOKE, COMMENT, REFRESH MATVIEW) | – | ✓ | ✓ |
| Rewrite `DROP TABLE … CASCADE` → RESTRICT | – | ✓ | ✓ |
| Split multi-command `ALTER TABLE` | – | ✓ | ✓ |
| Rewrite `INSERT … ON CONFLICT` → `MERGE` | – | ✓ | ✓ |
| Qualify custom/ClickHouse macros (`memory.main`) | – | ✓ | ✓ |
| Intercept `SHOW CREATE TABLE` | – | ✓ | ✓ |
| Map `public` schema → `main` | ✓ | ✓ | – |
| Physical DuckDB catalog | `memory` | `ducklake` | `iceberg` |

The Iceberg profile mirrors DuckLake's DDL/DML policy but keeps the physical
schema name `public` (DuckDB shadows `main` on REST catalogs), so the
`public`→`main` rewrite is disabled. The physical catalog is not exposed as the
PostgreSQL database unless the client explicitly connected with that database
name.

## Compatibility matrix

### Supported
- Connect, authenticate, simple + extended query protocol.
- `SELECT` (joins, aggregates, CTEs, window functions — whatever DuckDB supports).
- `INSERT`, `UPDATE`, `DELETE`, `COPY FROM`.
- `CREATE TABLE` / `CREATE SCHEMA` / `DROP TABLE` / `DROP SCHEMA`.
- `ALTER TABLE ADD COLUMN` (idempotent), single supported `ALTER` commands.
- `ALTER COLUMN … TYPE <t>` without a `USING` expression (DuckDB validates the cast).
- Introspection: `information_schema.{columns,tables,schemata,views}`, the
  `pg_catalog` compatibility views, `current_database()`, `\d`, JDBC/ODBC metadata.
- Canonical Iceberg→PostgreSQL type mapping (see below).

### Emulated / rewritten
- `INSERT … ON CONFLICT (cols) DO UPDATE/NOTHING` → `MERGE` (no enforced unique
  constraint exists to infer against).
- `SERIAL`/`BIGSERIAL`/`SMALLSERIAL` → plain integer types (no sequence).
- `current_database()`, `table_catalog`, and the `pg_catalog` surfaces report the
  client-visible PostgreSQL database consistently while query execution uses the
  resolved physical catalog.

### No-op (acknowledged, not executed; returns the PostgreSQL command tag)
- `CREATE INDEX`, `DROP INDEX`, `REINDEX`, `CLUSTER`, `VACUUM`, `ANALYZE`.
- `GRANT`, `REVOKE`, `COMMENT`, `REFRESH MATERIALIZED VIEW`.
- Table/column constraints in `CREATE TABLE` and `ALTER TABLE` (stripped).
- Volatile (`DEFAULT now()`) and `GENERATED` defaults (stripped).

### Unsupported (predictable PostgreSQL error)
| Feature | SQLSTATE |
|---|---|
| `ON CONFLICT ON CONSTRAINT <name>` | `0A000` feature_not_supported |
| `ALTER COLUMN … TYPE … USING <expr>` | `0A000` feature_not_supported |
| DML `RETURNING` via extended-query Describe | `0A000` |
| `FOR UPDATE` / `FOR SHARE` | stripped (no row locks) |

## Type mapping (canonical)

One canonical map (`server/pgtypes/iceberg.go`, `ForIceberg`) normalizes
Iceberg field types before they populate `information_schema.columns`:

| Iceberg | PostgreSQL |
|---|---|
| boolean | boolean |
| int | integer |
| long | bigint |
| float | real |
| double | double precision |
| decimal(p,s) | numeric(p,s) |
| date | date |
| time | time without time zone |
| timestamp | timestamp without time zone |
| timestamptz | timestamp with time zone |
| string | text |
| uuid | uuid |
| binary / fixed | bytea |
| struct / list / map / variant | jsonb |

## Known hard limits

These cannot be honored by the Iceberg + DuckDB substrate:
- Real PostgreSQL indexes.
- Enforced primary/unique/foreign/check constraints (and therefore fully correct
  `ON CONFLICT` semantics).
- PostgreSQL sequence / identity semantics.
- Arbitrary `ALTER COLUMN TYPE … USING expression`.
- Triggers, rules, stored procedures, extensions, domains, custom types.
- Row-level locks and PostgreSQL MVCC semantics.
- The PostgreSQL privilege model (unless Duckgres enforces it at the wire layer).

## Known gaps / follow-ups

- **Nested-type wire OID.** `information_schema` reports `jsonb` for nested types,
  but the wire-protocol column OID for a DuckDB `STRUCT`/`MAP`/`LIST` still falls
  back to `text` (`server/types.go mapDuckDBType`). Aligning the wire OID would
  affect all backends and needs live JDBC verification.
- **`LOCK TABLE` / advisory locks / `SET TRANSACTION`** are not yet explicitly
  normalized for Iceberg.
- **Iceberg optimistic-commit retry.** DuckLake commit conflicts map to `40001`;
  the equivalent Iceberg/Lakekeeper conflict signature is not yet classified.
- End-to-end validation of live Iceberg session catalog/search_path resolution
  requires the `tests/k8s` lane against real Lakekeeper + object storage.
