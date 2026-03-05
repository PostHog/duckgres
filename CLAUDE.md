# Claude Code Context for Duckgres

This file provides context for Claude Code sessions working on this codebase.

## Project Overview

Duckgres is a PostgreSQL wire protocol server backed by DuckDB. It allows any PostgreSQL client (psql, pgAdmin, lib/pq, psycopg2, JDBC, etc.) to connect and execute queries against DuckDB databases.

## Architecture

Duckgres supports three run modes: `standalone` (default), `control-plane`, and `duckdb-service`.

```
Standalone:    PostgreSQL Client → TLS → Duckgres Server → DuckDB (per-user database)
Control Plane: PostgreSQL Client → TLS/Auth/PG Protocol → Control Plane → Flight SQL (UDS) → Worker (DuckDB)
```

### Key Components

- **main.go**: Entry point, configuration loading (CLI flags, env vars, YAML), mode routing
- **server/server.go**: Server struct, connection handling, graceful shutdown, `CreateDBConnection()` (standalone function)
- **server/conn.go**: Client connection handling, query execution, COPY protocol
- **server/protocol.go**: PostgreSQL wire protocol message encoding/decoding
- **server/exports.go**: Exported wrappers for protocol functions (used by control plane workers)
- **server/catalog.go**: pg_catalog compatibility views and macros initialization
- **server/types.go**: Type OID mapping between DuckDB and PostgreSQL
- **server/ratelimit.go**: Rate limiting for brute-force protection
- **server/certs.go**: Auto-generation of self-signed TLS certificates
- **server/sysinfo.go**: System memory detection and auto memory limit computation
- **server/parent.go**: Child process spawning for ProcessIsolation mode
- **server/worker.go**: Per-connection child worker (ProcessIsolation mode)
- **transpiler/**: AST-based SQL transpiler (PostgreSQL → DuckDB)
  - `transpiler.go`: Main API, transform pipeline orchestration
  - `config.go`: Configuration types (DuckLakeMode, ConvertPlaceholders)
  - `transform/`: Individual transform implementations
- **controlplane/**: Multi-process control plane architecture
  - `control.go`: Control plane main loop (TCP listener, TLS, auth, PG protocol, SQL transpilation, connection routing)
  - `worker_mgr.go`: Flight SQL worker pool management (spawn, health check, least-connections routing, rolling update)
  - `session_mgr.go`: Session lifecycle management (maps PG connections to Flight SQL sessions on workers)
  - `handover.go`: Graceful deployment (listener FD transfer between control planes)
  - `sdnotify.go`: systemd sd_notify integration
  - `validation.go`: Configuration validation
- **duckdbservice/**: Standalone DuckDB Arrow Flight SQL service (used as worker in control-plane mode)
  - `service.go`: Flight SQL server lifecycle, gRPC setup
  - `flight_handler.go`: Arrow Flight SQL handler (DoPut, DoGet, GetFlightInfo, session management)
  - `arrow_helpers.go`: Arrow/DuckDB type mapping and conversion
  - `auth.go`: Bearer token authentication middleware
  - `config.go`: Service configuration (listen addr, bearer token, max sessions)

## PostgreSQL Wire Protocol

The server implements the PostgreSQL v3 protocol:

### Message Types (server/protocol.go)
- **Frontend (client→server)**: Query, Parse, Bind, Describe, Execute, Sync, Close, CopyData, CopyDone
- **Backend (server→client)**: AuthOK, RowDescription, DataRow, CommandComplete, ReadyForQuery, CopyInResponse, CopyOutResponse

### Query Flow
1. Client sends Query message ('Q')
2. Server parses SQL, rewrites pg_catalog references
3. Server executes via DuckDB's database/sql driver
4. Server sends RowDescription + DataRow messages
5. Server sends CommandComplete + ReadyForQuery

### Extended Query Protocol
Supports prepared statements (Parse/Bind/Execute) for parameterized queries and binary result formats.

## pg_catalog Compatibility

psql and other clients expect PostgreSQL system catalogs. We provide compatibility by:

1. **Creating views** in main schema (server/catalog.go `initPgCatalog()`):
   - `pg_database`, `pg_class_full`, `pg_collation`, `pg_policy`, `pg_roles`
   - `pg_statistic_ext`, `pg_publication`, `pg_publication_rel`, `pg_inherits`, etc.

2. **Creating macros** for PostgreSQL functions (server/catalog.go):
   - `pg_get_userbyid`, `pg_table_is_visible`, `format_type`, `pg_get_expr`
   - `obj_description`, `col_description`, `pg_get_indexdef`, etc.

3. **AST-based SQL transpilation** (transpiler/ package):
   The transpiler parses PostgreSQL SQL into an AST using pg_query_go (PostgreSQL's C parser),
   applies transforms, and deparses back to DuckDB-compatible SQL. Transforms include:
   - **PgCatalogTransform**: `pg_catalog.pg_class` → `pg_class_full`, strips schema prefix from functions
   - **TypeCastTransform**: `::pg_catalog.regtype` → `::VARCHAR`
   - **VersionTransform**: `version()` → PostgreSQL-compatible version string
   - **SetShowTransform**: Converts SET/SHOW commands, marks ignored parameters
   - **DDLTransform**: (DuckLake mode) Strips PRIMARY KEY, UNIQUE, REFERENCES, SERIAL types
   - **PlaceholderTransform**: Counts $1, $2 parameters for prepared statements

## COPY Protocol (server/conn.go)

Supports bulk data transfer:
- **COPY TO STDOUT**: Streams query results to client
- **COPY FROM STDIN**: Receives data from client, inserts row by row
- Supports CSV format with HEADER, DELIMITER, and NULL options

## Run Modes

- **standalone** (default): Single process, handles everything including TLS, auth, PG protocol, and DuckDB execution.
- **control-plane**: Multi-process. The control plane owns client connections end-to-end (TLS, auth, PG wire protocol, SQL transpilation) and routes queries to a pool of Flight SQL worker processes over Unix sockets.
- **duckdb-service**: Thin DuckDB execution engine exposed via Arrow Flight SQL. Spawned automatically by the control plane as worker processes, or run standalone for testing.

Key CLI flags for control plane mode:
- `--mode control-plane|duckdb-service|standalone`
- `--min-workers N` (default 0, pre-warm workers at startup)
- `--max-workers N` (default 0 = unlimited)
- `--memory-budget SIZE` (e.g., "24GB", default: 75% system RAM)
- `--socket-dir /path` (Unix sockets for Flight SQL workers)
- `--handover-socket /path` (graceful deployment between control planes)

Key CLI flags for duckdb-service mode:
- `--duckdb-listen` (listen address, e.g., `unix:///var/run/duckgres/duckdb.sock` or `:8816`)
- `--duckdb-token` (bearer token for authentication)
- `--duckdb-max-sessions` (max concurrent sessions, 0=unlimited)

## Configuration

Three-tier configuration (highest to lowest priority):
1. CLI flags (`--port`, `--config`, `--mode`, etc.)
2. Environment variables (`DUCKGRES_PORT`, etc.)
3. YAML config file
4. Built-in defaults

## Testing

```bash
# Build
go build -o duckgres .

# Run on non-standard port
./duckgres --port 35437

# Connect with psql
PGPASSWORD=postgres psql "host=127.0.0.1 port=35437 user=postgres sslmode=require"

# Test commands
\dt          # List tables
\d tablename # Describe table
\l           # List databases
```

## Common Development Tasks

### Adding a new pg_catalog view
1. Add view creation SQL in `initPgCatalog()` in `catalog.go`
2. If the view needs query rewriting (e.g., `pg_catalog.viewname` → `viewname`):
   - Add mapping in `transpiler/transform/pgcatalog.go` in `pgCatalogViewMappings`

### Adding a new PostgreSQL function
1. Add `CREATE MACRO` in the `functions` slice in `initPgCatalog()`
2. The transpiler automatically strips `pg_catalog.` prefix from function calls

### Adding a new transform
1. Create a new file in `transpiler/transform/` implementing the `Transform` interface
2. Register the transform in `transpiler/transpiler.go` `New()` function
3. Add tests in `transpiler/transpiler_test.go`

### Adding protocol support
1. Add message type constant in `protocol.go`
2. Add write function (e.g., `writeCopyData()`)
3. Handle in message loop in `conn.go`

## Dependencies

- `github.com/duckdb/duckdb-go/v2` - DuckDB Go driver
- `github.com/pganalyze/pg_query_go/v6` - PostgreSQL SQL parser (CGO, uses libpg_query)
- `gopkg.in/yaml.v3` - YAML config parsing

## Known Limitations

- No replication
- Some pg_catalog tables are stubs (return empty)
- Type OID mapping is incomplete (some types show as "unknown")
- DML RETURNING is not supported via extended query protocol (see below)

## DML RETURNING Detection (conn.go)

DML statements with RETURNING clauses produce result rows but **cannot be described without executing the mutation**. The extended query protocol's Describe step probes schema by executing the query, which would cause unintended side effects. We reject these at Describe time with SQLSTATE `0A000` (feature_not_supported).

### Architecture

Three functions form the detection chain:

- **`scanForReturning(upper, topLevelOnly)`** — SQL-aware lexer that scans for the RETURNING keyword while skipping strings (single-quoted, E-strings, dollar-quoted), double-quoted identifiers, block/line comments, and tracking parenthesis depth.
- **`containsReturning(upper)`** — wrapper that matches RETURNING at depth 0 only. Used for plain DML (INSERT/UPDATE/DELETE prefix).
- **`containsReturningAnyDepth(upper)`** — wrapper that matches RETURNING at any depth. Used for WITH-prefixed queries because writable CTEs place RETURNING inside `AS (...)` parens.
- **`isDMLReturning(query)`** — top-level guard called from `handleDescribe`. Routes to depth-0 or any-depth scanning based on the query prefix.

### Why WITH needs any-depth scanning

In writable CTEs, RETURNING is syntactically required inside the CTE body:
```sql
WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d
--                       ^^^^^^^^^ depth 1, inside AS (...)
```
Depth-0-only scanning structurally cannot detect this. The any-depth scan accepts a small false-positive risk (a column literally named `returning` in a CTE) in exchange for preventing mutation during Describe.

### Supporting functions

- **`stripLeadingNoise(query)`** — loops `stripLeadingComments` + `TrimLeft` to handle interleaved parentheses, whitespace, and comments before the query keyword.
- **`queryReturnsResults(query)`** — determines whether a query produces result rows (SELECT, WITH, VALUES, SHOW, DML RETURNING, etc.). Gates whether Describe attempts schema probing at all.

### Key edge cases with test coverage

| Case | Handling |
|------|----------|
| RETURNING inside subquery `(SELECT returning FROM s)` | Skipped (depth > 0 for plain DML) |
| RETURNING in string literal `'returning'` | Skipped by string scanner |
| RETURNING in E-string `E'foo\'s RETURNING bar'` | Skipped by backslash-escape-aware scanner |
| RETURNING in `$$`/`$tag$` dollar-quoted string | Skipped by dollar-quote scanner |
| RETURNING in `"double-quoted"` identifier | Skipped by identifier scanner |
| RETURNING in block/line comments | Skipped by comment scanner |
| `RETURNING*`, `RETURNING(id)`, `RETURNING,col` | Matched (trailing `*`, `(`, `,` accepted) |
| `RETURNING_ID`, `RETURNINGS` | Not matched (identifier continuation) |
| Writable CTE with RETURNING in AS (...) | Matched via any-depth scan for WITH prefix |
| Parenthesized queries with newlines `(\nSELECT 1)` | Handled by `stripLeadingNoise` |

### When modifying this code

- **False negatives are dangerous** — the Describe path executes the query, causing unintended mutations. Err on the side of false positives.
- **False positives are safe** — the client gets an error but no data corruption. A column named `returning` triggering the guard is acceptable.
- All detection is heuristic (string scanning). If precision becomes critical, consider using `pg_query_go` AST parsing instead.
- LIMIT 0 does NOT prevent CTE side effects — PostgreSQL CTEs are optimization fences, so writable CTEs execute even with LIMIT 0.
- DuckDB does not currently support MERGE. If it adds MERGE RETURNING in the future, add `MERGE` to the prefix check in `isDMLReturning`.

## TODO Reference

See `TODO.md` for the full feature roadmap and known issues.
