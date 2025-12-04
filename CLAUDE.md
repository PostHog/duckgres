# Claude Code Context for Duckgres

This file provides context for Claude Code sessions working on this codebase.

## Project Overview

Duckgres is a PostgreSQL wire protocol server backed by DuckDB. It allows any PostgreSQL client (psql, pgAdmin, lib/pq, psycopg2, JDBC, etc.) to connect and execute queries against DuckDB databases.

## Architecture

```
PostgreSQL Client → TLS → Duckgres Server → DuckDB (per-user database)
```

### Key Components

- **main.go**: Entry point, configuration loading (CLI flags, env vars, YAML)
- **server/server.go**: Server struct, connection handling, graceful shutdown
- **server/conn.go**: Client connection handling, query execution, COPY protocol
- **server/protocol.go**: PostgreSQL wire protocol message encoding/decoding
- **server/catalog.go**: pg_catalog compatibility (views, functions, query rewriting)
- **server/types.go**: Type OID mapping between DuckDB and PostgreSQL
- **server/ratelimit.go**: Rate limiting for brute-force protection
- **server/tls.go**: Auto-generation of self-signed TLS certificates

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

## pg_catalog Compatibility (server/catalog.go)

psql and other clients expect PostgreSQL system catalogs. We provide compatibility by:

1. **Creating views** in main schema that mirror pg_catalog tables:
   - `pg_database`, `pg_class_full`, `pg_collation`, `pg_policy`, `pg_roles`
   - `pg_statistic_ext`, `pg_publication`, `pg_publication_rel`, `pg_inherits`, etc.

2. **Creating macros** for PostgreSQL functions:
   - `pg_get_userbyid`, `pg_table_is_visible`, `format_type`, `pg_get_expr`
   - `obj_description`, `col_description`, `pg_get_indexdef`, etc.

3. **Query rewriting** to replace PostgreSQL-specific syntax:
   - `pg_catalog.pg_class` → `pg_class_full`
   - `OPERATOR(pg_catalog.~)` → `~`
   - `::pg_catalog.regtype` → `::VARCHAR`

## COPY Protocol (server/conn.go)

Supports bulk data transfer:
- **COPY TO STDOUT**: Streams query results to client
- **COPY FROM STDIN**: Receives data from client, inserts row by row
- Supports CSV format with HEADER option

## Configuration

Three-tier configuration (highest to lowest priority):
1. CLI flags (`--port`, `--config`, etc.)
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
2. Add regex pattern to rewrite `pg_catalog.viewname` to `viewname`
3. Add the replacement in `rewritePgCatalogQuery()`

### Adding a new PostgreSQL function
1. Add `CREATE MACRO` in the `functions` slice in `initPgCatalog()`
2. Add function name to `pgCatalogFunctions` slice for query rewriting

### Adding protocol support
1. Add message type constant in `protocol.go`
2. Add write function (e.g., `writeCopyData()`)
3. Handle in message loop in `conn.go`

## Dependencies

- `github.com/duckdb/duckdb-go/v2` - DuckDB Go driver
- `gopkg.in/yaml.v3` - YAML config parsing

## Known Limitations

- Single process (all users share one process)
- No replication
- Some pg_catalog tables are stubs (return empty)
- Type OID mapping is incomplete (some types show as "unknown")

## TODO Reference

See `TODO.md` for the full feature roadmap and known issues.
