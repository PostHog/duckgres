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
- **server/catalog.go**: pg_catalog compatibility views and macros initialization
- **server/types.go**: Type OID mapping between DuckDB and PostgreSQL
- **server/ratelimit.go**: Rate limiting for brute-force protection
- **server/certs.go**: Auto-generation of self-signed TLS certificates
- **transpiler/**: AST-based SQL transpiler (PostgreSQL → DuckDB)
  - `transpiler.go`: Main API, transform pipeline orchestration
  - `config.go`: Configuration types (DuckLakeMode, ConvertPlaceholders)
  - `transform/`: Individual transform implementations

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

## Running with DuckLake

To test duckgres with DuckLake (S3-backed storage with PostgreSQL metadata):

### 1. Start dependencies (PostgreSQL + MinIO)
```bash
docker-compose up -d
```

This starts:
- PostgreSQL on port 5433 (metadata store)
- MinIO on port 9000 (S3-compatible object storage)
- Creates `ducklake` bucket automatically

### 2. Create local test config
Create `duckgres_local_test.yaml`:
```yaml
host: "0.0.0.0"
port: 35437
data_dir: "./data"

users:
  postgres: "postgres"

extensions:
  - ducklake

ducklake:
  metadata_store: "postgres:host=localhost port=5433 user=ducklake password=ducklake dbname=ducklake"
  object_store: "s3://ducklake/data/"
  s3_provider: "config"
  s3_endpoint: "localhost:9000"
  s3_access_key: "minioadmin"
  s3_secret_key: "minioadmin"
  s3_region: "us-east-1"
  s3_use_ssl: false
  s3_url_style: "path"
```

### 3. Build and run
```bash
go build -o duckgres . && ./duckgres --config duckgres_local_test.yaml
```

### 4. Connect and test
```bash
PGPASSWORD=postgres psql "host=127.0.0.1 port=35437 user=postgres sslmode=require"
```

### Cleanup
```bash
pkill -f duckgres
docker-compose down
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

- Single process (all users share one process)
- No replication
- Some pg_catalog tables are stubs (return empty)
- Type OID mapping is incomplete (some types show as "unknown")

## TODO Reference

See `TODO.md` for the full feature roadmap and known issues.
