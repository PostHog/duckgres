# Duckgres

A PostgreSQL wire protocol compatible server backed by DuckDB. Connect with any PostgreSQL client (psql, pgAdmin, lib/pq, psycopg2, etc.) and get DuckDB's analytical query performance.

## Features

- **PostgreSQL Wire Protocol**: Full compatibility with PostgreSQL clients
- **TLS Encryption**: Required TLS connections with auto-generated self-signed certificates
- **Per-User Databases**: Each authenticated user gets their own isolated DuckDB database file
- **Password Authentication**: MD5 password authentication
- **Extended Query Protocol**: Support for prepared statements, binary format, and parameterized queries
- **DuckDB Extensions**: Configurable extension loading (ducklake enabled by default)
- **DuckLake Integration**: Auto-attach DuckLake catalogs for lakehouse workflows
- **Rate Limiting**: Built-in protection against brute-force attacks
- **Flexible Configuration**: YAML config files, environment variables, and CLI flags

## Quick Start

### Build

```bash
go build -o duckgres .
```

### Run

```bash
./duckgres
```

The server starts on port 5432 by default with TLS enabled. Database files are stored in `./data/`. Self-signed certificates are auto-generated in `./certs/` if not present.

### Connect

```bash
# Using psql (sslmode=require is needed for TLS)
PGPASSWORD=postgres psql "host=localhost port=5432 user=postgres sslmode=require"

# Or with any PostgreSQL driver
```

## Configuration

Duckgres supports three configuration methods (in order of precedence):
1. CLI flags (highest priority)
2. Environment variables
3. YAML config file
4. Built-in defaults (lowest priority)

### YAML Configuration

Create a `duckgres.yaml` file (see `duckgres.example.yaml` for a complete example):

```yaml
host: "0.0.0.0"
port: 5432
data_dir: "./data"

tls:
  cert: "./certs/server.crt"
  key: "./certs/server.key"

users:
  postgres: "postgres"
  alice: "alice123"

extensions:
  - ducklake
  - httpfs

ducklake:
  metadata_store: "postgres:dbname=ducklake"
  data_path: "s3://my-bucket/ducklake-data/"

rate_limit:
  max_failed_attempts: 5
  failed_attempt_window: "5m"
  ban_duration: "15m"
  max_connections_per_ip: 100
```

Run with config file:

```bash
./duckgres --config duckgres.yaml
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DUCKGRES_CONFIG` | Path to YAML config file | - |
| `DUCKGRES_HOST` | Host to bind to | `0.0.0.0` |
| `DUCKGRES_PORT` | Port to listen on | `5432` |
| `DUCKGRES_DATA_DIR` | Directory for DuckDB files | `./data` |
| `DUCKGRES_CERT` | TLS certificate file | `./certs/server.crt` |
| `DUCKGRES_KEY` | TLS private key file | `./certs/server.key` |
| `DUCKGRES_DUCKLAKE_METADATA_STORE` | DuckLake metadata store | - |
| `DUCKGRES_DUCKLAKE_DATA_PATH` | DuckLake data path | - |

### CLI Flags

```bash
./duckgres --help

Options:
  -config string    Path to YAML config file
  -host string      Host to bind to
  -port int         Port to listen on
  -data-dir string  Directory for DuckDB files
  -cert string      TLS certificate file
  -key string       TLS private key file
```

## DuckDB Extensions

Extensions are automatically installed and loaded when a user's database is first opened. The `ducklake` extension is enabled by default.

```yaml
extensions:
  - ducklake    # Default - DuckLake lakehouse format
  - httpfs      # HTTP/S3 file system access
  - parquet     # Parquet file support (built-in)
  - json        # JSON support (built-in)
  - postgres    # PostgreSQL scanner
```

## DuckLake Integration

DuckLake provides a SQL-based lakehouse format. When configured, the DuckLake catalog is automatically attached on connection:

```yaml
ducklake:
  # Metadata store (required to enable DuckLake catalog)
  metadata_store: "postgres:host=localhost dbname=ducklake"
  # Data path for table files
  data_path: "s3://my-bucket/ducklake-data/"
```

This runs the equivalent of:
```sql
ATTACH 'ducklake:postgres:host=localhost dbname=ducklake' (DATA_PATH 's3://my-bucket/ducklake-data/')
```

See [DuckLake documentation](https://ducklake.select/docs/stable/duckdb/usage/connecting) for more details.

## Rate Limiting

Built-in rate limiting protects against brute-force authentication attacks:

- **Failed attempt tracking**: Bans IPs after too many failed auth attempts
- **Connection limits**: Limits concurrent connections per IP
- **Auto-cleanup**: Expired records are automatically cleaned up

```yaml
rate_limit:
  max_failed_attempts: 5        # Ban after 5 failures
  failed_attempt_window: "5m"   # Within 5 minutes
  ban_duration: "15m"           # Ban lasts 15 minutes
  max_connections_per_ip: 100   # Max concurrent connections
```

## Usage Examples

```sql
-- Create a table
CREATE TABLE events (
    id INTEGER,
    name VARCHAR,
    timestamp TIMESTAMP,
    value DOUBLE
);

-- Insert data
INSERT INTO events VALUES
    (1, 'click', '2024-01-01 10:00:00', 1.5),
    (2, 'view', '2024-01-01 10:01:00', 2.0);

-- Query with DuckDB's analytical power
SELECT name, COUNT(*), AVG(value)
FROM events
GROUP BY name;

-- Use prepared statements (via client drivers)
-- Works with lib/pq, psycopg2, JDBC, etc.
```

## Architecture

```
┌─────────────────┐
│  PostgreSQL     │
│  Client (psql)  │
└────────┬────────┘
         │ PostgreSQL Wire Protocol (TLS)
         ▼
┌─────────────────┐
│    Duckgres     │
│    Server       │
└────────┬────────┘
         │ database/sql
         ▼
┌─────────────────┐
│    DuckDB       │
│  (per-user db)  │
│  + Extensions   │
│  + DuckLake     │
└─────────────────┘
```

## Supported Features

### SQL Commands
- `SELECT` - Full query support with binary result format
- `INSERT` - Single and multi-row inserts
- `UPDATE` - With WHERE clauses
- `DELETE` - With WHERE clauses
- `CREATE TABLE/INDEX/VIEW`
- `DROP TABLE/INDEX/VIEW`
- `ALTER TABLE`
- `BEGIN/COMMIT/ROLLBACK` (DuckDB transaction support)

### PostgreSQL Compatibility
- Extended query protocol (prepared statements)
- Binary and text result formats
- MD5 password authentication
- Basic `pg_catalog` system tables for client compatibility
- `\dt`, `\d`, and other psql meta-commands

## Limitations

- **Single Process**: Each user's database is opened in the same process
- **No Replication**: Single-node only
- **Limited System Catalog**: Some `pg_*` system tables are not available

## Dependencies

- [DuckDB Go Driver](https://github.com/duckdb/duckdb-go) - DuckDB database engine

## License

MIT
