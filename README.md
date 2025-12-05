# Duckgres

A PostgreSQL wire protocol compatible server backed by DuckDB. Connect with any PostgreSQL client (psql, pgAdmin, lib/pq, psycopg2, etc.) and get DuckDB's analytical query performance.

## Features

- **PostgreSQL Wire Protocol**: Full compatibility with PostgreSQL clients
- **TLS Encryption**: Required TLS connections with auto-generated self-signed certificates
- **Per-User Databases**: Each authenticated user gets their own isolated DuckDB database file
- **Password Authentication**: Cleartext password authentication over TLS
- **Extended Query Protocol**: Support for prepared statements, binary format, and parameterized queries
- **COPY Protocol**: Bulk data import/export with `COPY FROM STDIN` and `COPY TO STDOUT`
- **DuckDB Extensions**: Configurable extension loading (ducklake enabled by default)
- **DuckLake Integration**: Auto-attach DuckLake catalogs for lakehouse workflows
- **Rate Limiting**: Built-in protection against brute-force attacks
- **Graceful Shutdown**: Waits for in-flight queries before exiting
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
  metadata_store: "postgres:host=localhost user=ducklake password=secret dbname=ducklake"

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
| `DUCKGRES_DUCKLAKE_METADATA_STORE` | DuckLake metadata connection string | - |

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
  # Full connection string for the DuckLake metadata database
  metadata_store: "postgres:host=ducklake.example.com user=ducklake password=secret dbname=ducklake"
```

This runs the equivalent of:
```sql
ATTACH 'ducklake:postgres:host=ducklake.example.com user=ducklake password=secret dbname=ducklake' AS ducklake;
```

See [DuckLake documentation](https://ducklake.select/docs/stable/duckdb/usage/connecting) for more details.

### Quick Start with Docker

The easiest way to get started with DuckLake is using the included Docker Compose setup:

```bash
# Start PostgreSQL (metadata) and MinIO (object storage)
docker compose up -d

# Wait for services to be ready
docker compose logs -f  # Look for "Bucket ducklake created successfully"

# Start Duckgres with DuckLake configured
./duckgres --config duckgres.yaml

# Connect and start using DuckLake
PGPASSWORD=postgres psql "host=localhost port=5432 user=postgres sslmode=require"
```

The `docker-compose.yaml` creates:

**PostgreSQL** (metadata catalog):
- Host: `localhost`
- Port: `5433` (mapped to avoid conflicts)
- Database: `ducklake`
- User/Password: `ducklake` / `ducklake`

**MinIO** (S3-compatible object storage):
- S3 API: `localhost:9000`
- Web Console: `http://localhost:9001`
- Access Key: `minioadmin`
- Secret Key: `minioadmin`
- Bucket: `ducklake` (auto-created on startup)

The included `duckgres.yaml` is pre-configured to use both services.

### Object Storage Configuration

DuckLake can store data files in S3-compatible object storage (AWS S3, MinIO, etc.). Two credential providers are supported:

#### Option 1: Explicit Credentials (MinIO / Access Keys)

```yaml
ducklake:
  metadata_store: "postgres:host=localhost port=5433 user=ducklake password=ducklake dbname=ducklake"
  object_store: "s3://ducklake/data/"
  s3_provider: "config"            # Explicit credentials (default if s3_access_key is set)
  s3_endpoint: "localhost:9000"    # MinIO or custom S3 endpoint
  s3_access_key: "minioadmin"
  s3_secret_key: "minioadmin"
  s3_region: "us-east-1"
  s3_use_ssl: false
  s3_url_style: "path"             # "path" for MinIO, "vhost" for AWS S3
```

#### Option 2: AWS Credential Chain (IAM Roles / Environment)

For AWS S3 with IAM roles, environment variables, or config files:

```yaml
ducklake:
  metadata_store: "postgres:host=localhost user=ducklake password=ducklake dbname=ducklake"
  object_store: "s3://my-bucket/ducklake/"
  s3_provider: "credential_chain"  # AWS SDK credential chain
  s3_chain: "env;config"           # Which sources to check (optional)
  s3_profile: "my-profile"         # AWS profile name (optional)
  s3_region: "us-west-2"           # Override auto-detected region (optional)
```

The credential chain checks these sources in order:
- `env` - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- `config` - AWS config files (`~/.aws/credentials`, `~/.aws/config`)
- `sts` - AWS STS assume role
- `sso` - AWS Single Sign-On
- `instance` - EC2 instance metadata (IAM roles)
- `process` - External process credentials

See [DuckDB S3 API docs](https://duckdb.org/docs/stable/core_extensions/httpfs/s3api#credential_chain-provider) for details.

#### Environment Variables

All S3 settings can be configured via environment variables:
- `DUCKGRES_DUCKLAKE_OBJECT_STORE` - S3 path (e.g., `s3://bucket/path/`)
- `DUCKGRES_DUCKLAKE_S3_PROVIDER` - `config` or `credential_chain`
- `DUCKGRES_DUCKLAKE_S3_ENDPOINT` - S3 endpoint (for MinIO)
- `DUCKGRES_DUCKLAKE_S3_ACCESS_KEY` - Access key ID
- `DUCKGRES_DUCKLAKE_S3_SECRET_KEY` - Secret access key
- `DUCKGRES_DUCKLAKE_S3_REGION` - AWS region
- `DUCKGRES_DUCKLAKE_S3_USE_SSL` - Use HTTPS (true/false)
- `DUCKGRES_DUCKLAKE_S3_URL_STYLE` - `path` or `vhost`
- `DUCKGRES_DUCKLAKE_S3_CHAIN` - Credential chain sources
- `DUCKGRES_DUCKLAKE_S3_PROFILE` - AWS profile name

### Seeding Sample Data

A seed script is provided to populate DuckLake with sample e-commerce and analytics data:

```bash
# Seed with default connection (localhost:5432, postgres/postgres)
./scripts/seed_ducklake.sh

# Seed with custom connection
./scripts/seed_ducklake.sh --host 127.0.0.1 --port 5432 --user postgres --password postgres

# Clean existing tables and reseed
./scripts/seed_ducklake.sh --clean
```

The script creates the following tables:
- `categories` - Product categories (5 rows)
- `products` - E-commerce products (15 rows)
- `customers` - Customer records (10 rows)
- `orders` - Order headers (12 rows)
- `order_items` - Order line items (20 rows)
- `events` - Analytics events with JSON properties (15 rows)
- `page_views` - Web analytics data (15 rows)

Example queries after seeding:

```sql
-- Top products by price
SELECT name, price FROM products ORDER BY price DESC LIMIT 5;

-- Orders with customer info
SELECT o.id, c.first_name, c.last_name, o.total_amount, o.status
FROM orders o JOIN customers c ON o.customer_id = c.id;

-- Event funnel analysis
SELECT event_name, COUNT(*) FROM events GROUP BY event_name ORDER BY COUNT(*) DESC;
```

## COPY Protocol

Duckgres supports PostgreSQL's COPY protocol for efficient bulk data import and export:

```sql
-- Export data to stdout (tab-separated)
COPY tablename TO STDOUT;

-- Export as CSV with headers
COPY tablename TO STDOUT WITH CSV HEADER;

-- Export query results
COPY (SELECT * FROM tablename WHERE id > 100) TO STDOUT WITH CSV;

-- Import data from stdin
COPY tablename FROM STDIN;

-- Import CSV with headers
COPY tablename FROM STDIN WITH CSV HEADER;
```

This works with psql's `\copy` command and programmatic COPY operations from PostgreSQL drivers.

## Graceful Shutdown

Duckgres handles shutdown signals (SIGINT, SIGTERM) gracefully:

- Stops accepting new connections immediately
- Waits for in-flight queries to complete (default 30s timeout)
- Logs active connection count during shutdown
- Closes all database connections cleanly

The shutdown timeout can be configured:

```go
cfg := server.Config{
    ShutdownTimeout: 60 * time.Second,
}
```

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
- `COPY` - Bulk data loading and export (see below)

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
