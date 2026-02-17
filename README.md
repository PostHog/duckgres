# Duckgres

<p align="center">
  <img src="media/oh_duck.png" alt="Duckgres Mascot" width="200">
</p>

A PostgreSQL wire protocol compatible server backed by DuckDB. Connect with any PostgreSQL client (psql, pgAdmin, lib/pq, psycopg2, etc.) and get DuckDB's analytical query performance.

## Table of Contents

- [Features](#features)
- [Metrics](#metrics)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
  - [YAML Configuration](#yaml-configuration)
  - [Environment Variables](#environment-variables)
  - [CLI Flags](#cli-flags)
  - [PostHog Logging](#posthog-logging)
- [DuckDB Extensions](#duckdb-extensions)
- [DuckLake Integration](#ducklake-integration)
  - [Quick Start with Docker](#quick-start-with-docker)
  - [Object Storage Configuration](#object-storage-configuration)
  - [Seeding Sample Data](#seeding-sample-data)
- [COPY Protocol](#copy-protocol)
- [Graceful Shutdown](#graceful-shutdown)
- [Rate Limiting](#rate-limiting)
- [Usage Examples](#usage-examples)
- [Architecture](#architecture)
  - [Standalone Mode](#standalone-mode)
  - [Control Plane Mode](#control-plane-mode)
- [Two-Tier Query Processing](#two-tier-query-processing)
- [Supported Features](#supported-features)
- [Limitations](#limitations)
- [Dependencies](#dependencies)
- [License](#license)

## Features

- **PostgreSQL Wire Protocol**: Full compatibility with PostgreSQL clients
- **Two-Tier Query Processing**: Transparently handles both PostgreSQL and DuckDB-specific syntax
- **TLS Encryption**: Required TLS connections with auto-generated self-signed certificates
- **Per-User Databases**: Each authenticated user gets their own isolated DuckDB database file
- **Password Authentication**: Cleartext password authentication over TLS
- **Extended Query Protocol**: Support for prepared statements, binary format, and parameterized queries
- **COPY Protocol**: Bulk data import/export with `COPY FROM STDIN` and `COPY TO STDOUT`
- **DuckDB Extensions**: Configurable extension loading (ducklake enabled by default)
- **DuckLake Integration**: Auto-attach DuckLake catalogs for lakehouse workflows
- **Rate Limiting**: Built-in protection against brute-force attacks
- **Graceful Shutdown**: Waits for in-flight queries before exiting
- **Control Plane Mode**: Multi-process architecture with long-lived workers, zero-downtime deployments, and rolling updates
- **Flexible Configuration**: YAML config files, environment variables, and CLI flags
- **Prometheus Metrics**: Built-in metrics endpoint for monitoring

## Metrics

Duckgres exposes Prometheus metrics on `:9090/metrics`. The metrics port is currently fixed at 9090 and cannot be changed via configuration.

| Metric | Type | Description |
|--------|------|-------------|
| `duckgres_connections_open` | Gauge | Number of currently open client connections |
| `duckgres_query_duration_seconds` | Histogram | Query execution duration (includes `_count`, `_sum`, `_bucket`) |
| `duckgres_query_errors_total` | Counter | Total number of failed queries |
| `duckgres_auth_failures_total` | Counter | Total number of authentication failures |
| `duckgres_rate_limit_rejects_total` | Counter | Total number of connections rejected due to rate limiting |
| `duckgres_rate_limited_ips` | Gauge | Number of currently rate-limited IP addresses |
| `duckgres_flight_auth_sessions_active` | Gauge | Number of active Flight auth sessions on the control plane |
| `duckgres_control_plane_workers_active` | Gauge | Number of active control-plane worker processes |
| `duckgres_flight_sessions_reaped_total{trigger}` | Counter | Number of Flight auth sessions reaped (`trigger=periodic|forced`) |
| `duckgres_flight_max_workers_retry_total{outcome}` | Counter | Max-worker retry outcomes for Flight session creation (`outcome=attempted|succeeded|failed`) |

### Testing Metrics

- `scripts/test_metrics.sh` - Runs a quick sanity check (starts server, runs queries, verifies counts)
- `scripts/load_generator.sh` - Generates continuous query load until Ctrl-C
- `prometheus-docker-compose.yml` - Starts Prometheus locally to scrape metrics (UI at http://localhost:9091)

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

### Docker

```bash
docker build -t duckgres .
docker run --rm -p 5432:5432 -p 9090:9090 duckgres
```

Mount a config file and persist data:

```bash
docker run --rm \
  -p 5432:5432 -p 9090:9090 \
  -v ./duckgres.yaml:/app/duckgres.yaml \
  -v ./data:/app/data \
  duckgres
```

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
flight_port: 8815
flight_session_idle_ttl: "10m"
flight_session_reap_interval: "1m"
flight_handle_idle_ttl: "15m"
flight_session_token_ttl: "1h"
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
| `DUCKGRES_FLIGHT_PORT` | Control-plane Flight SQL ingress port (`0` disables) | `0` |
| `DUCKGRES_FLIGHT_SESSION_IDLE_TTL` | Flight auth session idle TTL | `10m` |
| `DUCKGRES_FLIGHT_SESSION_REAP_INTERVAL` | Flight auth session reap interval | `1m` |
| `DUCKGRES_FLIGHT_HANDLE_IDLE_TTL` | Flight prepared/query handle idle TTL | `15m` |
| `DUCKGRES_FLIGHT_SESSION_TOKEN_TTL` | Flight issued session token absolute TTL | `1h` |
| `DUCKGRES_DATA_DIR` | Directory for DuckDB files | `./data` |
| `DUCKGRES_CERT` | TLS certificate file | `./certs/server.crt` |
| `DUCKGRES_KEY` | TLS private key file | `./certs/server.key` |
| `DUCKGRES_MEMORY_LIMIT` | DuckDB memory_limit per session (e.g., `4GB`) | Auto-detected |
| `DUCKGRES_THREADS` | DuckDB threads per session | `runtime.NumCPU()` |
| `DUCKGRES_PROCESS_ISOLATION` | Enable process isolation (`1` or `true`) | `false` |
| `DUCKGRES_IDLE_TIMEOUT` | Connection idle timeout (e.g., `30m`, `1h`, `-1` to disable) | `24h` |
| `DUCKGRES_DUCKLAKE_METADATA_STORE` | DuckLake metadata connection string | - |
| `POSTHOG_API_KEY` | PostHog project API key (`phc_...`); enables log export | - |
| `POSTHOG_HOST` | PostHog ingest host | `us.i.posthog.com` |
| `ADDITIONAL_POSTHOG_API_KEYS` | **(Experimental)** Comma-separated list of additional PostHog API keys to publish logs to. Requires `POSTHOG_API_KEY` to be set. | - |
| `DUCKGRES_IDENTIFIER` | Suffix appended to the OTel `service.name` in PostHog logs (e.g., `duckgres-acme`); only used when `POSTHOG_API_KEY` is set | - |

### PostHog Logging

Duckgres can optionally export structured logs to [PostHog Logs](https://posthog.com/docs/logs) via the OpenTelemetry Protocol (OTLP). Logs are always written to stderr regardless of this setting.

To enable, set your PostHog project API key:

```bash
export POSTHOG_API_KEY=phc_your_project_api_key
./duckgres
```

For EU Cloud or self-hosted PostHog instances, override the ingest host:

```bash
export POSTHOG_API_KEY=phc_your_project_api_key
export POSTHOG_HOST=eu.i.posthog.com
./duckgres
```

### CLI Flags

```bash
./duckgres --help

Options:
  -config string           Path to YAML config file
  -host string             Host to bind to
  -port int                Port to listen on
  -flight-port int         Control-plane Arrow Flight SQL ingress port, 0=disabled
  -flight-session-idle-ttl string      Flight auth session idle TTL (e.g., '10m')
  -flight-session-reap-interval string Flight auth session reap interval (e.g., '1m')
  -flight-handle-idle-ttl string       Flight prepared/query handle idle TTL (e.g., '15m')
  -flight-session-token-ttl string     Flight issued session token absolute TTL (e.g., '1h')
  -data-dir string         Directory for DuckDB files
  -cert string             TLS certificate file
  -key string              TLS private key file
  -memory-limit string     DuckDB memory_limit per session (e.g., '4GB')
  -threads int             DuckDB threads per session
  -process-isolation       Enable process isolation (spawn child process per connection)
  -idle-timeout string     Connection idle timeout (e.g., '30m', '1h', '-1' to disable)
  -mode string             Run mode: standalone (default), control-plane, or duckdb-service
  -min-workers int         Pre-warm worker count at startup (control-plane mode, default 0)
  -max-workers int         Max worker processes, 0=unlimited (control-plane mode)
  -memory-budget string    Total memory for all DuckDB sessions (e.g., '24GB')
  -socket-dir string       Unix socket directory (control-plane mode)
  -handover-socket string  Handover socket for graceful deployment (control-plane mode)
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

Duckgres supports two run modes: **standalone** (single process, default) and **control-plane** (multi-process with worker pool).

### Standalone Mode

The default mode runs everything in a single process:

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

### Control Plane Mode

For production deployments, control-plane mode splits the server into a **control plane** and a pool of long-lived **worker processes**. The control plane owns client connections end-to-end (TLS, authentication, PostgreSQL wire protocol, SQL transpilation), while workers are thin DuckDB execution engines reachable via Arrow Flight SQL over Unix sockets. Optional control-plane Flight ingress (`flight_port`) also exposes Arrow Flight SQL directly with HTTP Basic auth (`Authorization: Basic ...`), compatible with Duckhog clients.

```
                    CONTROL PLANE (duckgres --mode control-plane)
                    ┌──────────────────────────────────────────────┐
  PG Client ──TLS──>│ PG TCP Listener                              │
 Flight SQL Client ─>│ Flight SQL TCP Listener (Basic Auth)         │
                    │ TLS Termination + Password Auth              │
                    │ PostgreSQL Wire Protocol                     │
                    │ SQL Transpilation (PG → DuckDB)              │
                    │ Rate Limiting                                │
                    │ Session Manager + Connection Router           │
                    │   │ Arrow Flight SQL (Unix socket)           │
                    │   ▼                                          │
                    └──────────────────────────────────────────────┘
                                                           │
                                                Flight SQL (UDS)
                                                           │
                    WORKER POOL                            ▼
                    ┌──────────────────────────────────────────────┐
                    │ Worker 1 (duckgres --mode duckdb-service)    │
                    │   Arrow Flight SQL Server (Unix socket)      │
                    │   Bearer Token Auth                          │
                    │   DuckDB Instance (long-lived)               │
                    │   ├── Session 1                               │
                    │   ├── Session 2                               │
                    │   └── Session N ...                           │
                    ├──────────────────────────────────────────────┤
                    │ Worker 2 ...                                  │
                    └──────────────────────────────────────────────┘
```

Start in control-plane mode:

```bash
# Start in control-plane mode (workers spawn on demand, 1 per connection)
./duckgres --mode control-plane --port 5432

# Enable Flight SQL ingress for Duckhog-compatible clients
./duckgres --mode control-plane --port 5432 --flight-port 8815

# Pre-warm 2 workers and cap at 10
./duckgres --mode control-plane --port 5432 --min-workers 2 --max-workers 10

# Connect with psql (identical to standalone mode)
PGPASSWORD=postgres psql "host=localhost port=5432 user=postgres sslmode=require"

# Flight SQL clients use Basic auth headers (user/password)
# Example endpoint: grpc+tls://localhost:8815
```

**Zero-downtime deployment** using the handover protocol:

```bash
# Start the first control plane with a handover socket
./duckgres --mode control-plane --port 5432 --handover-socket /var/run/duckgres/handover.sock

# Deploy a new version - it takes over the listener and workers without dropping connections
./duckgres-v2 --mode control-plane --port 5432 --handover-socket /var/run/duckgres/handover.sock
```

**Rolling worker updates** via signal:

```bash
# Replace workers one at a time (drains sessions before replacing each worker)
kill -USR2 <control-plane-pid>
```

## Two-Tier Query Processing

Duckgres uses a two-tier approach to handle both PostgreSQL and DuckDB-specific SQL syntax transparently:

```
┌─────────────────────────────────────────────────────────────────┐
│                        Incoming Query                           │
└─────────────────────────────┬───────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Tier 1: PostgreSQL Parser                      │
│                   (pg_query_go / libpg_query)                   │
└──────────────┬─────────────────────────────────┬────────────────┘
               │                                 │
          Parse OK                          Parse Failed
               │                                 │
               ▼                                 ▼
┌──────────────────────────┐    ┌─────────────────────────────────┐
│   Transpile PG → DuckDB  │    │   Tier 2: DuckDB Validation     │
│   (type mappings, etc.)  │    │   (EXPLAIN or direct execute)   │
└──────────────┬───────────┘    └──────────────┬──────────────────┘
               │                               │
               ▼                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Execute on DuckDB                           │
└─────────────────────────────────────────────────────────────────┘
```

### How It Works

1. **Tier 1 (PostgreSQL Parser)**: All queries first pass through the PostgreSQL parser. Valid PostgreSQL syntax is transpiled to DuckDB-compatible SQL (handling differences in types, functions, and system catalogs).

2. **Tier 2 (DuckDB Fallback)**: If PostgreSQL parsing fails, the query is validated directly against DuckDB using `EXPLAIN`. If valid, it executes natively. This enables DuckDB-specific syntax that isn't valid PostgreSQL.

### Supported DuckDB-Specific Syntax

The following DuckDB features work transparently through the fallback mechanism: `FROM`-first queries, `SELECT * EXCLUDE/REPLACE`, `DESCRIBE`, `SUMMARIZE`, `QUALIFY` clause, lambda functions, positional joins, `ASOF` joins, struct operations, `COLUMNS` expression, and `SAMPLE`.

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

- **Single Node**: No built-in replication or clustering
- **Limited System Catalog**: Some `pg_*` system tables are stubs (return empty)
- **Type OID Mapping**: Incomplete (some types show as "unknown")

## Dependencies

- [DuckDB Go Driver](https://github.com/duckdb/duckdb-go) - DuckDB database engine

## License

MIT
