# Client Compatibility Tests

Tests duckgres against real PostgreSQL client libraries across multiple languages. Each client connects over TLS, runs a shared query catalog, then exercises driver-specific features (DDL/DML, prepared statements, COPY, ORM, etc.).

Results are collected by a central HTTP server, stored in DuckDB, and exported as a timestamped `.duckdb` database for analysis.

## Prerequisites

- Docker and Docker Compose v2
- [just](https://github.com/casey/just) command runner
- (Optional) [DuckDB CLI](https://duckdb.org/docs/installation/) for querying results

## Quick Start

```bash
# Run all clients and produce a report
just all

# Query the results database
just query-last-run
```

## Commands

| Command | Description |
|---------|-------------|
| `just all` | Build and run all clients, produce report |
| `just build` | Build all container images |
| `just <client>` | Run a single client standalone (no report) |
| `just down` | Tear down containers and volumes |
| `just clean` | Remove the `results/` directory |
| `just summary` | Pass/fail counts by client and suite |
| `just client-results <client>` | Full results for a specific client |
| `just view-last-run` | Print the full results table |
| `just query-last-run` | Open the latest results database in DuckDB CLI |

Available single-client targets: `psycopg`, `pgx`, `psql`, `jdbc`, `tokio-postgres`, `node-postgres`, `sqlalchemy`.

## Clients

| Client | Language | Library | Image Base |
|--------|----------|---------|------------|
| psycopg | Python 3.12 | psycopg2-binary | python:3.12-slim |
| pgx | Go 1.24 | jackc/pgx/v5 | golang:1.24 |
| psql | Bash | psql CLI | postgres:17 |
| jdbc | Java 17 | org.postgresql:postgresql 42.7 | maven:3-eclipse-temurin-17 |
| tokio-postgres | Rust 1.84 | tokio-postgres 0.7 | rust:1.84-bookworm |
| node-postgres | Node.js 22 | pg 8.x | node:22-bookworm-slim |
| sqlalchemy | Python 3.12 | SQLAlchemy 2.x + psycopg2 | python:3.12-slim |

## Architecture

```
                                    +-----------------+
                                    | results-gatherer|
                                    | (HTTP + DuckDB) |
                                    +-------+---------+
                                            |
            POST /result                    | POST /shutdown
            (per test)                      | (from justfile)
                |                           |
    +-----------+---------------------------+-----------+
    |           |           |           |           |   |
 psycopg      pgx        psql       jdbc  tokio-postgres  sqlalchemy
    |           |           |           |           |   |
    +-----------+-----------+-----------+-----------+---+
                            |
                      compat-duckgres
                      (wire protocol)
```

### Lifecycle

1. `docker compose up -d` starts duckgres, the results gatherer, and all clients
2. Duckgres and the results gatherer expose healthchecks; clients block on `depends_on: service_healthy`
3. Each client runs its tests, POSTing results to the gatherer via `POST /result`
4. The justfile runs `docker wait` on all client containers
5. Once all clients have exited (pass or crash), the justfile POSTs `/shutdown` to the gatherer
6. The gatherer prints the report, exports results, and exits
7. `docker compose down -v` tears everything down

This means the gatherer never hangs waiting for clients that crashed without reporting.

### Standalone Mode

Running a single client (`just psycopg`, etc.) uses `docker compose run`, which starts only duckgres and the target client. The results gatherer is not involved; the client just prints to stdout. The HTTP reporting calls are fire-and-forget, so they silently fail when the gatherer isn't running.

## Test Structure

Every client runs two categories of tests:

### Shared Queries (`queries.yaml`)

A YAML catalog of 44 queries across 5 suites that every client executes. A query passes if it executes without error (row counts are reported but not validated).

| Suite | Count | Purpose |
|-------|-------|---------|
| `catalog_views` | 7 | Core pg_catalog views (`pg_database`, `pg_namespace`, `pg_type`, etc.) |
| `catalog_funcs` | 14 | PostgreSQL functions (`format_type`, `version()`, `current_setting`, etc.) |
| `info_schema` | 4 | `information_schema.tables`, `.columns`, `.schemata` |
| `catalog_joins` | 5 | Multi-table joins that real tools emit (psql `\dt`, `\dn`, `\l` queries) |
| `catalog_stubs` | 14 | Stub views that must exist but return empty (`pg_matviews`, `pg_policy`, etc.) |

### Client-Specific Suites

Each client tests driver-specific features beyond the shared catalog:

| Client | Extra Suites |
|--------|-------------|
| psycopg | `connection`, `ddl_dml`, `cursor_metadata`, `executemany`, `dict_cursor`, `copy` |
| pgx | `connection`, `ddl_dml`, `batch` (pgx.Batch / SendBatch) |
| psql | `psql_commands` (`\dt`, `\dn`, `\l`, `\di`, `\dv`, `\df`), `ddl_dml`, `copy` |
| jdbc | `connection`, `ddl_dml`, `metadata` (DatabaseMetaData), `batch`, `resultset_metadata` |
| tokio-postgres | `connection`, `ddl_dml`, `prepared` |
| node-postgres | `connection`, `ddl_dml`, `prepared`, `result_metadata` |
| sqlalchemy | `connection`, `core_ddl_dml`, `orm`, `inspection`, `raw_params` |

## Results

After `just all`, the `results/` directory contains:

```
results/
├── results_20250225_143012.json      # All results as JSON
├── results_20250225_143012.duckdb    # Queryable DuckDB database
├── psycopg/
│   ├── stdout.log
│   └── stderr.log
├── pgx/
│   ├── stdout.log
│   └── stderr.log
└── ...                               # Per-client log directories
```

### Results Database Schema

```sql
-- Query catalog (from queries.yaml)
CREATE TABLE queries (
    suite VARCHAR,
    name  VARCHAR,
    sql   VARCHAR
);

-- Test outcomes
CREATE TABLE results (
    client    VARCHAR,    -- e.g. 'psycopg', 'jdbc'
    suite     VARCHAR,    -- e.g. 'catalog_views', 'ddl_dml'
    test_name VARCHAR,
    status    VARCHAR,    -- 'pass' or 'fail'
    detail    VARCHAR,    -- row count or error message
    ts        TIMESTAMP
);

-- Coverage: LEFT JOIN of queries to results (shows untested queries)
CREATE VIEW coverage AS
SELECT q.suite, q.name, q.sql,
       r.client, r.status, r.detail, r.ts
FROM queries q
LEFT JOIN results r ON q.suite = r.suite AND q.name = r.test_name;
```

### Example Queries

```sql
-- Pass/fail summary by client
SELECT client,
       count(*) FILTER (WHERE status = 'pass') AS pass,
       count(*) FILTER (WHERE status = 'fail') AS fail
FROM results GROUP BY client;

-- All failures with details
SELECT client, suite, test_name, detail
FROM results WHERE status = 'fail'
ORDER BY client, suite;

-- Queries no client has tested
SELECT suite, name FROM coverage
WHERE client IS NULL;
```

## Adding a New Client

1. Create a directory under `scripts/client-compat/clients/` (e.g. `clients/dotnet/`)
2. Add a `Dockerfile` that:
   - Copies `entrypoint.sh` and `queries.yaml`
   - Sets `ENTRYPOINT ["/entrypoint.sh"]` and `CMD` to run your test script
3. Implement the test script:
   - Wait for duckgres (poll with `SELECT 1`, retry up to 30 times)
   - Load `queries.yaml` and run each query, reporting via `POST $RESULTS_URL/result`
   - Run any driver-specific tests
   - Connect with `sslmode=require` and accept self-signed certs
4. Add a service block in `docker-compose.yml` (copy an existing client, update names/paths)
5. Add the container name to the `clients` variable in `justfile`
6. Add a standalone just target
