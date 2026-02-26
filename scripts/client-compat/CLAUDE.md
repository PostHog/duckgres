# Claude Code Context for Client Compatibility Tests

## What This Is

Docker Compose-based test framework that runs real PostgreSQL client libraries against duckgres and collects results into a DuckDB database. Lives under `scripts/client-compat/`.

## Directory Layout

```
scripts/client-compat/
├── CLAUDE.md                   # you are here
├── README.md                   # human-facing docs
├── docker-compose.yml          # all services
├── justfile                    # task runner
├── Dockerfile.results          # results-gatherer image
├── results_gatherer.py         # HTTP server that collects results into DuckDB
├── report_client.py            # Python helper for reporting (shared by Python clients)
├── entrypoint.sh               # log-tee wrapper (shared by all clients)
├── queries.yaml                # shared query catalog executed by every client
├── clients/                    # one directory per client
│   ├── psycopg/
│   ├── pgx/
│   ├── psql/
│   ├── jdbc/
│   ├── tokio-postgres/
│   ├── node-postgres/
│   ├── sqlalchemy/
│   ├── dbt/
│   └── harlequin/
└── results/                    # output volume (.gitignored)
```

## Lifecycle / How It Works

1. `just all` → `docker compose up -d` starts duckgres, results-gatherer, and all clients
2. Clients block on healthchecks (`duckgres` TCP + `results-gatherer` HTTP `/health`)
3. Each client runs tests, POSTing `{client, suite, test_name, status, detail}` to `POST /result`
4. The justfile runs `docker wait` on all client containers (handles crashes gracefully)
5. Once all clients exit, the justfile `docker exec`s a `POST /shutdown` to the gatherer
6. Gatherer prints report, exports `results_<ts>.duckdb` and `.json`, exits 0
7. `docker compose down -v` tears everything down

**Key design point**: the gatherer does NOT track which clients are done. The justfile handles all lifecycle coordination via `docker wait`. This avoids hangs when clients crash before reporting.

## Reporting Protocol

Clients report results via HTTP to `$RESULTS_URL` (defaults to `http://results-gatherer:8080`):

```
POST /result
{
  "client": "psycopg",         # must match the client name
  "suite": "catalog_views",    # test grouping
  "test_name": "pg_database",  # unique within suite
  "status": "pass",            # "pass" or "fail"
  "detail": "4 rows"           # optional context (row count, error message)
}
```

All reporting is fire-and-forget (errors silently swallowed) so clients also work standalone without the gatherer.

### Python clients

Use `report_client.py` (copied into the image):

```python
from report_client import ReportClient

rc = ReportClient("my_client")
rc.report("suite_name", "test_name", "pass", "detail")
```

### Non-Python clients

Implement HTTP POST directly. See `clients/pgx/main.go` (Go), `clients/jdbc/.../JdbcCompatTest.java` (Java), `clients/tokio-postgres/src/main.rs` (Rust), or `clients/psql/test_psql.sh` (curl) for examples.

## Adding a New Client

Name the directory after the **client library**, not the language (e.g. `tokio-postgres` not `rust`, `pgx` not `go`).

### 1. Create the client directory

```
clients/<name>/
├── Dockerfile
└── <test script>
```

### 2. Write the Dockerfile

Build context is `scripts/client-compat/` (not the client dir). COPY paths must be relative to that:

```dockerfile
FROM python:3.12-slim-bookworm

RUN pip install --no-cache-dir <deps> pyyaml

COPY entrypoint.sh /entrypoint.sh
COPY queries.yaml /queries.yaml
COPY report_client.py /report_client.py           # Python clients only
COPY clients/<name>/test_<name>.py /test_<name>.py

ENTRYPOINT ["/entrypoint.sh", "python", "/test_<name>.py"]
```

For compiled languages, use a multi-stage build (see `clients/pgx/Dockerfile` or `clients/tokio-postgres/Dockerfile`).

### 3. Implement the test script

Every client must:

1. **Wait for duckgres** — poll with `SELECT 1`, retry up to 30 times with 1s sleep
2. **Run shared queries** — load `/queries.yaml`, execute each entry, report pass/fail
3. **Run client-specific tests** — whatever the library supports (DDL/DML, prepared statements, COPY, ORM, etc.)
4. **Connect with TLS** — use `sslmode=require` and accept self-signed certs
5. **Exit non-zero on failure** — so `docker wait` captures it

The shared queries YAML format:
```yaml
- suite: catalog_views
  name: pg_database
  sql: SELECT datname FROM pg_database WHERE datallowconn
```

### 4. Register in docker-compose.yml

Add a service block (copy an existing one and change names/paths):

```yaml
  <name>:
    build:
      context: .
      dockerfile: clients/<name>/Dockerfile
    container_name: compat-<name>
    depends_on:
      duckgres:
        condition: service_healthy
      results-gatherer:
        condition: service_healthy
    volumes:
      - ./results:/results
    environment:
      PGHOST: duckgres
      PGPORT: "5432"
      PGUSER: postgres
      PGPASSWORD: postgres
      RESULTS_URL: http://results-gatherer:8080
      LOG_DIR: /results/<name>
```

### 5. Register in justfile

Two changes:

1. Add the container name to the `clients` variable:
   ```
   clients := "compat-psycopg compat-pgx ... compat-<name>"
   ```

2. Add a standalone target:
   ```
   # Run <name> compatibility tests (standalone, no report)
   [group('client')]
   <name>: build
       docker compose run --rm <name>
       docker compose down -v
   ```

### 6. Update README.md

Add the client to the tables in the Clients, Client-Specific Suites, and available targets sections.

## Common Patterns

### Connection parameters

Always use env vars: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`. Defaults should match docker-compose values (`duckgres`, `5432`, `postgres`, `postgres`).

### TLS

Duckgres auto-generates a self-signed cert. Clients must accept it:
- Python (psycopg2): `sslmode="require"` (no cert validation by default)
- Go (pgx): `InsecureSkipVerify: true` in tls.Config
- Java (JDBC): `sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory`
- Rust (tokio-postgres): `danger_accept_invalid_certs(true)` on TlsConnector

### Suites

Group related tests under a suite name. Common conventions:
- `connection` — TLS, auth, server version, driver info
- `ddl_dml` / `core_ddl_dml` — CREATE, INSERT, UPDATE, DELETE, DROP
- Library-specific features get their own suite (`batch`, `copy`, `orm`, `prepared`, etc.)

## Results Database

Exported to `results/results_<timestamp>.duckdb` with:

- `queries` table — the shared query catalog from `queries.yaml`
- `results` table — all test outcomes (client, suite, test_name, status, detail, ts)
- `coverage` view — LEFT JOIN of queries to results (shows gaps)

Open with `just query-last-run` or `duckdb results/results_*.duckdb`.
