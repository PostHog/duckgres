# Duckgres

<p align="center">
  <img src="media/oh_duck.png" alt="Duckgres Mascot" width="200">
</p>

A PostgreSQL wire protocol compatible server backed by DuckDB. Connect with any PostgreSQL client (psql, pgAdmin, lib/pq, psycopg2, etc.) and get DuckDB's analytical query performance.

## Table of Contents

- [Features](#features)
- [Metrics](#metrics)
- [Runbooks](#runbooks)
  - [Perf Runbook](docs/perf-harness-runbook.md)
  - [Worker Upgrades & Canaries](docs/runbooks/worker-upgrades.md)
  - [Dev Scenario Runner](docs/runbooks/scenario-dev.md)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
  - [YAML Configuration](#yaml-configuration)
  - [Environment Variables](#environment-variables)
  - [CLI Flags](#cli-flags)
  - [PostHog Logging](#posthog-logging)
  - [PostHog Product-Analytics Events](#posthog-product-analytics-events)
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
  - [Remote Worker Backend](#remote-worker-backend)
- [Two-Tier Query Processing](#two-tier-query-processing)
- [Supported Features](#supported-features)
- [Transaction Isolation](#transaction-isolation)
- [Limitations](#limitations)
- [SQL Client Compatibility](#sql-client-compatibility)
- [Dependencies](#dependencies)
- [License](#license)

## Features

- **PostgreSQL Wire Protocol**: Compatibility with PostgreSQL clients for analytical workloads
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

See [docs/metrics.md](docs/metrics.md) for exact request-path boundaries,
labels, aggregation rules, PromQL examples, and admission metric migration.

| Metric | Type | Description |
|--------|------|-------------|
| `duckgres_connections_open` | Gauge | Process-wide number of currently open client connections, including native metadata-proxy sockets |
| `duckgres_connection_duration_seconds{org}` | Histogram | Worker-backed Duckgres connection lifetime, accept→disconnect (includes `_count`, `_sum`, `_bucket`); excludes native metadata-proxy connections, which use their dedicated duration family |
| `duckgres_metadata_proxy_connections_open{org}` | Gauge | Current admitted native metadata Postgres proxy connections; process-local, so sum across control-plane replicas |
| `duckgres_metadata_proxy_connection_attempts_total{org,outcome}` | Counter | Metadata proxy attempts by bounded terminal outcome |
| `duckgres_metadata_proxy_connection_duration_seconds{org}` | Histogram | Lifetime of admitted metadata proxy connections, including upstream bootstrap |
| `duckgres_metadata_proxy_upstream_connect_duration_seconds{org,outcome}` | Histogram | Internal metadata Postgres connect/auth latency; outcome is `success` or `error` |
| `duckgres_metadata_proxy_bytes_total{org,direction}` | Counter | Post-authentication pgwire bytes relayed in `client_to_upstream` or `upstream_to_client` direction |
| `duckgres_metadata_proxy_cancel_requests_total{outcome}` | Counter | Raw metadata-proxy CancelRequests handled as `session_terminated` on the owning control-plane replica or `not_local` on another replica |
| `duckgres_query_total{org,status,reason}` | Counter | Total non-empty query attempts. Valid status/reason pairs: `success/none`; `failure/user`, `failure/canceled`, `failure/conflict`; `error/metadata_connection_lost`, `error/system`. |
| `duckgres_query_duration_seconds{org}` | Histogram | Simple/extended query execution latency (includes `_count`, `_sum`, `_bucket`); use `duckgres_query_total` for attempt totals |
| `duckgres_auth_failures_total` | Counter | Process-wide authentication failures, including wrong-password metadata-proxy attempts; use `duckgres_metadata_proxy_connection_attempts_total{outcome="auth_failed"}` for the proxy-specific split |
| `duckgres_rate_limit_rejects_total` | Counter | Process-wide pre-TLS connection rejections due to rate limiting; these cannot be attributed to the worker or metadata endpoint because SNI is not available yet |
| `duckgres_rate_limited_ips` | Gauge | Number of currently rate-limited IP addresses |
| `duckgres_control_plane_workers_active` | Gauge | Number of active control-plane worker processes |
| `duckgres_control_plane_worker_acquire_seconds` | Histogram | Time spent acquiring a worker for a new session |
| `duckgres_control_plane_worker_queue_depth` | Gauge | Approximate number of session requests waiting on worker acquisition |
| `duckgres_control_plane_worker_spawn_seconds` | Histogram | Time spent spawning and health-checking a new worker |
| `duckgres_session_admission_evaluation_duration_seconds{decision,reason}` | Histogram | Latency of one DB-backed admission poll for the polling request |
| `duckgres_session_admission_evaluations_total{decision,reason}` | Counter | Admission request polls; repeated polls are distinct evaluations |
| `duckgres_session_admission_wait_seconds{org,outcome,reason}` | Histogram | End-to-end wait for one successfully enqueued admission request |
| `duckgres_session_admission_requests_total{org,outcome,reason}` | Counter | Exactly one terminal event per successfully enqueued admission request |
| `duckgres_session_admission_queue_depth{org}` | Gauge | Local callers waiting after successful durable enqueue; sum across replicas |
| `duckgres_session_admission_active_vcpus{org}` | Gauge | Requested vCPUs held by local live lease handles; cleanup-pending durable rows are excluded |
| `duckgres_session_admission_limit_vcpus{org}` | Gauge | Config-reconciled effective org cap for active org stacks; zero means unlimited, max across replicas |
| `duckgres_session_admission_reclaim_pending` | Gauge | Activated cleanup intents awaiting or executing exact database reclamation |
| `duckgres_session_admission_reclaim_attempts_total{outcome}` | Counter | Exact cleanup attempts by `success` or `error` outcome |
| `duckgres_session_admission_reclaim_reservations_in_use` | Gauge | Cleanup-ownership slots held before enqueue, while queued or live, and during pending cleanup |
| `duckgres_session_admission_reclaim_reservation_capacity` | Gauge | Cleanup-ownership slot capacity for this control-plane process (4096 per reclaimer by default) |
| `duckgres_session_admission_reclaim_reservation_rejections_total{reason}` | Counter | Reservations rejected because capacity was `full`, the reclaimer was `closed`, or the exact reference was a `duplicate` |
| `duckgres_session_start_duration_seconds{org,protocol,outcome}` | Histogram | Authenticated PostgreSQL session bootstrap through flushed `ReadyForQuery` |
| `duckgres_postgres_session_start_total{org,outcome,reason}` | Counter | Exactly one terminal result per authenticated PostgreSQL session start after server retries; `outcome` is `success\|failure` and bounded reasons distinguish operator-actionable failures from client/lifecycle noise |

### Testing Metrics

- `scripts/test_metrics.sh` - Runs a quick sanity check (starts server, runs queries, verifies counts)
- `scripts/load_generator.sh` - Generates continuous query load until Ctrl-C
- `scripts/perf_smoke.sh` - Runs the golden-query perf harness and writes artifacts to `artifacts/perf/<run_id>`
- `scripts/perf_nightly.sh` - Nightly wrapper with lock/timeout guards and optional artifact publisher
- `metrics-compose.yml` - Starts Prometheus and Grafana locally for metrics (Prometheus at http://localhost:9091, Grafana at http://localhost:3000)

### Query Log

When DuckLake uses a Postgres metadata store, Duckgres writes durable per-query
history to the native Postgres table `querylog.query_log_entries`. The query
log is queryable through `ducklake.system.query_log`, a live view over that
native Postgres table. The view is not DuckLake snapshot data.

Rows record SQL user (`user_name`), org, query text, duration, row counts,
errors, trace/span IDs, and profiling-derived resource usage. `cpu_time_s` is
DuckDB cumulative CPU/thread time in seconds, and `peak_buffer_memory_bytes` is
DuckDB's `system_peak_buffer_memory` in bytes, not process RSS.

`query_id` is a per-statement UUIDv7 minted when the query arrives. It is
time-ordered, appears on the statement's OTEL span (`duckgres.query_id`) and its
error logs, and is the key that correlates every query-log event for one
statement. A batched simple query (`SELECT 1; SELECT 2`) runs each statement
under its own `query_id`, with `parent_query_id` and `statement_index`
identifying the Query message they arrived in.

Statements produce a pair of events, using ClickHouse's `type` vocabulary
(`QueryStart` = 1, `QueryFinish` = 2, `ExceptionBeforeStart` = 3,
`ExceptionWhileProcessing` = 4):

- `QueryStart` is emitted when the statement begins executing.
- One terminal event follows: `QueryFinish`, or `ExceptionWhileProcessing` if it
  failed after execution began, or `ExceptionBeforeStart` if it failed **before
  execution began** — auth or policy denial, a transpile error, a failure to
  obtain a worker, or an extended-protocol `Describe` whose prepare the engine
  rejected. `ExceptionBeforeStart` events have no `QueryStart`, by definition.

  The boundary is *execution began*, not *an engine saw it*: `Describe` hands
  the statement to a worker to learn its result schema, so a binder error there
  is an `ExceptionBeforeStart` even though the engine did see the SQL. This is
  the same line ClickHouse draws — analysis-time failures are
  `ExceptionBeforeStart`. In practice this is the largest source of them, so
  when triaging, read `ExceptionBeforeStart` as "never ran", not as "never
  reached a worker".

**A `QueryStart` with no terminal event is a query that never came back** — a
worker OOM-killed mid-statement, a pod evicted. That row is the only evidence
such a query ever ran, so treat a sustained population of unpaired starts as an
incident signal, allowing for queries still in flight.

The `query_id` travels to the worker on every statement RPC
(`x-duckgres-query-id`), and the worker stamps it on its own logs — notably the
"Query appears stuck" warning. That is what closes the loop on an unpaired
`QueryStart`: the statement's own log row cannot exist, but the pod's last words
about it carry the same ID.

`event_time` is the statement's **start** time on every event type, including
terminal ones. This diverges from ClickHouse, where `event_time` is when the
event was logged: pinning both rows of a pair to the same instant keeps them in
one monthly partition and lets them join without a window function. A terminal
row's finish time is `event_time + query_duration_ms`.

`query_log.start_events` selects which statements get a `QueryStart`:

- `data` (default) — statements that touch data or change schema. Transaction
  control, `SET`/`RESET`/`SHOW`, and catalog introspection are skipped: they
  never hang, and they are the noisiest statements a driver sends.
- `all` — every statement.
- `off` — no start events.

Terminal events are always logged regardless of this setting, so nothing
disappears from the log; cheap statements simply have no paired start row. Also
settable via `DUCKGRES_QUERY_LOG_START_EVENTS`.

Each event also records **what the statement touches**, extracted from its
parse tree (`server/querymeta`):

- `access_kinds` — the access classes the statement needs, comma-separated:
  `read`, `write`, `ddl`, `config`, `admin`, `transaction`, `metadata`,
  `unknown`. A statement can be several at once: `WITH x AS (INSERT …) SELECT`
  is both a read and a write, which a classifier based on the command tag gets
  wrong.
- `query_metadata` — JSON with the resolved detail: `read_relations` and
  `write_relations` (split, because grants are directional), `columns`,
  `functions`, and `table_functions`.
- `metadata_complete` — **false when extraction could not see the whole
  statement.** DuckDB-native syntax (`ATTACH`, `CREATE SECRET`, `PIVOT`,
  `SUMMARIZE`) is not parseable as PostgreSQL and falls back to a coarse
  lexical classification.

That last column is load-bearing. These signals exist to let an authorization
policy be evaluated against real traffic before it denies anything, so
"referenced no relations" and "we could not tell what it referenced" must never
be the same answer: **a consumer that gates on `query_metadata` must treat
`metadata_complete = false` as unknown, and deny.**

`table_functions` is recorded alongside relations because `read_parquet('s3://…')`
reaches data without naming a relation, so a policy built on relation names alone
would not see it at all. Reading an external location is **supported usage** — a
tenant pointing `read_parquet` at their own bucket is a feature, and it is
classified as a plain `read`. The cross-tenant question is about the *target*,
not the function: an entry marked `external` records enough of the path (scheme,
host, path — credentials in a presigned URL's query string are stripped) for a
policy to decide whether it resolves inside managed DuckLake storage. Moving data
the other way, `COPY … TO 's3://…'`, keeps the `admin` class: egress is a
different risk from reading a location in.

Extraction runs on the **redacted** statement text, so credential material never
reaches the parser. It costs one parse per distinct statement, memoized per
process; disable with `query_log.metadata: false` or
`DUCKGRES_QUERY_LOG_METADATA=false`.

The column set has a single source of truth: `queryLogColumns` in
`server/querylog_schema.go`. It generates the `CREATE TABLE` DDL, the
`ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migration that brings already
provisioned tenants forward, the `INSERT` column list and argument order, the
partition-repair copy list, and the `ducklake.system.query_log` view. Adding a
column means appending one entry there; existing tenants pick it up on the next
sink initialization, and a view whose columns have drifted is rebuilt with
`CREATE OR REPLACE VIEW`. Append only — never reorder or remove an entry, and
an appended column must be nullable or carry a `DEFAULT` (a bare `NOT NULL`
column cannot be added to a populated table).

## Runbooks

- [Worker Upgrades & Canaries](docs/runbooks/worker-upgrades.md): Process for upgrading DuckDB/DuckLake versions, canarying builds for a subset of tenants, and global version management.
- [Node-local Cache Proxy Bypass](docs/runbooks/cache-proxy-bypass.md): Fail-open cache behavior, detection, and recovery.
- [Performance Harness](docs/perf-harness-runbook.md): Local smoke and nightly operations for performance testing.
- [Dev Scenario Runner](docs/runbooks/scenario-dev.md): Scheduled and manually dispatched scenario runs against the configured dev environment.
- [Control Plane Rollout](docs/runbooks/control-plane-rollout.md): Zero-downtime deployment process for the control plane itself.
- [Org Connection Admission](docs/runbooks/org-connection-admission.md): Global vCPU admission, exact cleanup ownership, failure recovery, and operational metrics.
- [Managed Warehouse Deprovision](docs/runbooks/managed-warehouse-deprovision.md): Destructive teardown process for managed warehouse infrastructure and org cleanup.
- [Resharding Operations](docs/runbooks/resharding.md): Runner recovery, durable respawn reset, safety checks, and local verification.

## Quick Start

The project uses [just](https://github.com/casey/just) as a command runner. Run `just` to see all available recipes.

### Build & Run

```bash
just build    # Build the binary
just run      # Run in standalone mode
```

The server starts on port 5432 by default with TLS enabled. Database files are stored in `./data/`. Self-signed certificates are auto-generated in `./certs/` if not present.

### Connect

```bash
just psql     # Connect via psql (port 5432)
just psql 35437  # Connect on a different port
```

### Docker

```bash
just docker   # Build image (tagged duckgres:dev)
docker run --rm -p 5432:5432 -p 9090:9090 duckgres:dev
```

Mount a config file and persist data:

```bash
docker run --rm \
  -p 5432:5432 -p 9090:9090 \
  -v ./duckgres.yaml:/app/duckgres.yaml \
  -v ./data:/app/data \
  duckgres:dev
```

## Configuration

Duckgres supports three configuration methods (in order of precedence):
1. CLI flags (highest priority)
2. Environment variables
3. YAML config file
4. Built-in defaults (lowest priority)

### Node-local cache proxy

Kubernetes workers can use the optional node-local NVMe cache proxy with
`DUCKGRES_CACHE_ENABLED=true`. The worker waits at most
`DUCKGRES_CACHE_PROXY_CONNECT_TIMEOUT` (default: `5s`, maximum: `10s`) for its initial health
check. It then starts normally: a worker-local forward router bypasses an
unhealthy proxy and fetches signed objects from the authoritative S3 source.
The router probes for recovery with capped exponential backoff and jitter, and
re-enables the local cache after a healthy probe. This setting is an environment
variable only; it is injected into worker pods alongside `NODE_IP`.

Cache-proxy loss affects cache performance, not worker readiness or PostgreSQL
session admission. A bypass does not hide HTTP/S3 responses from the proxy, and
does not replay writes; only a GET/HEAD whose local-proxy connection failed
before a response is received is retried against the authoritative source.

### YAML Configuration

Create a `duckgres.yaml` file (see `duckgres.example.yaml` for a complete example):

```yaml
host: "0.0.0.0"
port: 5432
data_dir: "./data"
session_init_timeout: "10s"
admission_reclaimer_max_reservations: 4096

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
  # Default: true. Disables postgres_scanner thread-local caching for the
  # hidden DuckLake metadata pool to reduce retained metadata connections.
  # Set to false to opt back into warm connection reuse.
  disable_metadata_thread_local_cache: true
  # Default: false. Also attach a Delta Lake catalog/table on worker boot.
  # Without delta_catalog_path, defaults to a sibling top-level delta/ prefix
  # beside the configured DuckLake object_store prefix.
  delta_catalog_enabled: false
  # delta_catalog_path: "s3://bucket/delta/"

process:
  min_workers: 0
  max_workers: 0
  retire_on_session_end: false

rate_limit:
  max_failed_attempts: 5
  failed_attempt_window: "5m"
  ban_duration: "15m"
  max_connections_per_ip: 100

query_log:
  enabled: true
  flush_interval: "5s"
  batch_size: 1000
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
| `DUCKGRES_MEMORY_LIMIT` | DuckDB memory_limit per session (e.g., `4GB`) | Auto-detected |
| `DUCKGRES_THREADS` | DuckDB threads per session | `runtime.NumCPU()` |
| `DUCKGRES_DISABLE_PARQUET_PREFETCHING` | Disable DuckDB Parquet prefetching for standalone/process workers and control-plane-spawned K8s workers. Boolean values use Go's accepted forms (`true`, `TRUE`, `1`, etc.). | `false` |
| `DUCKGRES_PROCESS_ISOLATION` | Enable process isolation (`1` or `true`) | `false` |
| `DUCKGRES_PROCESS_RETIRE_ON_SESSION_END` | Retire a process worker immediately after its last session ends instead of keeping it warm for reuse | `false` |
| `DUCKGRES_IDLE_TIMEOUT` | Connection idle timeout (e.g., `30m`, `1h`, `-1` to disable) | `24h` |
| `DUCKGRES_CLIENT_IDLE_TIMEOUT_MAX` | Maximum client-requested `duckgres.idle_timeout`; unset disables client overrides | disabled |
| `DUCKGRES_SESSION_INIT_TIMEOUT` | Session startup metadata initialization and catalog probe timeout | `10s` |
| `DUCKGRES_WORKER_QUEUE_TIMEOUT` | Max time to wait for worker acquisition and per-org/per-user vCPU resource admission; the managed K8s queue TTL uses this value | `60s` |
| `DUCKGRES_ADMISSION_RECLAIMER_MAX_RESERVATIONS` | Max queued/live admission identities whose cleanup ownership one control plane may retain; new admissions are rejected before enqueue when full | `4096` |
| `DUCKGRES_HANDOVER_DRAIN_TIMEOUT` | Max time to drain planned shutdowns and upgrades before forcing exit | `24h` in process mode, `15m` in remote K8s mode |
| `DUCKGRES_SNI_ROUTING_MODE` | Multi-tenant managed-hostname routing: `off`, `passthrough`, or `enforce`. Postgres uses the requested dbname first; managed SNI must resolve to the same org, and SNI supplies the database only when dbname is empty. | `off` |
| `DUCKGRES_MANAGED_HOSTNAME_SUFFIXES` | Comma-separated managed hostname suffixes such as `.dw.us.postwh.com` | - |
| `DUCKGRES_METADATA_HOSTNAME_SUFFIXES` | Comma-separated SNI suffixes for the explicitly enabled native metadata Postgres proxy, such as `.md.dev.postwh.com`, `.md.us.postwh.com`, or `.md.eu.postwh.com` | - |
| `DUCKGRES_METADATA_PROXY_MAX_CONNECTIONS_PER_ORG` | Maximum admitted metadata proxy sessions per org on each control-plane replica | `20` |
| `DUCKGRES_DUCKLAKE_METADATA_STORE` | DuckLake metadata connection string | - |
| `DUCKGRES_DUCKLAKE_DELTA_CATALOG_ENABLED` | Attach a Delta Lake catalog/table during worker boot/activation | `false` |
| `DUCKGRES_DUCKLAKE_DELTA_CATALOG_PATH` | Delta Lake catalog/table path; defaults to sibling `delta/` prefix at the DuckLake object-store root when enabled | Derived |
| `DUCKGRES_QUERY_LOG_ENABLED` | Enable per-query logging | `true` |
| `DUCKGRES_QUERY_LOG_FLUSH_INTERVAL` | Query-log flush interval for native Postgres writes | `5s` |
| `DUCKGRES_QUERY_LOG_BATCH_SIZE` | Query-log batch size for native Postgres inserts | `1000` |
| `DUCKGRES_STORAGE_SAMPLE_INTERVAL` | Storage-billing sampling cadence (Go duration): how often the leader CP reads each warehouse's tracked DuckLake footprint and credits byte-seconds. Env-only. | `30m` |
| `DUCKGRES_EXPLORATORY_TIER_ENABLED` | Exploratory worker tier (small-first routing, remote/K8s backend only): a connection that sends no `duckgres.worker_*` sizing options acquires NO worker at connect, and its first engine-touching statement lands on the small shape below; state-mutating statements and engine OOMs escalate it to the shape it would otherwise have started on. Env-only. | `false` |
| `DUCKGRES_EXPLORATORY_WORKER_CPU` | CPU request/limit of the exploratory worker pod (e.g. `1`, `500m`). Required (with the memory knob) for the tier to activate; a missing or invalid value logs a warning and leaves the tier OFF. Env-only. | - |
| `DUCKGRES_EXPLORATORY_WORKER_MEMORY` | Memory request/limit of the exploratory worker pod (e.g. `2Gi`). Same requirement as the CPU knob. Env-only. | - |
| `DUCKGRES_EXPLORATORY_WORKER_TTL` | Hot-idle TTL of exploratory worker pods (Go duration) — how long one stays parked for the org's next connection after its last one ends. Env-only. | `48h` |
| `POSTHOG_API_KEY` | PostHog project API key (`phc_...`); enables log export **and product-analytics events**. Application logs carry query text — to get events without exporting SQL, leave this unset and use `POSTHOG_ANALYTICS_API_KEY` | - |
| `POSTHOG_ANALYTICS_API_KEY` | PostHog project API key for product-analytics events **only**, leaving log export off. Takes precedence over `POSTHOG_API_KEY` for analytics | - |
| `POSTHOG_HOST` | PostHog ingest host (shared by both exporters) | `us.i.posthog.com` |
| `ADDITIONAL_POSTHOG_API_KEYS` | **(Experimental)** Comma-separated list of additional PostHog API keys to publish logs to. Requires `POSTHOG_API_KEY` to be set. | - |
| `DUCKGRES_IDENTIFIER` | Suffix appended to the OTel `service.name` (e.g., `duckgres-acme`). Applies to **both** the log export and the OTLP trace export — they share one resource — so setting it renames the service in traces too, not just logs | - |

### Client-requested idle timeout

The control plane closes inactive client sessions after its configured
`DUCKGRES_IDLE_TIMEOUT` (60 seconds by default). To let clients request a
longer, bounded timeout, set a positive `DUCKGRES_CLIENT_IDLE_TIMEOUT_MAX` on
the control plane. For example, with `DUCKGRES_CLIENT_IDLE_TIMEOUT_MAX=15m`:

```bash
PGOPTIONS='-c duckgres.idle_timeout=15m' psql "host=<host> dbname=ducklake sslmode=require"
```

Requests must be positive and no greater than the configured maximum. Leaving
the maximum unset disables client overrides, and clients cannot request an
unlimited timeout because idle sessions retain worker capacity.

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

### PostHog Product-Analytics Events

`POSTHOG_API_KEY` (and `POSTHOG_HOST`) also enables product-analytics event
capture via the PostHog capture API. This is separate from log export: logs go
to PostHog Logs, these are discrete events you can build insights and dashboards
on.

The two exporters can be enabled independently, and the distinction matters
because they carry different data. These events are metadata only. Application
logs are not: `logQuery` / `logQueryError` attach the statement, and
`usersecrets.RedactForLog` only rewrites secret DDL, so ordinary SQL and its
literals reach PostHog Logs.

| Set | Analytics events | Log export |
| --- | --- | --- |
| `POSTHOG_ANALYTICS_API_KEY` | ✅ | ❌ |
| `POSTHOG_API_KEY` | ✅ | ✅ |
| both | ✅ (analytics key) | ✅ (`POSTHOG_API_KEY`) |
| neither | ❌ | ❌ |

So a deployment serving customer data — where SQL must not be exported — sets
only `POSTHOG_ANALYTICS_API_KEY`:

```bash
export POSTHOG_ANALYTICS_API_KEY=phc_your_project_api_key
./duckgres
```

Existing single-key deployments are unaffected: `POSTHOG_API_KEY` keeps both
exporters on, exactly as before.

Events are attributed to an org using [PostHog group analytics](https://posthog.com/docs/product-analytics/group-analytics):
the `distinct_id` is the org name and each event carries a group of type
`organization`, so dashboards can break down and aggregate by org. In
single-tenant standalone mode (no org) the `distinct_id` is `standalone` and no
group is attached.

The org name is duckgres-internal, so the query events additionally carry a
`team_id` property — the PostHog `Team.id` for the connection (the connecting
user's team, else the org's oldest team; 0 when unknown or standalone). This is
the PostHog-native key that joins duckgres usage to the rest of PostHog (e.g.
product-intent cohorts for managed-warehouse activation). It is a config-snapshot
read stamped once per connection, and mirrors the informational team id the
compute-usage meter records.

Events never include SQL text, credentials, or secret values — only metadata.

Provisioning and deprovisioning are asynchronous: the admin API returns `202
Accepted` and the per-org provisioner controller drives the warehouse to its
terminal state. The lifecycle is therefore split into a `_begin` event (the
admin API accepted the request) and a terminal `_success` / `_failed` event (the
controller observed the warehouse reach Ready / Failed, or finish / fail
teardown), so you can build a provisioning funnel and alert on failures.

| Event | Fires when | Properties |
| --- | --- | --- |
| `warehouse_provision_begin` | Provisioning accepted by the admin API (warehouse not usable yet) | `database_name`, `metadata_store`, `ducklake_enabled` |
| `warehouse_provision_success` | Warehouse reaches Ready and is usable (provisioner controller) | `metadata_store`, `ducklake_enabled` |
| `warehouse_provision_failed` | Warehouse reaches Failed (provisioner controller) | `metadata_store`, `ducklake_enabled`, `reason` (`provisioning_timeout`/`crossplane_sync_failure`) |
| `warehouse_deprovision_begin` | Deprovisioning accepted by the admin API (teardown not finished yet) | — |
| `warehouse_deprovision_success` | All underlying resources deleted (provisioner controller) | — |
| `warehouse_deprovision_failed` | A teardown attempt failed (provisioner controller) | `reason` (`duckling_delete_failed`) |
| `warehouse_password_reset` | An org's root password is reset (admin API) | `username` |
| `query_initiated` | An accepted, non-empty client query is received | `user`, `team_id`, `trace_id` |
| `query_completed` | A statement finishes executing successfully | `user`, `team_id`, `trace_id`, `protocol`, `query_kind`, `duration_ms`, `cpu_seconds` (DuckDB CPU/thread-time), `result_rows` |
| `query_failed` | A query errors | `user`, `team_id`, `trace_id`, `error_code` (SQLSTATE), `error_category` (`user`/`system`/`conflict`/`metadata_connection_lost`) |

> Note: `warehouse_provision_success` / `_failed` and `warehouse_deprovision_success`
> are terminal and fire exactly once per warehouse (guarded on the state
> transition). Deletion has no terminal Failed state — the controller retries
> indefinitely — so `warehouse_deprovision_failed` represents a failed teardown
> *attempt* and may fire once per reconcile pass until teardown succeeds.

> The `_success` / `_failed` events are emitted by the Kubernetes provisioner
> controller, so they only fire in the remote/multitenant backend (built with
> `-tags kubernetes`). The `_begin` events fire wherever the admin provisioning
> API runs.

> Note: `query_initiated` fires once per accepted, non-empty simple-protocol
> Query or extended-protocol Execute. Retries, rewrites, cursor helpers, and
> generated COPY batches do not emit additional events. Capture is asynchronous
> and batched, so it stays off the query latency path.

> Note: `query_completed` fires on the terminal event of each *successfully*
> executed statement, carrying that statement's resource cost (`duration_ms`,
> `cpu_seconds`). Failures are covered by `query_failed` instead. It is emitted
> at statement granularity, so a single logical client request can produce more
> than one `query_completed` (e.g. cursor FETCHes or COPY batches) — unlike
> `query_initiated`. Filter by `query_kind` to isolate real data queries from
> utility statements. Emitted independently of the query-log configuration;
> capture is asynchronous and batched, so it stays off the query latency path.

### Query Logs

Structured logs separate the SQL received from a client from the statements
executed by a worker:

| Event | Scope | Meaning |
| --- | --- | --- |
| `Client query received.` | `client` | Emitted once with the bounded/redacted client SQL and `protocol=simple` or `protocol=extended`. |
| `Worker statement started.` | `worker` | A physical statement is about to run for the client operation. |
| `Worker statement finished.` | `worker` | The physical statement completed, with duration, affected rows, and SQLSTATE when applicable. |

Client-derived worker statements carry bounded/redacted executed SQL. Generated
rewrite and COPY work instead carries a typed `origin`, stable `operation`, and
compact metadata; generated SQL, placeholders, arguments, and values are not
logged. Worker statements do not create additional durable query-log records.

### CLI Flags

```bash
./duckgres --help

Options:
  -config string           Path to YAML config file
  -host string             Host to bind to
  -port int                Port to listen on
  -data-dir string         Directory for DuckDB files
  -cert string             TLS certificate file
  -key string              TLS private key file
  -memory-limit string     DuckDB memory_limit per session (e.g., '4GB')
  -threads int             DuckDB threads per session
  -process-isolation       Enable process isolation (spawn child process per connection)
  -idle-timeout string     Connection idle timeout (e.g., '30m', '1h', '-1' to disable)
  -mode string             Run mode: standalone (default), control-plane, duckdb-service, or reshard-runner
  -process-min-workers int Pre-warm process worker count at startup (control-plane mode, default 0)
  -process-max-workers int Max process workers, 0=auto-derived (control-plane mode)
  -process-retire-on-session-end
                          Retire a process worker immediately after its last session ends instead of keeping it warm for reuse (control-plane mode)
  -memory-budget string    Total memory for all DuckDB sessions (e.g., '24GB')
  -socket-dir string       Unix socket directory (control-plane mode)
  -handover-socket string  Handover socket for graceful deployment (control-plane mode)
  -sni-routing-mode string Hostname routing: off, passthrough, or enforce
  -managed-hostname-suffixes string
                          Comma-separated managed tenant hostname suffixes
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

  # Default: true. Disables postgres_scanner thread-local caching for the
  # hidden DuckLake metadata pool before ATTACH creates it.
  # Set to false to opt back into warm connection reuse.
  disable_metadata_thread_local_cache: true

  # Also attach a Delta Lake catalog/table as catalog "delta" during worker
  # boot/activation. If delta_catalog_path is omitted, Duckgres derives
  # s3://<bucket>/delta/ from ducklake.object_store. Prefer that isolated
  # prefix over the bucket root so DuckLake and Delta files do not collide.
  delta_catalog_enabled: false
  # delta_catalog_path: "s3://my-bucket/delta/"
```

This runs the equivalent of:
```sql
ATTACH 'ducklake:postgres:host=ducklake.example.com user=ducklake password=secret dbname=ducklake' AS ducklake;
-- when delta_catalog_enabled=true:
ATTACH 's3://my-bucket/delta/' AS delta (TYPE delta);
```

See [DuckLake documentation](https://ducklake.select/docs/stable/duckdb/usage/connecting) for more details.

`ducklake.disable_metadata_thread_local_cache` defaults to `true`. This applies a
pre-attach workaround for the hidden DuckLake metadata postgres pool so idle
worker threads do not retain metadata connections indefinitely. Set it to
`false` only if you explicitly want the older warm-reuse behavior and accept the
larger steady-state metadata connection footprint.

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
  delta_catalog_enabled: true       # attaches s3://ducklake/delta/ by default
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
- `DUCKGRES_DUCKLAKE_DELTA_CATALOG_ENABLED` - attach Delta catalog (`true`/`false`)
- `DUCKGRES_DUCKLAKE_DELTA_CATALOG_PATH` - Delta catalog/table path (e.g., `s3://bucket/delta/`)
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
- **Connection limits**: Limits concurrent connections per IP and, when configured, total concurrent sessions in standalone mode.
- **K8s multi-tenant resource limits**: Org and user `max_vcpus` bound the sum of active worker pod vCPUs admitted through runtime-store leases. 0 means unlimited.
- **Auto-cleanup**: Expired records are automatically cleaned up

```yaml
rate_limit:
  max_failed_attempts: 5        # Ban after 5 failures
  failed_attempt_window: "5m"   # Within 5 minutes
  ban_duration: "15m"           # Ban lasts 15 minutes
  max_connections_per_ip: 100   # Max concurrent connections
  max_connections: 16           # Standalone max total concurrent sessions (0 = unlimited)
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

Duckgres supports three primary run modes: **standalone** (single process, default), **control-plane** (multi-process with worker pool), and **duckdb-service** (worker process mode used by the control plane). A fourth utility mode, **reshard-runner**, is the entrypoint of the dedicated per-operation pods the multitenant control plane spawns to execute metadata-store reshards (see `docs/design/resharding.md`).

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

For production deployments, control-plane mode splits the server into a **control plane** and a pool of long-lived **worker processes**. The control plane exposes PostgreSQL wire protocol to clients and owns those connections end-to-end (TLS, authentication, SQL transpilation), while workers are thin DuckDB execution engines reachable internally via Arrow Flight SQL over Unix sockets.

```
                    CONTROL PLANE (duckgres --mode control-plane)
                    ┌──────────────────────────────────────────────┐
  PG Client ──TLS──>│ PG TCP Listener                              │
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

# Pre-warm 2 process workers and cap at 10
./duckgres --mode control-plane --port 5432 --process-min-workers 2 --process-max-workers 10

# Connect with psql (identical to standalone mode)
PGPASSWORD=postgres psql "host=localhost port=5432 user=postgres sslmode=require"

```

**Zero-downtime deployment** using the handover protocol:

```bash
# Start the first control plane with a handover socket
./duckgres --mode control-plane --port 5432 --handover-socket /var/run/duckgres/handover.sock

# Deploy a new version - it takes over the listener and workers without dropping connections
./duckgres-v2 --mode control-plane --port 5432 --handover-socket /var/run/duckgres/handover.sock
```

When running under **systemd** with `RuntimeDirectory`, ensure `RuntimeDirectoryPreserve=yes` is set in your unit file. This prevents systemd from cleaning up or remounting the socket directory as read-only when the old process exits during a handover.

**Rolling worker updates** via signal:

```bash
# Replace workers one at a time (drains sessions before replacing each worker)
kill -USR2 <control-plane-pid>
```

### Remote Worker Backend

In Kubernetes environments, `--worker-backend remote` is the multitenant path. It requires `--config-store`. Control-plane replicas coordinate through durable runtime rows in the config-store Postgres DB, spawn worker pods via the Kubernetes API, and communicate with them over gRPC (Arrow Flight SQL). Planned rolling deploys mark old replicas draining, fail readiness, and wait up to `handover_drain_timeout` before forcing shutdown. Unplanned control-plane failure drops live pgwire connections; clients reconnect through pgwire and receive a new worker session.

Managed-hostname routing is controlled by `--sni-routing-mode` and `--managed-hostname-suffixes`. For Postgres, an explicit startup `database`/`dbname` takes priority, but when SNI matches a managed suffix the hostname prefix and requested database must resolve to the same org. If the startup database is empty, the managed SNI prefix is used as the database fallback. Unknown `--sni-routing-mode` values behave like `off`.

The native metadata Postgres proxy is a separate, fail-closed SNI path selected
by `DUCKGRES_METADATA_HOSTNAME_SUFFIXES`. It is available only when an org's
warehouse explicitly has `metadata_proxy_enabled=true`, is ready, and uses a
CNPG-shard metadata store. The client must connect as `root` with the org's
existing Duckgres password and must send the exact non-empty
`dbname=metadata`. Duckgres resolves and uses the real metadata role, password,
database, and PgBouncer endpoint internally. The endpoint and password are
never sent to the client; the upstream role and database may be visible
through normal PostgreSQL introspection such as `current_user` and
`current_database()`. Managed-warehouse deployments configure their own
environment suffix (`.md.dev.postwh.com`, `.md.us.postwh.com`, or
`.md.eu.postwh.com`); suffixes are never inferred from the ordinary Duckgres
hostname. The per-org connection limit is enforced independently on every
control-plane replica. Internal target resolution and
connect/auth/synchronization have a fixed 10-second bootstrap deadline; the
deadline does not apply after the relay is established. An admin/UI update that
includes `metadata_proxy_enabled` reloads the local config snapshot and
notifies peer replicas; established sessions close on their next five-second
authorization recheck after the updated snapshot arrives.

The initial rollout is restricted operationally to dedicated,
single-customer CNPG shards. Do not enable `metadata_proxy_enabled` for an org
on a shared shard until upstream database `CONNECT` ACLs and role hardening are
in place: once the exact `metadata` database connection is established, the
customer's `root` credential intentionally receives full access available to
the internally resolved metadata role.

Metadata-proxy cancellation is deliberately session-terminating: when a raw
PostgreSQL `CancelRequest` reaches the control-plane replica that owns the
synthetic backend key, Duckgres closes that exact frontend and upstream
connection pair. PgBouncer cancellation keys are instance-local, so Duckgres
does not redial the pooler Service. Like existing Duckgres cancellation, the
raw follow-up TCP connection is control-plane-local behind the NLB; a request
routed to another replica is counted as
`duckgres_metadata_proxy_cancel_requests_total{outcome="not_local"}` and cannot
terminate the owning session.

Workers are spawned on demand: when an org opens a session with no reusable worker, the control plane creates a worker pod (sized from the connection's `duckgres.worker_cpu`/`worker_memory` request, or a default), activates it over the worker control RPC, and it becomes hot for that org. When its last session ends, the worker moves to `hot_idle` instead of being retired immediately: it keeps the org assignment and DuckLake attachment so any control-plane replica can reclaim it for the same org (by exact worker shape) without full reactivation, until its `duckgres.worker_ttl` expires. Hot-idle reuse is image/version strict. The janitor retires hot-idle workers at their TTL, but `default_worker_min_hot_idle` lets an org retain a minimum number of compatible default-profile hot-idle workers by skipping TTL retirement when the count is already at or below the floor. The default is `0` (disabled). The main lifecycle is: idle → reserved → activating → hot → hot_idle → retired. Workers can also move through `draining` during shutdown, rollout, or cleanup. (Spawn latency is hidden by the node-headroom controller, which keeps placeholder pods ready for real workers to preempt.)

```bash
# Local multitenant K8s workflow
just run-multitenant-kind
```

See [`k8s/README.md`](k8s/README.md) for the full architecture, configuration reference, manifest details, and the default local kind workflow via `just run-multitenant-kind`. The older OrbStack path remains available through `just run-multitenant-local` for manual macOS iteration.

On the multi-tenant path, the config store now keeps per-team managed-warehouse metadata in addition to team/user auth and limits. That team-scoped contract is the source of truth for the tenant warehouse DB, the tenant DuckLake metadata store (which may live on shared Aurora or a dedicated RDS instance), object-store settings, worker identity, secret references, and provisioning state. The older cluster-wide singleton config tables (global / ducklake / rate-limit / query-log) have been removed — they were never read at runtime; effective config comes from CLI flags/env and this per-team contract.

Config-store schema changes are applied from embedded, ordered SQL migrations at control-plane startup. See [`docs/runbooks/config-store-migrations.md`](docs/runbooks/config-store-migrations.md) for the local development flow, checksum behavior, and failure recovery steps.

The shared K8s pool spawns workers on-demand, reserves them per org, activates tenant runtime over the control-plane RPC channel, and keeps idle activated workers briefly available for same-org hot-idle reuse before janitor retirement.

Managed-warehouse contract notes:

- At most one managed-warehouse row exists per team. The row may be absent before first provisioning or after cleanup, but there is never more than one active warehouse contract for a team.
- Each org has a `data_imports_table_naming_version`. Migration `000034` assigns `legacy_batch_v1` to orgs that already exist and changes the database default to `copy_v1` for orgs created afterward. `GET /api/v1/orgs/:id/teams` returns the org-level value alongside the team rows so every data-import reader and writer derives the same physical table name. Operators can change the policy in the admin console or with `PUT /api/v1/orgs/:id` using `{"data_imports_table_naming_version":"copy_v1"}`. Migrate existing tables before changing an org that has already written data.
- The admin API exposes that contract at `GET /api/v1/teams/:name/warehouse` and `PUT /api/v1/teams/:name/warehouse`. Team list/get responses also include a nested `warehouse` object when present.
- Org rows support optional `max_vcpus` on `POST /api/v1/orgs` and `PUT /api/v1/orgs/:id`. In K8s multi-tenant mode, this caps the org's active admitted worker pod vCPUs; `0` means unlimited.
- Orgs automatically created during warehouse provisioning start with `max_vcpus=64`; `0` remains the explicit unlimited sentinel.
- User rows support an optional `max_vcpus` field on `POST /api/v1/users` and `PUT /api/v1/orgs/:id/users/:username`. `max_vcpus` limits the user's active admitted worker pod vCPUs in K8s multi-tenant mode; `0` means unlimited.
- `PUT /api/v1/orgs/:id/teams/:team_id/project-reader` creates or rotates the generated SQL login for a PostHog project. The login can read every current and future table in the project's team, data-import, and modeled-data schemas, plus its legacy events/persons relations. Writes, unqualified application relations, external-reader and introspection functions, other projects' schemas, and their catalog metadata are denied by the PostgreSQL query gateway. The plaintext password is returned only by the rotation response.
- The typed sections are `warehouse_database`, `metadata_store`, `s3`, `worker_identity`, and structured secret refs for `warehouse_database_credentials`, `metadata_store_credentials`, `s3_credentials`, and `runtime_config`. In shared worker mode, every non-empty secret ref must store an explicit `namespace`, and it must match `worker_identity.namespace`.
- Secret references only are stored in the config store. Secret material remains outside the database.
- The provisioning fields are stored directly on the warehouse row as overall `state` / `status_message`, per-resource `*_state` / `*_status_message`, plus `ready_at` and `failed_at`.
- Those state fields are open strings. Canonical values are `pending`, `provisioning`, `ready`, `failed`, `deleting`, and `deleted`, but callers may persist other values while workflows evolve.

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
- Cleartext password authentication over TLS
- Basic `pg_catalog` system tables for client compatibility
- `\dt`, `\d`, and other psql meta-commands

## Transaction Isolation

DuckDB provides **snapshot isolation** (MVCC), which is stricter than PostgreSQL's default `read committed`. In practice this means:

| Behavior | PostgreSQL (default) | Duckgres (DuckDB) |
|----------|---------------------|-------------------|
| Default isolation level | Read Committed | Snapshot (≈ Serializable) |
| Non-repeatable reads | Possible | Not possible |
| Phantom reads | Possible | Not possible |
| Write conflicts | Last writer wins | Second writer gets a conflict error |

Clients that issue `SET transaction_isolation` or `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...` will succeed silently — the setting is accepted but DuckDB always operates at snapshot isolation. `SHOW transaction_isolation` returns `read committed` for client compatibility.

Since DuckDB's isolation is strictly stronger than PostgreSQL's default, applications that work correctly under read committed will also work correctly here. The only observable difference is write-write conflicts: DuckDB will reject a concurrent write that PostgreSQL would silently accept under read committed.

## Limitations

- **Single Node**: No built-in replication or clustering
- **Limited System Catalog**: Some `pg_*` system tables are stubs (return empty)
- Unmapped DuckDB types (MAP, STRUCT, UNION, ENUM, BIT) fall back to OidText

## SQL Client Compatibility

Duckgres implements a subset of PostgreSQL's system catalog to satisfy introspection queries from common SQL clients, ORMs, and BI tools — enough for psql, pgAdmin, DBeaver, Metabase, Grafana, Superset, Tableau, Fivetran, Airbyte, dbt, and the standard drivers (psycopg, pgx, JDBC, node-postgres, tokio-postgres, SQLAlchemy) to connect and introspect.

The full, authoritative breakdown — every PostgreSQL feature with its support status and the specific test that proves it, plus the per-object `pg_catalog`/`information_schema`/function/startup-parameter reference — lives in **[docs/postgres-compatibility.md](docs/postgres-compatibility.md)**. That document is the single source of truth; update it in the same PR as any PostgreSQL-visible behavior change.

## Dependencies

- [DuckDB Go Driver](https://github.com/duckdb/duckdb-go) - DuckDB database engine

## License

MIT
