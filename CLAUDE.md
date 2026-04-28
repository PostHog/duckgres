# Claude Code Context for Duckgres

This file provides context for Claude Code sessions working on this codebase.

## Project Overview

Duckgres is a PostgreSQL wire protocol server backed by DuckDB. It allows any PostgreSQL client (psql, pgAdmin, lib/pq, psycopg2, JDBC, etc.) to connect and execute queries against DuckDB databases.

## Architecture

Duckgres has three deployment topologies, built from three run modes (`standalone`, `control-plane`, `duckdb-service`):

**1. Standalone** — single process. One binary running in `standalone` mode handles the PG wire protocol, auth, TLS, transpilation, and DuckDB execution itself. Each user gets their own DuckDB database in-process.
```
PG Client → TLS → Server (standalone) → DuckDB
```

**2. Control plane + local process workers** — single host, multiple processes. A `control-plane` parent process owns client connections (TLS, auth, PG wire, transpilation) and spawns child `duckdb-service` worker processes, communicating via Arrow Flight SQL over Unix sockets. Used for stronger isolation between sessions on a single host. Selected with `--worker-backend process` (the default).
```
PG Client → TLS/Auth/PG Protocol → Control Plane (process)
                                 → Flight SQL (UDS) → local Worker process (DuckDB)
```

**3. Control plane + remote workers on Kubernetes** — multitenant cluster deployment. The `control-plane` runs as its own pod and routes per-org traffic to dedicated `duckdb-service` worker pods over TCP+TLS. Worker pods are scheduled by the control plane via the K8s API; org config and worker state are persisted in a Postgres-backed config store. Selected with `--worker-backend remote`; requires a binary built with `-tags kubernetes`.
```
PG Client → TLS/Auth/PG Protocol → Control Plane pod
                                 → Flight SQL (TCP+TLS) → per-org Worker pod (DuckDB)
```

In topologies 2 and 3, the control plane also exposes an Arrow Flight SQL ingress (`--flight-port`) for clients that prefer Flight over the PG wire protocol.

### Key Components

- **main.go / config_resolution.go**: CLI flags; effective config resolution (CLI > env > YAML > defaults), including env-only K8s knobs.
- **server/** — PG wire protocol server and DuckDB execution
  - Wire protocol & connections: `server.go`, `conn.go`, `protocol.go`, `exports.go`
  - Execution: `executor.go`, `flight_executor.go`, `chsql.go`, `transient.go`
  - Catalog & types: `catalog.go`, `types.go`, `session_database_metadata.go`
  - Auth, TLS, rate limiting: `auth_policy.go`, `ratelimit.go`, `certs.go`, `acme.go`
  - DuckLake: `ducklake_migration.go`, `checkpoint.go`
  - Observability: `querylog.go`, `tracing.go`
  - ProcessIsolation child workers: `parent.go`, `worker.go`, `worker_activation.go`, `worker_control.go`
  - Flight SQL ingress (shared with control plane): `flightsqlingress/`
- **controlplane/** — Multi-process / multi-tenant control plane
  - Core: `control.go`, `session_mgr.go`, `worker_mgr.go`, `worker_pool.go` (process/k8s abstraction), `validation.go`, `sdnotify.go`
  - Flight SQL ingress adapter: `flight_ingress.go`
  - Runtime loops: `janitor.go`, `leader_loop.go`, `memory_rebalancer.go`, `runtime_tracker.go`
  - K8s / multitenant under build tag `kubernetes` (including: `multitenant.go`, `k8s_pool.go`, `k8s_factory.go`, `org_router.go`, `org_reserved_pool.go`, `sts_broker.go`, `shared_worker_activator.go`, `worker_rpc_security.go`, `janitor_leader_k8s.go`)
  - Subpackages: `admin/` (HTTP admin API, `kubernetes` tag), `provisioner/` (k8s controller, `kubernetes` tag), `provisioning/` (HTTP API), `configstore/` (Postgres-backed config)
- **duckdbservice/** — DuckDB Arrow Flight SQL service
  - Core: `service.go`, `flight_handler.go`, `arrow_helpers.go`, `auth.go`, `config.go`
  - Lifecycle, caching, profiling, metrics: `activation.go`, `transient.go`, `cache_proxy.go`, `profiling.go`, `progress.go`, `metrics.go`
- **transpiler/** — AST-based PostgreSQL → DuckDB SQL transpiler
  - Top-level: `transpiler.go`, `config.go`, `boolpredicates.go`, `show_create.go`
  - `transform/`: individual transforms; see registered pipeline in `transpiler.go` `New()`

## Run Modes

- **standalone** (default): Single process, handles everything including TLS, auth, PG protocol, and DuckDB execution.
- **control-plane**: Multi-process. Owns client connections end-to-end (TLS, auth, PG wire protocol, SQL transpilation, optional Flight SQL ingress) and routes queries to a worker pool.
  - **Process backend** (default, `--worker-backend process`): local Flight SQL workers over Unix sockets.
  - **Remote backend** (`--worker-backend remote`): per-org Kubernetes worker pods over TCP+TLS. Multitenant; requires `-tags kubernetes` and a Postgres-backed config store. Adds config store, org router, runtime tracker, janitor/leader election, and a provisioning/admin HTTP API.
- **duckdb-service**: Thin DuckDB execution engine exposed via Arrow Flight SQL. Spawned automatically by the control plane as worker processes, or run standalone for testing.

Key CLI flags for control-plane mode:
- `--mode control-plane|duckdb-service|standalone`
- `--worker-backend process|remote`
- `--process-min-workers N` / `--process-max-workers N`
- `--process-retire-on-session-end`
- `--worker-queue-timeout DURATION` / `--worker-idle-timeout DURATION`
- `--memory-budget SIZE` (default 75% RAM) / `--memory-rebalance`
- `--socket-dir /path` (process backend)
- `--handover-drain-timeout DURATION` (default `24h` process / `15m` remote; uses cloudflare/tableflip for FD passing)
- `--flight-port N` (Arrow Flight SQL ingress) plus `--flight-session-idle-ttl`, `--flight-session-reap-interval`, `--flight-handle-idle-ttl`, `--flight-session-token-ttl`
- `--ducklake-delta-catalog-enabled` / `--ducklake-delta-catalog-path`
- Remote backend (requires `--config-store`; `-tags kubernetes` for K8s pool):
  - Config store: `--config-store`, `--config-poll-interval`, `--internal-secret`
  - K8s pool: `--k8s-worker-image`, `--k8s-worker-namespace`, `--k8s-control-plane-id`, `--k8s-worker-port`, `--k8s-worker-secret`, `--k8s-worker-configmap`, `--k8s-worker-image-pull-policy`, `--k8s-worker-service-account`, `--k8s-max-workers`, `--k8s-shared-warm-target`
  - AWS / STS: `--aws-region`
  - Pod scheduling knobs (CPU/memory requests, node selector, tolerations) are env-only — see `config_resolution.go`.

Key CLI flags for duckdb-service mode:
- `--duckdb-listen` (e.g., `unix:///...` or `:8816`)
- `--duckdb-listen-fd` (internal; set by control plane)
- `--duckdb-token` (bearer auth)
- `--duckdb-max-sessions` (0=unlimited)

## Configuration

Configuration is resolved in `config_resolution.go` with the following precedence (highest to lowest):
1. CLI flags (`--port`, `--config`, etc.)
2. Environment variables (`DUCKGRES_PORT`, etc.)
3. YAML config file
4. Built-in defaults

Note: `--mode` is CLI-only (not loadable from YAML/env). A handful of K8s pod-scheduling knobs are env-only (no CLI flag).

## Development

The project uses [just](https://github.com/casey/just) as a command runner. Run `just` to see all available recipes for building, testing, running, metrics, and scripts.

## Dependencies

- `github.com/duckdb/duckdb-go/v2` - DuckDB Go driver
- `github.com/pganalyze/pg_query_go/v6` - PostgreSQL SQL parser (CGO, uses libpg_query)
- `gopkg.in/yaml.v3` - YAML config parsing

## Known Limitations

- No replication
- Some pg_catalog tables are stubs (return empty)
- Unmapped DuckDB types (MAP, STRUCT, UNION, ENUM, BIT) fall back to OidText
- DML RETURNING is not supported via extended query protocol (see below)

## DML RETURNING Detection

DML with RETURNING is rejected at extended-query Describe time with SQLSTATE `0A000` — the Describe path probes schema by executing the query, which would cause an unintended mutation. Detection lives in `isDMLReturning` and friends in `server/conn.go` (heuristic SQL-aware lexer, with any-depth scanning for WITH-prefixed writable CTEs). Invariants for anyone editing this code:

- **False negatives are dangerous** — they cause silent mutations during Describe. False positives are safe (just an error to the client). Err toward false positives.
- All detection is heuristic string scanning. If precision becomes critical, switch to `pg_query_go` AST parsing.
- LIMIT 0 does NOT prevent CTE side effects — Postgres CTEs are optimization fences, so writable CTEs execute even with LIMIT 0.
- DuckDB does not currently support MERGE. If it adds MERGE RETURNING, add `MERGE` to the prefix check in `isDMLReturning`.

## TODO Reference

See `TODO.md` for the full feature roadmap and known issues.
