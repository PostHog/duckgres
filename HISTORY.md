# DuckDB Concurrency: An Architectural History

How Duckgres evolved from a single shared DuckDB handle to a control plane with disposable per-connection Flight SQL workers — driven by production failures at every step.

## The Fundamental Tension

DuckDB is an embedded, single-process analytical database with C++ crash risks and write-write conflicts. Duckgres needs to serve many concurrent PostgreSQL clients reliably. Every architectural phase was a response to this tension.

---

## Phase 0: Shared Database per User (Dec 2025)

`9761236` — Initial commit.

A single `*sql.DB` per username, stored in a shared map on the Server struct. All connections from the same user shared one DuckDB handle. File-backed databases, no concurrency controls beyond Go's `database/sql` defaults.

---

## Phase 1: Concurrency Bugs Emerge (Dec 11–23, 2025)

Real clients (Fivetran, psql, pgAdmin) started connecting concurrently. DuckDB's single-writer constraints caused a cascade of issues.

**Per-connection DB isolation** (`4afe745`, Dec 11) — Temp tables leaked across sessions because all connections shared one DuckDB handle. Each connection now gets its own DuckDB instance.

**Connection pooling saga** (`778ed30` → `af276d9`, Dec 18) — DuckDB file locking caused hangs when multiple clients opened/closed the same DB file. Solution: switch to `:memory:` databases since data lives in RDS/S3 via DuckLake anyway.

**DuckLake concurrent attachment races** (`d8bc016` → `87542b6`, Dec 18) — Multiple connections creating S3 secrets and attaching DuckLake simultaneously caused write-write conflicts. Progressed through mutex → double-checked locking → timed semaphore.

**View creation races** (`0ab3739` → `f197f8d`, Dec 19) — pg_catalog views created in DuckLake's default database caused transaction conflicts. Fix: create views in the memory database instead.

**Optimistic concurrency retries** (`e1bb56f`, Dec 23) — Bumped DuckLake retry count from 10→100. Concurrent ETL commits conflicted on metadata snapshot IDs in the PostgreSQL metadata store.

---

## Phase 2: Connection Management (Jan 30 – Feb 3, 2026)

**Connection limits** (`e395fd7`, Jan 30) — `max_connections` config with atomic counter and Prometheus metrics.

**Query cancellation** (`2e3632d`, Jan 30) — PostgreSQL-compatible cancel request messages so clients can Ctrl+C long-running queries.

**Thread tuning** (`43c3f36`, Feb 3) — First explicit DuckDB parallelism config: `threads = 4x CPU count`.

---

## Phase 3: Process Isolation (Feb 4–10, 2026)

The fundamental problem: DuckDB's C++ runtime can SIGABRT or SIGSEGV, taking down the entire server and killing all active connections.

**Band-aids** (`9aad886` → `d476291`, Feb 4) — Explicit ROLLBACK before Close(), health checks before cleanup. Mitigated but didn't solve C++ crash propagation.

**Process isolation introduced** (`114d6cd`, Feb 4) — Each client connection handled in a separate child process. Parent survives DuckDB crashes. New `--process-isolation` flag, child spawned per connection, config passed via stdin pipe for security.

**Made default** (`4ebb1de`, Feb 10) — Production DuckLake deployment experienced cascading failures: metadata PostgreSQL died → DuckDB's C++ code crashed fatally → entire Duckgres process killed → all connections dropped.

---

## Phase 4: Control Plane v1 — FD Passing (Feb 5, 2026)

`d06cab3` — Split into control plane and data plane. CP accepts connections and passes file descriptors via `SCM_RIGHTS` to a pool of long-lived worker processes. Workers handle TLS, auth, PG protocol, transpilation, and DuckDB execution. Least-connections routing, health checks, rolling updates via SIGUSR2.

```
Client → Control Plane → (FD pass via SCM_RIGHTS) → Worker [TLS, auth, PG protocol, transpile, DuckDB]
```

---

## Phase 5: Control Plane v2 — Flight SQL (Feb 10–12, 2026)

Major architectural inversion. The control plane now owns client connections end-to-end; workers become thin DuckDB execution engines.

**DuckDB Flight SQL service** (`56d3848`, Feb 10) — Standalone `duckdbservice` package exposing DuckDB via Arrow Flight SQL over gRPC. Multi-session design with per-session isolated DuckDB instances.

**Zero-downtime handover** (`1f0ad9d` → `c29202b`, Feb 11) — On `systemctl reload`, the old CP spawns a new process, transfers listener FDs via handover protocol, notifies systemd of PID change. Old workers keep serving until clients disconnect naturally.

**Control plane v2** (`520fb71`, Feb 12) — CP handles TLS, auth, PG protocol, SQL transpilation. Workers only run DuckDB, accessed via Flight SQL over Unix sockets. Introduced `QueryExecutor` interface, `FlightExecutor`, `FlightWorkerPool`, `SessionManager`. Removed FD passing entirely.

```
Client → Control Plane [TLS, auth, PG protocol, transpile] → (Flight SQL over UDS) → Worker [DuckDB]
```

Benefits: clean error reporting when DuckDB crashes (CP still holds the connection), workers have zero PostgreSQL protocol knowledge, independent worker restart without dropping client connections.

---

## Phase 6: Memory Management (Feb 12–13, 2026)

Rapid iteration driven by production OOMs.

**OOM fix** (`2963449`, Feb 12) — 369GB memory peak. Each DuckDB worker independently claimed ~80% of RAM with no `memory_limit` set. Added auto-detect via `/proc/meminfo`, default `totalMem * 75% / 4`.

**1:1 worker-per-connection** (`3bee47f`, Feb 12) — Replaced fixed worker pool with elastic spawning. Each PG connection gets a dedicated duckdb-service worker. Memory/thread budgets dynamically rebalanced on every connect/disconnect.

**Stop subdividing threads** (`43bf901`, Feb 13) — Each session gets all cores. DuckDB's thread pool is cooperative; giving each session full access lets it use available CPU when others are idle.

**Disable dynamic rebalancing** (`90c01a7`, Feb 13) — Per-connection memory reallocation was too disruptive to active sessions. Changed to static allocation at creation time.

**Full memory budget per session** (`37ef5cd`, Feb 13) — The division logic always pinned sessions to the 256MB floor on large machines. Final decision: give every session the full 75% budget, let DuckDB spill to disk/swap if aggregate usage exceeds physical RAM.

---

## Phase 7: Production Hardening (Feb 13–17, 2026)

**Worker crash diagnostics** (`091b974`, Feb 13) — Log exit errors for dead workers, consecutive health check failure tracking (3 failures → force kill).

**gRPC message size** (`303dc12`, Feb 13) — Default 4MB was too small for large result sets via Flight SQL. Increased to 1GB.

**Parallel health checks** (`da4781a`, Feb 16) — Health checks run concurrently across all workers instead of sequentially.

**Flight ingress hardening** (`4d68a76` → `291dad3`, Feb 16–17) — Session identity, token lifecycle, deadlock fixes.

**Global connection limit** (`fddd19f`, Feb 17) — Default `CPUs * 2` to prevent resource exhaustion.

**Worker crash nil-pointer fix** (`3b43b03`, Feb 17) — arrow-go's `Close()` nils out `FlightServiceClient`, causing panics when session goroutines race with the health check closing a dead worker's shared gRPC client. Fixed with atomic dead flag + targeted panic recovery.

---

## Architecture Summary

| Phase | Architecture | Concurrency Model |
|-------|-------------|-------------------|
| 0 | Shared `*sql.DB` per user | One DuckDB handle shared across all connections per user |
| 1 | Per-connection `:memory:` DB | Each connection gets its own in-memory DuckDB; mutexes for DuckLake |
| 2 | Connection limits + tuning | Max connections, query cancellation, thread config |
| 3 | Process isolation (fork) | Each connection in a separate child process; parent survives crashes |
| 4 | Control plane v1 (FD passing) | CP routes connections via SCM_RIGHTS to a fixed worker pool |
| 5 | Control plane v2 (Flight SQL) | CP owns connections end-to-end; workers are thin DuckDB engines via Arrow Flight SQL |
| 6 | 1:1 elastic workers + memory mgmt | Per-connection worker spawning; full memory budget per session |
| 7 | Production hardened | Crash recovery, parallel health checks, connection limits, panic protection |
