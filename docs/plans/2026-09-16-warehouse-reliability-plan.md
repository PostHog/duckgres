# Managed-warehouse reliability: root causes and fix plan (2026-09-16)

Three distinct production failures were investigated on `mw-prod-us`. They were
initially conflated; they are unrelated and have different fixes.

---

## 1. Portola sqlmesh `COMMIT ... SSL error: unexpected eof`  — FIX IN FLIGHT

**Root cause (confirmed by source + measurement).**
`information_schema.tables` is defined as `... FROM duckdb_tables()`, and
`duckdb_tables()` calls `TableCatalogEntry::GetStorageInfo` for EVERY table in its
row loop purely to fill `estimated_size`/`index_count`. In DuckLake that landed on
`GetTableStats`, whose metadata query joins `ducklake_table_column_stats` and is
filtered `WHERE table_id = <N>` — one metadata round-trip per table, shipping every
column's min/max/extra_stats, to extract one integer (`record_count`). The stats
cache is keyed `<next_file_id, table_id>`, so every commit that writes a file
invalidates all of it.

**Measured** (portola catalog: 4,497 tables / 516,944 column-stats rows):

| | queries | rows | server-side |
|---|---|---|---|
| Current | 4,500 | 516,971 | 185 ms |
| Batched, cardinality-only | 1 | 4,500 | 1 ms |

Server execution is 185 ms — the cost is **not** database work, it is 4,500
sequential round-trips. ~1.25 ms/query on a local socket (5.6s floor with no
network); 4.4–9.1 ms/query through a pooler, which reproduces the observed
20–41s listing exactly. Scales linearly with table count.

The listing holds a read transaction open on portola's ATTACHed Postgres the
whole time; their `portola_warehouse` role caps `idle_in_transaction_session_timeout`
at 30s, so Postgres kills it and DuckDB reports SSL EOF on COMMIT.

**Fix:** PostHog/ducklake#43 (base `posthog/v1.5.5`). Cardinality-only batch path;
full `GetTableStats` untouched so query plans cannot change.

**Remaining work:**
- [ ] CI green on #43 (never built locally)
- [ ] Time a real listing on a large catalog: confirm 20–41s → sub-second
- [ ] Tag the fork; bump `DUCKLAKE_EXTENSION_TAG` in `Dockerfile` + `Dockerfile.worker`
- [ ] e2e assertion per the repo testing contract
- [ ] Promote to prod, then confirm EOFs stop

**Fallback if that slips:** raise the idle-in-transaction window from our side.
`PostHog/duckdb-postgres` already has `pg_idle_in_transaction_timeout_millis`, but
it is (a) never set by duckgres and (b) only applied in `PostgresScanConnect` —
NOT on the `PostgresTransaction` path that catalog listings actually use. The SET
must be issued *inside* the transaction (PgBouncer transaction pooling discards
session state between transactions, not within one), and the GUC is `USERSET`, so
a client may raise it above a role default without touching the customer's DB.
**It must raise only, never lower, and must preserve `0` (unlimited)** — the
DuckLake metadata store runs unlimited today and a legitimate posthog data-import
held an idle metadata transaction for 2h26m. A blanket 120s would have killed it.

Portola's own PR (portolans/backend#9397) does the same thing via
`ALTER ROLE portola_warehouse SET idle_in_transaction_session_timeout = '120s'`.
That is a valid stopgap but needs their DBA, and does not survive catalog growth.

---

## 2. Portola 5.5h refresh wedge — in-engine DuckDB deadlock  — NOT FIXED

**Root cause.** Worker `281869` ran
`INSERT INTO sqlmesh__reports.reports__cohorts_dist_d7_daily...` — `cohorts_dist.sql`,
"the expensive half: QUANTILE_CONT and percentile aggregations" over **18 GROUPING
SETS**, a heavy blocking collapse. It did ~6 min of real work (09:44–09:50, 2 cores)
then sat at **0.075 cores / ~380 B/s (TCP keepalive only) for ~5 hours**. All 62
threads parked in futex/nanosleep, none in a socket read; gdb recovers only Go
`runtime.futex` frames. Query duration logged **19,994,187 ms (5h33m)**, ended only
by the GitHub Actions 6h job timeout.

Worker was 20 min old (fresh credentials) and portola has zero `ExpiredToken` — this
is not a credential problem. Matches upstream duckdb/duckdb#24961: blocking-collapse
query hangs, `interrupt()` never honored, all threads park, CPU→0, needs SIGKILL.

**Blast radius.** `sqlmesh-deploy.yml` uses concurrency group `sqlmesh-prod` with
`cancel-in-progress: false`, so five consecutive hourly refreshes queued behind the
wedge and were cancelled.

**Plan:**
- [ ] **Containment first** — a statement timeout in duckgres. Nothing bounds a wedged
      statement today: no duckgres statement timeout, `idle_in_transaction_session_timeout=0`
      on the shards, and sqlmesh/psycopg wait forever. This is the single highest-value
      change for blast radius and is independent of the engine bug.
- [ ] Track duckdb/duckdb#24961; ship an engine build with the fix
- [ ] Workaround for portola: reduce grouping sets / split the model

---

## 3. PostHog Metabase failures — expired STS token on a pinned worker  — NOT FIXED

**Root cause.** Worker `279924` started 18:16 and held ONE long-lived Metabase
connection for ~19h. Per-tenant STS credentials expire (~1h), but worker-side
refresh (`RefreshS3Secret` via `SessionPool.reuseExistingActivation`) only fires when
a worker is **reused** for a new session or reclaimed from hot-idle — never
periodically for a worker continuously busy with one session. The per-connection
`StartCredentialRefresh` (5-min ticker) is deliberately not started on the
remote/sharedDB path ("the pool manages it") — but the pool only refreshes on reuse.

Result: 74 failures 00:04→13:35, **each ~256 s then fail**, with
`HTTP GET ... ExpiredToken: The provided token has expired`. Self-resolved only when
the worker was finally recycled.

The 256 s is the httpfs retry budget duckgres sets to ride out S3 503 SlowDown
(`applyHTTPFSRetryBudget`: retries=10, wait=500ms, backoff=2 ≈ 255s cumulative). That
budget is being spent on a **non-retryable** auth error. The httpfs fork does have
in-engine `RunWithCredentialRefresh` → `TryRefreshAuthParams`, but it only retries
once and only if something committed fresh credentials — nothing did.

**Plan:**
- [ ] Refresh STS credentials for long-lived **active** sessions, not just on reuse
- [ ] Do not spend the full retry budget on non-retryable auth 400s — fail fast or
      force a secret refresh
- [ ] Consider a max worker/session age so a pinned client connection cannot outlive
      its credentials indefinitely

---

## Cross-cutting

**Nothing bounds a hung or doomed statement anywhere in the stack.** Items 2 and 3
are different bugs with the same amplifier. A statement timeout plus an
idle-in-transaction bound would have turned a 5.5h outage into a bounded failure and
a 256 s stall into an immediate error. Highest-leverage single change after #43.

**Portola-side (their repo, no DBA needed):** 3,987 of 5,940 tables are dead sqlmesh
dev environments. Invalidating unused envs so the hourly janitor drops them is what
makes listings cheap permanently — the engine fix removes the per-table round-trip,
but catalog size still drives everything else.
