# Exploratory Small-Worker Tier (Small-First Routing)

**Date:** 2026-08-04
**Status:** Approved design, pre-implementation
**Scope:** control-plane remote/k8s backend only

## Problem

Today every connection acquires a full-size worker pod (org default profile,
else 8 vCPU / 16Gi) at connection start. Most managed-warehouse traffic is
short exploratory reads (`select count(1) from posthog.events`,
`select * from posthog.events limit 10`) that neither need that capacity nor
tolerate cold-spawn latency well. We want a small, warm, per-org pod that
serves this traffic by default, escalating to a normal-size worker only when
a query is heavy or the session accumulates state.

## Decisions (settled during brainstorming)

1. **Concurrency:** one session per small pod. The one-session-per-worker
   LOAD-BEARING CONTRACT is unchanged. A second concurrent connection spawns
   another small pod on demand.
2. **Escalation trigger:** hybrid — a minimal pluggable heuristic routes
   obviously-heavy reads straight to a normal worker; everything else runs
   optimistically on the small pod and is transparently re-executed on a
   normal worker if it exceeds the small pod's resources.
3. **Session state:** pin, never replay. The first state-mutating statement
   moves the connection to a dedicated normal-size worker for the rest of its
   life. No record-and-replay of temp tables / txn state.
4. **Eligibility:** all logins unconditionally, gated only by a
   deployment-level kill switch. Connections carrying explicit
   `duckgres.worker_*` startup GUCs bypass the tier entirely.

## Design

### 1. Tier & scheduling — reuse the profile machinery

- New deployment-level exploratory profile, env-only:
  - `DUCKGRES_EXPLORATORY_WORKER_CPU` (e.g. `2`)
  - `DUCKGRES_EXPLORATORY_WORKER_MEMORY` (e.g. `4Gi`)
  - `DUCKGRES_EXPLORATORY_WORKER_TTL` (default `48h`)
  - `DUCKGRES_EXPLORATORY_TIER_ENABLED` (kill switch; mw-dev before prod)
- Small pods are ordinary workers of this profile: unchanged
  `OrgReservedPool` acquire/claim/spawn, unchanged janitor and per-CP reaper.
- "Keep a pod for orgs active in the last 2 days" falls out of hot-idle +
  48h TTL: after a connection ends, the small pod parks hot-idle for 48h and
  the org's next connection claims it warm. No new scheduler, no activity
  tracker, no proactive spawner. A cold org pays one small spawn.
- Warm pods are per-**org** (worker pools are org-scoped; a PostHog team's
  project logins share the org pool).

### 2. Lazy acquisition

- Auth is CP-side; connection startup completes without acquiring a worker.
- The first statement that needs DuckDB triggers acquisition.
  Connect-and-quit connections (pool warmers, health checks, editor's
  speculative connections) never touch a pod.
- GUC-sized connections (`duckgres.worker_*`) bypass the tier but also
  acquire lazily at first statement, for one consistent acquisition point.

### 3. Classification & escalation (CP-side, pg_query-based)

Per statement, three outcomes:

- **Read-only** (SELECT / SHOW / EXPLAIN …): eligible for the small pod.
- **State-mutating or writing** (DML, DDL, COPY, CREATE SECRET, temp
  tables, BEGIN): **pin** — acquire a worker of the org's normal profile
  (org `default_worker_*`, else pool default), the session lives there for
  the rest of the connection. The small worker (stateless by construction)
  is released back to hot-idle. Never routes back.
- **Obviously-heavy read** (heuristic tier, minimal at v1, pluggable —
  first version may be as simple as "no LIMIT + references tables above a
  DuckLake-stats size threshold"): skip the small attempt, go straight to a
  normal-size worker; sticky for the connection.

### 4. Optimistic execution + re-execute fallback

- Reads run on the small pod under its natural memory limit (existing
  `workerDuckDBLimits`, ~75% of the small pod) plus an optional time budget.
- On OOM / budget exceeded: **iff no data rows have been streamed to the
  client**, the CP acquires a normal-size worker, re-executes, and streams
  from there — invisible to the client. If rows already went out, the error
  surfaces (a wire stream cannot be transparently restarted).
- The escalated worker is sticky for the rest of the connection.
- Client cancel is honored on whichever worker is active and never triggers
  re-execution.
- DML/DDL is never re-executed (classification routes it before execution).

### 5. State hygiene & billing

- Small-pod reuse across an org's users rides the existing
  `wipeUserSecrets` + `DestroySession` semantics; nothing new.
- Billing v1: a connection bills at the **largest** worker size it used.
  Conservative; connections that stay small bill small. Per-segment
  metering is an explicit follow-up, not v1. Implemented by stamping the
  escalation target's size on the connection at switch time — escalation
  only ever goes exploratory→standard, so that stamp IS the maximum.
- The new worker's session starts **cold**: the escalation switcher re-runs
  the full connect-time session-metadata init (attached-catalog probe,
  `InitSessionDatabaseMetadataWithAccess`, connect-time search_path /
  passthrough catalog) against it, from the same connect-time inputs
  (`ControlPlane.initSessionMetadata`).
- `duckgres.s3_cache = off` must be **re-applied** on the escalated worker.
  The bypass lives in the worker's `ducklake_s3` secret, and `CreateSession`
  deliberately restores the cache-proxy transport before a session starts, so
  a bypassed connection would otherwise silently start reading cached the
  moment it escalated. A failed re-apply fails the statement AND resets the
  session state to the worker's actual transport (`SHOW` must never report a
  transport the worker isn't in).

### 6. Observability & tests

- `duckgres_exploratory_escalations_total{reason="oom"|"state"|"heuristic"}`,
  tier label on acquire metrics, tier tag in the query log.
- Unit: classification, escalation state machine, profile resolution.
- e2e (`tests/mw-dev/e2e/harness.sh`): small-first claim; heavy-query
  escalation returns correct results; state-mutation pin; GUC bypass;
  `one_session_per_worker` extended to the small tier.
- CLAUDE.md worker-session contract section gets the tier addendum in the
  same PR (per the docs-in-sync rule).

## Known risks (accepted)

- The no-rows-sent re-execution rule means some mid-stream OOMs surface to
  users; heavy aggregations typically OOM before producing rows.
- "All logins unconditionally" means big ETL connections pay one wasted
  small attempt until the heuristic learns their shapes; `duckgres.worker_*`
  GUCs are their escape hatch.
- Escalation storms degrade to today's behavior (every connection on a
  normal worker) — an acceptable floor.
