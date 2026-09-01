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

#### Implementation decisions (Task 9)

**Scope narrowed from the bullet above.** Lazy acquisition applies ONLY when
the exploratory tier is active for the connection (`useExploratory` AND a
config store). GUC-sized, tier-disabled, passthrough, process-backend and
standalone connections keep the eager connect-time acquire byte for byte —
one consistent acquisition point was not worth re-validating every legacy
path against. Revisit if the tier becomes the only mode.

**ADMISSION-TIMING (accepted).** For tiered connections, everything that
happens as part of acquiring a worker now happens at the FIRST STATEMENT
rather than at connect: the org/user vCPU admission check, the org
`max_connections` limit, and worker-capacity backpressure. A client can
therefore complete a PostgreSQL handshake (auth OK, ParameterStatus,
BackendKeyData, ReadyForQuery) and only then be told `53400`/`53300` on its
first query, as a FATAL that terminates the connection. Accepted: the
alternative is admitting at connect, which requires acquiring at connect and
defeats the whole feature. Consequences to keep in mind:

- Those failures are connection-fatal at first statement, not connect-time
  rejections. Clients that distinguish "could not connect" from "query
  failed" will see the latter.
- The failure is classified control-plane side with the SAME logic the eager
  path uses (`sessionCreationErrorResponse` → `server.SessionAcquireError`),
  so the SQLSTATE and message are identical to what a connect-time rejection
  would have carried — only the timing differs.
- Admission-related counters that keyed off connect-time rejections will
  under-count for tiered orgs; `duckgres_session_activation_total{outcome}`
  (success | canceled | capacity | draining | error) plus
  `duckgres_session_activation_duration_seconds` are the replacement signal,
  since `duckgres_session_start_*` now only covers the handshake for these
  connections.

**Cancellation.** A first-statement acquire is registered for cancellation
(`createSessionWithRegisteredCancel`), so a `CancelRequest` aborts a slow
cold spawn exactly as it did at connect (surfaces as `57014`).

**TCP-FIN during activation (accepted gap).** The eager path runs a
pre-ready disconnect watcher so a client FIN during the slow acquire tears
the session down. The lazy path does not start one: after ReadyForQuery the
socket belongs to the message loop, which is blocked inside the statement
that triggered the acquire, so a FIN is not observed until the acquire
returns. A client that disconnects mid-activation therefore leaves the spawn
running to completion. This is bounded and self-healing — the existing
abandoned-spawn machinery parks the completed worker hot-idle for the org's
next connection (`ReleaseWorker` / `TransitionToHotIdleIfNoSessions`), and
the connection's own teardown destroys the session as soon as the acquire
returns. Nothing leaks in Reserved/Activating.

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
  `workerDuckDBLimits`; a small pod is below the 24Gi headroom crossover, so it
  keeps `min(6GiB, 40%)` back rather than a flat 25%) plus an optional time
  budget.
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

**Kill/disable interplay.** The switcher's destroy→create window is the one
point where a connection is invisible to the per-user kill/disable fan-out:
`DestroySessionsForUser` iterates sessions, and mid-escalation the connection
holds neither a session nor a registered conn-closer.

- **Disable (persistent block) is mitigated.** After the escalated session's
  metadata init succeeds and the conn-closer is re-registered, the switcher
  re-reads the user's disabled state from the same config snapshot the
  connect-time auth check reads (`ConfigStore.OrgUserSessionQueryAccess`;
  `ok=false` = missing or disabled). If it is set, the fresh session is
  destroyed and the escalation fails — which is connection-fatal, so the
  disabled user is dropped rather than resurrected on a bigger worker.
- **A one-shot `kill` landing exactly inside the window is an accepted miss.**
  `kill` is documented best-effort (terminate now, no reconnect block), and the
  miss is bounded by one worker acquire + session init. The e2e kill assertions
  run against steady-state connections, not mid-escalation ones. The same gap
  covers a lazily-activated connection that has authenticated but never run a
  statement — it has no session and no registered conn-closer, so `kill` cannot
  see it at all (not just for a bounded window); `disable` still covers it, via
  the activation-time re-check that refuses the session with 28000. Extending
  `kill` to authed-but-unactivated connections is a follow-up.

### 6. Observability & tests

As shipped:

- `duckgres_exploratory_escalations_total{reason="oom"|"state"|"heuristic",
  outcome="ok"|"canceled"|"capacity"|"draining"|"disabled"|"error"}` — every
  escalation ATTEMPT, so failures are visible and not just successes.
- `duckgres_session_activation_total{org,outcome}` +
  `duckgres_session_activation_duration_seconds{org}` for the lazy
  first-statement acquisition, whose capacity/draining/admission failures no
  longer land in the connect-time `duckgres_session_start_*` metrics. Its
  failure labels come from the same `server.AcquisitionFailureOutcome` helper as
  the escalation counter's, so the label sets cannot drift.
- A `worker_tier` column in the query log (`exploratory` | `standard`),
  rather than the tier label on acquire metrics this section originally
  sketched: the acquire metrics are per-worker-pool, while the question asked in
  practice ("which tier ran this statement?") is per-statement.
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
