# Compute-Seconds Billing — Design / Implementation Plan

Status: **DRAFT — iterating**. Last updated 2026-06-16.

This is a living design doc. Decisions marked ✅ are locked; ❓ are open forks.

> **Scope: remote Kubernetes backend only** (`--mode control-plane
> --worker-backend remote`, `-tags kubernetes`). This is where a client session
> holds a dedicated per-org worker **pod** (one-session-per-worker contract) with
> a known CPU/mem size, which is the thing we bill. Standalone and process
> backends are out of scope — no per-org worker pod, no `WorkerProfile`, metering
> is simply not wired there.

---

## 1. Goal

Meter and bill per-org compute usage of duckgres worker pods, reusing PostHog's
existing usage→billing plumbing (the pattern the feature flag service uses). No
custom side-channel store; no per-query rows in the config store; no early
landing path. We ship raw usage metrics like any other PostHog billable resource.

The DuckLake `system.query_log` is a **separate, independent** concern
(debugging/analytics) and is NOT the billing path. This plan does not depend on
it.

✅ Billing usage_keys (two raw metrics, summed externally by billing) =
**`managed_warehouse_compute_cpu_seconds`** and
**`managed_warehouse_compute_memory_seconds`**.

---

## 2. Metering model — two raw metrics

We meter two raw numbers per connection, over the connection's full lifetime
(connect → disconnect, idle-reap, or error):

```
managed_warehouse_compute_cpu_seconds    = vCPU × ceil(connection_seconds)   # vCPU-seconds
managed_warehouse_compute_memory_seconds = GiB  × ceil(connection_seconds)   # GiB-seconds
```

- `vCPU` / `GiB` = the **provisioned** worker size (`WorkerProfile`), not measured
  usage. Memory is provisioned in **GiB** (not GB) — keep that unit consistent in
  billing.
- The billing service applies the rates and sums. Shipping the two components raw
  lets billing aggregate however it sees fit, and makes splitting out Endpoints
  trivial (a tag or a parallel metric).
- Rounded up to the whole second, once per connection.
- Aggregated at org level.

Implementation note (doesn't change the model): to avoid truncation if a worker is
ever sized in fractions, count internally in integer **millicore-seconds** /
**MiB-seconds**; billing divides back to vCPU-/GiB-seconds.

Worked examples:

| worker pod   | conn   | ceil | cpu_seconds | memory_seconds |
|--------------|--------|------|-------------|----------------|
| 8cpu / 16GiB | 9.2s   | 10   | 80          | 160            |
| 8cpu / 64GiB | 10.0s  | 10   | 80          | 640            |
| 2cpu / 4GiB  | 0.3s   | 1    | 2           | 4              |

A connection holds exactly one worker pod for its whole life (one-session-per-
worker). Warm-idle pool time *between* connections is held by no connection →
billed to no one (our infra cost). Only live-connection time bills.

---

## 3. What is billed vs not (locked)

✅ **Billed** — the whole connection's worker-held wall-clock, because the
connection holds a dedicated pod the entire time:
- query execution, transpilation, Parse/Bind/Execute (all protocols)
- result rows streamed to the client — including slow-client transfer (worker
  pinned sending → fair to bill)
- inbound `COPY FROM` upload
- **idle-but-connected time** — an open connection holds the pod, unavailable to
  others → billed
- a long query that **dies on a user error** — pod held the whole time, and the
  connection still reaches its end report → billed (automatic)

❌ **Not billed / lost (for now):**
- Catalog ATTACH / worker activation — per-session setup before the client gets
  the session, attributed to no connection. Bigger catalog → slower attach, left
  amortized-free for v1.
- Warm-idle worker pool time (between connections) — our infra cost, not a user's.
- **Anything lost to a hard crash** — we report only at connection end (§4), so a
  CP-pod or worker-pod crash mid-connection loses that connection's entire bill.
  Accepted ("a 3h query that then crashes is lost for billing, for now").

---

## 4. Timing model — report once at connection end ✅

**We do NOT meter per query.** We bill the connection's wall-clock and **emit
once, when the connection ends.** A connection lives entirely on one CP pod (k8s
replaces CP pods on deploy; client connections do not migrate between pods), so
there is exactly one start and one end — no segments.

Why end-only:
- At connection end we know the **end reason** (clean disconnect, idle reap, user
  error, fatal error) — error handling is trivial and reads naturally.
- No per-query accumulator, no heartbeat, no drip. Simplest possible.
- **Loss model:** a connection contributes nothing until it ends. A hard crash
  before the end report loses that connection's entire bill. Accepted for v1.

```
conn start: client connect (CP assigns/spawns the org's worker pod)
conn end:   client disconnect | idle reap | fatal error
            | CP graceful shutdown closing the connection
on end →  secs = ceil(now - connStart)
          cpu_seconds = cores × secs ;  memory_seconds = gib × secs
          → add both to in-process per-org counter (§6), best-effort
```

### End reason (recorded, not yet acted on)
Capture the end reason at the report point for visibility (and future infra-error
exclusion). **v1 bills regardless of reason** — the only thing that escapes
billing is a crash that never reaches the report. No classification logic now.

### Plant point
The connection teardown path in `server/conn.go` (where the `clientConn` serve
loop exits / `Close`). Worker size (`workerCores`, `workerGiB`) is on the
`clientConn` (§5); `connStart` recorded at connection setup.

---

## 5. Code changes — duckgres (emit side, remote backend only)

### 5.1 Plumb worker pod size onto the connection
`WorkerProfile.CPU/Memory` is a local var in the CP connect handler
(`controlplane/control.go:1046`), NOT reachable at connection time. Thread the
normalized worker size through:

1. Add fields to `clientConn` (`server/conn.go:154`): `workerCores float64`,
   `workerGiB float64`, and `connStart time.Time` (constants for the session's
   worker). `workerCores == 0` (e.g. non-remote / unknown profile) → metering
   skipped.
2. Add params to `NewClientConn` (`server/exports.go:52`).
3. Compute + pass at the call site (`controlplane/control.go:1303`) —
   `workerProfile` in scope from `:1046`; reuse `parseK8sCPU`/`parseK8sMemory`
   from `workerDuckDBLimits` (`control.go:1345`) to normalize cpu→cores, mem→GiB.

### 5.2 Report at connection end
- Record `connStart` at connection setup.
- On teardown (serve-loop exit / Close): if `workerCores > 0`, compute
  `secs = ceil(now - connStart)` once, then
  `cpu_seconds = workerCores × secs` and `memory_seconds = workerGiB × secs`,
  record end reason, add **both** to the in-process per-org counter (§6.1).
  **Best-effort** — wrap so a metering error never blocks or fails connection
  teardown.
- No per-query hooks. (Optional: also set `cpu_seconds`/`memory_seconds` columns on
  the DuckLake query_log if/when enabled, for reconciliation — independent of
  billing.)

### 5.3 Graceful shutdown flush (CP churn correctness) ✅
Verified shutdown behavior (remote/k8s), with the facts that make report-at-end
safe:

- On SIGTERM the CP pod marks not-ready, stops accepting **new** connections, then
  blocks on `cp.wg.Wait()` for every existing connection goroutine to return
  (`controlplane/control.go:585-610`, `:1556-1631`, `:685/687`).
- It **waits for the whole CONNECTION to end (client disconnect/EOF)**, NOT just
  the current query — `messageLoop` loops over queries and returns only on client
  disconnect, idle-timeout, or fatal error (`server/conn.go:1047-1064`). A finished
  query does not end the connection.
- The drain wait is **UNBOUNDED in remote mode**: `HandoverDrainTimeout = 0`
  (`control.go:256-268`) — the old 15m default was removed after it cut in-flight
  customer queries (CLAUDE.md/help/README still say "15m" — stale). duckgres never
  force-closes a live connection; `activeConns` is just a counter.
- The only hard cutoff is the CP pod's k8s `terminationGracePeriodSeconds` →
  SIGKILL. ✅ **Prod = 24h.** With unbounded drain + a 24h grace, essentially every
  connection reaches its **natural end on the terminating pod** → its end report
  (§5.2) fires → billed in full.

So on shutdown:
1. Stop accepting new connections.
2. Existing connections run to their natural end (client disconnect) on this pod;
   each fires one complete end report (§5.2).
3. **Final flush** of the in-process counter to the config store before
   `os.Exit(0)`.

Correct across CP pods coming/going because the buffer is the shared config store
with UPSERT-increment (§6.3): a departing pod adds its counts before exit; the
leader drainer + survivors carry on.

> Side note: the unbounded remote drain (idle connections can hold a terminating
> CP pod open for up to the 24h grace wall) is a separate problem tracked in
> PostHog/duckgres#782 — not a billing blocker.

**Residual loss (A, accepted):** a connection still open at the 24h grace wall is
SIGKILLed → no end report → its whole bill lost; likewise a hard crash. Both are
the accepted crash case (§3/§4); with 24h grace this is rare. (If it ever matters
— short grace, very long idle sessions — the fix is per-connection periodic
checkpointing, see §10; NOT doing it now.)

e2e must assert: roll the CP deployment while a session is connected → the pod
stays `Terminating` until the connection ends and the session's metrics
still land.

---

## 6. Emit transport & reliability — duckgres → PostHog

### 6.1 Reliability principle (LOAD-BEARING) ✅
**Losing billing data is acceptable; failing a query/connection is not.** Metering
must never block or fail the request/teardown path.
- Connection end writes `cpu_seconds`+`memory_seconds` to an **in-process per-org counter only**
  (map + mutex, microseconds, no I/O). Even that is best-effort — swallow errors.
- Everything downstream (flush to buffer, ship to PostHog) is **async, off-path,
  retried**. Any downstream outage loses at most some counts; never delays/fails a
  client connection.
- duckgres takes **no new hard dependency** on PostHog ingestion availability.

### 6.2 Pipeline shape
```
conn end → in-proc per-org counter (mem)        [best-effort; never fails teardown]
        │  periodic flush (~15s), async         (tracks cpu_seconds + memory_seconds)
        ▼
   config store: duckgres_org_compute_usage (org × time-bucket → cpu_seconds, memory_seconds)
        │  leader drainer (one CP pod), ~60s, retried
        ▼
   PostHog capture("managed warehouse compute usage",
                   {distinct_id: org_id, cpu_seconds, memory_seconds})
        │
        ▼
   ClickHouse → usage_report sums each metric per org/period → POST billing service
```
Bucket keyed by **connection-end time**: `bucket_start = floor(end_time / width)`.

### 6.3 Durable buffer — config store (Postgres) ✅
No new infra (already a hard dependency of the remote control plane); aggregated
rows only (org × time-bucket), NOT per-query rows; survives CP restarts. Both raw
metrics live in one row (same bucket, same uuid downstream).

```
duckgres_org_compute_usage(
    org_id          TEXT,
    bucket_start    TIMESTAMPTZ,
    cpu_seconds     BIGINT NOT NULL,
    memory_seconds  BIGINT NOT NULL,
    PRIMARY KEY (org_id, bucket_start)
)
duckgres_org_compute_drain_state(
    org_id              TEXT PRIMARY KEY,
    last_drained_bucket TIMESTAMPTZ NOT NULL      -- high-water mark
)
```
Flush = UPSERT-increment (sums across CP pods):
```
INSERT INTO duckgres_org_compute_usage(org_id, bucket_start, cpu_seconds, memory_seconds)
VALUES (...)
ON CONFLICT (org_id, bucket_start)
DO UPDATE SET cpu_seconds    = duckgres_org_compute_usage.cpu_seconds    + EXCLUDED.cpu_seconds,
              memory_seconds = duckgres_org_compute_usage.memory_seconds + EXCLUDED.memory_seconds;
```
(Migrations go in the config store's Goose set — see `controlplane/configstore/`.)

### 6.4 Bucket close + drain (delivery = ship-then-delete ✅)

**Bucket** = aligned window, key `bucket_start = floor(end_time / width)` (width
60s). **Closed** (time-based, no coordination):
```
closed  ⇔  now ≥ bucket_start + width + grace
grace   ≥  in-proc flush_interval + clock_skew_margin   (60s / 15s / 15s → grace 30s)
```
Grace waits out CP-pod flush lag so every contribution has landed before drain.

**Delivery contract ✅ (ship-then-delete, at-least-once):** delete a bucket ONLY
after ingestion confirms success; a ship failure keeps the row and retries next
tick — never lose a bucket to a transient outage.

**Leader drain loop** (leader-only goroutine, ~60s; NOT a k8s CronJob — runs
alongside `leader_loop.go`/`janitor.go`):
```
for each org's closed, not-yet-drained buckets
    (bucket_start ≤ now - width - grace  AND  bucket_start > last_drained_bucket):
  read cpu_seconds, memory_seconds
  ship capture("managed warehouse compute usage",
               {distinct_id: org_id, cpu_seconds, memory_seconds},   -- both metrics, one event
               event_uuid = hash(org_id, bucket_start),   -- deterministic, stable across retries
               timestamp  = bucket_start)                 -- stable: same toDate on every retry
  on SUCCESS: TXN { advance last_drained_bucket = bucket_start; DELETE the row }
  on FAILURE: leave row; retry next tick                  -- no data loss
```
Both metrics ride **one event** per bucket (one uuid). Billing's two usage_keys are
derived by two gather queries summing each property (§7.2).

**Idempotency (effectively exactly-once for billing).** Only double-ship window =
crash after ingestion ack but before the delete commits → re-ship next tick. The
deterministic `event_uuid = hash(org_id, bucket_start)` + stable
`timestamp = bucket_start` + fixed event/distinct_id mean a re-ship is the **same
row** in ClickHouse's ReplacingMergeTree (dedup key includes `cityHash64(uuid)`),
so it collapses on merge. NOTE: these metrics are **summed** (`sum(property)`), not
`count(distinct)`, so unlike the FF count-metric they are NOT merge-timing-
independent at query time — to be safe the gather query should **dedup by uuid
before summing** (sum over `… GROUP BY uuid`, or `FINAL`) rather than trusting an
unmerged `sum()`. For the daily billing run merges are long done, so even a plain
`sum()` is correct in practice; the dedup-before-sum just removes the timing
assumption.

**High-water mark** also drops the late-write corner (a CP pod stalled longer than
`grace` re-INSERTing an already-drained bucket): `bucket_start >
last_drained_bucket` skips it, and the cleanup sweep removes it.

**Cleanup is free:** the drain DELETE *is* the cleanup — `duckgres_org_compute_usage`
only ever holds open/recent + retrying buckets; inactive orgs drain to empty.
`drain_state` is one tiny row per org. Safety sweep: hard-delete any lingering row
`≤ last_drained_bucket`.

### 6.5 PostHog ship — public ingestion (capture, like any SDK) ✅
The leader drainer ships each closed bucket as a **`capture()` event to PostHog's
public ingestion endpoint over HTTPS**, exactly like any customer SDK — authed by a
project API token. No private path, no new AWS infra.

- **Why public:** the managed-warehouse EKS cluster has **no private network path**
  to posthog (its VPC is not peered to posthog, and VPC peering is non-transitive) —
  but it **does** have **NAT egress to the internet**. So the public ingestion host
  is reachable today. PrivateLink was evaluated and rejected on cost: at our volume
  (one small event per active org per ~60s bucket = single-digit GB/month) the
  standing per-region cost of a PrivateLink (an NLB + interface endpoint per region)
  is not worth it versus the near-zero marginal cost over the already-present NAT.
  Data carries only `org_id` + `cpu_seconds`/`memory_seconds` — no PII — so public
  TLS is fine.
- **Requirement:** mw EKS egress must reach the ingestion host. NAT provides
  general internet egress; if a cluster egress NetworkPolicy/firewall is in place,
  allow the ingestion domain. No AWS-account-level infra.
- **Config (duckgres, remote backend):** ingestion base URL (e.g.
  `https://us.i.posthog.com`) + a project API token, env/CLI. Unset → metering
  ships nowhere (logs only), never fails a query.

This unblocks the FF-style path end to end:
`capture() → ClickHouse → usage_report gather → billing` (§7 now applies as-is).

The duckgres side is unchanged from §6.4: config-store buffer + leader
ship-then-delete with deterministic `event_uuid = hash(org_id, bucket_start)` +
`timestamp = bucket_start`; "ack" = the ingestion HTTP 2xx. (The buffer + leader +
idempotency still earn their keep: cross-pod aggregation, retry across ingestion
blips, and exactly-once-for-billing via the deterministic uuid.)

### Org identity ✅
`org_id` is available in the control plane per connection — the natural key
end-to-end (counter → buffer → capture `distinct_id`). (Token + any org_id→team
mapping = billing-team config, owner will handle.)

---

## 7. PostHog side — billing report (no enforcement in v1)

(Repo: `~/code/posthog/posthog`. Report path only.)

### How posthog usage→billing works (verified) — and where we plug in
Usage→billing = the Temporal `usage_report` workflow (`posthog/temporal/usage_report/`):
```
gather queries (ClickHouse + Postgres)  →  write per-org JSONL + manifest to S3
   →  SQS pointer  →  external billing service READS the S3 files
```
- **All usage is sourced from CH/PG gather queries** (`usage_report/queries.py`);
  the S3/SQS part is the outbound handoff to billing. There is no inbound
  "read usage from a bucket" path — and we don't need one.
- **Our plug-in point = ClickHouse**, exactly like feature flags: duckgres ships
  `"managed warehouse compute usage"` **capture events** (§6.5, each carrying both
  `cpu_seconds` and `memory_seconds`) which land in CH; we add **two gather queries**
  that sum each property (§7.2), and they ride the existing S3→SQS→billing handoff.
  No cross-account data plumbing.
- `org_id == posthog team_id` (`products/data_warehouse/backend/api/data_warehouse.py:846`)
  — identity is trivial. ✅
- Managed warehouse today = provisioning-proxy only, **zero metering**; no compute
  metrics, no managed-warehouse quota resource, no SKU. All net-new.
- `usage_report/activities.py`, `storage.py:31`, `settings/object_storage.py:45-48`;
  FF reference (capture-events-into-CH + gather): `flag_analytics.py:69/161`,
  `usage_report.py:557/584/968`.

### Integration seam ✅ (resolved — capture events land it in CH)
duckgres ships the two raw metrics as **`capture()` events to public ingestion**
(§6.5). Those events land in posthog's **ClickHouse** like any other event, so the
existing usage→billing pipeline picks them up via gather queries — **no cross-account
data plumbing, no new inbound consumer.** This is the feature-flag `"decide usage"`
pattern: ship usage events, then gather queries sum them.

Concretely: add two CH gather queries (one per property) for the `"managed
warehouse compute usage"` events (§7.2). They ride the existing `usage_report` →
S3 → SQS → billing handoff for free. The earlier "how does mw data reach posthog
CH/PG" blocker is gone — public capture *is* the landing.

### No enforcement in v1

✅ **No enforcement / quota-limiting in v1.** Meter + report only. No
`is_team_limited`, no admission gate, no shutdown in duckgres. (Enforcement
plumbing exists and can be wired later. Out of scope now.)

### 7.1 Register the billable resources (two)
- Add **two** members to `QuotaResource` (`ee/billing/quota_limiting.py:73`) +
  `UsageCounters` (`:119`):
  `MANAGED_WAREHOUSE_COMPUTE_CPU_SECONDS = "managed_warehouse_compute_cpu_seconds"`
  and `MANAGED_WAREHOUSE_COMPUTE_MEMORY_SECONDS = "managed_warehouse_compute_memory_seconds"`.
- Coordinate both usage_keys with the billing service `default_plans_config.yml`.
  Billing sums/weights them externally into the single user-facing compute line.

### 7.2 Usage report fields (two)
- Add `managed_warehouse_compute_cpu_seconds_in_period` and
  `managed_warehouse_compute_memory_seconds_in_period` (flat ints) to
  `UsageReportCounters` (`posthog/tasks/usage_report.py:120`).
- Add two CH gather queries over the `"managed warehouse compute usage"` events,
  token-gated, each **summing its property** (`sum(JSONExtractInt(properties,
  'cpu_seconds'))` / `'memory_seconds'`). To stay merge-timing-independent, dedup
  by event uuid before summing (sum over `… GROUP BY uuid`, or `FINAL`) rather than
  a plain `sum()` — see §6.4. (Mirror the existing sum-metric gather pattern,
  `usage_report.py` query-metric helpers, not the FF `count(distinct)`.)
- Assemble in `_get_team_report` (`:2402`); flows to billing via existing
  `send_report_to_billing_service` (`:425`). No new transport.

---

## 8. Testing

### duckgres (unit)
- `cpu_seconds = cores × ceil(conn_secs)` and `memory_seconds = gib × ceil(conn_secs)`
  math (single ceil shared by both, cpu/mem normalization, `workerCores==0` skip).
  End-reason capture. Best-effort property (a metering error never fails teardown).
  Flush UPSERT-increment of both columns + drain ship-then-delete + high-water +
  idempotent uuid.

### duckgres (e2e — `tests/e2e-mw-dev/harness.sh`, required per CLAUDE.md)
Provision an org with a known worker profile, then assert the emitted event's
`cpu_seconds` ≈ cores × ceil(conn) and `memory_seconds` ≈ gib × ceil(conn):
- run a query, disconnect → both metrics ≈ size × ceil(connection wall-clock).
- connect, sit **idle**, disconnect → idle time billed.
- long query that errors with a **user error**, then disconnect → still billed.
- large-result / throttled SELECT + a COPY FROM → their time inside the connection.
- **CP churn:** roll the control-plane deployment mid-session → the session's
  metrics still land (graceful drain + shutdown flush work).
- **reliability:** make the durable buffer unreachable → queries/connections all
  still succeed; counts silently dropped.
- both metadata backends (cnpg + ext) where it touches metadata.

### posthog (unit)
Two CH gather queries (sum cpu_seconds / memory_seconds, dedup-by-uuid) +
usage_report fields + two quota_limiting resource registrations, following the
sum-metric gather pattern.

---

## 9. Phasing

1. **Meter + plumbing (duckgres, no emit):** plumb worker pod size (cores, gib) onto
   `clientConn`, record `connStart`, compute `cpu_seconds`+`memory_seconds` at
   connection end, write to in-proc counter, log to slog. Unit tests. Reliability
   property in place. Verifiable in isolation.
2. **Durable buffer + flush + shutdown (duckgres):** the two config-store tables
   (Goose migration), periodic async flush, cross-pod aggregation,
   graceful-shutdown flush (§5.3). Assert flush failures never touch the request
   path.
3. **Drain → PostHog (duckgres):** leader drainer + posthog-go capture client +
   token, ship-then-delete with deterministic uuid/timestamp. e2e: event lands AND
   queries survive PostHog being down AND survive a CP roll.
4. **Billing report (posthog):** register the two resources + two usage_report
   gather queries (sum cpu/memory seconds) + send-to-billing. Coordinate both
   usage_keys with billing service.

Enforcement = possible later follow-up, NOT in this plan. Each phase
independently shippable/testable.

---

## 10. Open decisions (tracker)

- ❓ (owner: billing team, not code) provision a billing-analytics validity token
   for the `"managed warehouse compute usage"` event; register the two usage_keys
   `managed_warehouse_compute_cpu_seconds` + `managed_warehouse_compute_memory_seconds`
   (billing sums/weights them into one line item) + any org_id→team mapping.

Resolved / out of scope:
- ✅ **Intervals = bucket width 60s / in-proc flush 15s / grace 30s / drain tick 60s.**
- ✅ **Emit transport = public ingestion** (capture events over HTTPS via NAT,
   like any SDK). PrivateLink evaluated + rejected on cost (standing per-region
   NLB+endpoint cost not worth it at single-digit-GB/mo volume). Posthog
   integration = capture events land in CH + a gather query in
   `usage_report/queries.py` → rides existing S3→SQS→billing. No cross-account
   plumbing, no new posthog inbound consumer.
- ✅ Shutdown loss model = **A (report-at-end)**. Verified: remote drain is
   unbounded (`HandoverDrainTimeout=0`), waits for the whole connection to end
   (client disconnect, not query), no force-close; only cutoff is pod
   `terminationGracePeriodSeconds` = **24h in prod** → connections finish
   naturally and bill in full. Lose only connections >24h or hard crashes
   (accepted). Per-connection checkpointing (B) considered, NOT done now.
- ✅ **Scope = remote Kubernetes backend only** (per-org worker pod with a known
   size). Standalone/process not wired.
- ✅ Two raw usage_keys = `managed_warehouse_compute_cpu_seconds` +
   `managed_warehouse_compute_memory_seconds`; billing sums/weights them externally
   into one user-facing compute line item.
- ✅ Bill **connection wall-clock** (incl. idle); one whole worker pod per session.
- ✅ Report **once at connection end** — end reason known then; no segments (k8s
   CP pods don't hand connections over), no drip. A hard crash mid-connection
   loses that bill (accepted, incl. a 3h query).
- ✅ Units = two raw metrics `cpu_seconds = cores × ceil(conn_secs)` +
   `memory_seconds = gib × ceil(conn_secs)` (no R, no max/normalization — billing
   weights externally); single ceil per connection shared by both.
- ✅ User-error query billed (automatic); infra-failure handled only by
   crash-loses-it for v1 (no classifier).
- ✅ Graceful CP shutdown flushes connection-end reports + in-proc counter;
   correct across CP-pod churn via shared config-store UPSERT buffer.
- ✅ Durable buffer = config store, aggregated `duckgres_org_compute_usage`.
- ✅ Drain = leader goroutine (not CronJob); time-based bucket close; delivery
   ship-then-delete (at-least-once), deterministic uuid+timestamp → billable
   count(distinct) folds duplicates; per-org high-water + DELETE = cleanup.
- ✅ No enforcement / quota-limiting in v1.
- ✅ Reliability: request/teardown path never fails on metering; lose over fail.
- ✅ org_id is the identity key, available in the control plane.
- (deferred) ATTACH / activation cost recovery — out of scope v1.

---

## 11. Reference — key code locations

duckgres (remote backend):
- connection teardown / serve-loop exit (plant point) — `server/conn.go`
  (`clientConn` lifecycle; exact line TBD)
- graceful shutdown / drain — `--handover-drain-timeout`; CP stop path
  `controlplane/control.go` (`stopQueryLogger` neighborhood, `:1657/:1842`)
- worker profile origin — `controlplane/control.go:1046`; sizing
  `workerDuckDBLimits` `:1345` (`parseK8sCPU`/`parseK8sMemory`)
- clientConn struct — `server/conn.go:154`; `NewClientConn` `server/exports.go:52`;
  call site `control.go:1303`
- leader loop (drain home) — `controlplane/leader_loop.go`, `janitor.go`
- config store + migrations — `controlplane/configstore/` (Goose)
- ATTACH (not billed) — `server/server.go:1204` `ActivateDBConnection`
- query_log chokepoint (optional reconciliation column) — `server/querylog.go:487`

posthog:
- `QuotaResource` / billable `count(distinct)` — `ee/billing/quota_limiting.py:73`;
  `posthog/tasks/usage_report.py:557/584`
- usage report dataclass + assembly + send —
  `posthog/tasks/usage_report.py:120 / :2402 / :425`
- FF emit reference — `products/feature_flags/backend/flag_analytics.py:69/161`
