# Managed-warehouse compute billing — pull API

Status: **IMPLEMENTED** (duckgres side; endpoints live at
`GET /api/v1/billing/usage` + `POST /api/v1/billing/ack`, internal-secret
bearer auth). Supersedes the reporting hop of
[`billing-compute-seconds-plan.md`](./billing-compute-seconds-plan.md): billing
now **pulls** usage from duckgres instead of duckgres pushing capture events to
PostHog ingestion. The per-connection metering is unchanged — only how the data
leaves duckgres changes.

## Overview

duckgres meters compute usage into 60-second **buckets** in its config store. The
billing service periodically **pulls** the usage accumulated since its last ack,
processes it, and **acks the high watermark**; duckgres advances a cursor and
deletes everything up to it. A safety GC caps how long anything can linger. Direction is
posthog → mw (billing initiates), which the existing duckgres control-plane
ingress already supports.

```
duckgres:  connection ends → meter → accumulate into 60s config-store buckets
billing:   GET /usage  → process → POST /ack {watermark_high}
                                     → duckgres advances cursor + deletes ≤ watermark_high
safety:    GC hard-deletes buckets older than 30 days (logged)
```

## Buckets

Internally, one row per unique key, values accumulated:

- **Key:** `(org_id, team_id, query_source, cpu, mem_gib, bucket_start)` where
  `query_source` is `standard` | `endpoints`, `cpu` is vCPU, `mem_gib` is GiB, and
  `bucket_start = floor(connection_end / 60s)`. Worker size is stored as **exact
  decimals** (SQL `NUMERIC`, not IEEE floats), so equal sizes always group into the
  same bucket and fractional sizes keep full precision.
- **Values:** `cpu_seconds` (vCPU-seconds), `memory_seconds` (GiB-seconds) —
  summed over every connection that falls in that key + minute.
- `team_id` is **informational** (an **integer**, matching PostHog's `Team.id` —
  a JSON number on every surface): duckgres does NOT own team-level billing
  attribution — the **external billing service maps org → team(s) itself**. The
  stamped value is a best-effort hint recorded at connection end: the
  **connecting user's team** (`duckgres_org_users.team_id`, e.g. a
  project-reader login) when it has one, else the **org's oldest team** (min
  `created_at`, ties broken by the smaller `team_id` — in practice the
  provision-time first team), else `0` (defensive: unknown org / stale
  snapshot). Team changes/deletions never re-attribute existing buckets — an
  already-stamped bucket keeps whatever team id it was recorded under.
  `query_source` (`standard` | `endpoints`) is set by a session
  GUC (`duckgres.query_source`), defaulting to `standard` when unset; the meter
  reads it per connection. (If it's changed mid-connection the per-connection meter
  uses a single value — the final one.) The GUC is **validated at
  SET time** against the closed `{standard, endpoints}` set — case-insensitive,
  normalized to lowercase; empty resets to the default; any other value is
  rejected with SQLSTATE `22023` (`invalid value for "duckgres.query_source":
  must be "standard" or "endpoints"`) on every set path (simple SET, batched,
  extended-protocol Parse). An invalid `-c duckgres.query_source=…` startup
  option rejects the connection (FATAL `22023`), matching the
  `duckgres.worker_*` startup options. As defense in depth,
  `server.ConnectionBilling` clamps a non-canonical value to `standard` at the
  metering boundary, so client input can never put unbounded-cardinality junk
  in the bucket key or the billing export.
- Worker size (`cpu`, `mem_gib`) is part of the key, so different worker sizes
  accumulate — and bill — separately. Units match the values (vCPU / GiB).

**60-second resolution is kept internally** (source of truth), but the API
**aggregates on read**: a `GET` sums all closed buckets in the window into **one
row per key** (below). The fine buckets stay internally; billing gets one
compacted row per key per pull.

A bucket is **closed** (eligible to serve) once `now ≥ bucket_start + 60s + grace`
(grace ≈ 30s). `grace` must exceed the in-process→config-store flush interval so
that, across **every** replica and **every** org, all contributions for that
minute have landed before it is served — a minute that is still being written is
never inside the watermark window (see [No data loss](#no-data-loss)).

## API

Control-plane HTTP, over the existing posthog → mw duckgres ingress.
Auth: `Authorization: Bearer <internal secret>`.

### `GET /api/billing/usage`

Returns compute usage **aggregated since the last ack — one row per key per day**
`(org_id, team_id, query_source, cpu, mem_gib, date)`, summing every closed bucket
in the window. `date` is the **UTC** calendar day, so each row belongs to exactly
one billing day (a window that straddles midnight yields two rows per key — one
per day; a same-day window yields one). Size is reported as `cpu` (vCPU) and
`mem_gib` (GiB) — same units as the values:

```json
{
  "watermark_low":  "2026-07-01T12:30:00Z",
  "watermark_high": "2026-07-01T12:40:00Z",
  "usage": [
    {
      "date": "2026-07-01",
      "query_source": "endpoints" | "standard",
      "org_id": "org_abc",
      "team_id": 12345,
      "cpu": 8,
      "mem_gib": 16,
      "cpu_seconds": 4800,
      "memory_seconds": 9600
    }
  ]
}
```

(`cpu`/`mem_gib` are exact decimals — e.g. `1.5`, `0.5` — for fractional worker
sizes; stored as `NUMERIC` so grouping is exact.)

- Sums every closed bucket in the window `(watermark_low, watermark_high]` into one
  row per key **per UTC day** (so cross-midnight windows split into per-day rows),
  where:
  - `watermark_low` = duckgres's current cursor (= the last value billing acked) —
    the window start.
  - `watermark_high` = the latest closed minute (`now − grace`) — what billing acks.
- **Consistency check:** billing should assert `watermark_low` == its own recorded
  last-acked value before applying. A mismatch means something desynced (a lost/
  half-applied ack; HTTP and Postgres aren't one transaction) → a potential hole →
  alert/reconcile instead of silently under-billing. It's a cross-check, not a
  correctness fix (delete-on-ack already prevents loss); it makes a *bug* visible.
- **Can't fall behind.** Everything since the last ack is aggregated into one row
  per key per day, so the response size is bounded by **active keys × days in the
  window** — independent of how *busy* billing-downtime was. A week of downtime
  returns ~7 rows per key (one per day), not thousands of minute-buckets. One `ack`
  advances past all of it.

### `POST /api/billing/ack`

```json
{ "watermark_high": "2026-07-01T12:40:00Z" }
```

- duckgres advances its cursor to `watermark_high` and **deletes all buckets ≤
  watermark_high**. Idempotent — a value at or below the current cursor is a no-op,
  so a retried ack is always safe. Pass back the exact `watermark_high` from the
  `GET` response (it becomes the next pull's `watermark_low`).

## No data loss

- Buckets are deleted **only** when an ack advances the cursor past them.
- Each `GET` returns the **complete** usage since the last ack. If billing crashes
  after processing but before acking, the next `GET` returns the same window
  (extended up to a later `now`) — safe to reprocess, because billing finalizes
  only on **ack**: the ack is the commit boundary. Delivery is effectively
  at-least-once with `watermark_high` as the idempotency edge.
- Only **closed** buckets are aggregated, so a minute is never served while it is
  still accumulating.
- **Cross-org completeness (no TOCTOU on the delete):** `watermark_high` is always a
  fully-closed minute (`≤ now − grace`), and `grace` exceeds the flush interval — so
  by the time a minute is served (and later deleted on ack), every replica's buckets
  for **every** org in that minute have already landed. A partially-inserted current
  minute is never in the window, so an ack can't delete buckets that were never
  served.

## Team attribution is external

duckgres has no "billing team" / "default team" concept: the external billing
service owns the org → team(s) mapping. The `team_id` in every bucket key and
API row is **informational only** — "the best-effort team at record time"
(compute: the connecting user's team else the org's oldest team; storage: the
org's oldest team; see above). Team changes and deletions **never re-attribute
existing buckets**: unacked buckets keep whatever team id they were stamped
with, and consumers must treat the field as a hint, not authority.

## No infinite accumulation

- Ack deletes everything `≤ watermark_high` immediately.
- **Safety GC:** hard-delete any bucket older than **30 days** regardless of ack,
  logging the dropped count. This bounds table size even if billing stops pulling
  entirely. (A nonzero GC-drop count = billing isn't keeping up — alert on it.)

## Reliability

- Metering and the client **query path are untouched** and never blocked by the
  API: `GET` is a read (aggregate), `ack` and GC are deletes — all on the config
  store, off the query hot path.
- The cursor is a single config-store row. **Any control-plane replica** can serve
  `GET`/`ack`; ack is idempotent, so concurrent pulls / multiple replicas are safe.

## What changes vs the push design

- **Remove:** leader drain → `capture()` to PostHog ingestion, and the
  `DUCKGRES_BILLING_INGEST_URL` / `_TOKEN` config.
- **Keep:** per-connection metering → config-store 60s buckets.
- **Extend:** the bucket table gains `team_id`, `query_source`, `cpu`, `mem_gib`
  (`NUMERIC`) in the key (new migration); add a single `last_acked` cursor row; add
  the HTTP API (aggregate-on-read into one row per key per UTC day + watermark ack)
  + safety GC.
- **Add:** the `duckgres_org_teams` table (migration 000024) as the source of
  the informational bucket `team_id` (connecting user's team else the org's
  oldest team — no billing ownership; the `is_billing_team` marker was dropped
  in migration 000029, and team attribution is owned by the external billing
  service); a `duckgres.query_source` session GUC (`standard` | `endpoints`,
  default `standard`, validated at SET time — anything else is a `22023`
  error) read by the meter; a bearer secret for the API.

## Storage metric

Third raw metric: **`managed_warehouse_storage_gib_seconds`** — bytes stored ×
seconds, the integral of the warehouse's tracked S3 footprint. Billing divides
externally (÷3600 = GiB-hours, ÷2,592,000 = GiB-months), exactly as it already
sums `cpu_seconds` / `memory_seconds`.

**Source of truth:** the org's DuckLake metadata Postgres — DuckLake records
`file_size_bytes` for every Parquet file it writes, so the tracked footprint is
one SQL query, zero S3 calls:

```sql
SELECT COALESCE((SELECT SUM(file_size_bytes) FROM ducklake_data_file), 0)
     + COALESCE((SELECT SUM(file_size_bytes) FROM ducklake_delete_file), 0),
       (SELECT COUNT(*) FROM ducklake_files_scheduled_for_deletion);
```

(No snapshot filter — time-travel-retained files stay billable until snapshot
expiry, which is correct. `ducklake_table_info()` / `ducklake_table_stats` are
NOT used: the former filters to the current snapshot, the latter is
approximate.)

**Sampling contract (level → flow):** a leader-only sampler visits every org
with a Ready DuckLake warehouse every **30 minutes** (env-only override
`DUCKGRES_STORAGE_SAMPLE_INTERVAL`, used by e2e). Each successful sample
credits exactly `tracked_bytes × interval_seconds` byte-seconds into the
sample-minute's bucket, keyed `(org_id, team_id, bucket_start)` — no
query_source or worker size (compute dimensions); `team_id` is the org's
**oldest team** (informational, same external-attribution contract as
compute). No elapsed-time tracking: a
missed sample (org unreachable, leader failover) under-bills one interval and
is deliberately best-effort, like compute. Values accumulate as NUMERIC
byte-seconds; the API serves exact-decimal GiB-seconds (÷2³⁰ is a finite
decimal).

**API shape:** the same `GET /usage` response gains a `storage` array over the
same watermark window; the same `ack` deletes both metric families ≤
`watermark_high` atomically, and the 30-day safety GC covers both:

```json
{
  "watermark_low":  "…",
  "watermark_high": "…",
  "usage":   [ { …compute rows as above… } ],
  "storage": [
    {
      "date": "2026-07-08",
      "org_id": "org_abc",
      "team_id": 12345,
      "gib_seconds": 18000000.5
    }
  ]
}
```

**Known drift vs true bucket bytes** (tracked footprint ≠ `aws s3 ls`):
orphans from crashed writes, incomplete multipart uploads (need a bucket
lifecycle `AbortIncompleteMultipartUpload` rule — composition change), and
files between `expire_snapshots` and `cleanup_old_files` whose size row is
already gone. The sampler exports
`duckgres_org_storage_pending_delete_files` as the drift gauge (alert on
sustained nonzero) plus `duckgres_org_storage_tracked_bytes` for
observability; a periodic S3-Inventory reconcile is an ops runbook item, not
part of the meter.

## Defaults / house-keeping

- Bucket width 60s, grace 30s.
- Endpoint paths above are indicative — adjust to the control-plane API's house
  style.
- Values exposed as vCPU-seconds / GiB-seconds; worker size as vCPU / GiB. All
  stored as exact decimals (`NUMERIC`), so fractional worker sizes keep full
  precision with no float-equality issues.
- Single billing consumer assumed → one server-side `last_acked` cursor shared
  by both metric families. (If a second independent consumer is ever needed,
  give each its own named cursor.)
- Storage sampling every 30 minutes; each sample credits exactly one interval.
