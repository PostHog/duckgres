# Managed-warehouse compute billing — pull API

Status: **DRAFT**. Supersedes the reporting hop of
[`billing-compute-seconds-plan.md`](./billing-compute-seconds-plan.md): billing
now **pulls** usage from duckgres instead of duckgres pushing capture events to
PostHog ingestion. The per-connection metering is unchanged — only how the data
leaves duckgres changes.

## Overview

duckgres meters compute usage into 60-second **buckets** in its config store. The
billing service periodically **pulls** the usage accumulated since its last ack,
processes it, and **acks a watermark**; duckgres advances a cursor and deletes
everything up to it. A safety GC caps how long anything can linger. Direction is
posthog → mw (billing initiates), which the existing duckgres control-plane
ingress already supports.

```
duckgres:  connection ends → meter → accumulate into 60s config-store buckets
billing:   GET /usage  → process → POST /ack {watermark}
                                     → duckgres advances cursor + deletes ≤ watermark
safety:    GC hard-deletes buckets older than 30 days (logged)
```

## Buckets

Internally, one row per unique key, values accumulated:

- **Key:** `(org_id, team_id, cpu, mem_gib, bucket_start)` where `cpu` is vCPU and
  `mem_gib` is GiB, `bucket_start = floor(connection_end / 60s)`. Worker size is
  stored as **exact decimals** (SQL `NUMERIC`, not IEEE floats), so equal sizes
  always group into the same bucket and fractional sizes keep full precision.
- **Values:** `cpu_seconds` (vCPU-seconds), `memory_seconds` (GiB-seconds) —
  summed over every connection that falls in that key + minute.
- `team_id` is looked up from the config store (**org → team mapping**) at
  connection time.
- Worker size (`cpu`, `mem_gib`) is part of the key, so different worker sizes
  accumulate — and bill — separately. Units match the values (vCPU / GiB).

**60-second resolution is kept internally.** The pull API aggregates on read (see
below), so billing never has to chase individual minutes — but the fine buckets
still exist as the source of truth.

A bucket is **closed** (eligible to serve) once `now ≥ bucket_start + 60s + grace`
(grace ≈ 30s). This guarantees every in-flight contribution for that minute has
landed before billing can read it.

## API

Control-plane HTTP, over the existing posthog → mw duckgres ingress.
Auth: `Authorization: Bearer <internal secret>`.

### `GET /api/billing/usage`

Returns compute usage **aggregated since the last ack**, one row per worker size
per `(org_id, team_id)`. Size is reported as `cpu` (vCPU) and `mem_gib` (GiB) —
same units as the values:

```json
{
  "watermark": "2026-07-01T12:40:00Z",
  "usage": [
    {
    compute_type: "endpoints",
      "org_id": "org_abc",
      "team_id": "12345",
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

- Sums all **closed** buckets in `(last_acked, watermark]`, where
  `watermark` = the latest closed minute (`now − grace`).
- **One row per key regardless of elapsed time.** A pull returns everything since
  the last ack aggregated, so the response size is bounded by the number of active
  keys — **not** by how long billing was away. A week of downtime returns the same
  row count, just larger sums. Billing can never fall behind chasing 60s buckets.

### `POST /api/billing/ack`

```json
{ "watermark": "2026-07-01T12:40:00Z" }
```

- duckgres advances its cursor to `watermark` and **deletes all buckets ≤
  watermark**. Idempotent — a watermark at or below the current cursor is a no-op,
  so a retried ack is always safe. Pass back the exact `watermark` from the `GET`
  response.

## No data loss

- Buckets are deleted **only** when an ack advances the cursor past them.
- Each `GET` returns the **complete** usage since the last ack. If billing crashes
  after processing but before acking, the next `GET` returns the same window
  (extended up to a later `now`) — safe to reprocess, because billing finalizes
  only on **ack**: the ack is the commit boundary. Delivery is effectively
  at-least-once with the watermark as the idempotency edge.
- Only **closed** buckets are aggregated, so a minute is never served while it is
  still accumulating.

## No infinite accumulation

- Ack deletes everything `≤ watermark` immediately.
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
- **Extend:** the bucket table gains `team_id`, `cpu`, `mem_gib` (`NUMERIC`) in the
  key (new migration); add a single `last_acked` cursor row; add the HTTP API
  (aggregate-on-read + watermark ack) + safety GC.
- **Add:** config-store `org → team` lookup (thread `team_id` onto the connection);
  a bearer secret for the API.

## Defaults / house-keeping

- Bucket width 60s, grace 30s.
- Endpoint paths above are indicative — adjust to the control-plane API's house
  style.
- Values exposed as vCPU-seconds / GiB-seconds; worker size as vCPU / GiB. All
  stored as exact decimals (`NUMERIC`), so fractional worker sizes keep full
  precision with no float-equality issues.
- Single billing consumer assumed → one server-side `last_acked` cursor. (If a
  second independent consumer is ever needed, give each its own named cursor.)
