# Managed-warehouse compute billing — pull API

Status: **DRAFT**. Supersedes the reporting hop of
[`billing-compute-seconds-plan.md`](./billing-compute-seconds-plan.md): billing
now **pulls** buckets from duckgres instead of duckgres pushing capture events to
PostHog ingestion. The per-connection metering is unchanged — only how the data
leaves duckgres changes.

## Overview

duckgres meters compute usage into 60-second **buckets** in its config store. The
billing service periodically **pulls** closed buckets over a small HTTP API,
processes them, and **acks**; duckgres deletes acked buckets. A safety GC caps how
long anything can linger. Direction is posthog → mw (billing initiates), which the
existing duckgres control-plane ingress already supports.

```
duckgres:  connection ends → meter → accumulate into config-store buckets
billing:   GET /buckets  → process → POST /buckets/ack  → duckgres deletes
safety:    GC hard-deletes anything older than 30 days (logged)
```

## Buckets

One row per unique key, values accumulated:

- **Key:** `(org_id, team_id, cpu_millicores, mem_mib, bucket_start)`
  where `bucket_start = floor(connection_end / 60s)`.
- **Values:** `cpu_seconds` (vCPU-seconds), `memory_seconds` (GiB-seconds) —
  summed over every connection that falls in that key + minute.
- `team_id` is looked up from the config store (**org → team mapping**) at
  connection time.
- Worker size (`cpu_millicores`, `mem_mib`) is part of the key, so different
  worker sizes accumulate — and bill — separately.

A bucket is **closed** (eligible to serve) once `now ≥ bucket_start + 60s + grace`
(grace ≈ 30s). This guarantees every in-flight contribution for that minute has
already landed before billing can read it.

## API

Control-plane HTTP, over the existing posthog → mw duckgres ingress.
Auth: `Authorization: Bearer <internal secret>`.

### `GET /api/billing/buckets?limit=N`

Returns up to `N` **closed, not-yet-acked** buckets, oldest first:

```json
[
  {
    "id": "9f2c…",
    "org_id": "org_abc",
    "team_id": "12345",
    "cpu_millicores": 8000,
    "mem_mib": 16384,
    "bucket_start": "2026-07-01T12:34:00Z",
    "cpu_seconds": 80,
    "memory_seconds": 160
  }
]
```

Billing keeps calling until it gets an empty list.

### `POST /api/billing/buckets/ack`

```json
{ "ids": ["9f2c…", "7a10…"] }
```

duckgres deletes those buckets. **Idempotent** — unknown or already-deleted ids
are ignored, so a retried ack is always safe.

## No data loss

- A bucket is deleted **only after an explicit ack**. The flow is
  read → process → ack.
- If billing crashes after processing but before acking, it simply **re-pulls the
  same buckets** on the next cycle. Delivery is **at-least-once**; billing dedups
  by the natural key `(org_id, team_id, cpu_millicores, mem_mib, bucket_start)`
  (each key is emitted once).
- Only **closed** buckets are served, so a bucket is never handed over while it is
  still accumulating.

## No infinite accumulation

- Acked buckets are deleted immediately.
- **Safety GC:** hard-delete any bucket older than **30 days** regardless of ack,
  logging the dropped count. This bounds table size even if billing stops pulling
  entirely. (A nonzero GC-drop count = billing isn't keeping up — alert on it.)

## Reliability

- Metering and the client **query path are untouched** and never blocked by the
  API: serving is reads, ack and GC are deletes — all on the config store, off the
  query hot path.
- **Any control-plane replica** can serve `GET`/`ack` (state is the shared config
  store); ack-delete is idempotent, so concurrent pulls / multiple replicas are
  safe.

## What changes vs the push design

- **Remove:** leader drain → `capture()` to PostHog ingestion, and the
  `DUCKGRES_BILLING_INGEST_URL` / `_TOKEN` config.
- **Keep:** per-connection metering → config-store buckets.
- **Extend:** the bucket table gains `team_id`, `cpu_millicores`, `mem_mib` in the
  key (new migration); add the HTTP API + safety GC.
- **Add:** config-store `org → team` lookup (thread `team_id` onto the connection);
  a bearer secret for the API.

## Defaults / house-keeping

- `limit` default ~1000; `id` is a stable per-bucket identifier used for ack.
- Bucket width 60s, grace 30s (unchanged from the push design).
- Endpoint paths above are indicative — adjust to the control-plane API's house
  style.
- Values are exposed as vCPU-seconds / GiB-seconds; internally accumulated in
  exact millicore-/MiB-seconds so no precision is lost for fractional worker sizes.
