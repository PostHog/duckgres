# cache-proxy

`cache-proxy` is a forward HTTP proxy used by DuckDB `httpfs` traffic. It caches
cacheable `GET` responses on local disk, serves matching peer cache entries when
available, and forwards cache misses to origin object storage.

## Runtime Defaults

| Setting | Default | Notes |
| --- | --- | --- |
| `CACHE_DIR` | `/cache` | Local disk cache directory. |
| `CACHE_MAX_PERCENT` | `80` | Maximum percent of the cache filesystem to use. |
| `LISTEN_ADDR` | `:8080` | Forward proxy listener. |
| `PEER_ADDR` | `:8081` | Peer cache API listener. |
| `HEALTH_ADDR` | `:8082` | Health and Prometheus metrics listener. |
| `CACHE_HOST_SUFFIXES` | empty | Empty means all `GET` hosts are cacheable. Otherwise, cache only hosts containing one of the comma-separated suffixes. |
| `CACHE_BLOCK_MODE` | `off` | `on` enables block-aligned caching; any other value (including unset) keeps the legacy exact-range path. See [Block-aligned mode](#block-aligned-mode). |
| `CACHE_BLOCK_SIZE_BYTES` | `8388608` (8 MiB) | Fixed block size for block-aligned mode. Ignored when block mode is off. |
| `CACHE_BLOCK_MAX_SPAN_BLOCKS` | `8` | Max blocks coalesced into one origin range fetch. Ignored when block mode is off. |
| `CACHE_PEER_FETCH_MAX_CONCURRENCY` | `32` | Process-wide limit for admitted peer lookup/body transfers. Each request is additionally limited to 8 peer fills. Must be positive. |
| `CACHE_PEER_FETCH_MAX_BYTES` | `CACHE_PEER_FETCH_MAX_CONCURRENCY × CACHE_BLOCK_SIZE_BYTES` | Process-wide byte reservations for admitted peer transfers (256 MiB with the source defaults; 32 MiB with 1 MiB blocks at concurrency 32). Must be positive. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `DUCKGRES_TRACE_ENDPOINT` | empty | OTLP/HTTP trace endpoint. Unset → tracing is a no-op. |
| `OTEL_EXPORTER_OTLP_TRACES_PATH` | empty | Overrides the OTLP path (e.g. VictoriaTraces' `/insert/opentelemetry/v1/traces`). Mirrors the main duckgres binary. |

The concurrency default was selected from an isolated 16/32/64 contention
sweep. A cap of 64 removed synthetic origin fallback but did not produce a
stable latency improvement and doubled the worst-case byte reservation; 32
kept the safer resource bound while materially reducing fallback versus 16.

## Block-aligned mode

The legacy cache key is `sha256(url|range)` — an exact match on the client's
`Range` header. DuckDB's Parquet reader rarely issues the same byte range
twice, even across repeat runs of the same query: footer probes, row-group
reads, and column-chunk reads all drift by a few bytes depending on prior
reader state, so a second run of an identical query produces a mostly
disjoint set of ranges from the first. Measured on one workload, 0 of 7,370
range keys from a second run matched any key cached by the first — a 100%
miss rate on a workload that should have been fully warm.

Block-aligned mode fixes this by keying the cache on fixed-size blocks of the
underlying object instead of the client's exact range. The key is
`sha256(url|blk|idx|blockSize)`, where `idx` is the block index (`start /
blockSize`) and `blockSize` is part of the key so a config change can't serve
a wrong-sized entry — old-size entries just become unreachable and age out
normally. A request is served by locating the blocks its range overlaps
(locally, then from peers, then coalescing missing runs into origin range
fetches bounded by `CACHE_BLOCK_MAX_SPAN_BLOCKS` per request) and assembling
the requested byte range from them. Because the key no longer depends on the
exact range, two requests over the same bytes with different start/end always
hit the same cached blocks.

**Trade-off:** the first read of an uncached object always fetches at least
one full block, even if the request only needs a few bytes — a 40 KB Parquet
footer read on an uncached object still fetches one full 8 MiB block. This
first-touch amplification is bounded by `CACHE_BLOCK_SIZE_BYTES` and
amortized away by object immutability — every subsequent read of any byte
range overlapping that block is a pure cache hit, including reads with
different boundaries than the one that populated it.

Only requests with a specific shape are served from block-aligned entries;
everything else keeps behaving exactly as it does with block mode off:

| Request shape | Path |
| --- | --- |
| Absolute `Range: bytes=start-end` on a cacheable bucket `GET` | Block-aligned |
| Suffix range (`bytes=-N`), open-ended (`bytes=N-`), multi-range, or missing `Range` | Legacy exact-range path |
| Non-`GET` (`HEAD`, `PUT`, ...) | Passthrough (never cached), unchanged |
| Non-bucket `GET` (`CACHE_HOST_SUFFIXES` doesn't match) | Passthrough, unchanged |
| `CACHE_BLOCK_SIZE_BYTES` or `CACHE_BLOCK_MAX_SPAN_BLOCKS` misconfigured (≤ 0) | Legacy exact-range path |
| Requested block span cannot fit in the configured cache capacity | Legacy exact-range path |

Origin `206 Partial Content` responses are accepted only when their
`Content-Range` exactly matches the requested block span (allowing the final
span to end at the advertised object size). The validated object size is used
to clamp ranges that cross EOF and return `416` for ranges that start at or
beyond EOF. Before a `206` response is committed, every required cache block is
opened so LRU eviction cannot truncate an in-progress assembled response.

### Peer racing and overload behavior

Peer probes retain their 150 ms timeout, and block fills run in parallel with
at most 8 workers per client request. All requests share the process-wide
count and byte ceilings above. Time spent queued for either permit counts
against the peer's adaptive head start; a fill that is still queued when that
deadline expires is permanently shed to the coalesced origin path instead of
starting late and adding more load.

The origin hedge starts after the rolling p50 of the last 64 successful peer
block fetches, clamped to 25–150 ms (25 ms before any samples exist). Peer and
origin then run concurrently. A peer win cancels an origin span once no other
request still needs that shared span. Origin hedges continue to use contiguous
miss-run coalescing and the shared origin-span flight, so hedging does not split
one miss into per-block origin requests. Each validated origin block commit
immediately cancels the matching peer transfer for every waiter sharing that
origin flight; later blocks remain independently viable if the origin span
subsequently fails.

A process-local circuit breaker compares one-block peer latency with origin's
time through its first validated, atomically committed block. That first-block
sample comes from the existing coalesced span, so it retains request/TTFB cost
without adding or fragmenting origin reads. Eight sustained comparisons with
peer latency above 1.5× origin open the breaker; origin winning by itself is
not evidence, so a peer that is only marginally slower does not get disabled.
When cancellation has already proved that a peer exceeded 1.5×, that lower
bound is included in the peer EWMA and in a separate sustained-evidence streak
so a formerly healthy EWMA cannot hide an abrupt slowdown. If prompt
cancellation leaves the result ambiguous, at most one process-wide diagnostic
every 5 seconds restarts that block fetch with an instantaneous limiter
acquisition and a hard 1.5× deadline. The user response still uses origin
immediately; the bounded sample makes the slowdown measurable without
preserving every losing transfer.

While open, requests start origin immediately and one non-blocking peer
recovery sample is allowed every 5 seconds. It is compared with the current
first-block commit in the normal coalesced origin span. Until that commit
arrives, a 1.5× rolling-origin-EWMA deadline provides the initial bound. The
response never waits for the sample or for the rest of that span; the
diagnostic peer may run only to the active 1.5× boundary before cancellation.
Three samples within 1.5× close the breaker.
These constants are deliberately fixed; the two resource ceilings are the
operational tuning controls.

The legacy exact-range path uses the same process-wide controller when an
absolute range provides a safe byte reservation. Suffix, open-ended, and
missing ranges bypass peers because their transfer size cannot be bounded.

### Peer-path recovery runbook

1. Check `cache_proxy_peer_breaker_state`, hedge winners, queue duration, and
   the in-flight count/byte gauges. A breaker value of `1` is protective: the
   proxy is serving origin traffic and will probe for recovery automatically.
2. If `cache_proxy_peer_fetch_shed_total{reason="deadline"}` rises while both
   in-flight gauges stay at their ceilings, increase the constrained ceiling
   (`CACHE_PEER_FETCH_MAX_CONCURRENCY` or `CACHE_PEER_FETCH_MAX_BYTES`) and
   restart the proxy. Keep the byte ceiling at least one block.
3. If peer traffic itself is destabilizing the node, unset `PEER_SERVICE` and
   restart. This disables peer discovery and sends misses to origin without
   disabling the local cache. Restore it after peer latency and error rate
   recover.
4. To undo tuning, restore concurrency to `32` and remove the explicit byte
   override so it again follows concurrency × `CACHE_BLOCK_SIZE_BYTES`.

## Tracing

When a trace endpoint is set the proxy exports OpenTelemetry spans under
`service.name=duckgres-cache-proxy`. Each cacheable request is its own root span
(`cache.get`, with `cache.origin_fetch` / `cache.peer_fetch` children); `CONNECT`
tunnels emit `cache.connect` and non-cached methods emit `cache.forward`.

These are **standalone traces** — DuckDB `httpfs` sends no `traceparent`, so they
are deliberately **not** stitched into the duckgres query trace. Correlate to a
query by hand on the shared attributes: `client.address` (the worker pod IP, →
org/session via Kubernetes), the S3 object (`server.address` + `url.path` +
`duckgres.s3.range`), span timestamp, and `cache.source` (`hit`/`peer`/`miss`).
`org_id` is intentionally absent — the proxy has no per-request tenant identity.

> The cache proxy is not deployed in the `tests/e2e-mw-dev` environment
> (`DUCKGRES_CACHE_ENABLED` is off there), so this behavior is gated by the unit
> test `cmd/cache-proxy/tracing_test.go`, not an e2e harness assertion.

Origin `GET` misses are retried up to 4 total attempts for transient failures:
HTTP `408`, `429`, `500`, `502`, `503`, `504`, request timeouts, and common
transport resets. Retries start with a 100 ms backoff and cap at 1 second.

Terminal origin responses such as `400`, `403`, `404`, and `416` are not retried
and are forwarded back to DuckDB verbatim. Failed origin responses are never
stored in the cache.
