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
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `DUCKGRES_TRACE_ENDPOINT` | empty | OTLP/HTTP trace endpoint. Unset → tracing is a no-op. |
| `OTEL_EXPORTER_OTLP_TRACES_PATH` | empty | Overrides the OTLP path (e.g. VictoriaTraces' `/insert/opentelemetry/v1/traces`). Mirrors the main duckgres binary. |

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
