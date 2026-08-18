# cache-proxy

`cache-proxy` is a forward HTTP proxy used by DuckDB `httpfs` traffic. It caches
cacheable `GET` responses on local disk, serves matching peer cache entries when
available, and forwards cache misses to origin object storage.

## Runtime Defaults

| Setting | Default | Notes |
| --- | --- | --- |
| `CACHE_DIR` | `/cache` | Local disk cache directory. |
| `CACHE_MAX_PERCENT` | `80` | Ceiling for the cache's share of the cache filesystem, clamped to what is actually free (minus a 5%-of-total reserve). Recomputed every minute: when something outside the cache consumes disk, the budget only ever shrinks, so the cache never evicts healthy entries to make room for writes the disk can't take. |
| `LISTEN_ADDR` | `:8080` | Forward proxy listener. |
| `PEER_ADDR` | `:8081` | Peer cache API listener. |
| `CACHE_PEER_LOOKUP_MODE` | `probe` | `probe` preserves the existing `/cache/has` fanout. `summary` uses periodically pushed Bloom-filter hints and performs at most two direct peer GETs per request; any other value causes startup to fail. |
| `CACHE_PROXY_ID` | pod name, node name, then hostname | Stable opaque proxy identity carried in summary metadata; it must not be a customer or object identifier. |
| `HEALTH_ADDR` | `:8082` | Health and Prometheus metrics listener. |
| `CACHE_HOST_SUFFIXES` | empty | Empty means all `GET` hosts are cacheable. Otherwise, cache only hosts containing one of the comma-separated suffixes. |
| `CACHE_BLOCK_MODE` | `off` | `on` enables block-aligned caching; any other value (including unset) keeps the legacy exact-range path. See [Block-aligned mode](#block-aligned-mode). |
| `CACHE_BLOCK_SIZE_BYTES` | `8388608` (8 MiB) | Fixed block size for block-aligned mode. Ignored when block mode is off. |
| `CACHE_BLOCK_MAX_SPAN_BLOCKS` | `8` | Max blocks coalesced into one origin range fetch. Ignored when block mode is off. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `DUCKGRES_TRACE_ENDPOINT` | empty | OTLP/HTTP trace endpoint. Unset → tracing is a no-op. |
| `OTEL_EXPORTER_OTLP_TRACES_PATH` | empty | Overrides the OTLP path (e.g. VictoriaTraces' `/insert/opentelemetry/v1/traces`). Mirrors the main duckgres binary. |

## Cluster-wide fetch dedup

### Pushed-summary lookup mode

Set `CACHE_PEER_LOOKUP_MODE=summary` only after deploying an image containing
the summary endpoint to every cache-proxy pod. Each proxy snapshots at most
1,000,000 opaque SHA-256 cache locators, builds a versioned Bloom filter at a
1% target false-positive rate, and publishes it with a 45-second TTL about
every 20 seconds (with jitter). The uncompressed summary body is capped at
2 MiB and resident peer summaries at 512 MiB; an oversized snapshot is skipped rather than truncated, so published
filters never have false negatives for their source snapshot. At 1%, filters
are about 1.20 bytes per entry: roughly 117 KiB for 100k entries and 1.14 MiB
for 1m entries, plus small metadata and wire encoding overhead.

On a local miss the requester tests received, non-expired filters locally. No
positive hint goes straight to origin once every discovered peer has supplied a
valid summary. During startup or after a peer joins, only peers that have not
yet supplied a valid summary retain the legacy probe fallback; this preserves
cold-start peer-hit behavior while summaries converge. Positive hints are ranked by the opaque
locator and stable proxy identity, then at most two peers receive a direct
`/cache/get`; 404s, timeouts, stale hints, incompatible peers, and missing
summaries all safely fall back to origin. There is no cache-body replication.
The peer transport is the existing internal HTTP boundary and does not itself
provide authentication; membership validation only bounds retained hints.

Roll out in non-production first. During a rolling enablement, missing
summaries reduce peer hits but remain safe. Validate that peer probes per
logical lookup approach zero after peer-summary coverage converges, direct peer GET attempts stay at or below two,
peer bytes remain useful, origin latency/bytes do not regress excessively,
and summary memory/publication traffic remain bounded. Recover by restoring
`CACHE_PEER_LOOKUP_MODE=probe`; do not delete cache contents.

When several nodes want the same key at the same moment, only the first to
start the fill should ever hit the origin. The peer API makes each node's
in-flight fetches visible to the rest:

- `GET /cache/has?key=…` — `200` the entry is cached (counts as an access for
  LRU recency); `202` the entry isn't cached yet but a local fill is
  mid-flight; `404` neither.
- `GET /cache/get?key=…[&flight=1]` — streams the entry. With `flight=1` and
  the entry not yet on disk, the peer blocks (bounded by `peerFillWait`,
  10 s) for its in-flight fill to land and then serves those bytes, instead
  of 404ing the requester back to the origin for the same bytes it is already
  fetching.

A missing key therefore resolves as: local index → peer that has it (`200`,
first answer wins) → peer mid-flight on it (`202`, wait for that fill) →
origin. Transfers from a peer have no whole-request timeout (a multi-MB body
moving over a loaded link must not be killed for being large) — only a
response-header deadline sized to cover the bounded flight wait.

All lookup state comes from the in-memory index under the cache mutex, not
from filesystem stats, so `/cache/has` and eviction/size accounting can never
disagree about whether an entry exists.

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

## Tracing

When a trace endpoint is set the proxy exports OpenTelemetry spans under
`service.name=duckgres-cache-proxy`. A cacheable request emits `cache.get`, with
`cache.origin_fetch`, `cache.peer_lookup`, and `cache.peer_get` children as
needed; block-aligned origin span fetches emit `cache.origin_span_fetch` (or
`cache.origin_span_refetch` for the presence re-fetch backstop). Peer
transfers emit `cache.peer_serve` on the selected remote proxy.
`CONNECT` tunnels emit `cache.connect` and non-cached methods emit
`cache.forward`.

The proxy extracts W3C context at its forward-proxy ingress. Requests from a
Duckgres worker therefore join the existing query trace; requests without a
`traceparent` retain the prior standalone-root behavior. Peer lookup and
transfer requests also propagate W3C context. A lookup is one
`cache.peer_lookup` span with bounded `cache.peer_probe` events carrying each
observed peer outcome (`present`, `in_flight`, `negative`, `timeout`,
`transport_error`, or `canceled`) and duration; the proxy intentionally does not create a
span for every probe. `org_id` is intentionally absent — the proxy has no
per-request tenant identity.

> The cache proxy is not deployed in the `tests/e2e-mw-dev` environment
> (`DUCKGRES_CACHE_ENABLED` is off there). Unit tests in
> `cmd/cache-proxy/tracing_test.go` cover propagation behavior; validate the
> complete trace in a cache-enabled dev deployment.

### Looking up a query trace

Run the query to validate, then obtain its terminal `trace_id` from
`ducklake.system.query_log`. In Grafana, open **Explore**, select the
**VictoriaTraces** datasource, choose **TraceID**, and paste that value.

A cache-enabled query should include both `duckgres` and
`duckgres-cache-proxy`, with Flight `DoGet` and its related cache spans. A peer
hit also includes `cache.peer_lookup` plus `cache.peer_get` and
`cache.peer_serve`. A comparison using `s3_cache=passthrough` instead shows
`cache.forward` and origin work, without cache-hit or peer spans.

Do not use Grafana Traces Drilldown for this lookup: the VictoriaTraces
datasource is configured through the Jaeger plugin, so its `Datasource was not
found` message does not indicate missing trace data. Treat trace IDs as
diagnostic identifiers; do not add them to Prometheus labels or shared
artifacts.

Origin `GET` misses are retried up to 4 total attempts for transient failures:
HTTP `408`, `429`, `500`, `502`, `503`, `504`, request timeouts, and common
transport resets. Retries start with a 100 ms backoff and cap at 1 second.
This applies both to the legacy exact-range path and to block-aligned span
fetches (including the presence re-fetch backstop), so a brief origin blip is
absorbed by the proxy instead of surfacing to DuckDB as a `502`.

Terminal origin responses such as `400`, `403`, `404`, and `416` are not retried
and are forwarded back to DuckDB verbatim — as is the final status once the
retry budget is exhausted. Failed origin responses are never stored in the
cache.
