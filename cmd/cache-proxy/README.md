# cache-proxy

`cache-proxy` is a forward HTTP proxy used by DuckDB `httpfs` traffic. It caches
cacheable `GET` responses on local disk, serves matching peer cache entries when
available, and forwards cache misses to origin object storage.

## Runtime Defaults

| Setting | Default | Notes |
| --- | --- | --- |
| `CACHE_DIR` | `/cache` | Local disk cache directory. |
| `CACHE_MAX_PERCENT` | `80` | Convergent target for the cache's share of the cache filesystem, clamped to what is actually free (minus a 5%-of-total reserve). Recomputed every minute: when something outside the cache consumes disk, the budget only ever shrinks, so the cache never evicts healthy entries to make room for writes the disk can't take. |
| `CACHE_MAX_ENTRIES` | `1000000` | Convergent LRU entry-count target, enforced alongside the disk-byte target. Bounds steady-state local cache-index memory. |
| `LISTEN_ADDR` | `:8080` | Forward proxy listener. |
| `PEER_ADDR` | `:8081` | Peer cache API listener. |
| `CACHE_PEER_LOOKUP_MODE` | `probe` | `probe` preserves the existing fleet-wide `/cache/has` behavior. `summary` pulls Bloom-filter hints and uses them to eliminate definite-negative peers before bounded `/cache/has` confirmation; any other value causes startup to fail. |
| `CACHE_SUMMARY_MEMORY_LIMIT_BYTES` | `536870912` (512 MiB) | Total local Bloom-state budget: counting filter, snapshot and pull-work reserve, and a deterministic receiver-selected subset of remote peer summaries. Peers that do not fit remain uncovered. |
| `CACHE_PEER_MAX_PROBES` | `5` | In summary mode, maximum parallel `/cache/has` confirmations per client request across all missing blocks. |
| `CACHE_MAX_PEER_PROBES_IN_FLIGHT` | `64` | Per-pod non-blocking cap on concurrent summary-mode `/cache/has` confirmations. When exhausted, confirmations are skipped and the request fetches origin. |
| `CACHE_PROXY_ID` | pod name, node name, then hostname | Stable opaque proxy identity carried in summary metadata; it must not be a customer or object identifier. |
| `HEALTH_ADDR` | `:8082` | Health and Prometheus metrics listener. |
| `CACHE_HOST_SUFFIXES` | empty | Empty means all `GET` hosts are cacheable. Otherwise, cache only hosts containing one of the comma-separated suffixes. |
| `CACHE_BLOCK_MODE` | `off` | `on` enables block-aligned caching; any other value (including unset) keeps the legacy exact-range path. See [Block-aligned mode](#block-aligned-mode). |
| `CACHE_BLOCK_SIZE_BYTES` | `8388608` (8 MiB) | Fixed block size for block-aligned mode. Ignored when block mode is off. |
| `CACHE_BLOCK_MAX_SPAN_BLOCKS` | `8` | Max blocks coalesced into one origin range fetch. Ignored when block mode is off. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `DUCKGRES_TRACE_ENDPOINT` | empty | OTLP/HTTP trace endpoint. Unset → tracing is a no-op. |
| `OTEL_EXPORTER_OTLP_TRACES_PATH` | empty | Overrides the OTLP path (e.g. VictoriaTraces' `/insert/opentelemetry/v1/traces`). Mirrors the main duckgres binary. |

The byte and entry-count limits are convergent rather than strict admission
limits. Cache fills write concurrently and make room using the cache state
visible before they commit, so several fills completing together may leave the
cache temporarily above either target. Entry-count overshoot is bounded by the
number of concurrently committing distinct entries; byte overshoot is bounded
by the aggregate size of those concurrent commits. A subsequent cache insertion
evicts least-recently-used entries back toward both targets. This keeps body and
filesystem I/O concurrent instead of serializing it behind the cache-index lock.

## Cluster-wide fetch dedup

### Pulled-summary lookup mode

Set `CACHE_PEER_LOOKUP_MODE=summary` only after deploying an image containing
the summary endpoint to every cache-proxy pod. Each proxy maintains a bounded,
versioned counting Bloom filter incrementally as cache entries are committed
and evicted. About every 20 seconds, with per-process jitter, it creates an
immutable snapshot with a 45-second TTL and pulls snapshots from the peers it
has selected to track. Snapshotting copies the bits; it does not scan or rehash
the cache index. `GET /cache/summary` supports `ETag`/`If-None-Match`, has a
strict 2 MiB body limit, and never includes raw URLs, ranges, object paths, or
cache locators.

The filter is sized for 1,000,000 opaque SHA-256 cache locators at a 1% target
false-positive rate: approximately 1.14 MiB of published bits plus 18.3 MiB of
local 16-bit counters. The 512 MiB default aggregate budget also reserves room
for the local filter, overlapping immutable snapshot generations, a temporary
raw snapshot, and four concurrent bounded pulls. The remaining approximately
474.8 MiB fits 415 fixed-size peer filters. At smaller fleet sizes, approximate
per-pod Bloom-state usage is 47.5 MiB for 10 nodes, 70.3 MiB for 30 nodes, and
150.3 MiB for 100 nodes. These
figures exclude the exact local cache index and general Go/HTTP runtime memory.

The default local cache entry limit keeps this filter at its 1,000,000-key,
1% design point. If an operator raises that limit, the filter continues
accepting entries and its false-positive rate rises smoothly; monitor
`cache_proxy_summary_bloom_false_positive_ratio` and
`cache_proxy_summary_bloom_saturated`. Counter overflow is deliberately
sticky, which can add false positives but cannot create false negatives.

Each receiver ranks discovered peers deterministically from its own stable
identity and peer address, then pulls only the subset that fits its remaining
summary budget. This admission happens before network transfer. Unselected,
expired, incompatible, and not-yet-pulled peers are explicitly uncovered.
Four workers pull selected peers with a two-second whole-response timeout,
short connect and response-header deadlines, a 15-second whole-cycle deadline,
fair rotation across deadline-limited cycles, and cancellation on shutdown.
Peer ETags are retained only in a small bounded form, and the summary endpoint
has its own two-second write deadline without constraining streamed peer cache
bodies. Failed pulls retain the last valid, unexpired snapshot; there is no
retry queue. The next membership-triggered or periodic pull is the retry.

On a local miss the requester tests current, compatible, non-expired summaries
locally. A Bloom negative eliminates that covered peer. Bloom positives are
only candidates: they are never trusted as proof that the peer has the entry.
The requester deterministically selects at most `CACHE_PEER_MAX_PROBES`
candidates from the positives and uncovered peers. With at least two slots it
reserves one for an uncovered peer when both classes exist; a one-slot override
ranks both classes together. It then sends parallel `/cache/has`
confirmations. The first useful `200` stored or `202` in-flight claim wins.
Losing confirmations are canceled, and only the confirmed holder receives the
subsequent `/cache/get`. The per-pod in-flight limit is non-blocking: when all
64 default permits are occupied, excess work skips peer confirmation and goes
to origin rather than queuing.

When every discovered peer has a valid summary and none is positive, the
request goes directly to origin with no peer RPC. This also means summary mode
does not discover a just-started in-flight fill whose key is absent from the
last snapshot; simultaneous first access on multiple pods can duplicate that
origin fetch. Entries committed after a peer's last snapshot may similarly be
missed until the next pull. These are safe locality losses, not correctness
failures. False positives cost only a bounded `/cache/has`, and evictions
advertised by an older snapshot cannot cause an unconfirmed body GET.

DNS membership is refreshed every 10 seconds. A newly selected peer triggers
an immediate pull and remains uncovered until a valid snapshot arrives; a new
peer outside the receiver's memory-selected subset remains uncovered. When a
peer disappears, its snapshot is removed on the next successful membership
refresh and it is no longer selected for lookup. Until then, transport failure
falls back safely. There is no cache-body replication. The peer transport is
the existing internal HTTP boundary and does not itself provide authentication;
membership and selection checks bound retained hints but are not authorization.

Roll out in non-production first. During a rolling enablement, peers without a
compatible GET summary endpoint remain uncovered; their bounded confirmation
or origin fallback is safe. Validate that physical probes per logical lookup
remain below the configured cap, confirmed peer GETs stay useful, Bloom
false-positive rate remains within the rollout budget, origin latency and bytes
do not regress excessively, and summary pull traffic and resident memory remain
bounded. Recover by restoring `CACHE_PEER_LOOKUP_MODE=probe`; do not delete
cache contents. See
[the pulled-summary design and runbook](../../docs/design/cache-proxy-pulled-summary-lookup.md)
for sizing, failure behavior, and detailed rollout checks.

In `probe` mode, when several nodes want the same key at the same moment, the
fleet-wide lookup lets later requesters observe the first node's in-flight
fill. Summary mode preserves the `202` mechanism among its bounded confirmation
candidates, but a fully covered Bloom-negative lookup goes directly to origin
as described above. The peer API exposes cached and in-flight state:

- `GET /cache/has?key=…` — `200` the entry is cached (counts as an access for
  LRU recency); `202` the entry isn't cached yet but a local fill is
  mid-flight; `404` neither.
- `GET /cache/get?key=…[&flight=1]` — streams the entry. With `flight=1` and
  the entry not yet on disk, the peer blocks (bounded by `peerFillWait`,
  10 s) for its in-flight fill to land and then serves those bytes, instead
  of 404ing the requester back to the origin for the same bytes it is already
  fetching.

A probe-mode missing key therefore resolves as: local index → peer that has
it (`200`, first answer wins) → peer mid-flight on it (`202`, wait for that
fill) → origin. Summary mode inserts local Bloom elimination before a bounded
version of that confirmation step. Transfers from a peer have no whole-request
timeout (a multi-MB body moving over a loaded link must not be killed for being
large) — only a response-header deadline sized to cover the bounded flight wait.

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
