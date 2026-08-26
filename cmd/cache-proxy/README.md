# cache-proxy

`cache-proxy` is a forward HTTP proxy used by DuckDB `httpfs` traffic. It caches
cacheable `GET` responses on local disk, serves matching peer cache entries when
available, and forwards cache misses to origin object storage.

## Runtime Defaults

| Setting | Default | Notes |
| --- | --- | --- |
| `CACHE_DIR` | `/cache` | Local disk cache directory. |
| `CACHE_MAX_PERCENT` | `80` | Target for committed cache bytes. The proxy retains a 5%-of-total-disk reserve and accounts for its own committed files as reclaimable, so a restart of an 80%-full cache does not mistake the cache itself for external disk use. Capacity is refreshed every minute; reductions apply immediately and recovery requires two consecutive healthy samples. |
| `CACHE_MAX_ENTRIES` | `1000000` | Soft admission and convergence target for tracked LRU entries. Startup loads inspectable committed entries up to the independent 10,000,000-entry hard safety guardrail; values above that guardrail are clamped. |
| `LISTEN_ADDR` | `:8080` | Forward proxy listener. |
| `PEER_ADDR` | `:8081` | Peer cache API listener. |
| `CACHE_PEER_LOOKUP_MODE` | `probe` | `probe` preserves the existing fleet-wide `/cache/has` behavior. `summary` pulls Bloom-filter hints and uses them to eliminate definite-negative peers before bounded `/cache/has` confirmation; any other value causes startup to fail. |
| `CACHE_SUMMARY_MEMORY_LIMIT_BYTES` | unset | Optional emergency ceiling for total Bloom state. The effective default is `min(1 GiB, 20% of GOMEMLIMIT)`; an explicit value can lower, but never raise, that derived ceiling. It includes the local counting filter, snapshot/pull reserves, and retained remote summaries. |
| `CACHE_SUMMARY_PUBLISH_FORMAT` | `fixed` | `fixed` publishes the legacy v2 1M-entry layout during receiver rollout. `dynamic` atomically selects a disk-derived local counting layout and publishes self-describing v3 summaries. Unknown values fail startup. Change only after every receiver is dual-format compatible. |
| `CACHE_PEER_MAX_PROBES_PER_REQUEST` | `5` | In summary mode, maximum parallel `/cache/has` confirmations per client request across all missing blocks. `CACHE_PEER_MAX_PROBES` is a deprecated alias. |
| `CACHE_MAX_CONCURRENT_PEER_PROBES` | `64` | Per-pod non-blocking cap on active summary-mode `/cache/has` HTTP requests and sockets. It is not a process goroutine limit. When exhausted, confirmations are skipped and the request fetches origin. `CACHE_MAX_PEER_PROBES_IN_FLIGHT` is a deprecated alias. |
| `CACHE_PROXY_ID` | pod name, node name, then hostname | Stable opaque receiver identity used for deterministic peer-summary selection; it must not be a customer or object identifier. |
| `HEALTH_ADDR` | `:8082` | Health and Prometheus metrics listener. |
| `CACHE_HOST_SUFFIXES` | empty | Empty means all `GET` hosts are cacheable. Otherwise, cache only hosts containing one of the comma-separated suffixes. |
| `CACHE_BLOCK_MODE` | `off` | `on` enables block-aligned caching; any other value (including unset) keeps the legacy exact-range path. See [Block-aligned mode](#block-aligned-mode). |
| `CACHE_BLOCK_SIZE_BYTES` | `8388608` (8 MiB) | Fixed block size for block-aligned mode. When block mode is off, it remains the planning estimate used to derive dynamic Bloom capacity. |
| `CACHE_BLOCK_MAX_SPAN_BLOCKS` | `8` | Max blocks coalesced into one origin range fetch. Ignored when block mode is off. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `DUCKGRES_TRACE_ENDPOINT` | empty | OTLP/HTTP trace endpoint. Unset → tracing is a no-op. |
| `OTEL_EXPORTER_OTLP_TRACES_PATH` | empty | Overrides the OTLP path (e.g. VictoriaTraces' `/insert/opentelemetry/v1/traces`). Mirrors the main duckgres binary. |

Cache bodies stream concurrently into temporary files. Only the short final
rename plus exact-index, LRU, byte-count, and counting-Bloom update is serialized.
At or above the soft entry target, a new-key commit performs one LRU swap and
therefore cannot increase the tracked entry count. Byte capacity remains a
strict request-path bound: a commit removes enough older entries to preserve
the filesystem reserve. The existing compatibility exception retains a single
object that is itself larger than the byte ceiling after draining other
entries. A single background worker converges any restart or
capacity overage at no more than 1,000 successful deletions per second.
Replacements do not add entries, though a larger replacement can evict older
entries to stay within the byte ceiling.
Concurrent temporary files consume real disk but are not yet tracked cache
entries.

## Capacity startup, recovery, and failures

The cache capacity calculation is:

```text
diskTarget = CACHE_MAX_PERCENT / 100 * totalDiskBytes
reserve = 5% * totalDiskBytes
reclaimable = max(0, freeBytes + committedCacheBytes - reserve)
cacheCapacity = min(diskTarget, reclaimable)
```

Only successfully committed files whose names are valid cache keys contribute
to `committedCacheBytes`. Interrupted files in `.tmp` are removed during
startup and are never cached or counted as evictions. Invalid or unrelated
root-directory entries are left in place and remain external disk usage.

When another writer consumes local disk, the cache lowers its capacity and a
rate-limited worker evicts least-recently-used committed entries to preserve
the reserve.
When that pressure disappears, it expands only after two consecutive refreshes
confirm recovery; no deleted entries are recreated automatically. If the proxy
cannot enumerate the cache directory completely at startup, it exits rather
than serving from a partial index. An individual valid-looking file whose
metadata cannot be inspected is preserved, excluded from cache ownership, and
treated conservatively as external disk usage. Once running, a failed
pressure-driven deletion leaves the entry indexed and is logged, then is
retried with backoff while the cache remains over its limit. Failed and
already-absent deletions are never reported as evictions.

This compatibility release continues to use configured `CACHE_MAX_ENTRIES`
(default 1,000,000) as the soft target. It is no longer a startup survivor
limit: a restart loads every inspectable entry up to the fixed 10,000,000-entry
hard metadata guardrail, then converges gradually. Only an exceptional cache
above the hard guardrail is pruned during startup. The scanner enumerates in
1,024-entry chunks, selects the newest hard-limit set with bounded memory, and
spools non-survivors under `.tmp`; it does not remove a committed file until a
complete successful enumeration. Hard-guardrail removals are sequential,
cancellation-aware, and use the same metered eviction path as runtime pressure.
An unexpected hard-prune unlink failure aborts startup; an already-absent race
is benign and is not counted as an eviction. Persisted coarse read recency,
with file write time as the fallback, determines
the survivor order. Equal timestamps use the opaque cache key as a deterministic
tie-breaker.

## Durable restart recency

Every successful local body read and peer access updates the exact in-memory
LRU synchronously. At most once per minute per resident key, the proxy also
queues the opaque 64-hex cache key and coarse timestamp for an asynchronous
mtime update. The bounded queue holds 65,536 waiting keys, one worker performs
filesystem metadata writes, and repeated touches of the same key coalesce. A
full queue or metadata failure affects only restart survivor accuracy; request
handlers never wait for it and cached bodies remain usable. No source URL,
query string, object path, or organization identifier is persisted.

On graceful shutdown the HTTP servers stop accepting work first, then accepted
recency updates drain within the shared 10-second shutdown deadline. Once cache
shutdown begins, new cache mutations are rejected while existing bodies remain
readable, preventing a commit from racing the closed recency writer. A startup
SIGTERM cancels directory enumeration or hard pruning promptly. Deploy this
recency-writing release for a representative access window before enabling a
later rollout that can grow caches beyond the compatibility soft target.

## Cluster-wide fetch dedup

### Pulled-summary lookup mode

Set `CACHE_PEER_LOOKUP_MODE=summary` only after deploying an image containing
the summary endpoint to every cache-proxy pod. Each proxy incrementally tracks
its local entries in a counting Bloom filter and periodically exposes an
immutable snapshot through `GET /cache/summary`. Receivers accept both the
legacy fixed v2 wire representation and the self-describing dynamic v3
representation. Before pulling, each receiver chooses a deterministic peer
prefix charged conservatively at the largest accepted v3 bitset; actual
decoded bitsets are admitted against the remaining memory budget.
Probe-mode pods do not build or serve summaries, so a canary starts with
partial coverage and uses the bounded fallback while snapshots converge.

The publisher remains `fixed` by default. First deploy dual-format receivers
everywhere with that default. After the cache-proxy memory/GOMEMLIMIT rollout
is complete, set `CACHE_SUMMARY_PUBLISH_FORMAT=dynamic`; the same setting
selects the disk-derived local counting layout and v3 publisher together, so a
dynamic index can never be mislabeled as v2. A restart rebuilds the selected
layout from the bounded exact cache index without deleting cache bodies.
Rollback the publisher by restoring `fixed`; receivers remain dual-format.

Bloom filters are used only to eliminate definite negatives. Bloom-positive
and not-yet-covered peers remain candidates and must pass an exact, bounded
`/cache/has` confirmation before the requester sends one `/cache/get` to the
confirmed holder. The per-request and pod-wide settings above bound candidate
selection and active confirmation HTTP work; skipped or failed peer work falls
back to origin.

DNS membership is refreshed every 10 seconds. Newly selected peers receive a
priority pull and remain uncovered until a valid snapshot arrives; departed
peer summaries are removed at the next successful refresh. A fully covered
Bloom-negative miss goes directly to origin, so summaries can trade some
cold-key fleet-wide deduplication for bounded request-time work.

Roll out in non-production, monitor summary coverage and memory, confirmation
amplification, confirmed GET usefulness, and origin latency/bytes, then enable
gradually. Roll back lookup independently by restoring
`CACHE_PEER_LOOKUP_MODE=probe`; cache contents do not need deletion. The
authoritative protocol, sizing model, failure table, rollout procedure, and
runbook are in
[the pulled-summary design](../../docs/design/cache-proxy-pulled-summary-lookup.md).

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
