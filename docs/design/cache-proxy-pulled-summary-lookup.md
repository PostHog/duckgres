# Cache-proxy pulled-summary peer lookup

## Status and scope

This design replaces request-time fleet-wide cache discovery with
receiver-driven Bloom-summary synchronization and bounded exact confirmation.
It applies only when `CACHE_PEER_LOOKUP_MODE=summary`. The default remains
`probe`, which preserves the existing fleet-wide `/cache/has` behavior.

The design does not introduce worker affinity, organization-aware placement,
a central key directory, proactive cache-body replication, or ownership of
origin fills. Summaries contain only opaque cache-locator membership hints.

## Goals

- Bound request-time peer RPCs independently of fleet size.
- Bound each pod's Bloom state and synchronization work.
- Use Bloom filters only for definite-negative elimination; verify every
  candidate through the peer's exact cache index before fetching its body.
- Make missing, stale, malformed, or incompatible summaries reduce locality
  without failing a client request.
- Preserve the current local cache, peer-transfer, and origin byte semantics.

## Configuration and bounds

| Setting | Default | Bound |
| --- | --- | --- |
| `CACHE_PEER_LOOKUP_MODE` | `probe` | `probe` or `summary`; unknown values fail startup. |
| `CACHE_MAX_ENTRIES` | `1000000` | Configured compatibility admission/convergence target. Startup may retain inspectable entries above it, up to the fixed 10,000,000-entry metadata guardrail. |
| `CACHE_MAX_PERCENT` | `80` | Percentage-of-total-disk target for committed cache bytes. The active capacity also accounts for reclaimable cache-owned files while retaining a 5%-of-total-disk reserve. |
| `CACHE_SUMMARY_MEMORY_LIMIT_BYTES` | unset | Optional emergency ceiling. Effective summary memory is `min(1 GiB, 20% of GOMEMLIMIT, explicit override when set)`. It includes the local counting Bloom, snapshot/pull reserve, and retained remote bits. |
| `CACHE_SUMMARY_PUBLISH_FORMAT` | `fixed` | `fixed` builds the legacy 1M-entry counting layout and publishes v2. `dynamic` builds the disk-derived layout and publishes self-describing v3. Unknown values fail startup. |
| `CACHE_PEER_MAX_PROBES_PER_REQUEST` | `5` | Maximum summary-mode `/cache/has` confirmations per client request, shared across block misses. `CACHE_PEER_MAX_PROBES` is a deprecated alias. |
| `CACHE_MAX_CONCURRENT_PEER_PROBES` | `64` | Pod-wide, non-blocking cap on active confirmation HTTP requests and sockets. `CACHE_MAX_PEER_PROBES_IN_FLIGHT` is a deprecated alias. |

Body copies remain concurrent: each fill streams into a temporary file without
holding the cache-index lock. The short final commit is serialized across the
rename, LRU eviction, exact-index and byte accounting, and counting-Bloom
update. At or above the soft entry target, a new-key commit performs one LRU
swap and cannot increase the tracked entry count. Byte capacity remains strict:
a commit removes enough older entries to preserve the reserve, except for the
existing compatibility behavior that retains one object larger than the
ceiling after draining other entries. A rate-limited
background worker handles pre-existing restart and capacity overage. Concurrent
temporary files consume real disk but are not yet tracked cache entries.

### Disk capacity and startup ownership

The cache derives its active byte capacity from each filesystem sample:

```text
diskTarget = CACHE_MAX_PERCENT / 100 * totalDiskBytes
reserve = 5% * totalDiskBytes
reclaimable = max(0, freeBytes + committedCacheBytes - reserve)
cacheCapacity = min(diskTarget, reclaimable)
```

`committedCacheBytes` contains only regular files whose names are valid cache
keys. These files are cache-owned and reclaimable through eviction, so a
restart does not mistake a full cache for external disk pressure. Interrupted
temporary files are removed before the startup scan. Invalid or unrelated
root-directory entries remain on disk and count only through reduced free
space.

Startup scans in 1,024-entry chunks and computes inspectable owned bytes before
capacity decisions. The configured entry target is soft: every inspectable
entry is loaded when the directory remains within the fixed 10,000,000-entry
hard guardrail. Any soft entry or byte overage converges after startup through
one background deletion at a time, capped at 1,000 successful deletions per
second. A valid-looking file whose metadata cannot be inspected is preserved,
excluded from the index and owned-byte total, and therefore remains external
disk usage.

Only a directory above the hard guardrail selects startup survivors. The
scanner retains the newest bounded set by persisted coarse access mtime and
opaque-key tie-break, while spooling known non-survivors under `.tmp`. It does
not delete committed files until enumeration and spool closure both succeed.
The exceptional prune is sequential, observable, cancellation-aware, and uses
the centralized eviction metrics. If temporary cleanup, filesystem sampling,
directory enumeration, or a required hard-prune unlink fails, initialization
fails rather than serving from a partial or policy-violating index. An
already-absent unlink race is benign and is not an eviction.

Capacity is refreshed every minute. A lower ceiling is published immediately
and the convergence worker evicts least-recently-used entries to restore the reserve. An
increase requires two consecutive healthy samples before it is applied, which
prevents short-lived external disk usage from flapping the ceiling. Runtime
deletion failures leave the entry indexed, are not counted as successful
evictions, and are retried with backoff while the cache remains over target.

### Durable access recency

Successful body reads and explicit peer accesses always move the in-memory LRU
synchronously. Once per minute bucket per key, a nonblocking asynchronous path
persists the access time in that committed file's mtime. The work queue holds at
most 65,536 waiting opaque keys, one metadata worker is active, and repeated
touches coalesce. Queue overflow or metadata failure degrades only restart
ordering. File write mtime is the fallback when a key has no persisted read
history; source URLs and request metadata are never written. Accepted work
drains within the process shutdown deadline. Cache shutdown is terminal for
mutations but does not invalidate existing readers, so replacement commits
cannot race a recency writer that has stopped accepting work.

The process-level backstop remains the pod memory limit and Go memory policy.
The settings above bound the largest cache-proxy-owned contributors, but do not
replace a container memory limit or account exactly for Go, HTTP, tracing, and
kernel memory.

## Local counting Bloom filter

Each cache proxy owns one counting Bloom filter. Cache commits add the opaque
SHA-256 cache locator and evictions remove it. Sixteen-bit counters
allow incremental deletion. A counter that reaches `uint16` saturation becomes
sticky: this may add false positives but cannot create a false negative for a
key represented by that filter.

With `CACHE_SUMMARY_PUBLISH_FORMAT=fixed`, the filter remains designed for
1,000,000 entries at a 1% per-peer false-positive rate:

- 9,585,064 snapshot bits, approximately 1.14 MiB;
- approximately 18.3 MiB of local 16-bit counters;
- hash count derived from the target item count and false-positive rate.

`CACHE_MAX_ENTRIES=1000000` keeps the default cache at this design point. If an
operator raises the entry target, additions continue rather than disabling
summaries; the predicted false-positive rate rises smoothly and is exported as
a metric. The filter is a hint and never authorizes a body transfer.

With `CACHE_SUMMARY_PUBLISH_FORMAT=dynamic`, the design entry count is derived
from stable physical disk target capacity and configured block size, capped by
the 10,000,000-entry metadata guardrail. A 9.7M-entry design uses about 11 MiB
of snapshot bits and 180–190 MiB of 16-bit counters. The publish-format setting
chooses both the local counting layout and the wire version; they cannot be
configured independently. Startup derives the layout after the bounded scan
and statfs sample, then rehashes the bounded exact index once so existing cache
keys are represented before any snapshot is served.

About every 20 seconds, with per-process jitter, the proxy copies the current
bitset into an immutable, versioned wire snapshot. It does not rescan or rehash
the cache index. Each snapshot has a 60-second advertised TTL and a strict
16 MiB encoded-body limit. Receivers accept:

- legacy v2: summary/cache-layout versions, timestamps, the exact fixed Bloom
  parameters, and Bloom bits;
- dynamic v3: those fields plus current item count, design item count, and
  dimensions derived from the declared design count.

V3 dimensions, counts, encoded length, decoded length, unknown fields, and
timestamps are validated before the decoded bitset is admitted. V2 remains
strictly fixed and rejects dynamic dimensions, allowing old and new summaries
to coexist without silently reinterpreting a legacy payload.

Raw URLs, signed query strings, ranges, object paths, organization identifiers,
and cache locators are never serialized or logged.

The no-false-negative statement applies to entries present in the exact local
snapshot. An entry committed after that snapshot is absent until a newer
snapshot is pulled. An entry evicted after the snapshot can remain positive
until refresh; exact confirmation prevents that stale positive from causing a
body read.

## Receiver-driven synchronization

Each pod discovers peer addresses from the headless Service every 10 seconds.
It ranks the current membership deterministically using its stable receiver
identity and peer address. Before any summary RPC, it selects only as many
peers as fit the remote-summary portion of
`CACHE_SUMMARY_MEMORY_LIMIT_BYTES`. Different receivers can select different
subsets, distributing degraded coverage when a fleet is larger than the
memory budget. Because a receiver does not know a peer's v2/v3 dimensions
before pulling it, deterministic selection charges every peer at the largest
accepted 10M-entry raw bitset. Receive-side admission then charges the actual
decoded bitset and atomically preserves an older valid record when a
replacement does not fit. This deliberately favors a safe deterministic prefix
over arrival-order-dependent overcommit.

The selected peers are pulled through `GET /cache/summary`:

- the endpoint serves the current immutable snapshot;
- `ETag` and `If-None-Match` support conditional pulls between rebuilds;
- retained ETags are syntax-checked and capped at 128 bytes;
- pull response headers are capped at 16 KiB;
- the summary response has a two-second handler-local write deadline;
- four workers bound pull concurrency;
- a pull has a two-second whole-response timeout, a 200 ms connect deadline,
  and a 500 ms response-header deadline;
- a whole pull cycle is capped at 15 seconds and rotates its starting peer by
  the work submitted, so slow early peers cannot starve the same suffix;
- declared and actual bodies above 16 MiB are rejected;
- cancellation closes requests, bodies, workers, timers, and the coordinator.

The receiver validates current selection before expensive decoding, then
validates the version, cache layout, declared Bloom parameters, counts,
timestamps, and body size before atomically replacing the last record for that
peer. A failed or invalid pull leaves the previous valid record in place until
its advertised expiration. `304 Not Modified` retains
the record but does not extend it beyond that expiration. Failures do not create
an independent retry queue; the next periodic or membership-triggered cycle is
the retry. Newly selected peers that have not yet started their priority pull
remain in the bounded pending-membership set.

### Membership transitions

On startup, a pod builds its local snapshot immediately. The initial DNS
membership selection triggers an immediate pull, so it does not wait for the
periodic timer. Until a selected peer's first valid response arrives, that peer
is uncovered and eligible for bounded exact confirmation.

When a peer appears, each receiver recomputes its deterministic subset:

- if selected, the new peer is pulled immediately;
- if not selected because of the memory ceiling, it remains uncovered;
- if it displaces another selected peer, the displaced record is removed before
  further lookup and the new selected peer begins uncovered.

When a peer disappears, the next successful DNS refresh removes it from both
the selected set and resident summary store. It is not probed after removal.
During the discovery delay, connection failure is treated as an ordinary peer
miss and the request falls back safely.

## Request path

Summary mode performs the following steps for a local cache miss:

1. Test every current, compatible, non-expired retained summary locally.
2. Eliminate covered peers whose Bloom filter is negative.
3. Treat positive and uncovered peers as candidates, not holders.
4. Deterministically choose at most `CACHE_PEER_MAX_PROBES_PER_REQUEST` candidates. With a
   budget of at least two and both classes present, reserve at least one slot
   for an uncovered peer so false positives cannot entirely suppress
   convergence. A one-slot override ranks both classes together.
5. Issue parallel `/cache/has` confirmations under the pod-wide semaphore.
6. Use the first useful `200` stored or `202` in-flight response and cancel
   losing confirmations.
7. Issue `/cache/get` only to the one confirmed holder for that cache-key
   lookup. A failed transfer falls back to origin.
8. If no confirmation succeeds, fetch from origin.

The semaphore is deliberately non-blocking. If all
`CACHE_MAX_CONCURRENT_PEER_PROBES` permits are occupied, excess confirmations
are skipped instead of queued, and those requests use origin. This bounds
active `/cache/has` HTTP requests, sockets, and aggregate peer work during a
miss storm. It does not bound total process goroutines: each client request has
its own handler and may briefly create up to its
`CACHE_PEER_MAX_PROBES_PER_REQUEST` candidate-coordination goroutines.

Block-aligned requests can contain several distinct cache keys. They share one
`CACHE_PEER_MAX_PROBES_PER_REQUEST` budget across their missing blocks, so the request does
not multiply confirmation work by its block count. Each successful block
confirmation can lead to one peer body transfer, capped at two peer block GETs
for the entire client request.

### Bloom false positives and fleet size

For `N` covered peers with independent per-peer false-positive probability
`p`, a true fleet miss produces approximately `Binomial(N, p)` confirmation
requests before applying the cap. At the 1% design point, the average is 0.1,
0.3, and 1.0 confirmations for 10, 30, and 100 covered peers respectively.
At 100 peers, the 99th percentile is approximately four. The cap of five is a
circuit breaker for correlated filters, saturation, malformed all-positive
state that passes validation, and fleets larger than the design range.

False-positive independence is not guaranteed: caches can overlap and use the
same hash layout. Operators must therefore rely on the hard confirmation cap,
not the average-case calculation, for capacity planning.

### Known cold-key limitation

If all discovered peers are covered and every summary is negative, the request
goes directly to origin without a peer RPC. Periodic summaries do not contain
newly started in-flight fills, so simultaneous first access to a cold key can
issue duplicate origin fetches on several pods. Similarly, a peer entry added
after its last snapshot can be missed until the next pull.

Preserving complete fleet-wide in-flight dedup would require negative-peer
sampling, fill ownership, or a separate in-flight protocol. Those mechanisms
either restore request-time work or expand this design's scope, so they are not
included here.

## Known limitations and follow-up work

The initial production target is the current fleet size of approximately
10–20 cache-proxy nodes. The request-time probe and memory bounds remain hard
outside that range, but some synchronization and observability properties need
more work before treating the documented 100-node examples or the 415-peer
memory maximum as a sustained operating target:

- Pull throughput, not only memory, must eventually cap the selected peer set.
  Four workers, two-second per-peer timeouts, and a 15-second cycle budget can
  refresh the current fleet within the 60-second TTL. A much larger set of
  consistently slow peers can require several rotations, allowing records to
  expire before their next refresh. Before scaling materially beyond the
  current fleet, derive the selection cap from worst-case refresh throughput or
  redesign scheduling so a full rotation fits within the TTL.
- The periodically rebuilt body contains new creation and expiration times, so
  its ETag changes even when the Bloom bits do not. Conditional GETs therefore
  help only for repeated pulls between rebuilds; they do not currently remove
  the regular full-snapshot transfer. A future lease/ETag design can separate
  unchanged filter content from freshness renewal.
- `cache_proxy_summary_resident_count` counts retained records and can include
  an expired record until the next successful membership refresh prunes it.
  `cache_proxy_summary_valid_resident_peers` excludes expired records, and
  `cache_proxy_summary_selected_peers` supplies its selected denominator.
  There is still no discovered-peer gauge, and the age histogram is observed
  only for Bloom-positive records. Add discovered-peer coverage before making
  readiness or autoscaling depend on fleet-wide summary convergence.
- Lookup currently scans current membership and tests retained filters under a
  shared read lock, then ranks candidates. This is acceptable at 10–20 nodes
  but request CPU, allocations, and writer-lock contention scale with fleet
  size. Use bounded top-K selection and immutable lookup snapshots before large
  fleets.
- `cache_proxy_peer_hits_total` counts successful peer body transfers, while
  `cache_proxy_peer_fetches_total` counts logical lookups. A block request can
  transfer two peer blocks after one logical lookup, so their ratio is a
  transfers-per-lookup measure and can exceed one; it is not a percentage.

Probe mode does not allocate, build, or serve Bloom summaries. It returns 404
from `/cache/summary`. Enabling summary mode on a canary therefore starts with
partial coverage and uses the bounded uncovered-peer/origin fallback while the
canary and compatible peers build and pull their first snapshots.

## Memory model

The effective summary ceiling is the smaller of 1 GiB, 20% of the runtime
`GOMEMLIMIT`, and the optional explicit emergency ceiling. It is divided
conservatively into:

- the actual local raw bitset plus its 16-bit counting cells;
- the currently served maximum 16 MiB body, the JSON encoder's bounded buffer
  and returned clone, plus one actual local raw snapshot;
- four simultaneous pulls, each reserving the response body, the JSON decoder
  buffer, the bounded encoded-bit-field copy, a maximum decoded 10M-entry
  bitset, and bounded response headers;
- retained remote raw bitsets with everything left over.

The pull reserve uses v3 maxima even while the pod publishes fixed v2, because
dual-format receivers can encounter a dynamic peer during rollout. Peer
selection also charges each selected peer at the maximum raw v3 bitset; actual
resident accounting uses each decoded bitset's length. Consequently:

- a fixed local layout reserves roughly 300–310 MiB before remote summaries;
- a 9.7M-entry dynamic layout reserves roughly 480–490 MiB before remote
  summaries;
- a 1 GiB ceiling retains the current 10–20-node fleet comfortably even at
  the largest supported layout;
- a smaller explicit ceiling is rejected at startup if it cannot hold the
  local/transient reserve plus one maximum peer summary.

These numbers deliberately exclude the exact local cache index. Its key-count
growth is independently bounded toward `CACHE_MAX_ENTRIES`; the disk target
also bounds the number of fixed-size blocks that can be retained. Actual RSS
includes map/list entries, strings, goroutine stacks, HTTP buffers, tracing,
and allocator overhead.

## Failure behavior

| Failure | Result |
| --- | --- |
| Summary endpoint absent during rolling deployment | Peer remains uncovered; bounded confirmation or origin is used. |
| Pull timeout or transport failure | Last valid unexpired record remains; normal cadence retries. |
| Oversized, malformed, expired, or incompatible body | Body is rejected without replacing the last valid record. |
| Record expires | Peer becomes uncovered until a valid pull succeeds. |
| Summary memory ceiling reached | Receiver tracks only its deterministic subset; other peers remain uncovered and request work stays capped. |
| Bloom false positive or stale eviction | Exact `/cache/has` returns a miss; no body GET is attempted. |
| Global confirmation semaphore full | Confirmation is skipped without queuing; request falls back to origin. |
| Peer disappears between discovery and lookup | Confirmation or GET fails and the request falls back to origin. |
| Synchronizer shuts down | Context cancellation stops timers, pulls, workers, and response-body reads. |

No client request fails solely because summary synchronization or a peer is
unavailable. Origin remains the correctness fallback.

## Metrics and rollout

Summary and peer metrics have no peer, node, organization, URL, locator, or
worker identity labels. The primary rollout signals are:

- `cache_proxy_peer_probes_total / cache_proxy_peer_fetches_total`: physical
  confirmation amplification; it should remain below the configured cap and
  near the fleet-aware false-positive expectation when coverage is complete;
- `cache_proxy_summary_confirmed_gets_total`: confirmed peer body transfers;
- peer and origin bytes and latency: useful locality versus fallback cost;
- `cache_proxy_summary_pulls_total` and
  `cache_proxy_summary_serves_total`: synchronization health;
- `cache_proxy_summary_selected_peers`, `cache_proxy_summary_resident_count`,
  `cache_proxy_summary_valid_resident_peers`, and summary age: selected,
  retained, and usable-summary health, subject to the coverage limitations
  above;
- `cache_proxy_summary_resident_bytes` versus
  `cache_proxy_summary_memory_limit_bytes`: conservative Bloom-state accounting
  versus its derived/effective ceiling. Resident bytes include the current
  local filter, maximum transient reserve, and retained remote bits; they are
  not process RSS;
- predicted Bloom false-positive rate, saturation, and bit occupancy.

Rollout procedure:

1. Deploy dual-format receivers to every cache-proxy pod with
   `CACHE_SUMMARY_PUBLISH_FORMAT=fixed`. Existing v2 publishing is unchanged.
2. Enable `summary` mode in non-production first. Expect partial coverage while
   the restarted/canary pods build and pull their initial snapshots.
3. Confirm summaries converge after startup and membership changes, pull work
   remains bounded, and request-time probes never exceed the configured cap.
4. Apply the cache-proxy memory limit and `GOMEMLIMIT` resource rollout, then
   verify every pod completed it with stable memory.
5. Set `CACHE_SUMMARY_PUBLISH_FORMAT=dynamic` in non-production. Mixed v2/v3
   peers are expected during the rolling restart.
6. Confirm `cache_proxy_summary_resident_bytes` remains below
   `cache_proxy_summary_memory_limit_bytes`, Bloom dimensions match each disk
   tier, and process RSS retains sufficient pod-limit headroom.
7. Compare peer usefulness, origin bytes, and origin latency with fixed mode,
   then roll dynamic publishing gradually through production.

During a rolling deployment, a peer without the compatible GET summary endpoint
remains uncovered; bounded confirmation or origin fallback is used until the
endpoint is available.

Rollback dynamic publishing by restoring
`CACHE_SUMMARY_PUBLISH_FORMAT=fixed`, which rebuilds the fixed local filter on
restart. Roll back lookup independently by restoring
`CACHE_PEER_LOOKUP_MODE=probe`. Cache keys and on-disk layout are unchanged, so
do not delete cache contents.

## Operator runbook

If origin traffic or latency regresses:

1. Check summary pull outcomes, resident count, summary age, and uncovered-peer
   behavior.
2. Check Bloom FPR, saturation, and cache entry count. Lower
   `CACHE_MAX_ENTRIES` if filters are materially beyond their design point.
3. Check skipped probes. If the pod has memory and socket headroom, adjust
   `CACHE_MAX_CONCURRENT_PEER_PROBES`; otherwise retain the non-blocking fallback.
4. Restore `CACHE_PEER_LOOKUP_MODE=probe` if locality is unacceptable while
   investigating.

If process memory approaches the pod limit:

1. Reduce `CACHE_SUMMARY_MEMORY_LIMIT_BYTES`; fewer peer summaries will be
   selected automatically and request-time work remains capped.
2. Reduce `CACHE_MAX_ENTRIES` to lower exact-index memory and return the Bloom
   filter toward its target FPR.
3. Reduce `CACHE_MAX_CONCURRENT_PEER_PROBES` if concurrent HTTP work contributes
   to the peak.
4. Verify the pod memory limit and Go memory policy leave headroom for native,
   kernel, and request buffers.

Changing summary memory does not require cache deletion. A restart recomputes
the deterministic selected subset and rebuilds the local counting filter from
the bounded on-disk cache index.
