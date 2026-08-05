# Runbook: Node-local Cache Proxy Bypass

## Impact

When `duckgres-cache-proxy` is unavailable on a worker node, workers remain
ready and PostgreSQL sessions continue. They bypass the local NVMe cache and
fetch signed object data from the authoritative S3 source. The proxy's own
peer-cache path is unavailable while that daemon is down, so affected workers
may read more from S3 and have higher read latency/cost.

The alert statement is therefore factually correct: **Workers on affected nodes
will bypass local NVMe cache and fetch more data from peers or S3.** Healthy
proxies can still satisfy cache misses from peers; bypassed workers use S3
directly until their local proxy recovers.

## Detection

- `duckgres_worker_cache_proxy_mode == 0` on a cache-enabled worker.
- Increasing `duckgres_worker_cache_proxy_bypass_transitions_total` or
  `duckgres_worker_cache_proxy_bypassed_operations_total`.
- Cache-proxy pod CrashLoopBackOff/OOMKilled events on the corresponding node.

## Response

1. Confirm the worker is serving sessions; do not recycle it solely to restore
   cache performance.
2. Inspect the node-local `duckgres-cache-proxy` pod for OOM or storage errors.
   The existing `scripts/probe_worker_egress.sh` script can verify the node
   host-port health endpoint.
3. Restore the daemon or its NVMe capacity. Workers probe `/health` with capped
   exponential backoff and jitter, then automatically re-enable caching after a
   successful health check.
4. Verify `duckgres_worker_cache_proxy_mode` returns to `1` and
   `duckgres_worker_cache_proxy_recoveries_total` increases. Query/read errors
   that persist after bypass are source-path errors and must be investigated as
   S3, credentials, or data-integrity failures rather than cache availability.

## Startup behavior

`DUCKGRES_CACHE_PROXY_CONNECT_TIMEOUT` bounds the initial health check and
defaults to `5s`. After the timeout, the worker starts in bypass mode; increasing
this value only delays a cache decision and should not be used as a substitute
for fixing a repeatedly unhealthy cache proxy.
