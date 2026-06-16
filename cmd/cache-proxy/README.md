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

Origin `GET` misses are retried up to 4 total attempts for transient failures:
HTTP `408`, `429`, `500`, `502`, `503`, `504`, request timeouts, and common
transport resets. Retries start with a 100 ms backoff and cap at 1 second.

Terminal origin responses such as `400`, `403`, `404`, and `416` are not retried
and are forwarded back to DuckDB verbatim. Failed origin responses are never
stored in the cache.
