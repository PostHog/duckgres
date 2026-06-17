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
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `DUCKGRES_TRACE_ENDPOINT` | empty | OTLP/HTTP trace endpoint. Unset → tracing is a no-op. |
| `OTEL_EXPORTER_OTLP_TRACES_PATH` | empty | Overrides the OTLP path (e.g. VictoriaTraces' `/insert/opentelemetry/v1/traces`). Mirrors the main duckgres binary. |

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
