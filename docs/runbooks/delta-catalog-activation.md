# Delta Catalog Activation

## When To Use

- Workers fail during boot or tenant activation after enabling `ducklake.delta_catalog_enabled`.
- Logs include `Delta catalog configured but attachment failed`.
- Queries against catalog `delta` fail after a rollout that changed object-store settings.

## Checks

1. Confirm the configured location. If `ducklake.delta_catalog_path` is empty, Duckgres derives `s3://<bucket>/delta/` from `ducklake.object_store`; it does not use the bucket root.
2. Verify the Delta table exists and contains `_delta_log` at the configured path.
3. Check that the worker can load the DuckDB `delta` extension from its configured `extension_directory`.
4. Verify S3 credentials, endpoint, region, `s3_use_ssl`, and `s3_url_style`; the Delta catalog reuses the DuckLake S3 secret settings.
5. During shutdown, workers switch to `memory`, detach `delta`, then detach `ducklake`. If shutdown hangs, inspect logs for `Failed to detach worker Delta catalog during shutdown` before forcing pod/process retirement.

## Recovery

1. Disable `ducklake.delta_catalog_enabled` and roll workers if the Delta path is unavailable but DuckLake service must continue.
2. Fix the path or object-store credentials, then re-enable the setting.
3. Prefer an isolated prefix such as `s3://<bucket>/delta/`; do not place Delta directly at the bucket root used by DuckLake.
