# Perf Harness Runbook

## Scope

Golden-query performance signal collection for `pgwire` and `flight` protocols.
This is observability-only; there is no pass/fail performance gate.

## Local Operator Steps

1. Start Duckgres with control-plane Flight ingress enabled.
2. Run `./scripts/perf_smoke.sh`.
3. Inspect `artifacts/perf/<run_id>/summary.json` and `query_results.csv`.

## Nightly Operator Steps

1. Trigger `./scripts/perf_nightly.sh` from the perf-runner host.
2. Validate that lock acquisition succeeds (`flock`).
3. Validate run completion before timeout.
4. Upload/store `artifacts/perf/<run_id>` in durable storage.

## Failure Recovery

1. If run hangs: collect `runner.log`, Duckgres logs, and `server_metrics.prom`.
2. If protocol errors spike: rerun with smoke catalog and inspect per-query rows/errors.
3. If artifact corruption occurs: rerun with a new `DUCKGRES_PERF_RUN_ID` and preserve the failed run directory.

