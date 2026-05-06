# Dev Deploy PGWire Scenario Replay Runbook

## Purpose

Replay the dev Duckgres PGWire scenario suite against:

```bash
psql "host=perfdev.dw.dev.postwh.com port=5432 dbname=perfdev user=bill sslmode=require"
```

The runbook covers:

- Baseline analytical, concurrency, worker reuse, cleanup, catalog, auth, cancel, and stability pressure scenarios.
- Long-query boundaries from ~20s through 5m cancel/kill.
- One-hour boundary testing across 10m, 30m, and attempted 60m behavior.
- Loki evidence via Kubernetes port-forward to `loki-logs-read`.

## Prerequisites

Required tools:

```bash
psql
kubectl
curl
jq
nc
aws
```

Required access:

```bash
export AWS_PROFILE=<managed warehouse prod us profile>
export PGPASSWORD='<bill-password>'
export DSN="host=perfdev.dw.dev.postwh.com port=5432 dbname=perfdev user=bill sslmode=require"
```

Kubernetes context and Loki service:

```bash
kubectl --context posthog-mw-dev -n monitoring get svc loki-logs-read
kubectl --context posthog-mw-dev -n monitoring port-forward svc/loki-logs-read 3100:3100
curl -sS http://127.0.0.1:3100/ready
```

Known working Loki selectors:

```logql
{namespace="duckgres"}
{namespace="duckgres", app="duckgres"}
{namespace="duckgres", app="duckgres-worker"}
{namespace="duckgres"} |= "user=bill"
{namespace="duckgres"} |= "<RUN_ID>"
```

## Safety Notes

- These scenarios create isolated schemas and drop them at the end.
- The 25-way concurrency, churn, idle pressure, and hour-boundary tests intentionally stress worker lifecycle behavior.
- Do not run the one-hour boundary test during a sensitive dev deploy window unless the goal is to test disruption behavior.
- The replay scripts create timestamped artifact directories under `artifacts/dev-deploy-scenarios/`.

## Preflight

Run before any scenario:

```bash
aws sts get-caller-identity
nc -vz perfdev.dw.dev.postwh.com 5432
psql "$DSN" -v ON_ERROR_STOP=1 -c "SELECT 1" -c "SELECT current_database(), current_user"
kubectl --context posthog-mw-dev -n monitoring get svc loki-logs-read
```

## Replay 1: Short Query Scenario Suite

Command:

```bash
bash scripts/dev-test-scenarios/run-short.sh
```

Expected duration: about 5-10 minutes.

Coverage:

- AWS/network/psql/Loki preflight.
- Isolated schema with 1M `events` rows and 1K `users` rows.
- Typical analytics:
  - aggregation
  - hourly bucket
  - high-cardinality group-by
  - window query
  - join query
- Short simulated long query.
- Concurrent sessions at 5, 10, and 25.
- 100 quick sequential sessions.
- 10 worker reuse cycles.
- Session cleanup with 10 held clients.
- Prepared statements.
- Transaction rollback and error recovery.
- Temp table isolation.
- Catalog/introspection compatibility.
- Large sort and 100K-row streaming.
- Long query plus 100 churn sessions.
- Ctrl-C cancel and killed-client behavior.
- 20 held idle sessions.
- Wrong-password and wrong-database checks.
- Final schema cleanup.
- Loki captures for run marker, `user=bill`, and pressure/error terms.

Primary outputs:

```text
artifacts/dev-deploy-scenarios/<RUN_ID>/execution-report.md
artifacts/dev-deploy-scenarios/<RUN_ID>/summary.tsv
artifacts/dev-deploy-scenarios/<RUN_ID>/sql/
artifacts/dev-deploy-scenarios/<RUN_ID>/outputs/
artifacts/dev-deploy-scenarios/<RUN_ID>/activity/
artifacts/dev-deploy-scenarios/<RUN_ID>/loki/
```

## Replay 2: Long Query Scenario Suite

Command:

```bash
bash scripts/dev-test-scenarios/run-long.sh
```

Expected duration: a little over 1 hour if the one-hour query remains active until the intended cancel point. It may run longer if the server cancels but the psql client remains blocked.

Coverage:

- Isolated schema with 3M synthetic rows.
- Complete self-join boundaries:
  - 1M cutoff
  - 1.5M cutoff
  - 2M cutoff
- 3M cutoff query intended for 5m cancel.
- 4M supplemental active cancel at ~5m.
- 4M killed-client test at ~2m.
- Post-cancel and post-kill `SELECT 1`.
- Isolated schema with 14M synthetic rows.
- One large self-join intended to cross:
  - 10 minutes
  - 30 minutes
  - 60 minutes
- `pg_stat_activity` snapshots at checkpoints.
- Cancel at the 60-minute boundary when still active.
- Post-boundary `SELECT 1`.
- Final cleanup.
- Loki captures around query start/finish/cancel, the hour marker, and pressure/error terms.

Primary outputs:

```text
artifacts/dev-deploy-scenarios/<RUN_ID>-long-boundaries/long-boundary-report.md
artifacts/dev-deploy-scenarios/<RUN_ID>-long-boundaries/summary.tsv
artifacts/dev-deploy-scenarios/<RUN_ID>-long-boundaries/sql/
artifacts/dev-deploy-scenarios/<RUN_ID>-long-boundaries/outputs/
artifacts/dev-deploy-scenarios/<RUN_ID>-long-boundaries/activity/
artifacts/dev-deploy-scenarios/<RUN_ID>-long-boundaries/loki/
artifacts/dev-deploy-scenarios/<RUN_ID>-hour-boundary/hour-boundary-report.md
artifacts/dev-deploy-scenarios/<RUN_ID>-hour-boundary/summary.tsv
artifacts/dev-deploy-scenarios/<RUN_ID>-hour-boundary/sql/
artifacts/dev-deploy-scenarios/<RUN_ID>-hour-boundary/outputs/
artifacts/dev-deploy-scenarios/<RUN_ID>-hour-boundary/activity/
artifacts/dev-deploy-scenarios/<RUN_ID>-hour-boundary/loki/
```

## Manual Loki Queries

Use this shape to replay log evidence:

```bash
export LOKI_URL="http://127.0.0.1:3100"
export START_NS="<unix_start_seconds>000000000"
export END_NS="<unix_end_seconds>000000000"
export RUN_ID="<run-id>"

curl -sS -G "$LOKI_URL/loki/api/v1/query_range" \
  --data-urlencode 'query={namespace="duckgres"} |= "'"${RUN_ID}"'"' \
  --data-urlencode "start=${START_NS}" \
  --data-urlencode "end=${END_NS}" \
  --data-urlencode 'limit=1000' \
  --data-urlencode 'direction=backward' | jq .
```

Pressure/error query:

```bash
curl -sS -G "$LOKI_URL/loki/api/v1/query_range" \
  --data-urlencode 'query={namespace="duckgres"} |~ "ERROR|Canceled|context canceled|unresponsive|timeout|worker|Failed to create session"' \
  --data-urlencode "start=${START_NS}" \
  --data-urlencode "end=${END_NS}" \
  --data-urlencode 'limit=1000' \
  --data-urlencode 'direction=backward' | jq .
```

## Cleanup Verification

After each replay:

```bash
psql "$DSN" -v ON_ERROR_STOP=1 -c "SELECT current_database(), current_user"
psql "$DSN" -v ON_ERROR_STOP=1 -At -c "SELECT pid FROM pg_stat_activity"
lsof -nP -iTCP:3100 -sTCP:LISTEN
```

If a script is interrupted, manually drop its schema using the schema name printed in the report:

```bash
psql "$DSN" -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS <schema_name> CASCADE"
```
