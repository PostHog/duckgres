# Query Log Writer Runbook

Use this when durable rows are missing from `ducklake.system.query_log`, Kafka
query-log lag is rising, or you need to confirm that per-query resource usage is
available after a rollout.

## Mental Model

Duckgres has two query-log paths:

- Direct DuckLake sink: the control plane writes query-log rows straight into
  each tenant's `ducklake.system.query_log`. This is the default when no Kafka
  query-log producer config is present.
- Kafka sink: control-plane pods publish query-log events to Kafka when
  `DUCKGRES_QUERY_LOG_SINK=kafka`; the `duckgres-query-log-writer` Deployment
  consumes those events and writes generated rows into the same tenant table.

Both paths write the same logical row shape, including `cpu_time_s` and
`peak_buffer_memory_bytes`. `cpu_time_s` is DuckDB profiling CPU/thread time in
seconds. `peak_buffer_memory_bytes` is DuckDB peak buffer memory, not process
RSS or cgroup memory.

The Kafka writer commits offsets only after a successful durable write or an
intentional drop for an invalid/non-targetable event. Retryable write failures
should hold the partition in place until the writer can write or an operator
fixes the underlying target.

## Check Producer Mode

Confirm the control plane is configured to publish to Kafka:

```bash
kubectl -n duckgres get deploy duckgres-control-plane \
  -o jsonpath='{range .spec.template.spec.containers[0].env[*]}{.name}{" value="}{.value}{" config_key="}{.valueFrom.configMapKeyRef.key}{" optional="}{.valueFrom.configMapKeyRef.optional}{"\n"}{end}' \
  | grep -E 'DUCKGRES_QUERY_LOG'

kubectl -n duckgres get configmap duckgres-query-log-kafka \
  -o jsonpath='sink={.data.sink}{"\n"}brokers={.data.brokers}{"\n"}topic={.data.topic}{"\n"}client_id={.data.client_id}{"\n"}'
```

For the Kafka path, expect `DUCKGRES_QUERY_LOG_SINK=kafka` plus non-empty Kafka
broker, topic, and client-id settings. If `DUCKGRES_QUERY_LOG_SINK` is absent
or the ConfigMap has `sink` set to anything other than `kafka`, the control
plane is using the direct DuckLake sink and the writer will not receive new
events.

## Check Writer Health

```bash
kubectl -n duckgres get deploy duckgres-query-log-writer
kubectl -n duckgres logs deploy/duckgres-query-log-writer --tail=200
kubectl -n duckgres port-forward svc/duckgres-query-log-writer-metrics 9090:9090
curl -s localhost:9090/metrics | rg 'duckgres_query_log_kafka_writer'
```

Useful signals:

- `outcome="inserted"` should increase while writer rows are inserted.
- `outcome="committed"` should increase after corresponding Kafka offsets are
  committed.
- `outcome="retried"` means the writer is preserving Kafka offsets and retrying
  a target write.
- `outcome="failed"` means the writer hit a non-recoverable processing error.
- `outcome="dropped"` should be rare and means the event was invalid or could
  not be mapped to a tenant target.

## Check Kafka Lag

Use the cluster Kafka exporter or Kafka admin tooling to inspect Kafka lag for:

- topic: the configured `DUCKGRES_QUERY_LOG_KAFKA_TOPIC`
- consumer group: `duckgres-query-log-writer`, unless overridden by
  `DUCKGRES_QUERY_LOG_KAFKA_GROUP_ID`

Kafka lag increasing while `outcome="inserted"` and `outcome="committed"` are
flat usually means the writer is not running, cannot reach Kafka, or is blocked
on tenant DuckLake writes. Kafka lag increasing while `outcome="retried"` is
rising usually means the writer is deliberately holding offsets behind a
retryable write failure.

## Check Tenant Rows

Run this against the affected tenant DuckLake:

```sql
SELECT
    event_time,
    user_name,
    query_duration_ms,
    cpu_time_s,
    peak_buffer_memory_bytes,
    type,
    left(query, 160) AS query
FROM ducklake.system.query_log
ORDER BY event_time DESC
LIMIT 20;
```

Top recent resource consumers:

```sql
SELECT
    user_name,
    normalized_query_hash,
    count(*) AS queries,
    sum(cpu_time_s) AS total_cpu_time_s,
    max(peak_buffer_memory_bytes) AS max_peak_buffer_memory_bytes
FROM ducklake.system.query_log
WHERE event_time > now() - INTERVAL '1 hour'
GROUP BY 1, 2
ORDER BY total_cpu_time_s DESC
LIMIT 20;
```

If `cpu_time_s` or `peak_buffer_memory_bytes` is always zero, check whether the
query completed before profiling output was available. OOMs or worker crashes
can still miss profiling data because DuckDB may not finish writing the profile.

## Missing Schema Or Table

Check whether the table exists in the tenant DuckLake:

```sql
SELECT catalog_name, schema_name
FROM ducklake.information_schema.schemata
WHERE schema_name = 'system';

SELECT table_catalog, table_schema, table_name
FROM ducklake.information_schema.tables
WHERE table_schema = 'system'
  AND table_name = 'query_log';
```

A missing schema or missing table usually means no query-log writer has
successfully attached to that tenant DuckLake yet, query logging is disabled, or
the writer cannot resolve the tenant target. Check writer logs for target
resolution, credential, attach, or `ensureQueryLogTable` errors.

## Recover A Stuck Writer

If Kafka lag is rising and logs show stuck writer connections or repeated target
write failures:

1. Fix the underlying tenant target first: credentials, network access, object
   store permissions, or metadata store availability.
2. Restart the writer to clear process-local DuckDB connections:

```bash
kubectl -n duckgres rollout restart deploy/duckgres-query-log-writer
```

If restart is not enough, scale to zero and back:

```bash
kubectl -n duckgres scale deploy/duckgres-query-log-writer --replicas=0
kubectl -n duckgres scale deploy/duckgres-query-log-writer --replicas=1
```

Because offsets are committed after durable writes, restart/retry should not
skip retryable events. Event IDs make normal single-consumer Kafka replays safe
for the same tenant table; they are not a storage-level uniqueness guarantee
across multiple independent writer groups.

## Roll Back Kafka Query Logging

To return to the direct DuckLake sink:

```bash
kubectl -n duckgres scale deploy/duckgres-query-log-writer --replicas=0
kubectl -n duckgres delete configmap duckgres-query-log-kafka
kubectl -n duckgres rollout restart deploy/duckgres-control-plane
```

After rollback, new control-plane pods should no longer show
`DUCKGRES_QUERY_LOG_SINK=kafka`, and new query-log rows should be written by the
direct sink.
