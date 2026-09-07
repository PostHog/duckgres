# Scan bytes and storage billing

Trino's built-in HTTP event listener sends completion events to Duckgres. Duckgres
commits normalized per-query usage to its existing config-store PostgreSQL before
responding. Billing pulls immutable batches containing scan bytes and the existing
storage GiB-seconds. This replaces DuckDB CPU/memory metering and the destructive
watermark API entirely.

## Accounting contract

- Bill `statistics.physicalInputBytes` from every completed query, including
  failures and user cancellations (`FAILED`, with `USER_CANCELED` error code).
  Incomplete statistics are retained and charged at the reported byte count.
- This is Trino's measured reader input, not logical table size or every byte
  charged by the object store. Projection, pruning and metadata-only operations
  affect it. The DuckLake connector includes actual reads of position-delete
  files after the delete-read-statistics fix. Merely deleting a file or object
  does not scan its entire contents; a row-level DELETE can read table data and
  existing delete files, and those reads contribute to the query's counter.
  Writing a new delete file is output, not scanned input. Repeated physical
  reads by different splits are counted repeatedly.
- The server-configured cell ID plus Trino query ID is the deduplication key.
  First accepted delivery wins; a retry never changes its amount or ownership.
- `context.user` maps to the tenant's database-name principal. OPA forbids
  impersonation. The provisioner durably records identities before publishing
  authentication. Historical mappings survive organization deletion; assigning
  a retired principal to another organization is rejected. The two built-in
  provisioner/observer identities are exempt. Client-selected `source` is only
  diagnostic and cannot exempt or reattribute a query.
- Unknown principals are retained without attribution and held out of export
  until a mapping exists. Do not guess their organization or drop them.
- `team_id` is informational (oldest team when attributed), not a billing owner.
  Billing maps `org_id` to its own customer/team contract.
- Query ID, cell, identity/ownership, completion/receipt timestamps, state, error
  code, physical and processed input, completeness, source and Trino version are
  retained. SQL, plans and exception messages are discarded on ingestion.
- All usage and delivered batches are retained indefinitely. Ack marks delivery;
  it never deletes usage. Admin daily/monthly views read this retained history.

Storage sampling is unchanged: the elected leader, every `30m` by default
(`DUCKGRES_STORAGE_SAMPLE_INTERVAL`), sums tracked DuckLake data and delete-file
sizes including historical files, then credits bytes × sampling interval. Failed
samples undercount an interval. Storage remains exact-decimal GiB-seconds
(byte-seconds / 2^30). No new scan price is configured; the admin pricing calculator
continues to price storage only.

## HTTP interfaces

`POST /api/v1/trino/usage` accepts native `QueryCompletedEvent` JSON. Authorization
is `Bearer <usage-token>`, a dedicated per-cell credential from Kubernetes Secret
`trino-usage-token`, key `token`. It grants no admin access. The endpoint is wired
when the Trino provisioner is enabled. Request limit is 16 MiB; persistence timeout
is 10 seconds. Valid new or duplicate deliveries return 200; database failures
return 503 for listener retry. Invalid events return 400, excessive bodies 413,
and bad credentials 401. Only completion events are accepted.

The following billing routes use existing admin/internal-secret authentication:

1. `POST /api/v1/billing/batches/next?limit=10000` creates or repeats the outstanding
   batch. `limit` is optional, default/max 10,000, and bounds each family separately
   (query events and storage buckets). There is one consumer and one outstanding
   batch per config store. No available attributable usage returns `{"batch":null}`.
2. `POST /api/v1/billing/batches/<batch_id>/ack` marks that exact batch acknowledged
   and returns `{"acked":"<batch_id>"}`. Repeating an acknowledged ID succeeds.
   Unknown IDs return 404; malformed UUIDs return 400. There is no watermark.
3. `GET /api/v1/billing/batches/<batch_id>` retrieves the retained original batch,
   before or after ack, for reconciliation/replay. It does not reset delivery.

Billing responses negotiate gzip. Database operations have a 30-second timeout.
A batch response has this shape:

```json
{
  "batch": {
    "batch_id": "f6812c40-4e36-43d2-bd2a-0c9dba371cde",
    "created_at": "2026-09-01T00:02:00Z",
    "billing_month": "2026-09",
    "scans": [
      {"date":"2026-08-31","org_id":"example-org","team_id":1,"bytes_scanned":123456789,"query_count":2}
    ],
    "storage": [
      {"date":"2026-09-01","org_id":"example-org","team_id":1,"gib_seconds":1800.5}
    ]
  }
}
```

`billing_month` is the UTC month when the batch is first created. Row dates preserve
the original completion/sample day. Late completions, delayed delivery or a billing
outage can therefore contribute to a later invoice without reopening old months.
Query counts include zero-byte queries. JSON amounts are exact numbers; consumers
must use arbitrary-precision integers/decimals, not IEEE-754 floats for accounting.

The consumer must atomically persist the batch ID and its charges before acking.
On a repeated batch ID, reuse that durable result and retry the ack. If billing
crashes after charging but before ack, this rule prevents a duplicate charge.
Using an external invoice API also requires that API's idempotency mechanism keyed
by batch ID (and row identity where necessary). A Duckgres ack alone cannot make
an external charge exactly-once.

## Why batches are stable

A PostgreSQL singleton row serializes batch creation/ack. A transaction claims
committed query rows by membership, not a timestamp or maximum sequence ID, so an
earlier ID committed later remains eligible. The response is frozen and stored
in the same transaction. Concurrent callers receive the same outstanding batch.

Storage keeps its additive sampling rows. Each row tracks the byte-seconds already
claimed, and a batch retains the exact claimed delta. Row locks serialize claiming
with sampler increments. An increment in the same minute after export becomes a
new delta in a later batch; it cannot mutate an outstanding batch or disappear
behind a watermark.

## Delivery limits and failure recovery

The listener uses its in-memory asynchronous retry queue. Configure completion
only, 18 retries, 1-second initial delay, exponential factor 2 and a 1-minute cap
(roughly 13 minutes of scheduled backoff, plus request times). It retries network
errors, 408, 429 and 5xx responses. A coordinator crash before successful ingestion,
queue loss, retry exhaustion, or a permanent 4xx can lose a completion. This is the
accepted best-effort boundary; Duckgres cannot reconstruct an event it never got.
Once its INSERT commits, retries are deduplicated and downstream delivery is durable.

The earlier “persistent outbox” alternative would have persisted pending sends on
the coordinator. It would survive coordinator replacement only if its backing
storage also survived and a replacement resumed delivery. Even then, a crash after
query work but before inserting its completion into the outbox could still lose
usage. Closing that gap needs integration with durable query-completion state. We
are deliberately using the simpler built-in listener with its accepted rare loss.

Operational checks and recovery:

- Watch Trino HTTP-listener delivery failures and Duckgres ingestion 4xx/5xx.
  A persistent 401 is credential mismatch; a 404 is wrong endpoint/version; a 413
  requires investigating payload sizes. These permanent responses need operator
  intervention, not more transient retries. Never log whole events or tokens.
- Inspect `duckgres_trino_query_usage WHERE org_id IS NULL` for unresolved
  principals, and `WHERE batch_id IS NULL` for backlog. Confirm catalog ownership
  before repairing a historical mapping; the next batch resolves pending rows.
- Inspect `duckgres_billing_consumer.outstanding_batch_id` and retained
  `duckgres_billing_batches.created_at/acknowledged_at` for a stuck consumer. Restart it
  using the normal next/deduplicate/ack sequence. Do not manually advance or delete
  rows to unstick delivery. Use retained batch GETs to reconcile an external invoice.
- Preserve the usage-token Secret. It is immutable and guarded by a distinct
  bootstrap sentinel. If it disappears after initialization, restore the original
  from backup; startup fails rather than silently changing credentials that Trino
  still has in its environment. Deliberate rotation requires coordinated Secret,
  sentinel and CP/Trino restarts during a maintenance window.
- Monitor database size and back up the config store. No retention job deletes
  this history. Records lost by the old API before upgrade cannot be recovered.

## Rollout and local validation

1. Deploy the DuckLake delete-read-statistics fix to the Trino engine.
2. Apply chart RBAC granting the provisioner access to `trino-usage-token`, keeping
   `queryUsage.enabled=false`. This must precede the new CP startup bootstrap.
3. Pause the old billing consumer, reconcile its last acknowledged delivery, and
   upgrade all Duckgres CP replicas. Migration 39 adds the ledger/batches and seeds
   existing tenant identities. Remaining storage rows are eligible for the first
   batch; old CPU/memory records remain inert and are never exported. Remove every
   old CP before enabling billing: the old ack/GC code must not delete storage.
4. Enable Trino chart `queryUsage.enabled=true`. Its endpoint defaults to the local
   Duckgres control-plane service, with the Secret injected into the coordinator.
   The optional observability listener can run alongside it using separate files.
   Render before applying: `./scripts/helm-template.sh trino` in the charts repo.
5. Run a query that reads data, confirm its native bytes match its retained query
   row and the batch, then verify retry/ack/replay. Test failure and cancellation
   as well. Start the replacement billing consumer using the contract above.

For local checks, use `just test-configstore-integration` with PostgreSQL on
`127.0.0.1:35432` (`postgres`/`postgres`, database `testdb`; the harness can start
Docker when unavailable), `just test-controlplane`, `just test-controlplane-k8s`,
`just ui-test` and `just lint`. Database tests cover concurrent consumers/sampling,
late commits, deduplication, failures/cancellation, exact large sums, retained
ownership and replay. `tests/mw-dev/e2e/harness.sh` covers the real storage sampler
and `tests/mw-dev/e2e/trino.sh` covers actual native listener delivery and batch ack.
The cloud end-to-end lane requires its normal isolated deployment; package tests
alone do not establish deployment-level delivery.
