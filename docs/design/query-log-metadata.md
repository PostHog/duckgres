# Query Log: Event Model, Query Metadata, and RBAC Signals

Status: **phases 0–2 implemented**; phases 3–4 outstanding (see §5).
Owner: TBD.

Implemented so far:

| | |
| --- | --- |
| Column registry + `ADD COLUMN` migration + replace-on-drift view | `server/querylog_schema.go`, `querylog_postgres.go`, `querylog_view.go` |
| `query_id` (UUIDv7), `parent_query_id`, `statement_index` | `server/query_id.go`, `query_metrics.go` |
| `QueryStart` / terminal pair, `ExceptionBeforeStart` split, `query_log.start_events` | `server/query_event.go`, `query_start_policy.go`, `querylog.go` |
| Relation/column/function/access extraction + `metadata_complete` | `server/querymeta/`, `server/query_metadata.go` |
| `query_id` propagation to the worker | `server/wire/protocol.go`, `flightclient`, `duckdbservice/query_id.go` |
| e2e coverage | `query_log_round_trip`, `query_log_access_metadata` in `tests/mw-dev/e2e/harness.sh` |

Deviations from the plan below, decided during implementation:

- **`event_time` is the statement's start on every event type**, not the
  event's own timestamp as in ClickHouse. Both rows of a pair land in one
  monthly partition and join without a window function. A terminal row's finish
  time is `event_time + query_duration_ms`. No separate `query_start_time`
  column is needed as a result.
- **`query_log.metadata` is a boolean**, not `off|basic|full`. Extraction is a
  single pass; splitting it into tiers would be configuration without a
  behavioural difference worth the surface.
- **Extraction is synchronous, with an LRU** rather than routed through an async
  enricher. It feeds the `QueryStart` event, which is emitted before the
  statement runs, and a future gate needs it before deciding whether the
  statement may run at all — neither can wait on a background queue.
- **Terminal events are still control-plane-owned.** `query_id` reaches the
  worker and the worker records it, but the ownership transfer described in §2.2
  is not done; see §5 Phase 1 (remaining).
- **`QueryAccessPolicy.Authorize` is untouched.** It should eventually be
  re-expressed over `querymeta.Metadata` (§3.1), but rewriting a fail-closed
  authorizer belongs in its own change with its own review, not in one that adds
  logging.

Guide: [`clickhouse.com/docs/reference/system-tables/query_log`](https://clickhouse.com/docs/reference/system-tables/query_log).

## Goal

Three things, in dependency order:

1. **A ClickHouse-shaped event model.** The control plane mints a `query_id`
   (UUIDv7) per inbound statement and emits a `QueryStart` row; the component
   that actually executes emits the terminal row (`QueryFinish` /
   `ExceptionWhileProcessing`), or the CP emits `ExceptionBeforeStart` if the
   statement never began executing. Same `Enum8` vocabulary as CH:
   `QueryStart=1, QueryFinish=2, ExceptionBeforeStart=3, ExceptionWhileProcessing=4`.
2. **Extract what the statement touches** — catalog / schema / table / column —
   and **what kind of access it is** (read, write, DDL, config, admin).
3. **Record 2 as durable signals now, so RBAC can enforce on them later.** The
   log is the audit trail and the shadow-mode proving ground; the in-memory
   `Metadata` it is built from is the future policy input.

Point 3 raises the bar on point 2: this is not best-effort observability. An
authorization decision that will one day be derived from this extraction must
never be able to read "no tables referenced" from a statement we simply failed
to parse.

Non-goals: replacing Prometheus metrics or OTEL traces; a second storage
backend; shipping enforcement (this plan ends at shadow mode).

---

## 1. Where we are today

```
clientConn.logQuery()                      server/querylog.go
  → executor sink (FlightExecutor.Log)     server/flightclient/flight_executor.go
      one DoAction RPC per entry, JSON, async goroutine, bounded in-flight
  → worker doLogQuery handler              duckdbservice/flight_handler.go:496
  → QueryLogger batch flush                server/querylog.go
  → INSERT querylog.query_log_entries      server/querylog_postgres.go  (tenant metadata PG, monthly partitions)
  → ducklake.system.query_log view         server/querylog_view.go       (live view via postgres_scan)
```

26 columns. One row per query, always terminal, always originated by the CP.
**The worker is a persistence relay** — in the multitenant remote backend the CP
has no per-tenant metadata-store config, so `NewQueryLogSink` returns nil and
every entry physically lands via the worker's sink. That stays true here; what
changes is who *originates* records, not who writes them.

### Already computed, then discarded

| Available at | Discarded today |
| --- | --- |
| `observe.parseProfilingOutput` (full DuckDB profile JSON already reaches the CP in a gRPC trailer) | `rows_returned`, `result_set_size`, `total_bytes_read`, `total_memory_allocated`, planner/optimizer timings, the whole operator tree. We persist 3 of these. |
| `clientConn` | `workerPod`, `querySource` (the **billing** GUC), `queryAccessPolicy`, `passthrough`, `txStatus`, `physicalCatalog`, `workerMillicores`/`workerMiB` |
| `queryMetricsScope` (`server/query_metrics.go`) | the `status`/`reason` classification — Prometheus only |
| transpiler | the Tier-1 AST, which transforms fired, `ParamCount`, `FallbackToNative` |
| `wire.QueryLogEntry.EventID` | declared, **never populated anywhere** — a free `query_id` column waiting for a value |

### Structural gaps

1. **No `query_id`** — nothing correlates rows, traces, or a support ticket.
2. **A query that never finishes leaves no row at all.** Worker OOM-kill or pod
   eviction mid-query produces zero evidence. Worst gap for incident triage.
3. **Flight SQL ingress writes nothing** — `server/flightsqlingress/` never calls
   `logQuery`. Flight traffic is invisible. For an audit trail this is
   disqualifying: the log must not have a bypass.
4. **`normalized_query_hash` is a regex+FNV approximation** while `pg_query_go`
   — already a dependency — exposes `FingerprintToUInt64` / `Normalize`.
5. **No relation-level metadata** — no `tables`, `schemas`, `columns`,
   `used_functions`.
6. **Three independent parses for scoped users**: `Authorize`
   (`query_access.go`), `rewriteScopedMetadataQuery`
   (`ingress.go:1281`), and the transpiler each call `pg_query.Parse`.
7. **No retention.** Partitions are created and never dropped.
8. **No schema evolution.** `CREATE TABLE/VIEW IF NOT EXISTS` means adding a
   column is a no-op on every existing tenant.
9. **No e2e coverage** — `tests/mw-dev/e2e/harness.sh` has zero `query_log`
   assertions.

---

## 2. The event model

### 2.1 Type vocabulary

Keep the column `TEXT` with CH-identical spellings (existing rows already use
`QueryFinish` / `ExceptionWhileProcessing`, so **zero data migration**), and
define the Enum8 codes on the Go type so a CH-shaped export is a straight
mapping:

```go
type QueryEventType uint8
const (
    QueryStart               QueryEventType = 1
    QueryFinish              QueryEventType = 2
    ExceptionBeforeStart     QueryEventType = 3
    ExceptionWhileProcessing QueryEventType = 4
)
```

We do **not** add a 5th value. Our extra semantics (cancelled, worker lost,
client disconnected, abandoned handle) go in the `reason` column — which we are
adding anyway from the existing `queryMetricsScope` classification. The enum
stays CH-faithful; the nuance lives beside it.

Note this reclassifies today's behaviour: `logQuery` currently stamps
`ExceptionWhileProcessing` whenever `errCode != ""`, including for transpile
errors and policy denials that never began executing. Those become
`ExceptionBeforeStart`. The rule is mechanical — `execStartAt.IsZero()`.

**The boundary is "execution began", not "an engine saw it."** Prod data settled
this: the largest population of `ExceptionBeforeStart` is an extended-protocol
`Describe` failing at prepare with a binder error — the statement reaches the
worker (Describe asks it for the result schema) and still never runs. Reading the
class as "never reached a worker" would mislead triage. ClickHouse draws the line
the same way: analysis-time failures are `ExceptionBeforeStart`.

### 2.2 Who emits what

```
CP: mint query_id (UUIDv7)
    ├─ extract metadata (§3), emit QueryStart              [type=1]
    ├─ never began executing → emit ExceptionBeforeStart  [type=3]  ← CP owns
    └─ dispatched to engine, propagate query_id
           │
           ▼
    engine (worker pod, or the local executor in standalone/process mode)
           └─ emit QueryFinish | ExceptionWhileProcessing  [type=2|4]  ← engine owns
```

**Why the engine owns the terminal**, rather than keeping it CP-side:

- It is the **only bypass-free choke point**. PG wire, Flight SQL ingress, admin
  impersonation, and internal maintenance SQL all converge on the worker. Anchor
  the audit record there and gap #3 closes structurally instead of by
  remembering to add a call site.
- It has the profiling JSON **locally**, including for failed queries, where the
  CP's trailer-based capture is unreliable.
- It survives CP-side abandonment: client disconnect mid-query, CP pod eviction.

**Why the CP still owns `ExceptionBeforeStart`:** the engine either never sees
these statements or never runs them, so it cannot report their outcome. Auth
failure, transpile error, policy denial, worker acquisition failure at org cap —
the worker does not know the statement exists. An extended-protocol `Describe`
rejected at prepare does reach the worker, but only as a schema probe: there is
no execution whose end the worker could report.

**Honest limitation:** the highest-frequency incident — worker OOM-killed by its
own heavy query — is precisely the case where the engine *cannot* emit. That is
covered by the **absence** of a terminal row for a `QueryStart`, which is
exactly ClickHouse's semantics and is why `QueryStart` is worth its volume. The
CP additionally emits `ExceptionWhileProcessing` with `reason='worker_lost'`
when the execution RPC fails at the transport level — a case where the engine
provably could not have logged.

### 2.3 Delivery semantics: at-least-once, dedupe on read

There is no distributed exactly-once between CP and worker. For an audit trail
the correct bias is explicit: **a duplicate is an annoyance, a missing row is a
failure.** So:

- Emitters aim for one terminal per `query_id` via the ownership rule above.
- Readers dedupe: `DISTINCT ON (query_id, type)` preferring the earliest
  `event_time`. Document this in the README alongside the view.
- A `duckgres_query_log_duplicate_terminal_total` counter (detected worker-side
  at insert time via a cheap recent-`query_id` set) tells us if the ownership
  rule is leaking. Sustained nonzero = the rule is wrong, fix the rule.

We deliberately do **not** add a unique constraint. The table is partitioned by
`event_time`, so any unique index must include the partition key — and the CP's
and engine's rows for one query legitimately carry different `event_time`s.

### 2.4 Anchoring terminal emission on drain tokens

The worker already maintains `activeWork` (`duckdbservice/service.go`), a
refcount of in-flight work units, under a **load-bearing, tested invariant**:
*take exactly one token when work starts, release exactly one when it ends, on
every path* — including `reapIdle` releasing tokens stranded by a
`GetFlightInfo` whose `DoGet` never arrived.

That is the query lifecycle, already rigorously maintained. **Emit the terminal
event where the drain token is released.** Exactly-one-terminal-per-work-unit
then inherits an invariant the codebase already treats as load-bearing and
already tests, instead of being a second, parallel, weaker bookkeeping scheme.
The abandoned-handle reap becomes a terminal with `reason='abandoned'`.

### 2.5 Propagating `query_id`

`FlightExecutor.withSession` (`flight_executor.go:194`) already appends
`x-duckgres-session`, `x-duckgres-worker-id`, `x-duckgres-cp-instance-id`,
`x-duckgres-owner-epoch` to the outgoing gRPC context. Add
`x-duckgres-query-id`, sourced **from the context**, not from executor state:

```go
ctx = querylog.WithQueryID(ctx, obs.queryID)   // at the CP choke point
// withSession reads it back and appends the header
```

Context-sourced propagation means the Flight SQL ingress path gets it for free —
both front-ends funnel into the same `FlightExecutor.QueryContext` /
`ExecContext`. Implementation requirement: paths that currently pass the
*connection* context to execution need a per-query derived context.

The engine also needs the small set of per-query dimensions it cannot infer:
`query_source` (billing key, changeable mid-session via SET) and `protocol`.
Ship those in the same header set. Everything session-stable (username, org,
team, `session_id`, client address, application name) is already known to the
worker from `CreateSession` / activation, or is sent once there — not per query.

### 2.6 Row shape across the pair

CH writes the full column set on both rows. We write:

- **`QueryStart`** — identity, client/session context, query text, and all
  §3 metadata. Resource columns zero.
- **terminal** — identity (`query_id`, `session_id`, org/user), outcome, and all
  resource/timing columns. Query text is repeated (truncated) so a terminal row
  is self-sufficient for triage; the rest is joined on `query_id`.

`query_start_time` is stamped identically on both rows so "started, never
finished" is a single scan, and so a reader can pair rows without a window
function.

### 2.7 Volume control

`QueryStart` roughly doubles row count, and today the CP sends **one DoAction
RPC per entry**. Mitigations, in order of importance:

1. **Batch the forward RPC.** `FlightExecutor.Log` should coalesce entries into
   one `DoAction` per flush interval rather than one per entry. This is worth
   doing on its own merits and becomes necessary at 2× entries.
2. **`query_log.start_events: data | all | off`** (default `data`). `data`
   emits `QueryStart` only for statements whose `access_kinds` include
   read/write/ddl/admin — skipping transaction control, `SET`, and pure catalog
   introspection (psql/JDBC chatter, which never hangs and never needs in-flight
   visibility). **Terminal events remain universal**, so nothing disappears from
   the log; cheap statements simply have no paired start row.
3. **Sampling is a pair decision.** `query_log.sample_rate` must be evaluated
   once per `query_id` and applied to both rows. Never sample independently —
   half a pair is worse than neither. Errors and slow queries are never sampled
   out.
4. **Retention** (§5) is a prerequisite, not a follow-up.

---

## 3. Query metadata extraction (and the RBAC hook)

### 3.1 One walk, four call sites

`QueryAccessPolicy.Authorize` (`server/query_access.go:111`) already does most
of this work — it walks the parse tree by proto reflection, finds every
`RangeVar` and `FuncCall`, honours CTE scoping and shadowing, and classifies
statement nodes. And it is already invoked at exactly the four inbound-statement
choke points:

| | |
| --- | --- |
| `server/conn.go:1243` | PG wire, simple query |
| `server/conn_extended_query.go:42` | PG wire, extended query |
| `server/flightsqlingress/ingress.go:1250` | Flight ingress, query |
| `server/flightsqlingress/ingress.go:1268` | Flight ingress, exec |

**The RBAC hook already exists; it is just currently gated on
`policy != nil`.** The plan is not to build a parallel mechanism but to:

1. Extract the walker into **`server/querymeta/`** (no DuckDB dependency, usable
   by CP, ingress, and tests).
2. Give it two consumers over **one traversal**: `Extract() Metadata` and
   `Authorize(policy)`. Collapsing the duplicate parses in `Authorize`,
   `rewriteScopedMetadataQuery`, and the transpiler is a straight win for scoped
   users today (gap #6).
3. Run `Extract` unconditionally at those four sites; run `Authorize` when a
   policy exists. When RBAC v2 lands, enforcement is `policy.Evaluate(metadata)`
   at the same four lines — no new traversal, no new bypass surface.

### 3.2 Two taxonomies, not one

Syntax kind and privilege class are different questions and conflating them is
how RBAC models rot.

**`query_kind`** — CH parity, syntactic, scalar: `Select, Insert, Update,
Delete, Create, Drop, Alter, Copy, Explain, Set, Show, Begin, Commit, Other`.
This replaces today's `classifyQuery`, a `switch` on the *command tag*, which
misclassifies a writable-CTE `WITH ... INSERT` as `Select`.

**`access_kinds`** — the RBAC-relevant class, a **set** (a statement can be more
than one; a writable CTE both reads and writes):

| kind | means | examples |
| --- | --- | --- |
| `read` | reads tenant data | SELECT, COPY TO from a table |
| `write` | mutates tenant data | INSERT/UPDATE/DELETE/MERGE, COPY FROM |
| `ddl` | changes schema | CREATE/DROP/ALTER/TRUNCATE |
| `config` | changes session/global settings | SET, RESET, DISCARD |
| `admin` | changes the security or storage envelope | CREATE/DROP SECRET, ATTACH/DETACH, INSTALL/LOAD, COPY to an external URI |
| `transaction` | BEGIN/COMMIT/ROLLBACK | |
| `metadata` | catalog introspection only | `pg_catalog`, `information_schema` |
| `unknown` | **we could not determine it** | unparsed statement |

`unknown` is load-bearing: it is what a future gate denies on.

### 3.3 Relations: read set vs write set

Grants are directional (`SELECT ON x`, `INSERT ON y`), so the extraction splits
them: `read_relations` and `write_relations`, each an array of
`{catalog, schema, table, raw}`.

- `raw` preserves what the user actually wrote; `catalog`/`schema` are resolved
  where we can (connection's `physicalCatalog`, `search_path`, DuckLake default).
  Unqualified names in a multi-schema search path are recorded resolved-if-certain
  and flagged otherwise — never guessed.
- CTE names and derived tables are **not** relations. The existing `Authorize`
  walker already gets this right (`visibleCTEs`, recursive-CTE ordering); reuse
  it rather than re-deriving it.
- `pg_catalog` / `information_schema` reads are classified `metadata`, not
  `read` — policies almost always treat them differently, and duckgres rewrites
  them anyway.

### 3.4 Columns: honest about what is knowable

Column-level extraction without a catalog is partial, and the plan says so
rather than pretending otherwise:

| case | outcome |
| --- | --- |
| `t.col`, `schema.tbl.col` | attributed |
| unqualified, exactly one relation in scope | attributed |
| unqualified, multiple relations in scope | name recorded, **unresolved** |
| `SELECT *` / `t.*` | `select_star` flag (per relation where known) |
| `USING` / `NATURAL JOIN` | implicit columns recorded |
| through CTEs / derived tables | attributed to the CTE; chasing to base relations is v2 |

Emitted as `columns` (array of `{relation?, name}`) plus `columns_resolved`
(bool) and `select_star` (bool). For a future column-level policy, unresolved
means "requires catalog resolution", which means **deny**, never allow. Full
resolution needs a catalog snapshot in the CP — real work, explicitly v2.

### 3.5 Table functions: gate the target, not the function

`read_parquet('s3://...')`, `read_csv`, `postgres_scan`, `glob` reach data
without naming a relation, so a policy that only looks at relation names cannot
see them at all. Extraction therefore records table functions — name **and
arguments** — as first-class targets alongside relations.

But reading an external location is **supported usage, not an escalation**:
tenants are meant to read parquet from their own buckets. The cross-tenant
concern is the *target*, not the function — reads whose path resolves inside the
warehouse's managed DuckLake storage are how one tenant would reach another's
data. So:

- external reads classify as `read`, not `admin`. Flagging every `read_parquet`
  as admin-class would bury shadow-mode analysis in legitimate traffic.
- the entry is marked `external`, meaning "these args are a path to resolve",
  and preserves scheme + host + path — enough for the policy's path check.
- `COPY … TO '<uri>'` keeps `admin`: moving tenant data OUT is a different risk
  from reading a location in.

Arguments are sanitized before storage: query strings and userinfo are dropped,
because a presigned URL carries its credential there.

### 3.6 Soundness invariants

1. **Failure is never silence.** Every `Metadata` carries
   `metadata_complete bool` + `incomplete_reason`. Unparsed SQL (`FallbackToNative`,
   DuckDB-native syntax), a walker error, or a truncated array all set it false.
   Empty-because-we-failed and empty-because-nothing-was-touched must never be
   the same value on the wire or in the column.
2. **Extraction is synchronous.** Because RBAC will one day gate on it and
   because it populates the `QueryStart` row emitted *before* execution, the
   §3 subset cannot live in an async enricher. Only genuinely optional
   derivations (`Normalize`, fingerprinting of never-parsed Tier-0 statements)
   stay async.
3. **Budget a parse per statement.** `Classify` keeps Tier-0 statements
   unparsed today, and plain analytics SQL (`SELECT ... FROM events WHERE ...`)
   is exactly Tier 0. Extraction forces a parse there. `pg_query.Parse` is
   ~20–80 µs for typical statements — negligible against query execution,
   not negligible against a 1 ms catalog lookup. Mitigate with an LRU keyed by
   exact query text holding the (small) `Metadata`, not the AST; prepared
   statements and repetitive BI/dbt SQL hit it. Measure in `tests/perf` before
   default-on, and keep `query_log.metadata: off` as a kill switch.
4. **Shadow mode is the deliverable, not enforcement.** Once `access_kinds`,
   `read_relations`, `write_relations`, `used_table_functions`, and
   `metadata_complete` are in the log, a candidate policy can be evaluated
   *offline* against real traffic — "this grant set would have denied N
   statements from M users" — before a single request is refused. Ship the
   signals, prove the policy on them, then enforce.

---

## 4. Column catalog

**[obs]** = per-query observation record, **[profile]** = DuckDB profiling JSON,
**[meta]** = querymeta, **[session]** = connection state, **[new]** = new plumbing.
Rows: **S** = QueryStart, **T** = terminal, **B** = both.

### Identity & correlation

| column | type | CH analog | rows | source |
| --- | --- | --- | --- | --- |
| `query_id` | TEXT | `query_id` | B | **[obs]** UUIDv7; finally populates the dead `wire.QueryLogEntry.EventID` |
| `session_id` | TEXT | (`session_log`) | B | **[session]** UUID per connection |
| `query_start_time` | TIMESTAMPTZ | `query_start_time` | B | identical on both rows |
| `parent_query_id` | TEXT | `initial_query_id` | B | **[obs]** CP-synthesised statements (writable-CTE rewrite, cleanup) |
| `statement_index` | INT | `script_query_number` | B | **[obs]** position in a simple-query batch |
| `is_internal` | BOOL | `is_internal` | B | **[obs]** impersonation, checkpointer, activation SQL |
| `hostname` | TEXT | `hostname` | B | **[session]** CP pod / worker pod name of the emitter |
| `worker_pod` | TEXT | — | B | **[session]** on `clientConn` already, never logged |
| `worker_vcpu` / `worker_gib` | NUMERIC | — | T | **[session]** joins the log to compute billing |

### Timing

| column | CH analog | rows | source |
| --- | --- | --- | --- |
| `event_time` (exists) | `event_time` | B | this row's own timestamp |
| `query_duration_ms` (exists) | `query_duration_ms` | T | |
| `transpile_us`, `exec_us`, `first_row_us` (TTFB), `queue_wait_us` | — | T | **[obs]** |
| `planning_ms`, `scan_ms`, `compute_ms` | — | T | **[profile]** `collectOperatorTimings` computes scan/compute today and drops both |

### Resource usage

| column | CH analog | rows | source |
| --- | --- | --- | --- |
| `read_rows` | `read_rows` | T | **[profile]** operator `rows_scanned` rollup |
| `read_bytes` | `read_bytes` | T | **[profile]** `total_bytes_read` |
| `result_rows` (exists), `written_rows` (exists) | `result_rows`, `written_rows` | T | |
| `result_bytes` | `result_bytes` | T | **[profile]** `result_set_size` |
| `memory_usage_bytes` | `memory_usage` | T | **[profile]** `total_memory_allocated` |
| `peak_buffer_memory_bytes`, `cpu_time_s`, `postgres_scan_ms` (all exist) | — | T | |
| `postgres_scan_rows`, `operator_count`, `max_operator_cardinality` | — | T | **[profile]** |
| `profile_events` | `ProfileEvents` map | T | **[profile]** JSON map of per-operator `{time, cardinality, rows_scanned}`; the escape hatch — new DuckDB keys land here with no migration |
| `plan_digest` | — | T | **[profile]** hash of operator names + tree shape; same fingerprint + different digest = plan regression |

### Query text, shape, and access

| column | CH analog | rows | source |
| --- | --- | --- | --- |
| `query`, `transpiled_query`, `is_transpiled` (exist) | `query` | S (query repeated truncated on T) | |
| `normalized_query` | `formatted_query` | S | **[meta]** `pg_query.Normalize` |
| `query_fingerprint` | `normalized_query_hash` | S | **[meta]** `FingerprintToUInt64`. Added **alongside** the existing regex hash — silently changing that value would break every saved dashboard. Retire it later, announced. |
| `query_kind` (exists) | `query_kind` | S | **[meta]** §3.2, AST-derived |
| `access_kinds` | — | S | **[meta]** §3.2 — the RBAC class set |
| `read_relations`, `write_relations` | `databases`+`tables` | S | **[meta]** §3.3 |
| `columns`, `columns_resolved`, `select_star` | `columns` | S | **[meta]** §3.4 |
| `used_functions`, `used_table_functions` | `used_functions`, `used_table_functions` | S | **[meta]** §3.5 — args redacted |
| `metadata_complete`, `incomplete_reason` | — | S | **[meta]** §3.6.1 — the fail-closed flag |
| `param_count`, `param_types` | — | S | **[obs]** never param *values* |
| `transpile_tier`, `transforms` | — | S | **[obs]** `direct` / `transformed` / `native_fallback` + which transforms fired |

Deliberately skipped (CH-engine-specific or unknowable here): `partitions`,
`projections`, `views`, `used_dictionaries`, `used_storages`,
`used_aggregate_function_combinators`, `used_row_policies`, `distributed_depth`,
`revision`, `thread_ids`.

### Client & session

| column | CH analog | rows | source |
| --- | --- | --- | --- |
| `user_name`, `org_id`, `current_database`, `client_address`, `client_port`, `application_name`, `pid`, `protocol` (exist) | `user`, —, `current_database`, `address`, `port`, `client_name` | B | |
| `interface` | `interface` | B | **[new]** `pg` \| `flight` \| `admin` |
| `is_secure`, `tls_version`, `tls_cipher` | `is_secure` | S | **[session]** from `*tls.Conn` handshake state |
| `auth_method`, `access_scope` | — | S | **[session]** password/passthrough/internal; root vs project-scoped |
| `query_source` | — | B | **[session]** the `duckgres.query_source` GUC — already a billing dimension, currently unjoinable to the log |
| `team_id` | — | B | **[session]** resolved as `compute_meter` does (user's team, else org's oldest) |
| `log_comment` | `log_comment` | S | **[new]** free-text `duckgres.log_comment` GUC, length-capped |
| `client_metadata`, `client_traceparent` | `http_user_agent`/`http_referer` | S | **[new]** sqlcommenter map parsed from the **original inbound text** (comments do not survive deparse); `traceparent` additionally links our span to the client's trace |
| `settings` | `Settings` map | S | **[new]** session GUCs differing from default; needs a small `sessionSettings` map fed by the transpiler's parsed `VariableSetStmt` |
| `statement_name`, `portal_name`, `cursor_name`, `in_transaction` | — | S | **[obs]** |

### Outcome

| column | CH analog | rows | source |
| --- | --- | --- | --- |
| `type` (exists) | `type` | B | §2.1 |
| `exception_code`, `exception` (exist) | `exception_code`, `exception` | T | |
| `status`, `reason` | — | T | **[obs]** the `queryMetricsScope` classification we already compute for Prometheus — plus `worker_lost`, `abandoned`, `connection_closed` |
| `retried`, `fallback_used` | — | T | **[obs]** `exec_fallback.go` paths |

---

## 5. Phasing

### Phase 0 — foundations (no new user-visible columns)

1. **Single column registry.** The column list is spelled out in five places
   that must agree: `postgresQueryLogCreateTableSQL`, `postgresQueryLogColumns`
   (partition-repair copy), the `INSERT` text in `insertQueryLogEntries`,
   `queryLogEntryInsertArgs`, and `duckLakeQueryLogViewSelectSQL`. Replace with
   one ordered `[]queryLogColumn{name, pgType, arg func(QueryLogEntry) any}`
   that generates all five. **Adding a column must be a one-line diff** —
   otherwise every phase below is five chances to drift.
2. **Idempotent schema migration**: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
   in `ensurePostgresQueryLogTableContext` (partitioned parents propagate),
   behind the existing `queryLogStorageCache`.
3. **View replace-on-drift**: `verifyDuckLakeQueryLogViewContext` compares the
   view's actual column set against the registry and issues
   `CREATE OR REPLACE VIEW` on mismatch. `CREATE VIEW IF NOT EXISTS` never
   updates an existing tenant.
4. **`query_id` + the observation record** (§2.5, §4) — UUIDv7 minted per query,
   lifecycle bound to the existing `beginQueryMetrics`/`finishQueryMetrics`,
   echoed in the OTEL span and `logQueryError`. `logQuery` keeps its signature
   (no churn at ~40 call sites) and merges the observation.
5. **First `query_log` e2e assertion** in `tests/mw-dev/e2e/harness.sh` — run a
   marked query, assert the round trip through `ducklake.system.query_log` on
   **both** metadata backends (cnpg + ext). None exists today.

**Spike, before any JSON column ships:** how DuckDB's postgres scanner maps
`jsonb` and `text[]` through the view. Default: `JSONB` in Postgres (native,
indexable, queryable by the admin console) with the view casting to `JSON`.
Fallback if lossy: JSON **text** in `TEXT` columns + `CAST(col AS JSON)` in the
view.

### Phase 1 — the event model (partly done)

Done: `QueryStart` / `ExceptionBeforeStart` / `start_events` / `query_id`
propagation to the worker.

Remaining: engine-owned terminals (§2.2), terminal emission anchored on drain
tokens (§2.4), the batched forward RPC (§2.7), paired sampling, and the free
`[profile]` / `[session]` columns.

### Phase 1 — the event model (original scope)

`QueryStart` / `ExceptionBeforeStart` reclassification / engine-owned terminals /
`query_id` propagation via `withSession` / terminal emission anchored on drain
tokens / batched forward RPC / `start_events` + paired sampling. Plus the free
**[profile]** and **[session]** columns, which need no parsing.

Ends the "query that vanished" gap: a `QueryStart` with no terminal older than
the in-flight grace window is a durable, queryable incident signal — effectively
a persistent `pg_stat_activity`.

### Phase 2 — querymeta and the RBAC signals

`server/querymeta` extracted from the `Authorize` walker; one traversal, two
consumers; wired unconditionally at the four choke points; the collapsed
duplicate parses; `query_kind` / `access_kinds` / read+write relations /
columns / table functions / `metadata_complete`; `normalized_query` +
`query_fingerprint`; the LRU.

### Phase 3 — client-supplied metadata

sqlcommenter parsing on the original text; `duckgres.log_comment` and
`duckgres.query_id` GUCs; `client_metadata`; `settings`; client-traceparent span
linkage.

Precedent to respect: `duckgres.query_source` is a **closed enum validated at
SET time** because it is a billing key. `log_comment` / `query_id` are free text
— length-capped, treated as untrusted, never interpolated. They are
attacker-controlled strings that land in a log table and an admin UI.

### Phase 4 — coverage, cost, and shadow RBAC

- **Retention**: monthly `DROP TABLE querylog.query_log_entries_YYYYMM` beyond
  `query_log.retention` (default ~90d) under the advisory-lock pattern the
  partition-create path already uses. **Required before the fat columns ship.**
- **Admin console**: surface the new dimensions and the start/terminal pairing
  on the Queries/Errors pages.
- **Shadow-mode RBAC report**: evaluate a candidate grant set against logged
  `access_kinds` / relations / table functions and report what it *would* have
  denied. This is the artifact that makes enforcement a safe next PR.

---

## 6. Invariants (promote to `CLAUDE.md` when Phase 1 lands)

1. **Never on the hot path** — except the §3 extraction, which is deliberately
   synchronous (RBAC will gate on it) and budgeted for. A metadata or logging
   failure never fails a query.
2. **Redact first, always.** Extraction and enrichment consume post-
   `usersecrets.RedactForLog` / `RedactErrorForLog` text, and are skipped
   entirely for secret DDL. No new sink (`profile_events`, `client_metadata`,
   `normalized_query`, `settings`, `used_table_functions` args) may become a
   path around the redactors.
3. **Never log parameter values.** `param_types` yes, values never.
4. **Empty ≠ failed.** `metadata_complete=false` is mandatory wherever
   extraction was partial. A future gate reading "no relations" from a failed
   parse is a security hole, and this flag is the thing that prevents it.
5. **Reset per query.** Every observation field is cleared at query start; a
   `logQuery` from a path that never began a scope emits zeroes. (The existing
   `lastProfilingSummary` comment documents this exact hazard.)
6. **One terminal per query, anchored on the drain token.** Take one, release
   one, emit one — on every path, including the abandoned-handle reap.
7. **At-least-once, dedupe on read.** Never trade a missing audit row for a
   duplicate one.
8. **Sampling and `start_events` are pair-consistent.** Never half a pair.
9. **Bounded everything.** Query text stays at `maxQueryLength` (4096);
   `normalized_query` 4096; `profile_events` 8 KiB; arrays capped at 64 entries
   with the cap setting `metadata_complete=false`. This table lives in the
   tenant's *metadata* Postgres — the same DB every DuckLake `postgres_scan`
   hits on the query hot path.
10. **Version skew degrades, never breaks.** `wire.QueryLogEntry` is plain JSON:
    a new CP against an old worker loses new fields (the worker's column list
    wins); an old CP against a new worker writes NULLs. The schema migration
    lives worker-side, because the worker owns the INSERT.
11. **The log and the metrics agree by construction.** `status`/`reason` come
    from the same `queryMetricsScope` that feeds `duckgres_query_total` — never
    a second classifier.

## 7. Risks

| risk | mitigation |
| --- | --- |
| Row count 2×, row size 2–4×, on the metadata PG that also serves catalog reads | batched forward RPC, `start_events: data`, paired sampling, retention, caps, `profile_events` as one blob |
| Synchronous parse on every statement (Tier-0 regression) | LRU on `Metadata`, perf-harness measurement before default-on, `metadata: off` kill switch |
| Two emitters → duplicate or missing terminals | drain-token anchoring, explicit ownership rule, duplicate counter, read-side dedupe |
| Column extraction is partial | `columns_resolved` / `metadata_complete`; unresolved means deny in any future gate, never allow |
| RBAC built on table names alone is bypassable | `used_table_functions` as first-class access targets (§3.5) |
| Five-place column drift | Phase 0 registry, before anything else |
| Existing tenants never see new columns | Phase 0 `ADD COLUMN IF NOT EXISTS` + replace-on-drift; assert in e2e against a warehouse provisioned *before* the change |
| `normalized_query_hash` change breaks dashboards | new column; old one untouched until announced retirement |
| sqlcommenter / `log_comment` are attacker-controlled | length caps, no interpolation, treated as data in the admin UI |

## 8. Test obligations

Per `CLAUDE.md`, each phase ships with:

- **Unit**: `server/querylog_test.go` (pair assembly, reset-per-query, redaction
  of every new sink, `ExceptionBeforeStart` classification);
  `server/querymeta/*_test.go` (fixtures for CTE shadowing, writable CTEs, DML
  targets, `information_schema`, `SELECT *`, ambiguous unqualified columns,
  table functions, unparsed → `metadata_complete=false`);
  `duckdbservice/*_test.go` (one terminal per drain token, incl. the
  abandoned-handle reap); `server/querylog_postgres_test.go` (migration
  idempotency, repair-path column parity); `server/querylog_view_test.go`
  (replace-on-drift).
- **Integration** (`tests/integration/`): real Postgres — add a column, restart,
  assert old rows readable and new rows populated.
- **e2e** (`tests/mw-dev/e2e/harness.sh`), on cnpg **and** ext backends:
  - `query_log_pair` — a marked query yields exactly one `QueryStart` and one
    `QueryFinish` sharing a `query_id`, with `read_bytes > 0` on the terminal.
  - `query_log_metadata` — asserts `read_relations` contains the marker table,
    `access_kinds` = `[read]`, and that an `INSERT` yields `[write]`.
  - `query_log_exception_before_start` — a policy-denied or unparseable
    statement yields `ExceptionBeforeStart` and no terminal.
  - `query_log_orphan` — a query whose worker is killed mid-flight leaves a
    `QueryStart` with no `QueryFinish`.
  - `query_log_flight` — a Flight SQL ingress query is logged with
    `interface='flight'`.
- **Docs**: `README.md` §Query Log (column list, the event pair, the read-side
  dedupe rule), this file, and the `CLAUDE.md` invariants block.
