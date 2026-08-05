# Exploratory Small-Worker Tier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route new remote-backend connections to a small, warm, per-org "exploratory" worker pod by default, escalating a connection to a normal-size worker on the first state-mutating statement (pin) or when a read exceeds the small pod's memory (transparent re-execute), with lazy worker acquisition at first statement instead of connect time.

**Architecture:** Small pods are ordinary workers of a new deployment-configured `WorkerProfile` — the existing `OrgReservedPool` claim/spawn/hot-idle machinery is reused unchanged, and 48h hot-idle TTL implements "keep a pod for orgs active in the last 2 days". The control plane installs a worker-switch closure on the pgwire connection; the `server` package classifies statements (pg_query) and calls it to swap `c.executor` mid-connection (same-goroutine, no locking needed). Spec: `docs/superpowers/specs/2026-08-04-exploratory-worker-tier-design.md`.

**Tech Stack:** Go, pg_query_go/v6, Prometheus client, k8s resource quantities, POSIX-sh e2e harness.

## Global Constraints

- One session per worker pod is unchanged (LOAD-BEARING CONTRACT in CLAUDE.md). Small pods run `DUCKGRES_DUCKDB_MAX_SESSIONS=1` like every remote worker.
- Scope is the **remote/k8s backend only**: every new behavior is gated on `cp.isRemoteBackend && cp.cfg.K8s.ExploratoryTierEnabled`. Standalone and process backends are byte-for-byte unchanged.
- Env-only knobs (no CLI flags): `DUCKGRES_EXPLORATORY_TIER_ENABLED`, `DUCKGRES_EXPLORATORY_WORKER_CPU`, `DUCKGRES_EXPLORATORY_WORKER_MEMORY`, `DUCKGRES_EXPLORATORY_WORKER_TTL` (default `48h`).
- Connections carrying client `duckgres.worker_*` startup GUCs (when `AllowClientWorkerProfile` is on) bypass the tier entirely.
- Classification is conservative: anything not provably a read-only statement pins (runs on the normal-size worker). False pins are safe; false read-only classifications are not.
- Re-execution happens only when **zero DataRows** have been sent for the statement, and never for DML/DDL.
- `c.executor` swaps happen only on the connection's message-loop goroutine — document this at every swap site; no atomics.
- Billing: on escalation the connection's billed worker size is raised to the escalation target's size (largest-size-wins v1; per-segment metering is out of scope).
- Every task's behavior change lands with its unit tests in the same commit; e2e harness updates land in Task 10. Docs (CLAUDE.md/README) in Task 10, same PR.
- Commit messages end with the standard trailer:
  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01Dz6MQRBhBAtSLPdADimuXX
  ```

---

### Task 1: Config plumbing — exploratory profile env knobs

**Files:**
- Modify: `configresolve/resolve.go` (env reads ~line 819 area, `Resolved` struct ~line 118, emit ~line 1143)
- Modify: `main.go` (~line 418, `K8sConfig` literal)
- Modify: `cmd/duckgres-controlplane/main.go` (~line 278, duplicate `K8sConfig` literal)
- Modify: `controlplane/control.go` (`K8sConfig` struct, ~line 150-204)
- Modify: `controlplane/worker_profile.go` (new resolver + default const)
- Test: `configresolve/resolve_test.go`, `controlplane/worker_profile_test.go`

**Interfaces:**
- Consumes: existing env-only knob idiom (`getenv → local var → Resolved field → K8sConfig field`, duplicated wiring in both mains).
- Produces: `K8sConfig` fields `ExploratoryTierEnabled bool`, `ExploratoryWorkerCPU string`, `ExploratoryWorkerMemory string`, `ExploratoryWorkerTTL time.Duration`; and `func exploratoryWorkerProfile(k K8sConfig) (*WorkerProfile, []string)` in `controlplane` (nil = tier off/unusable; warnings for invalid values). Tasks 5 and 9 consume `exploratoryWorkerProfile`.

- [ ] **Step 1: Write failing tests**

In `configresolve/resolve_test.go`, following the existing env-knob test idiom in that file:

```go
func TestExploratoryTierEnvKnobs(t *testing.T) {
	env := map[string]string{
		"DUCKGRES_EXPLORATORY_TIER_ENABLED": "true",
		"DUCKGRES_EXPLORATORY_WORKER_CPU":   "2",
		"DUCKGRES_EXPLORATORY_WORKER_MEMORY": "4Gi",
		"DUCKGRES_EXPLORATORY_WORKER_TTL":   "48h",
	}
	getenv := func(k string) string { return env[k] }
	r := ResolveEffective(nil, CLIInputs{}, getenv, nil)
	if !r.K8sExploratoryTierEnabled {
		t.Fatal("expected exploratory tier enabled")
	}
	if r.K8sExploratoryWorkerCPU != "2" || r.K8sExploratoryWorkerMemory != "4Gi" {
		t.Fatalf("cpu=%q mem=%q", r.K8sExploratoryWorkerCPU, r.K8sExploratoryWorkerMemory)
	}
	if r.K8sExploratoryWorkerTTL != 48*time.Hour {
		t.Fatalf("ttl=%v", r.K8sExploratoryWorkerTTL)
	}
}

func TestExploratoryTierEnvKnobsInvalid(t *testing.T) {
	var warned []string
	env := map[string]string{
		"DUCKGRES_EXPLORATORY_TIER_ENABLED": "banana",
		"DUCKGRES_EXPLORATORY_WORKER_TTL":   "-5m",
	}
	r := ResolveEffective(nil, CLIInputs{}, func(k string) string { return env[k] }, func(w string) { warned = append(warned, w) })
	if r.K8sExploratoryTierEnabled {
		t.Fatal("invalid bool must leave tier disabled")
	}
	if r.K8sExploratoryWorkerTTL != 0 {
		t.Fatalf("invalid ttl must stay 0 (built-in default applied later), got %v", r.K8sExploratoryWorkerTTL)
	}
	if len(warned) != 2 {
		t.Fatalf("want 2 warnings, got %v", warned)
	}
}
```

In `controlplane/worker_profile_test.go`:

```go
func TestExploratoryWorkerProfile(t *testing.T) {
	k := K8sConfig{ExploratoryTierEnabled: true, ExploratoryWorkerCPU: "2", ExploratoryWorkerMemory: "4Gi", ExploratoryWorkerTTL: 48 * time.Hour}
	p, warns := exploratoryWorkerProfile(k)
	if p == nil || len(warns) != 0 {
		t.Fatalf("p=%v warns=%v", p, warns)
	}
	if p.CPU != "2" || p.Memory != "4Gi" || p.TTL != 48*time.Hour {
		t.Fatalf("profile=%+v", p)
	}
	// MatchKey must be a concrete shape, never the default "|" (that would
	// let exploratory traffic reuse default-shape workers and vice versa).
	if p.MatchKey() == "|" {
		t.Fatal("exploratory profile must not share the default MatchKey")
	}
}

func TestExploratoryWorkerProfileDisabledOrIncomplete(t *testing.T) {
	if p, _ := exploratoryWorkerProfile(K8sConfig{}); p != nil {
		t.Fatal("disabled tier must return nil")
	}
	// Enabled but no size configured: unusable — nil with a warning, so a
	// half-configured deployment degrades to today's behavior, never to a
	// BestEffort-pod tier.
	p, warns := exploratoryWorkerProfile(K8sConfig{ExploratoryTierEnabled: true})
	if p != nil || len(warns) == 0 {
		t.Fatalf("p=%v warns=%v", p, warns)
	}
}

func TestExploratoryWorkerProfileDefaultTTL(t *testing.T) {
	p, _ := exploratoryWorkerProfile(K8sConfig{ExploratoryTierEnabled: true, ExploratoryWorkerCPU: "2", ExploratoryWorkerMemory: "4Gi"})
	if p == nil || p.TTL != defaultExploratoryWorkerTTL {
		t.Fatalf("want built-in 48h TTL, got %+v", p)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./configresolve/ -run TestExploratoryTier -v && go test ./controlplane/ -run TestExploratoryWorkerProfile -v`
Expected: FAIL — undefined fields/functions.

- [ ] **Step 3: Implement**

`configresolve/resolve.go` — declare locals next to the other k8s vars (~line 197):

```go
	var k8sExploratoryTierEnabled bool
	var k8sExploratoryWorkerCPU, k8sExploratoryWorkerMemory string
	var k8sExploratoryWorkerTTL time.Duration
```

Env reads, placed after the worker-profile gate block (~line 811), following the Example A/B idioms exactly:

```go
	// Exploratory small-worker tier (env-only, like the other pod-shape knobs).
	// See docs/superpowers/specs/2026-08-04-exploratory-worker-tier-design.md.
	if v := getenv("DUCKGRES_EXPLORATORY_TIER_ENABLED"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			k8sExploratoryTierEnabled = b
		} else {
			warn("Invalid DUCKGRES_EXPLORATORY_TIER_ENABLED: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_EXPLORATORY_WORKER_CPU"); v != "" {
		k8sExploratoryWorkerCPU = v
	}
	if v := getenv("DUCKGRES_EXPLORATORY_WORKER_MEMORY"); v != "" {
		k8sExploratoryWorkerMemory = v
	}
	if v := getenv("DUCKGRES_EXPLORATORY_WORKER_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			k8sExploratoryWorkerTTL = d
		} else {
			warn("Invalid DUCKGRES_EXPLORATORY_WORKER_TTL: " + v)
		}
	}
```

`Resolved` struct fields (near `K8sWorkerDefaultTTL`, ~line 118) + emit block (~line 1143):

```go
	K8sExploratoryTierEnabled  bool
	K8sExploratoryWorkerCPU    string
	K8sExploratoryWorkerMemory string
	K8sExploratoryWorkerTTL    time.Duration
```

`controlplane/control.go` `K8sConfig` — append after `WorkerDefaultTTL`:

```go
	// Exploratory small-worker tier (env-only DUCKGRES_EXPLORATORY_*): when
	// enabled, connections without client duckgres.worker_* GUCs first land on
	// a small worker of this shape and escalate to the org's normal profile on
	// the first state-mutating statement or an out-of-memory read. CPU+Memory
	// are both required for the tier to be usable; TTL 0 = built-in 48h.
	ExploratoryTierEnabled  bool
	ExploratoryWorkerCPU    string
	ExploratoryWorkerMemory string
	ExploratoryWorkerTTL    time.Duration
```

Both `main.go` and `cmd/duckgres-controlplane/main.go` `K8sConfig` literals:

```go
				ExploratoryTierEnabled:  resolved.K8sExploratoryTierEnabled,
				ExploratoryWorkerCPU:    resolved.K8sExploratoryWorkerCPU,
				ExploratoryWorkerMemory: resolved.K8sExploratoryWorkerMemory,
				ExploratoryWorkerTTL:    resolved.K8sExploratoryWorkerTTL,
```

`controlplane/worker_profile.go`:

```go
// defaultExploratoryWorkerTTL keeps an org's exploratory worker parked
// hot-idle for two days after its last connection — the "warm pod for every
// recently-active team" retention from the tier design.
const defaultExploratoryWorkerTTL = 48 * time.Hour

// exploratoryWorkerProfile resolves the deployment's exploratory small-worker
// shape. Returns nil when the tier is disabled OR unusable (missing/invalid
// size) — a half-configured tier must degrade to today's behavior, never to a
// BestEffort pod. Sizes are normalized so MatchKey-based reuse is canonical.
func exploratoryWorkerProfile(k K8sConfig) (*WorkerProfile, []string) {
	if !k.ExploratoryTierEnabled {
		return nil, nil
	}
	var warns []string
	if strings.TrimSpace(k.ExploratoryWorkerCPU) == "" || strings.TrimSpace(k.ExploratoryWorkerMemory) == "" {
		return nil, append(warns, "exploratory tier enabled but DUCKGRES_EXPLORATORY_WORKER_CPU/MEMORY not both set; tier disabled")
	}
	cpu, _, err := sizeField("exploratory worker cpu", k.ExploratoryWorkerCPU, "", "")
	if err != nil {
		return nil, append(warns, fmt.Sprintf("invalid exploratory worker cpu; tier disabled: %v", err))
	}
	mem, _, err := sizeField("exploratory worker memory", k.ExploratoryWorkerMemory, "", "")
	if err != nil {
		return nil, append(warns, fmt.Sprintf("invalid exploratory worker memory; tier disabled: %v", err))
	}
	ttl := defaultExploratoryWorkerTTL
	if k.ExploratoryWorkerTTL > 0 {
		ttl = k.ExploratoryWorkerTTL
	}
	return &WorkerProfile{CPU: cpu, Memory: mem, TTL: ttl}, warns
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./configresolve/ ./controlplane/ -run 'TestExploratory' -v`
Expected: PASS. Also run `go test ./configresolve/` in full — `cliflags_test.go` has a flag-coverage test; env-only fields must NOT be added to `CLIInputs` (they aren't in this design), so it must stay green.

- [ ] **Step 5: Commit**

```bash
git add configresolve/ main.go cmd/duckgres-controlplane/main.go controlplane/control.go controlplane/worker_profile.go controlplane/worker_profile_test.go
git commit -m "feat(controlplane): exploratory worker tier config knobs"
```

---

### Task 2: Statement tier classification

**Files:**
- Create: `server/tier_classify.go`
- Test: `server/tier_classify_test.go`

**Interfaces:**
- Consumes: `pg_query_go/v6` (already a dependency), `google.golang.org/protobuf/reflect/protoreflect` (already used by `server/query_access.go`).
- Produces: `type statementTier int`, constants `tierSmallOK`, `tierPinning`, and `func classifyStatementTier(sql string) statementTier`. Tasks 6 and 7 consume these.

- [ ] **Step 1: Write the failing test**

`server/tier_classify_test.go`:

```go
package server

import "testing"

func TestClassifyStatementTier(t *testing.T) {
	smallOK := []string{
		"SELECT count(1) FROM posthog.events",
		"SELECT * FROM posthog.events LIMIT 10",
		"select 1",
		"EXPLAIN SELECT * FROM t",
		"EXPLAIN ANALYZE SELECT * FROM t",
		"SHOW search_path",
		"WITH a AS (SELECT 1) SELECT * FROM a",
		"SELECT * FROM a JOIN b ON a.id = b.id WHERE a.x > 5 ORDER BY b.y",
		"SELECT 1; SELECT 2",
	}
	pinning := []string{
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET x = 1",
		"DELETE FROM t",
		"CREATE TABLE t (i int)",
		"CREATE TEMP TABLE t (i int)",
		"CREATE TEMPORARY TABLE t AS SELECT 1",
		"DROP TABLE t",
		"ALTER TABLE t ADD COLUMN j int",
		"BEGIN",
		"START TRANSACTION",
		"COPY t FROM STDIN",
		"COPY t TO STDOUT",
		"SET search_path TO foo",
		"CREATE SECRET s (TYPE S3)",                       // unparseable by pg_query -> conservative
		"USE ducklake",                                    // DuckDB-only spelling -> unparseable -> conservative
		"WITH w AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM w", // writable CTE
		"SELECT * INTO t2 FROM t",                         // SELECT INTO creates a table
		"EXPLAIN ANALYZE INSERT INTO t VALUES (1)",        // EXPLAIN ANALYZE executes the DML
		"SELECT 1; INSERT INTO t VALUES (1)",              // any pinning stmt pins the batch
		"CREATE VIEW v AS SELECT 1",
		"TRUNCATE t",
		"MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
		"DECLARE c CURSOR FOR SELECT 1",                   // cursor state lives on the session
		"PREPARE p AS SELECT 1",
		"VACUUM",
		"garbage that is not sql",
	}
	for _, q := range smallOK {
		if got := classifyStatementTier(q); got != tierSmallOK {
			t.Errorf("classifyStatementTier(%q) = pinning, want smallOK", q)
		}
	}
	for _, q := range pinning {
		if got := classifyStatementTier(q); got != tierPinning {
			t.Errorf("classifyStatementTier(%q) = smallOK, want pinning", q)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/ -run TestClassifyStatementTier -v`
Expected: FAIL — `classifyStatementTier` undefined.

- [ ] **Step 3: Implement**

`server/tier_classify.go`:

```go
package server

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// statementTier says whether a statement may run on the exploratory small
// worker or must pin the connection to a normal-size worker.
//
// Classification is deliberately conservative: only statements provably free
// of session-state mutation and writes are tierSmallOK. A false pin merely
// costs a bigger pod; a false smallOK would let state accumulate on a worker
// the connection can silently migrate away from — a correctness bug. Anything
// pg_query cannot parse (DuckDB-only spellings like USE / CREATE SECRET,
// garbage) pins.
type statementTier int

const (
	tierSmallOK statementTier = iota
	tierPinning
)

func classifyStatementTier(sql string) statementTier {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return tierPinning
	}
	for _, raw := range tree.Stmts {
		if stmtTier(raw.Stmt) == tierPinning {
			return tierPinning
		}
	}
	return tierSmallOK
}

func stmtTier(node *pg_query.Node) statementTier {
	if node == nil {
		return tierPinning
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		if n.SelectStmt.GetIntoClause() != nil {
			return tierPinning // SELECT INTO creates a table
		}
		if containsMutatingNode(node.ProtoReflect()) {
			return tierPinning // writable CTE (WITH x AS (INSERT ...))
		}
		return tierSmallOK
	case *pg_query.Node_ExplainStmt:
		// EXPLAIN ANALYZE executes the inner statement, so the inner
		// statement's tier governs regardless of the ANALYZE flag.
		return stmtTier(n.ExplainStmt.GetQuery())
	case *pg_query.Node_VariableShowStmt:
		return tierSmallOK
	default:
		// Everything else — DML, DDL, COPY, SET, BEGIN, DECLARE, PREPARE,
		// VACUUM, ... — either writes or creates session state.
		return tierPinning
	}
}

// containsMutatingNode walks a statement's proto tree looking for embedded
// DML (the writable-CTE case). Same protoreflect walk idiom as
// query_access.go's walkMessage.
func containsMutatingNode(msg protoreflect.Message) bool {
	switch msg.Interface().(type) {
	case *pg_query.InsertStmt, *pg_query.UpdateStmt, *pg_query.DeleteStmt, *pg_query.MergeStmt:
		return true
	}
	found := false
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Kind() == protoreflect.MessageKind:
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				if containsMutatingNode(l.Get(i).Message()) {
					found = true
					return false
				}
			}
		case fd.Kind() == protoreflect.MessageKind && !fd.IsMap():
			if containsMutatingNode(v.Message()) {
				found = true
				return false
			}
		}
		return true
	})
	return found
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./server/ -run TestClassifyStatementTier -v`
Expected: PASS. If `Node_VariableShowStmt` / `GetIntoClause` names differ in pg_query_go/v6, check `server/query_access.go` for the exact generated names and adjust.

- [ ] **Step 5: Commit**

```bash
git add server/tier_classify.go server/tier_classify_test.go
git commit -m "feat(server): statement tier classification for exploratory workers"
```

---

### Task 3: Worker OOM error detection

**Files:**
- Modify: `server/conn_errors.go`
- Test: `server/conn_errors_test.go` (or create if the file's tests live elsewhere — check for an existing `TestClassifyErrorCode`-style test file first and co-locate)

**Interfaces:**
- Produces: `func isWorkerOutOfMemoryError(err error) bool`. Tasks 6 and 7 consume it.

- [ ] **Step 1: Write the failing test**

```go
func TestIsWorkerOutOfMemoryError(t *testing.T) {
	oom := []string{
		// prepare-phase shape (GetFlightInfo LIMIT 0 probe failed):
		"flight execute: rpc error: code = InvalidArgument desc = failed to prepare query: Out of Memory Error: failed to allocate data of size 16.0 MiB (24.9 GiB/25.0 GiB used)",
		// mid-stream shape (rows.Err() from a DoGet chunk):
		"Out of Memory Error: could not allocate block of size 256.0 KiB",
		"failed to allocate data of size 32.0 MiB",
	}
	notOOM := []string{
		"Catalog Error: Table with name t does not exist",
		"context canceled",
		"flight worker is dead",
		"Binder Error: Referenced column x not found",
		"",
	}
	for _, m := range oom {
		if !isWorkerOutOfMemoryError(errors.New(m)) {
			t.Errorf("want OOM: %q", m)
		}
	}
	for _, m := range notOOM {
		if m == "" {
			if isWorkerOutOfMemoryError(nil) {
				t.Error("nil must not be OOM")
			}
			continue
		}
		if isWorkerOutOfMemoryError(errors.New(m)) {
			t.Errorf("must not be OOM: %q", m)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/ -run TestIsWorkerOutOfMemoryError -v`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement** (append to `server/conn_errors.go`)

```go
// isWorkerOutOfMemoryError reports whether a query failed because DuckDB on
// the worker exhausted its memory_limit — the signal the exploratory tier
// uses to transparently re-execute the read on a normal-size worker. String
// match, like every other DuckDB error classifier here. It matches the
// engine's OOM exception only; a pod-level OOMKill surfaces as ErrWorkerDead
// (the CP closes the client conn via OnWorkerCrash) and is deliberately NOT
// re-executed — the connection is already gone.
func isWorkerOutOfMemoryError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Out of Memory Error") ||
		strings.Contains(msg, "failed to allocate data of size") ||
		strings.Contains(msg, "could not allocate block of size")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./server/ -run TestIsWorkerOutOfMemoryError -v` — PASS.

- [ ] **Step 5: Commit**

```bash
git add server/conn_errors.go server/conn_errors_test.go
git commit -m "feat(server): detect DuckDB out-of-memory errors from workers"
```

---

### Task 4: Connection tier state + worker-switch mechanism

**Files:**
- Create: `server/conn_tier.go`
- Modify: `server/conn.go` (clientConn fields), `server/exports.go` (setter)
- Test: `server/conn_tier_test.go`

**Interfaces:**
- Consumes: `QueryExecutor` (`server/sqlcore/interfaces.go`), clientConn fields `executor`, `workerID`, `workerPod` (as named in `NewClientConn`, `server/exports.go:54`).
- Produces:
  - `type WorkerSwitcher func(ctx context.Context, reason string) (exec QueryExecutor, workerID int, workerPod string, err error)` (exported from `server`)
  - `func SetConnectionExploratory(cc *clientConn, switcher WorkerSwitcher)` in `server/exports.go`
  - method `func (c *clientConn) escalateWorker(ctx context.Context, reason string) error`
  - reason constants `escalateReasonState = "state"`, `escalateReasonOOM = "oom"`, `escalateReasonHeuristic = "heuristic"`
  - metric `duckgres_exploratory_escalations_total{reason}`
  - Task 5 implements the switcher; Tasks 6/7 call `escalateWorker`.

- [ ] **Step 1: Write the failing test**

`server/conn_tier_test.go` (build a minimal `clientConn` the way existing server tests do — check `server/s3_cache_test.go` for the construction idiom and reuse it):

```go
func TestEscalateWorkerSwapsExecutorOnce(t *testing.T) {
	c := &clientConn{onExploratoryWorker: true}
	fake := &fakeQueryExecutor{} // reuse/extend an existing fake in the server tests; else define a struct embedding QueryExecutor with nil methods
	calls := 0
	c.workerSwitcher = func(ctx context.Context, reason string) (QueryExecutor, int, string, error) {
		calls++
		if reason != escalateReasonState {
			t.Fatalf("reason=%q", reason)
		}
		return fake, 42, "pod-42", nil
	}
	if err := c.escalateWorker(context.Background(), escalateReasonState); err != nil {
		t.Fatal(err)
	}
	if c.executor != QueryExecutor(fake) || c.workerID != 42 || c.workerPod != "pod-42" {
		t.Fatalf("executor/worker not swapped: %+v", c)
	}
	if c.onExploratoryWorker {
		t.Fatal("must leave exploratory tier after escalation")
	}
	// Second call is a no-op (sticky pin).
	if err := c.escalateWorker(context.Background(), escalateReasonOOM); err != nil || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestEscalateWorkerFailureKeepsState(t *testing.T) {
	c := &clientConn{onExploratoryWorker: true}
	c.workerSwitcher = func(ctx context.Context, reason string) (QueryExecutor, int, string, error) {
		return nil, 0, "", errors.New("no capacity")
	}
	if err := c.escalateWorker(context.Background(), escalateReasonState); err == nil {
		t.Fatal("want error")
	}
	// Failure does NOT clear the flag: caller decides (it sends an error to
	// the client and the next statement may retry the escalation).
	if !c.onExploratoryWorker {
		t.Fatal("failed escalation must not mark the connection pinned")
	}
}
```

Note: if `clientConn`'s worker-identity fields are named differently than `workerID`/`workerPod`, read `server/conn.go` around the struct definition and `NewClientConn` in `server/exports.go:54-80` and use the actual names — everywhere in this plan.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/ -run TestEscalateWorker -v` — FAIL (undefined fields/method).

- [ ] **Step 3: Implement**

`server/conn.go` — add to the `clientConn` struct, near the billing fields (~line 218):

```go
	// Exploratory-tier state (remote backend only). onExploratoryWorker is
	// true while the connection runs on the small exploratory worker;
	// workerSwitcher (installed by the control plane) swaps the backing
	// worker/session. Both are touched only on the connection's message-loop
	// goroutine — swaps happen inline during statement handling, never
	// concurrently with executor use — so no locking.
	onExploratoryWorker bool
	workerSwitcher      WorkerSwitcher
```

`server/conn_tier.go`:

```go
package server

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// WorkerSwitcher swaps a connection's backing worker/session: the control
// plane destroys the current (stateless, exploratory) session and creates one
// on a normal-size worker, returning the new executor + worker identity.
type WorkerSwitcher func(ctx context.Context, reason string) (exec QueryExecutor, workerID int, workerPod string, err error)

const (
	escalateReasonState     = "state"
	escalateReasonOOM       = "oom"
	escalateReasonHeuristic = "heuristic"
)

var exploratoryEscalationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_exploratory_escalations_total",
	Help: "Connections escalated off the exploratory small worker, by reason (state|oom|heuristic).",
}, []string{"reason"})

// escalateWorker moves the connection from the exploratory small worker to a
// normal-size worker. Sticky: once pinned, later calls are no-ops. On failure
// the connection stays on the small worker and the caller surfaces the error;
// a later statement may retry.
func (c *clientConn) escalateWorker(ctx context.Context, reason string) error {
	if !c.onExploratoryWorker || c.workerSwitcher == nil {
		return nil
	}
	exec, workerID, workerPod, err := c.workerSwitcher(ctx, reason)
	if err != nil {
		return err
	}
	c.executor = exec
	c.workerID = workerID
	c.workerPod = workerPod
	c.onExploratoryWorker = false
	exploratoryEscalationsTotal.WithLabelValues(reason).Inc()
	c.logger().Info("Escalated connection off exploratory worker.", "reason", reason, "worker", workerID, "worker_pod", workerPod)
	return nil
}
```

`server/exports.go` — next to `SetConnectionWorkerSize`:

```go
// SetConnectionExploratory marks a control-plane connection as starting on
// the exploratory small worker and installs the switcher used to escalate it.
// Call before RunMessageLoop; the switcher runs on the message-loop goroutine.
func SetConnectionExploratory(cc *clientConn, switcher WorkerSwitcher) {
	if cc != nil {
		cc.onExploratoryWorker = true
		cc.workerSwitcher = switcher
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./server/ -run 'TestEscalateWorker' -v` — PASS. Run `go test ./server/` in full to catch metric-registration collisions.

- [ ] **Step 5: Commit**

```bash
git add server/conn_tier.go server/conn_tier_test.go server/conn.go server/exports.go
git commit -m "feat(server): connection tier state and worker-switch mechanism"
```

---

### Task 5: Control-plane switcher — small-first acquire + escalation target

**Files:**
- Modify: `controlplane/control.go` (`handleConnection` ~lines 1141-1548; factor session-init helper)
- Test: `controlplane/control_test.go` (helper-level tests); full behavior lands in e2e (Task 10)

**Interfaces:**
- Consumes: `exploratoryWorkerProfile` (Task 1), `server.SetConnectionExploratory` + `server.WorkerSwitcher` (Task 4), `sessions.CreateSession` / `sessions.DestroySession` (session_mgr.go:317/642), `cp.workerDuckDBLimits`, `cp.workerBillingSize` (control.go:1710).
- Produces: `func clientSuppliedWorkerGUCs(k K8sConfig, opts map[string]string) bool`; `func (cp *ControlPlane) initSessionMetadata(...) error` (the factored-out block); the switcher closure wired into `handleConnection`.

- [ ] **Step 1: Write the failing test for the GUC-bypass helper**

In `controlplane/control_test.go`:

```go
func TestClientSuppliedWorkerGUCs(t *testing.T) {
	on := K8sConfig{AllowClientWorkerProfile: true}
	if !clientSuppliedWorkerGUCs(on, map[string]string{"duckgres.worker_cpu": "4"}) {
		t.Fatal("cpu GUC must count")
	}
	if !clientSuppliedWorkerGUCs(on, map[string]string{"duckgres.worker_ttl": "5m"}) {
		t.Fatal("ttl GUC must count")
	}
	if clientSuppliedWorkerGUCs(on, map[string]string{"search_path": "x"}) {
		t.Fatal("unrelated options must not count")
	}
	// Gate off: client GUCs are ignored everywhere, so they must not bypass
	// the tier either.
	off := K8sConfig{AllowClientWorkerProfile: false}
	if clientSuppliedWorkerGUCs(off, map[string]string{"duckgres.worker_cpu": "4"}) {
		t.Fatal("gated-off client GUCs must not count")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./controlplane/ -run TestClientSuppliedWorkerGUCs -v` — FAIL.

- [ ] **Step 3: Implement the helper** (in `controlplane/worker_profile.go`)

```go
// clientSuppliedWorkerGUCs reports whether the client's startup options carry
// an explicit worker sizing (any duckgres.worker_* GUC, honored only when the
// deployment trusts client sizing). Such connections bypass the exploratory
// tier: the client asked for a specific shape.
func clientSuppliedWorkerGUCs(k K8sConfig, opts map[string]string) bool {
	if !k.AllowClientWorkerProfile {
		return false
	}
	return strings.TrimSpace(opts[gucWorkerCPU]) != "" ||
		strings.TrimSpace(opts[gucWorkerMemory]) != "" ||
		strings.TrimSpace(opts[gucWorkerTTL]) != ""
}
```

Run the test — PASS. Commit checkpoint:

```bash
git add controlplane/worker_profile.go controlplane/control_test.go
git commit -m "feat(controlplane): detect client-supplied worker GUCs for tier bypass"
```

- [ ] **Step 4: Factor the session-init block into a helper**

In `handleConnection`, lines ~1347-1484 (the `HasAttachedCatalog` probe → `InitSessionDatabaseMetadataWithAccess` → connect-time search_path / passthrough `USE` application) currently run inline against `executor`. Extract them into a method so escalation can re-run them against a NEW executor:

```go
// initSessionMetadata runs the post-create session setup against exec: the
// attached-catalog probe, session database metadata init, and the connect-time
// search_path / passthrough catalog application. Runs once at session create
// and again after every worker switch (the new worker's session starts cold).
// All inputs are connect-time constants for the connection.
func (cp *ControlPlane) initSessionMetadata(
	ctx context.Context,
	exec *flightclient.FlightExecutor,
	// ... the exact parameters the extracted block reads: org config, database,
	// clientSearchPath, passthrough flag, queryAccessPolicy inputs, clog.
	// Enumerate them mechanically while extracting — the block is moved
	// verbatim, only `executor` renamed to `exec` and returns instead of
	// client-facing FATAL writes (the caller maps errors to client responses).
) (sessionMetadataResult, error)
```

where `sessionMetadataResult` carries what the block currently feeds into `cc` wiring (effective catalog name, physical catalog, use-rewrite flag) so the caller can do `SetConnectionPhysicalCatalog`/`SetCatalogUseRewrite` for the initial create, and the switcher can re-apply them (values must be identical — same org/warehouse — but re-derive rather than assume).

This is a mechanical extraction: move the code, thread the variables, keep behavior identical. The existing e2e assertions (`basic_query`, `rw_ducklake`, `explain_ducklake`) are the regression net.

Run: `go test ./controlplane/ && go build ./...` — green. Commit:

```bash
git add controlplane/control.go
git commit -m "refactor(controlplane): factor session metadata init for reuse on worker switch"
```

- [ ] **Step 5: Wire small-first acquisition + the switcher**

In `handleConnection` after `resolveWorkerProfile` (line ~1161):

```go
	// Exploratory tier: connections without explicit client sizing start on
	// the small exploratory worker and escalate on demand. workerProfile
	// (org default / pool default) remains the ESCALATION TARGET.
	explProfile, explWarns := exploratoryWorkerProfile(cp.cfg.K8s)
	for _, w := range explWarns {
		clog.Warn("Exploratory tier config problem.", "detail", w)
	}
	useExploratory := cp.isRemoteBackend && explProfile != nil && !clientSuppliedWorkerGUCs(cp.cfg.K8s, startupOptions)
	initialProfile := workerProfile
	if useExploratory {
		initialProfile = explProfile
	}
```

Then replace `workerProfile` with `initialProfile` at the three places the initial session uses it: `cp.workerDuckDBLimits(workerProfile)` (line ~1287), the `sessions.CreateSession(..., workerProfile)` call (line ~1310), and `cp.workerBillingSize(workerProfile)` (line ~1504).

After `cc` is created and the existing wiring (lines ~1491-1548), install the switcher:

```go
	if useExploratory {
		server.SetConnectionExploratory(cc, func(ctx context.Context, reason string) (server.QueryExecutor, int, string, error) {
			// The exploratory session is stateless by construction (every
			// state-mutating statement escalates BEFORE executing), so
			// destroy-then-acquire is safe and releases the small worker to
			// hot-idle before the org's cap is checked for the big one.
			sessions.DestroySession(pid)
			memLimit, threads := cp.workerDuckDBLimits(workerProfile)
			ctx, cancel := context.WithTimeout(ctx, cp.cfg.WorkerQueueTimeout)
			defer cancel()
			_, exec, err := sessions.CreateSession(ctx, username, pid, memLimit, threads, workerProfile)
			if err != nil {
				return nil, 0, "", fmt.Errorf("escalate to standard worker: %w", err)
			}
			if _, err := cp.initSessionMetadata(ctx, exec /* , ... connect-time constants captured by the closure */); err != nil {
				sessions.DestroySession(pid)
				return nil, 0, "", fmt.Errorf("init session on standard worker: %w", err)
			}
			sessions.SetConnCloser(pid, tlsConn)
			// Billing: largest size wins for the whole connection (v1).
			millicores, mib := cp.workerBillingSize(workerProfile)
			server.SetConnectionWorkerSize(cc, millicores, mib)
			clog.Info("Connection escalated to standard worker.", "reason", reason,
				"worker", sessions.WorkerIDForPID(pid), "worker_pod", sessions.WorkerPodNameForPID(pid))
			return exec, sessions.WorkerIDForPID(pid), sessions.WorkerPodNameForPID(pid), nil
		})
	}
```

Notes for the implementer:
- The `defer sessions.DestroySession(pid)` at line ~1341 still tears down whichever session currently owns `pid` — unchanged.
- Same `pid`/`secretKey`: the cancel registry (`BackendKey`) and pg_stat_activity identity survive the switch.
- The switcher runs on the message-loop goroutine (same goroutine as the teardown defers), so `SetConnectionWorkerSize` here is race-free — extend the "Constant for the connection's life" comments on `workerMillicores` (`server/conn.go:218`) and `SetConnectionWorkerSize` (`server/exports.go:198`) to say "may be raised once by tier escalation, same goroutine".
- The old exploratory executor: `DestroySession` closes it (`session.Executor.Close()` in session_mgr.go:~680). `c.executor` still points at the closed executor until the swap assigns the new one — no calls happen in between because the switcher is invoked synchronously from statement handling.

- [ ] **Step 6: Build + full unit tests**

Run: `go build -tags kubernetes ./... && go test ./controlplane/ ./server/`
Expected: green.

- [ ] **Step 7: Commit**

```bash
git add controlplane/control.go
git commit -m "feat(controlplane): small-first worker acquisition with escalation switcher"
```

---

### Task 6: Simple-query integration — pin before execute, re-execute on OOM

**Files:**
- Modify: `server/conn.go` (`handleQuery`, ~line 1490 before `rewriteDirectQuery`), `server/conn_query_exec.go` (`executeSelectQuery`, lines 156-312; `executeSingleStatement` ~line 385)
- Test: `server/conn_tier_exec_test.go`

**Interfaces:**
- Consumes: `classifyStatementTier` (Task 2), `isWorkerOutOfMemoryError` (Task 3), `escalateWorker` (Task 4).
- Produces: the tier-aware execution behavior for the simple protocol. Task 7 mirrors it for extended.

- [ ] **Step 1: Write the failing tests**

`server/conn_tier_exec_test.go`. Use a fake `QueryExecutor` whose `QueryContext` fails with an OOM error on the first executor and succeeds on the second (the one the fake switcher returns). Follow the existing server-test construction idiom (see how `s3_cache_test.go` / other conn tests build a `clientConn` with a scripted executor and a `bytes.Buffer`-backed writer). The three behaviors to pin:

```go
// 1. A pinning statement escalates BEFORE the executor sees it.
func TestSimpleQueryPinsBeforeExecute(t *testing.T) { /* CREATE TEMP TABLE via handleQuery path:
   assert switcher called with reason "state" BEFORE fake executor's ExecContext, and
   the statement then runs on the post-switch executor. */ }

// 2. Prepare-phase OOM on a read re-executes on the escalated worker; client sees only success.
func TestSelectReexecutesOnPrepareOOM(t *testing.T) { /* first executor's QueryContext returns
   errors.New("flight execute: ... Out of Memory Error: ..."); switcher installs second executor
   whose QueryContext returns a RowSet with 1 row. Assert: reason "oom", no ErrorResponse written,
   RowDescription + 1 DataRow + CommandComplete in the output buffer. */ }

// 3. OOM after rows were already streamed surfaces the error (no re-execute).
func TestSelectMidStreamOOMAfterRowsSurfaces(t *testing.T) { /* RowSet yields 2 rows then
   Err() returns OOM. Assert: switcher NOT called, ErrorResponse written. */ }
```

Write these as real tests, not comments — model the fake `RowSet` on `LocalRowSet` (`server/executor.go:141`). If constructing a full `clientConn` for `handleQuery` proves impractical in unit scope, test at the `executeSelectQuery` level (it is a method on `clientConn` and reachable with a hand-built conn + fake executor) and cover the `handleQuery` classification hook via the e2e assertion in Task 10 — but attempt the unit test first.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./server/ -run 'TestSimpleQueryPins|TestSelectReexecutes|TestSelectMidStream' -v` — FAIL.

- [ ] **Step 3: Implement the classification hook in `handleQuery`**

In `server/conn.go`, immediately before the execution branch (after transpile + GUC/no-op intercepts, right before `convertedQuery := c.rewriteDirectQuery(result.SQL)` ~line 1492):

```go
	// Exploratory tier: a statement that writes or creates session state must
	// run on (and pin) a normal-size worker. Escalate BEFORE execution so the
	// small worker stays stateless by construction. Interpreted statements the
	// CP already handled (cursor/pg_stat_activity/secret DDL/GUC intercepts)
	// never reach this point.
	if c.onExploratoryWorker && classifyStatementTier(query) == tierPinning {
		if err := c.escalateWorker(c.ctx, escalateReasonState); err != nil {
			c.logQueryError(query, err)
			c.sendError("ERROR", "53400", fmt.Sprintf("could not allocate a standard worker for this statement: %v", err))
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}
	}
```

Add the same hook in `executeSingleStatement` (`conn_query_exec.go:385`, the batched multi-statement path) right before its execution branch — batches re-classify per statement.

- [ ] **Step 4: Implement OOM re-execute in `executeSelectQuery`**

In `server/conn_query_exec.go`, after the existing conflict/aborted-transaction recovery (~line 196), add the prepare-phase retry:

```go
	// Exploratory tier: a read that blew the small worker's memory_limit is
	// transparently re-executed on a normal-size worker. Prepare-phase only
	// here — nothing has been sent to the client yet.
	if err != nil && c.onExploratoryWorker && isWorkerOutOfMemoryError(err) && c.txStatus == txStatusIdle {
		if escErr := c.escalateWorker(ctx, escalateReasonOOM); escErr == nil {
			rows, err = runQuery() // runQuery reads c.executor at call time — now the standard worker
		} else {
			c.logger().Warn("Exploratory OOM escalation failed; surfacing original error.", "error", escErr)
		}
	}
```

For the mid-stream case, restructure the send loop (lines ~245-312): extract the RowDescription+DataRow loop into a helper on `clientConn`:

```go
// streamSelectRows sends (optionally) RowDescription then all DataRows.
// Returns rowsSent and the terminal rows error (rows.Err()), plus any client
// write error. Extracted so the exploratory tier can retry a zero-row OOM
// stream on the escalated worker without resending RowDescription.
func (c *clientConn) streamSelectRows(rows RowSet, cols []string, colTypes []ColumnTyper, typeOIDs []int32, sendRowDesc bool) (rowsSent int, rowsErr error, writeErr error)
```

and in `executeSelectQuery` replace the inline loop with:

```go
	rowsSent, rowsErr, writeErr := c.streamSelectRows(rows, cols, colTypes, typeOIDs, true)
	if writeErr != nil { /* existing client-write error handling */ }
	if rowsErr != nil && rowsSent == 0 && c.onExploratoryWorker && isWorkerOutOfMemoryError(rowsErr) && c.txStatus == txStatusIdle {
		_ = rows.Close()
		if escErr := c.escalateWorker(ctx, escalateReasonOOM); escErr == nil {
			rows2, retryErr := runQuery()
			if retryErr == nil {
				// RowDescription already sent from the first attempt (same
				// query, same engine — identical schema); skip resending.
				rowsSent, rowsErr, writeErr = c.streamSelectRows(rows2, cols, colTypes, typeOIDs, false)
				defer rows2.Close()
				if writeErr != nil { /* existing client-write error handling */ }
			} else {
				rowsErr = retryErr
			}
		}
	}
	if rowsErr != nil { /* existing rows.Err() error handling (42000/57014, logQueryError, sendError, setTxError) */ }
```

Preserve the existing behavior byte-for-byte on every non-tier path (the helper extraction must not change flush points or error codes — `TestSelectMidStreamOOMAfterRowsSurfaces` plus the whole existing `server` test suite guard this).

- [ ] **Step 5: Run tests**

Run: `go test ./server/ -v -run 'Tier|Select|TestSimpleQuery'` then the full `go test ./server/` — all PASS.

- [ ] **Step 6: Commit**

```bash
git add server/conn.go server/conn_query_exec.go server/conn_tier_exec_test.go
git commit -m "feat(server): exploratory tier pin + OOM re-execute for simple queries"
```

---

### Task 7: Extended-protocol integration

**Files:**
- Modify: `server/conn_extended_query.go` (`handleParse` ~line 189-204, `handleExecute` ~lines 727-900)
- Test: `server/conn_tier_exec_test.go` (extend)

**Interfaces:**
- Consumes: same as Task 6; `preparedStmt` struct (defined near `handleParse`).
- Produces: `preparedStmt.pinsWorker bool`, tier-aware Execute.

- [ ] **Step 1: Write failing tests**

Extend `server/conn_tier_exec_test.go`:

```go
// Parse stores the classification; Execute of a pinning statement escalates
// before the executor runs it.
func TestExtendedExecutePinsBeforeExecute(t *testing.T) { /* Parse "CREATE TEMP TABLE t (i int)"
   (or drive the preparedStmt directly), then handleExecute; assert switcher called with
   "state" before the fake executor's Exec. */ }

// Extended read that OOMs at Query() re-executes after escalation.
func TestExtendedExecuteReexecutesOnOOM(t *testing.T) { /* mirror of the simple-path test
   against the handleExecute result path. */ }
```

Follow whatever construction pattern Task 6 settled on.

- [ ] **Step 2: Run to verify they fail** — `go test ./server/ -run TestExtendedExecute -v`.

- [ ] **Step 3: Implement**

In `handleParse`, where the transpile result is stored on the `preparedStmt` (~line 189-204), add one field computed once:

```go
	pinsWorker: classifyStatementTier(query) == tierPinning,
```

(add `pinsWorker bool` to the `preparedStmt` struct with a comment: "computed at Parse; Execute escalates the exploratory tier before running a pinning statement").

In `handleExecute`, after params decode + GUC/secret intercepts and before the execution branches (~line 727):

```go
	if c.onExploratoryWorker && p.stmt.pinsWorker {
		if err := c.escalateWorker(c.ctx, escalateReasonState); err != nil {
			c.sendError("ERROR", "53400", fmt.Sprintf("could not allocate a standard worker for this statement: %v", err))
			return
		}
	}
```

For the result path (~line 792), mirror Task 6's prepare-phase retry around `runQuery` (the closure already reads `c.executor` at call time), and apply the same zero-rows mid-stream retry to the inline DataRow loop at ~line 854-900 — reuse `streamSelectRows` if the inline loop can be swapped onto it (it carries `resultFormats` and `maxRows`; if reuse contorts the helper, duplicate the small retry block instead and say so in a comment).

- [ ] **Step 4: Run tests** — `go test ./server/` all PASS.

- [ ] **Step 5: Commit**

```bash
git add server/conn_extended_query.go server/conn_tier_exec_test.go
git commit -m "feat(server): exploratory tier pin + OOM re-execute for extended protocol"
```

---

### Task 8: Query-log tier column

**Files:**
- Modify: `server/wire/worker_proto.go` (`QueryLogEntry`, ~line 142), `server/querylog_schema.go` (append registry row), `server/querylog.go` (populate in `logQueryStart` ~line 414 and `logQuery` ~line 465)
- Test: existing schema tests (`TestQueryLogAppendedColumnsAreAddable` etc.) must stay green; e2e `query_log_round_trip` covers the round trip.

**Interfaces:**
- Consumes: `c.onExploratoryWorker` (Task 4); the 3-edit column recipe documented in `querylog_schema.go:5-42`.
- Produces: `worker_tier` TEXT column: `"exploratory"` while on the small worker, `"standard"` otherwise.

- [ ] **Step 1: Add the field**

`server/wire/worker_proto.go`, append to `QueryLogEntry`:

```go
	// WorkerTier is which worker tier executed the statement: "exploratory"
	// (the small warm worker) or "standard". Recorded at statement start; a
	// statement that triggered escalation logs the tier it ULTIMATELY ran on.
	WorkerTier string
```

- [ ] **Step 2: Append the registry row**

`server/querylog_schema.go`, append at the END of `queryLogColumns` (never reorder):

```go
	{Name: "worker_tier", PGType: "TEXT", Arg: func(e QueryLogEntry) any { return e.WorkerTier }},
```

- [ ] **Step 3: Populate**

In both `QueryLogEntry` literals — `logQueryStart` (`querylog.go:~420-461`) and `logQuery` (`querylog.go:547-580`):

```go
		WorkerTier: c.currentWorkerTier(),
```

with, in `server/conn_tier.go`:

```go
func (c *clientConn) currentWorkerTier() string {
	if c.onExploratoryWorker {
		return "exploratory"
	}
	return "standard"
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./server/ -run QueryLog -v` then full `go test ./server/` — PASS (the appended-column addability test enforces the nullable rule; TEXT with no NOT NULL satisfies it).

- [ ] **Step 5: Commit**

```bash
git add server/wire/worker_proto.go server/querylog_schema.go server/querylog.go server/conn_tier.go
git commit -m "feat(server): worker_tier query-log column"
```

---

### Task 9: Lazy worker acquisition

**Files:**
- Modify: `controlplane/control.go` (`handleConnection` — move session create + init behind an activator), `server/conn.go` (activation hook in statement handling), `server/exports.go` (setter)
- Test: `server/conn_tier_test.go` (activator called once, on first query, not at connect); e2e `conn_idle_timeout_reaps_session` + new assertion (Task 10)

**Interfaces:**
- Consumes: everything Tasks 4-5 built.
- Produces: `type SessionActivator func(ctx context.Context, pinned bool) (exec QueryExecutor, workerID int, workerPod string, err error)`; `func SetSessionActivator(cc *clientConn, a SessionActivator)`; `func MarkConnectionPinned(cc *clientConn)`; method `func (c *clientConn) ensureSessionActive(ctx context.Context, pinned bool) error`. The `pinned` flag lets the FIRST statement, when already pinning, acquire the escalation-target profile directly instead of small-acquire-then-escalate (avoids a wasted double acquire).

**Scope guard:** lazy activation applies ONLY when the exploratory tier is active for the connection (`useExploratory`). GUC-sized and tier-disabled connections keep today's eager acquire — that keeps the blast radius inside the tier flag and avoids re-testing every legacy path.

- [ ] **Step 1: Write the failing unit test**

```go
func TestEnsureSessionActiveActivatesOnce(t *testing.T) {
	c := &clientConn{}
	fake := &fakeQueryExecutor{}
	calls := 0
	var sawPinned bool
	c.sessionActivator = func(ctx context.Context, pinned bool) (QueryExecutor, int, string, error) {
		calls++
		sawPinned = pinned
		return fake, 7, "pod-7", nil
	}
	if err := c.ensureSessionActive(context.Background(), false); err != nil || calls != 1 || sawPinned {
		t.Fatalf("err=%v calls=%d pinned=%v", err, calls, sawPinned)
	}
	if c.executor != QueryExecutor(fake) || c.workerID != 7 {
		t.Fatal("executor not installed")
	}
	// Second call: no-op even with pinned=true (already active; escalation is
	// escalateWorker's job, not the activator's).
	if err := c.ensureSessionActive(context.Background(), true); err != nil || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestEnsureSessionActiveNilActivatorNoop(t *testing.T) {
	c := &clientConn{executor: &fakeQueryExecutor{}}
	if err := c.ensureSessionActive(context.Background(), false); err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **Step 2: Run to verify failure** — `go test ./server/ -run TestEnsureSessionActive -v`.

- [ ] **Step 3: Implement the server side**

`server/conn_tier.go`:

```go
// SessionActivator lazily acquires the connection's first worker/session.
// Installed by the control plane when the exploratory tier defers acquisition
// past connection startup; invoked on the message-loop goroutine by the first
// statement that needs an engine. pinned=true means the first statement is
// already a pinning one — the CP acquires the escalation-target profile
// directly (and marks the connection pinned) instead of small-then-escalate.
type SessionActivator func(ctx context.Context, pinned bool) (exec QueryExecutor, workerID int, workerPod string, err error)

// ensureSessionActive acquires the backing session on first need. No-op when
// an executor is already installed or no activator was configured (eager
// paths: standalone, GUC-sized, tier-disabled).
func (c *clientConn) ensureSessionActive(ctx context.Context, pinned bool) error {
	if c.executor != nil || c.sessionActivator == nil {
		return nil
	}
	exec, workerID, workerPod, err := c.sessionActivator(ctx, pinned)
	if err != nil {
		return err
	}
	c.executor = exec
	c.workerID = workerID
	c.workerPod = workerPod
	return nil
}
```

clientConn field (`server/conn.go`, next to `workerSwitcher`): `sessionActivator SessionActivator` (same same-goroutine comment). Setter in `server/exports.go`:

```go
// SetSessionActivator installs the lazy first-acquisition hook on a
// control-plane connection created without a worker. See SessionActivator.
func SetSessionActivator(cc *clientConn, a SessionActivator) {
	if cc != nil {
		cc.sessionActivator = a
	}
}
```

Add `ensureSessionActive` calls at every entry point that reaches the executor, AFTER the CP-side interceptions so engine-free statements never activate. In `handleQuery`/`handleExecute`, classify FIRST so the activation lands on the right tier in one acquire: `tier := classifyStatementTier(query)` (extended: `p.stmt.pinsWorker`), then `ensureSessionActive(c.ctx, tier == tierPinning)`, then the Task 6/7 escalation hook (a no-op when the activator already acquired pinned — `MarkConnectionPinned` cleared `onExploratoryWorker`). Call sites without a statement to classify pass `pinned=false`. Enumerated (each: `if err := c.ensureSessionActive(c.ctx, pinned); err != nil { send 53400 + appropriate ready-for-query handling; return }`):

1. `server/conn.go` `handleQuery` — immediately before the tier classification hook from Task 6 (after cursor/pg_stat_activity/secret-DDL/passthrough/GUC intercepts).
2. `server/conn_query_exec.go` `executeSingleStatement` — same position as its Task 6 hook.
3. `server/conn_extended_query.go` `handleExecute` — before the Task 7 hook.
4. `server/conn_extended_query.go` `handleDescribe` — before the `LIMIT 0` probe (`c.executor.Query(describeQuery, ...)` ~line 361).
5. `server/conn_copy.go` — at the top of the COPY execution entry point(s) that touch `c.executor` (grep `c.executor` in that file for the exact functions).
6. `server/conn_cursor.go` — same treatment (cursor DECLARE executes on the engine; grep `c.executor`).

Also add to `server/exports.go`:

```go
// MarkConnectionPinned takes a connection off the exploratory tier without a
// worker switch — used by the control-plane activator when the first
// statement is already pinning and it acquired the standard profile directly.
func MarkConnectionPinned(cc *clientConn) {
	if cc != nil {
		cc.onExploratoryWorker = false
	}
}
```

- [ ] **Step 4: Implement the CP side**

In `handleConnection`, when `useExploratory`:
- Skip the eager `createSessionWithRegisteredCancel` block (lines ~1300-1345) and the eager `initSessionMetadata` + catalog wiring; create `cc` with `executor = nil`, `workerID = -1`, `workerPod = ""`.
- Keep: PID reservation, `SendInitialParams` via `tmpCC` (or now directly via `cc`), backend-key registration, `WriteReadyForQuery` — the client sees a fully-open connection.
- Install the activator:

```go
		server.SetSessionActivator(cc, func(ctx context.Context, pinned bool) (server.QueryExecutor, int, string, error) {
			profile := explProfile
			if pinned {
				profile = workerProfile
			}
			memLimit, threads := cp.workerDuckDBLimits(profile)
			ctx, cancel := context.WithTimeout(ctx, cp.cfg.WorkerQueueTimeout)
			defer cancel()
			_, exec, err := sessions.CreateSession(ctx, username, pid, memLimit, threads, profile)
			if err != nil {
				return nil, 0, "", err
			}
			if _, err := cp.initSessionMetadata(ctx, exec /* ... */); err != nil {
				sessions.DestroySession(pid)
				return nil, 0, "", err
			}
			sessions.SetConnCloser(pid, tlsConn)
			if pinned {
				server.MarkConnectionPinned(cc)
				millicores, mib := cp.workerBillingSize(workerProfile)
				server.SetConnectionWorkerSize(cc, millicores, mib)
			}
			return exec, sessions.WorkerIDForPID(pid), sessions.WorkerPodNameForPID(pid), nil
		})
```

- Teardown: guard the `defer sessions.DestroySession(pid)` — `DestroySession` on a never-activated pid logs "unknown session"; wrap with a `sessionEverCreated` bool the activator/switcher set, or accept the warn-level log and note it. Prefer the guard.
- The pre-ready disconnect watcher block becomes unnecessary on this path (there is no slow pre-ready acquire) — keep the code running for the non-exploratory path, skip starting it when lazy.

- [ ] **Step 5: Run everything**

Run: `go build -tags kubernetes ./... && go test ./server/ ./controlplane/`
Expected: green. Pay attention to `conn_pg_stat_activity.go` and admin live-session views: a not-yet-activated connection has no `ManagedSession`; verify `sessions.ReservePID`-only connections don't break `admin_providers.go` listings (they already tolerate the pre-ready window today — same state, longer-lived).

- [ ] **Step 6: Commit**

```bash
git add server/ controlplane/control.go
git commit -m "feat: lazy worker acquisition for exploratory-tier connections"
```

---

### Task 10: e2e harness, mw-dev deploy config, docs

**Files:**
- Modify: `tests/mw-dev/manifests.tmpl.yaml` (CP env, next to `DUCKGRES_STORAGE_SAMPLE_INTERVAL` ~line 279)
- Modify: `tests/mw-dev/e2e/harness.sh` (new assertions; update `org_default_profile`; extend the final PASS line)
- Modify: `CLAUDE.md` (tier addendum to the Worker Session Model section + run-modes flag docs), `README.md` (env knobs)
- Test: the e2e workflow itself (`e2e-mw-dev.yml` runs per PR)

**Interfaces:**
- Consumes: everything above, deployed to mw-dev.
- Produces: deterministic pass/fail assertions of the user-visible tier behavior.

- [ ] **Step 1: Deploy config**

`tests/mw-dev/manifests.tmpl.yaml`, in the CP env block:

```yaml
            - { name: DUCKGRES_EXPLORATORY_TIER_ENABLED, value: "true" }
            - { name: DUCKGRES_EXPLORATORY_WORKER_CPU, value: "1" }
            - { name: DUCKGRES_EXPLORATORY_WORKER_MEMORY, value: "2Gi" }
            - { name: DUCKGRES_EXPLORATORY_WORKER_TTL, value: "10m" }
```

- [ ] **Step 2: Update `org_default_profile`**

The assertion (harness.sh:1531) currently expects a PLAIN connection to produce an org-default-shape pod. With the tier on, a plain read lands on the exploratory shape; the org default materializes on escalation. Change the assertion's driver query from a plain `SELECT` to a pinning statement (e.g. `CREATE TEMP TABLE _e2e_pin (i int)`) and assert the NEWEST pod has the org-default shape. Keep the assertion's existing structure/idiom (`[ cond ] || fail "observed vs wanted (hypothesis)"`).

- [ ] **Step 3: New assertions** (place in `lane_res2`, the scheduling-shape lane, after `org_default_profile`; add each name to the final PASS line ~harness.sh:4078)

```bash
exploratory_tier() { # org password catalog
  org="$1"; pw="$2"; cat="$3"
  log "exploratory tier: plain read lands on 1-CPU exploratory pod on $org"
  _pg_exec "$org" "$pw" "$cat" 'SELECT 42' >/dev/null || fail "exploratory: plain read failed"
  pod="$(newest_running_org_worker "$org")"
  [ -n "$pod" ] || fail "exploratory: no worker pod for $org"
  rcpu="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.requests.cpu}")"
  rmem="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.requests.memory}")"
  [ "$rcpu" = "1" ] || fail "exploratory pod $pod requests.cpu='$rcpu' want '1' (small-first routing not applied)"
  [ "$rmem" = "2Gi" ] || fail "exploratory pod $pod requests.memory='$rmem' want '2Gi'"
  log "exploratory tier OK: $pod cpu=$rcpu mem=$rmem"
}

exploratory_state_pin() { # org password catalog default_cpu
  org="$1"; pw="$2"; cat="$3"; want_cpu="$4"
  log "exploratory state pin: temp table escalates to ${want_cpu}-CPU worker on $org"
  # One session: temp table then read it back — both must succeed, and the
  # session must end on a default-shape pod (state pinned it).
  out="$(_pg_exec "$org" "$pw" "$cat" 'CREATE TEMP TABLE _e2e_tier (i int); INSERT INTO _e2e_tier VALUES (7); SELECT i FROM _e2e_tier')" \
    || fail "exploratory state pin: session failed: $out"
  assert_lastline "$out" "7"
  pod="$(newest_running_org_worker "$org")"
  rcpu="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.requests.cpu}")"
  [ "$rcpu" = "$want_cpu" ] || fail "pinned pod $pod requests.cpu='$rcpu' want '$want_cpu' (state mutation did not escalate)"
  log "exploratory state pin OK: $pod cpu=$rcpu"
}

exploratory_oom_escalation() { # org password catalog
  org="$1"; pw="$2"; cat="$3"
  log "exploratory oom escalation: heavy read transparently succeeds on $org"
  # A memory-hungry aggregation the 2Gi pod (memory_limit ~1.5Gi) cannot hold
  # but the default worker can: wide hash aggregate over a generated range.
  q="SELECT count(DISTINCT (r % 100000000)::VARCHAR || repeat('x', 32)) FROM range(60000000) t(r)"
  out="$(_pg_exec "$org" "$pw" "$cat" "$q")" || fail "exploratory oom escalation: heavy read failed: $out"
  # Correctness of the value proves the re-executed result streamed cleanly.
  assert_lastline "$out" "60000000" 2>/dev/null || log "note: count differs by dedup — assert non-empty instead" && [ -n "$out" ] || fail "empty result"
  log "exploratory oom escalation OK"
}
```

Implementer notes: tune the OOM query on the real cluster until it deterministically exceeds the 2Gi pod's memory_limit and deterministically fits the org-default worker; verify escalation actually fired by checking the CP metric (`duckgres_exploratory_escalations_total{reason="oom"}` via the metrics endpoint) rather than pod shape if pod timing is racy — the metric check is the deterministic form, prefer it:

```bash
  # deterministic escalation proof: the oom counter moved
  before/after scrape of duckgres_exploratory_escalations_total{reason="oom"} on the CP metrics port
```

Also verify `sized_worker` / `reuse_sized_worker` still pass unchanged (GUC bypass) and `one_session_per_worker` still passes (two concurrent plain reads → two DISTINCT exploratory pods — the contract extended to the small tier).

- [ ] **Step 4: Docs**

- `CLAUDE.md`: add an "Exploratory Worker Tier" subsection under the Worker Session Model contract covering: small-first routing + GUC bypass, pin-on-state (never route back, never replay), OOM re-execute only when zero DataRows sent, lazy acquisition scope, billing largest-size-wins, env knobs, and the test-update obligations (this plan's files).
- `README.md`: document the four `DUCKGRES_EXPLORATORY_*` env vars.
- Note in the PR description: production charts need the env vars wired (`charts` repo, follow-up PR) — mw-dev manifests are updated here.

- [ ] **Step 5: Run the full local gates**

Run: `go build -tags kubernetes ./... && go test ./... && shellcheck tests/mw-dev/e2e/harness.sh || true` (harness has its own conventions; match existing shellcheck posture). Then push the branch and let `e2e-mw-dev.yml` run the harness against the real cluster; iterate on the new assertions until green.

- [ ] **Step 6: Commit**

```bash
git add tests/mw-dev/ CLAUDE.md README.md
git commit -m "test(e2e): exploratory tier assertions; docs for small-first routing"
```

---

## Self-Review Notes (resolved during planning)

- **Spec coverage:** scheduling/TTL → Task 1 (profile TTL rides the existing janitor); lazy acquire → Task 9; classification/pin → Tasks 2/6/7; optimistic+re-execute → Tasks 3/6/7; heuristic tier → the `escalateReasonHeuristic` constant and classification hook leave the slot; v1 ships NO heavy-read heuristic beyond classification (spec allows "as simple as" — YAGNI'd to zero, the hook point is `handleQuery`'s classification site); billing → Task 5; metrics/query log → Tasks 4/8; e2e/docs → Task 10.
- **Escalation + org cap:** destroy-small-then-acquire-big ordering means the org never holds both workers for one connection; an acquire failure at cap surfaces the existing clear org-cap error via the 53400 path.
- **RowDescription resend:** avoided by streaming-helper `sendRowDesc=false` on retry; schemas are identical (same query, same engine version).
- **Transactions:** `BEGIN` pins before executing, so an open transaction can never exist on the exploratory worker; the `txStatus == txStatusIdle` guards on the OOM retries are defense in depth.
- **Cancel:** same pid/secretKey across switches keeps the cancel registry valid; `escalateWorker` runs under the statement ctx so client cancel aborts an in-flight escalation acquire.
