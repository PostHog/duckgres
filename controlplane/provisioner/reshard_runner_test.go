//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// ---- fakes -----------------------------------------------------------------

type fakeReshardStore struct {
	mu sync.Mutex

	op             *configstore.ReshardOperation
	warehouseState configstore.ManagedWarehouseProvisioningState
	warehouseUpds  []map[string]interface{}

	// drain script: successive OrgConnectionDrainState results (last repeats)
	drainSeq []configstore.OrgConnectionDrainStatus
	drainIdx int
	// worker script: successive ListWorkerRecordsForOrg results (last repeats)
	workerSeq [][]configstore.WorkerRecord
	workerIdx int
	retired   []int

	logs  []string
	steps []string
}

func newFakeReshardStore(op *configstore.ReshardOperation) *fakeReshardStore {
	return &fakeReshardStore{
		op:             op,
		warehouseState: configstore.ManagedWarehouseStateReady,
		drainSeq:       []configstore.OrgConnectionDrainStatus{{}},
		workerSeq:      [][]configstore.WorkerRecord{{}},
	}
}

func (f *fakeReshardStore) ListClaimableReshardOperations(time.Duration) ([]int64, error) {
	return []int64{f.op.ID}, nil
}

func (f *fakeReshardStore) ClaimReshardOperation(id int64, runnerCP string, _ time.Duration) (*configstore.ReshardOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.op.State = configstore.ReshardStateRunning
	f.op.RunnerCP = runnerCP
	f.op.RunnerEpoch++
	now := time.Now().UTC()
	f.op.StartedAt = &now
	cp := *f.op
	return &cp, nil
}

func (f *fakeReshardStore) GetReshardOperation(int64) (*configstore.ReshardOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *f.op
	return &cp, nil
}

func (f *fakeReshardStore) UpdateReshardStep(_ int64, _ string, _ int64, step string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.op.Step = step
	f.steps = append(f.steps, step)
	return nil
}

func (f *fakeReshardStore) UpdateReshardFields(_ int64, _ string, _ int64, updates map[string]interface{}) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := updates["blocked_at"].(time.Time); ok {
		f.op.BlockedAt = &v
	}
	if v, ok := updates["unblocked_at"].(time.Time); ok {
		f.op.UnblockedAt = &v
	}
	if v, ok := updates["compaction_was_present"].(bool); ok {
		f.op.CompactionWasPresent = v
	}
	if v, ok := updates["compaction_was_enabled"].(bool); ok {
		f.op.CompactionWasEnabled = v
	}
	if v, ok := updates["tables_copied"].(int64); ok {
		f.op.TablesCopied = v
	}
	if v, ok := updates["rows_copied"].(int64); ok {
		f.op.RowsCopied = v
	}
	if v, ok := updates["source_endpoint"].(string); ok {
		f.op.SourceEndpoint = v
	}
	if v, ok := updates["source_user"].(string); ok {
		f.op.SourceUser = v
	}
	if v, ok := updates["source_database"].(string); ok {
		f.op.SourceDatabase = v
	}
	return nil
}

func (f *fakeReshardStore) TouchReshardHeartbeat(int64, string, int64) error { return nil }

func (f *fakeReshardStore) FinishReshardOperation(_ int64, _ string, _ int64, state configstore.ReshardState, errMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.op.State = state
	f.op.Error = errMsg
	return nil
}

func (f *fakeReshardStore) AppendReshardLog(_ int64, level, message string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = append(f.logs, level+": "+message)
	return nil
}

func (f *fakeReshardStore) SetWarehouseResharding(string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.warehouseState != configstore.ManagedWarehouseStateReady {
		return fmt.Errorf("warehouse not ready: %w", configstore.ErrWarehouseStateMismatch)
	}
	f.warehouseState = configstore.ManagedWarehouseStateResharding
	return nil
}

func (f *fakeReshardStore) UpdateWarehouseState(_ string, expected configstore.ManagedWarehouseProvisioningState, updates map[string]interface{}) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.warehouseState != expected {
		return fmt.Errorf("warehouse expected %q: %w", expected, configstore.ErrWarehouseStateMismatch)
	}
	if s, ok := updates["state"].(configstore.ManagedWarehouseProvisioningState); ok {
		f.warehouseState = s
	}
	f.warehouseUpds = append(f.warehouseUpds, updates)
	return nil
}

func (f *fakeReshardStore) OrgConnectionDrainState(string) (configstore.OrgConnectionDrainStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.drainSeq[f.drainIdx]
	if f.drainIdx < len(f.drainSeq)-1 {
		f.drainIdx++
	}
	return s, nil
}

func (f *fakeReshardStore) ListWorkerRecordsForOrg(string) ([]configstore.WorkerRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.workerSeq[f.workerIdx]
	if f.workerIdx < len(f.workerSeq)-1 {
		f.workerIdx++
	}
	return w, nil
}

func (f *fakeReshardStore) RetireHotIdleWorker(record *configstore.WorkerRecord) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retired = append(f.retired, record.WorkerID)
	return true, nil
}

func (f *fakeReshardStore) hasLog(substr string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, l := range f.logs {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}

type ducklingCall struct {
	kind  string // "cnpg" | "external" | "compaction"
	shard string
	ext   ExternalMetadataStoreSpec
	comp  *bool
}

type fakeDuckling struct {
	mu sync.Mutex
	// status returned by Get; tests mutate it to simulate composition
	// convergence. The fake flips endpoint/type on Set* calls itself.
	status DucklingStatus

	compEnabled, compPresent bool
	calls                    []ducklingCall
}

func (f *fakeDuckling) Get(context.Context, string) (*DucklingStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := f.status
	return &cp, nil
}

func (f *fakeDuckling) GetCompactionSetting(context.Context, string) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.compEnabled, f.compPresent, nil
}

func (f *fakeDuckling) SetCompactionEnabled(_ context.Context, _ string, enabled *bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ducklingCall{kind: "compaction", comp: enabled})
	return nil
}

func (f *fakeDuckling) SetMetadataStoreCnpg(_ context.Context, _ string, shard string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ducklingCall{kind: "cnpg", shard: shard})
	// Simulate the composition converging instantly.
	f.status.MetadataStore.Type = configstore.MetadataStoreKindCnpgShard
	f.status.MetadataStore.Endpoint = shard + "-pooler.cnpg-shards.svc.cluster.local"
	f.status.MetadataStore.User = "mdstore_org"
	f.status.MetadataStore.Database = "mdstore_org"
	f.status.MetadataStore.Password = "pinned-pw"
	f.status.ReadyCondition = true
	return nil
}

func (f *fakeDuckling) SetMetadataStoreExternal(_ context.Context, _ string, ext ExternalMetadataStoreSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ducklingCall{kind: "external", ext: ext})
	f.status.MetadataStore.Type = configstore.MetadataStoreKindExternal
	f.status.MetadataStore.Endpoint = ext.Endpoint
	f.status.MetadataStore.User = ext.User
	f.status.MetadataStore.Database = ext.Database
	f.status.MetadataStore.Password = "ext-pw" // what "ESO" synced
	f.status.ReadyCondition = true
	return nil
}

func (f *fakeDuckling) callKinds() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	kinds := make([]string, len(f.calls))
	for i, c := range f.calls {
		kinds[i] = c.kind
	}
	return kinds
}

type copierCall struct {
	kind   string // "probe" | "copy" | "counts" | "droptables" | "dropdb"
	source CatalogEndpoint
	target CatalogEndpoint
	dbName string
}

type fakeCopier struct {
	mu    sync.Mutex
	calls []copierCall

	copyResult CatalogCopyResult
	copyErr    error
	// countsAfter is what SnapshotCounts returns (the stability recheck);
	// defaults to copyResult.PerTableRows (stable source).
	countsAfter map[string]int64
}

func (f *fakeCopier) record(c copierCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
}

func (f *fakeCopier) Probe(_ context.Context, ep CatalogEndpoint) error {
	f.record(copierCall{kind: "probe", target: ep})
	return nil
}

func (f *fakeCopier) Copy(_ context.Context, source, target CatalogEndpoint, log func(string, string)) (CatalogCopyResult, error) {
	f.record(copierCall{kind: "copy", source: source, target: target})
	if f.copyErr != nil {
		return CatalogCopyResult{}, f.copyErr
	}
	log("info", "fake copy done")
	return f.copyResult, nil
}

func (f *fakeCopier) SnapshotCounts(context.Context, CatalogEndpoint) (map[string]int64, error) {
	f.record(copierCall{kind: "counts"})
	if f.countsAfter != nil {
		return f.countsAfter, nil
	}
	return f.copyResult.PerTableRows, nil
}

func (f *fakeCopier) DropCatalogTables(_ context.Context, ep CatalogEndpoint, _ func(string, string)) error {
	f.record(copierCall{kind: "droptables", target: ep})
	return nil
}

func (f *fakeCopier) DropDatabase(_ context.Context, ep CatalogEndpoint, dbName string) error {
	f.record(copierCall{kind: "dropdb", target: ep, dbName: dbName})
	return nil
}

func (f *fakeCopier) kinds() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	kinds := make([]string, len(f.calls))
	for i, c := range f.calls {
		kinds[i] = c.kind
	}
	return kinds
}

// ---- harness ----------------------------------------------------------------

func testRunner(store *fakeReshardStore, duckling reshardDucklingClient, copier *fakeCopier) *ReshardRunner {
	return &ReshardRunner{
		store:              store,
		duckling:           duckling,
		copier:             copier,
		cpID:               "cp-test",
		pollInterval:       time.Hour, // scanOnce called manually
		configPollInterval: time.Millisecond,
		heartbeatInterval:  time.Hour,
		staleAfter:         time.Minute,
		flipTimeout:        2 * time.Second,
		hotIdleGrace:       time.Millisecond,
		loopPoll:           5 * time.Millisecond,
	}
}

func runOp(t *testing.T, r *ReshardRunner, store *fakeReshardStore) {
	t.Helper()
	op, err := store.ClaimReshardOperation(store.op.ID, r.cpID, r.staleAfter)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.execute(context.Background(), op)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("operation did not finish in time")
	}
}

func cnpgOp() *configstore.ReshardOperation {
	return &configstore.ReshardOperation{
		ID: 1, OrgID: "org-a", DucklingName: "org-a",
		SourceKind: configstore.MetadataStoreKindCnpgShard, FromShard: "shard-001",
		TargetKind: configstore.MetadataStoreKindCnpgShard, ToShard: "shard-002",
		DrainTimeoutSeconds: 5,
		CreatedAt:           time.Now().UTC(),
	}
}

func cnpgSourceStatus() DucklingStatus {
	var st DucklingStatus
	st.MetadataStore.Type = configstore.MetadataStoreKindCnpgShard
	st.MetadataStore.Endpoint = "shard-001-pooler.cnpg-shards.svc.cluster.local"
	st.MetadataStore.User = "mdstore_org"
	st.MetadataStore.Database = "mdstore_org"
	st.MetadataStore.Password = "pinned-pw"
	st.ReadyCondition = true
	return st
}

// ---- tests ------------------------------------------------------------------

// TestReshardHappyPathCnpgToCnpg pins the full step sequence, the flip-then-
// copy ordering, the source drop, compaction pause/restore, the maintenance
// window stamps, and the final report.
func TestReshardHappyPathCnpgToCnpg(t *testing.T) {
	store := newFakeReshardStore(cnpgOp())
	duckling := &fakeDuckling{status: cnpgSourceStatus(), compPresent: true, compEnabled: true}
	copier := &fakeCopier{copyResult: CatalogCopyResult{
		Tables: 3, Rows: 42, Bytes: 1000,
		PerTableRows: map[string]int64{"ducklake_metadata": 1, "ducklake_snapshot": 40, "ducklake_table": 1},
	}}
	runOp(t, testRunner(store, duckling, copier), store)

	if store.op.State != configstore.ReshardStateSucceeded {
		t.Fatalf("state = %s (err %q), want succeeded", store.op.State, store.op.Error)
	}
	wantSteps := []string{"blocking", "draining", "pausing_compaction", "cutover", "copying", "verifying", "cleaning_up", "finalizing"}
	if fmt.Sprint(store.steps) != fmt.Sprint(wantSteps) {
		t.Fatalf("steps = %v, want %v", store.steps, wantSteps)
	}
	// flip → copy → recheck → drop source, in that order.
	kinds := copier.kinds()
	// probe(target, flip wait) → probe(source) → probe(target) → copy →
	// stability recheck → source drop.
	if fmt.Sprint(kinds) != fmt.Sprint([]string{"probe", "probe", "probe", "copy", "counts", "dropdb"}) {
		t.Fatalf("copier calls = %v", kinds)
	}
	// The copy's SOURCE is the recorded pre-flip endpoint, not the new one.
	for _, call := range copier.calls {
		if call.kind == "copy" && !strings.HasPrefix(call.source.Host, "shard-001-pooler") {
			t.Fatalf("copy read from %q, want the recorded shard-001 source", call.source.Host)
		}
	}
	// Compaction paused (false) then restored to explicit true.
	dkinds := duckling.callKinds()
	if fmt.Sprint(dkinds) != fmt.Sprint([]string{"compaction", "cnpg", "compaction"}) {
		t.Fatalf("duckling calls = %v", dkinds)
	}
	if duckling.calls[0].comp == nil || *duckling.calls[0].comp {
		t.Fatal("compaction was not paused to explicit false")
	}
	if duckling.calls[2].comp == nil || !*duckling.calls[2].comp {
		t.Fatal("compaction was not restored to explicit true")
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready", store.warehouseState)
	}
	if store.op.BlockedAt == nil || store.op.UnblockedAt == nil {
		t.Fatal("maintenance window not stamped")
	}
	if !store.hasLog("reshard report (succeeded)") || !store.hasLog("maintenance mode (connections blocked)") {
		t.Fatalf("report missing from log: %v", store.logs)
	}
}

// TestReshardHappyPathExtToCnpg pins that an external source is NEVER
// deleted and the warehouse row is reconciled to cnpg.
func TestReshardHappyPathExtToCnpg(t *testing.T) {
	op := cnpgOp()
	op.SourceKind = configstore.MetadataStoreKindExternal
	op.FromShard = ""
	op.SourceEndpoint = "db.example.rds.amazonaws.com"
	store := newFakeReshardStore(op)

	var st DucklingStatus
	st.MetadataStore.Type = configstore.MetadataStoreKindExternal
	st.MetadataStore.Endpoint = "db.example.rds.amazonaws.com"
	st.MetadataStore.User = "postgres"
	st.MetadataStore.Database = "postgres"
	st.MetadataStore.Password = "rds-pw"
	st.ReadyCondition = true
	duckling := &fakeDuckling{status: st}
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1, PerTableRows: map[string]int64{"ducklake_metadata": 1}}}

	runOp(t, testRunner(store, duckling, copier), store)

	if store.op.State != configstore.ReshardStateSucceeded {
		t.Fatalf("state = %s (err %q), want succeeded", store.op.State, store.op.Error)
	}
	for _, k := range copier.kinds() {
		if k == "dropdb" {
			t.Fatal("external source was dropped — never allowed")
		}
	}
	if !store.hasLog("external source left untouched") {
		t.Fatal("missing external-source-untouched log")
	}
	// Warehouse row reconciled to cnpg.
	last := store.warehouseUpds[len(store.warehouseUpds)-1]
	if last["metadata_store_kind"] != configstore.MetadataStoreKindCnpgShard {
		t.Fatalf("warehouse row not reconciled to cnpg: %v", last)
	}
}

// TestReshardHappyPathCnpgToExt pins the ESCAPE-HATCH ordering: copy and
// verify BEFORE the flip (the flip deletes the cnpg source), and no explicit
// source drop (the flip is the cleanup).
func TestReshardHappyPathCnpgToExt(t *testing.T) {
	op := cnpgOp()
	op.TargetKind = configstore.MetadataStoreKindExternal
	op.ToShard = ""
	op.TargetEndpoint = "escape.rds.amazonaws.com"
	op.TargetPasswordSecret = "escape-secret"
	op.TargetUser = "postgres"
	op.TargetDatabase = "postgres"
	store := newFakeReshardStore(op)
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1, PerTableRows: map[string]int64{"ducklake_metadata": 1}}}

	r := testRunner(store, duckling, copier)
	r.StashExternalPassword(op.ID, "ext-pw")
	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateSucceeded {
		t.Fatalf("state = %s (err %q), want succeeded", store.op.State, store.op.Error)
	}
	// Copy + stability recheck happen BEFORE the flip; no dropdb ever.
	if fmt.Sprint(copier.kinds()) != fmt.Sprint([]string{"probe", "probe", "copy", "counts"}) {
		t.Fatalf("copier calls = %v", copier.kinds())
	}
	dkinds := duckling.callKinds()
	if fmt.Sprint(dkinds) != fmt.Sprint([]string{"compaction", "external", "compaction"}) {
		t.Fatalf("duckling calls = %v, want flip AFTER copy", dkinds)
	}
	wantSteps := []string{"blocking", "draining", "pausing_compaction", "copying", "verifying", "cutover", "finalizing"}
	if fmt.Sprint(store.steps) != fmt.Sprint(wantSteps) {
		t.Fatalf("steps = %v, want %v", store.steps, wantSteps)
	}
	last := store.warehouseUpds[len(store.warehouseUpds)-1]
	if last["metadata_store_kind"] != configstore.MetadataStoreKindExternal || last["metadata_store_endpoint"] != "escape.rds.amazonaws.com" {
		t.Fatalf("warehouse row not reconciled to external: %v", last)
	}
}

// TestReshardDrainTimeoutRollsBack pins that a never-draining org rolls back
// without any flip and unblocks the warehouse.
func TestReshardDrainTimeoutRollsBack(t *testing.T) {
	op := cnpgOp()
	op.DrainTimeoutSeconds = 1
	store := newFakeReshardStore(op)
	store.drainSeq = []configstore.OrgConnectionDrainStatus{{ActiveLeases: 2}}
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	copier := &fakeCopier{}

	runOp(t, testRunner(store, duckling, copier), store)

	if store.op.State != configstore.ReshardStateFailed || !strings.Contains(store.op.Error, "drain timed out") {
		t.Fatalf("state=%s err=%q, want drain-timeout failure", store.op.State, store.op.Error)
	}
	for _, k := range duckling.callKinds() {
		if k == "cnpg" || k == "external" {
			t.Fatal("drain-timeout rollback must not flip the duckling")
		}
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready after rollback", store.warehouseState)
	}
	if !store.hasLog("reshard report (failed)") {
		t.Fatal("failure report missing")
	}
}

// TestReshardFlipTimeoutRollsBackShardValue pins the rollback of a cnpg→cnpg
// flip that never converges: the SOURCE shard value is patched back (the key
// is never removed) and the org is unblocked.
func TestReshardFlipTimeoutRollsBackShardValue(t *testing.T) {
	store := newFakeReshardStore(cnpgOp())
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	// Break convergence: Set keeps the CR pointing at the SOURCE (simulates a
	// bogus target shard the composition can't render).
	stuck := &stuckCnpgDuckling{fakeDuckling: duckling}
	copier := &fakeCopier{}

	runOp(t, testRunner(store, stuck, copier), store)

	if store.op.State != configstore.ReshardStateFailed || !strings.Contains(store.op.Error, "did not become ready") {
		t.Fatalf("state=%s err=%q, want flip-timeout failure", store.op.State, store.op.Error)
	}
	// Last cnpg patch must be the flip-back to the source shard value.
	var shardPatches []string
	for _, call := range duckling.calls {
		if call.kind == "cnpg" {
			shardPatches = append(shardPatches, call.shard)
		}
	}
	if len(shardPatches) != 2 || shardPatches[0] != "shard-002" || shardPatches[1] != "shard-001" {
		t.Fatalf("shard patches = %v, want [shard-002 shard-001] (flip, then value patched back)", shardPatches)
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready after rollback", store.warehouseState)
	}
}

// stuckCnpgDuckling records patches but never converges the status.
type stuckCnpgDuckling struct {
	*fakeDuckling
}

func (s *stuckCnpgDuckling) SetMetadataStoreCnpg(_ context.Context, _ string, shard string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, ducklingCall{kind: "cnpg", shard: shard})
	return nil // status unchanged — composition "stuck"
}

// TestReshardCancelDuringDrain pins the cancel path: op ends cancelled, no
// flip, warehouse unblocked, report present.
func TestReshardCancelDuringDrain(t *testing.T) {
	op := cnpgOp()
	op.DrainTimeoutSeconds = 60
	store := newFakeReshardStore(op)
	store.drainSeq = []configstore.OrgConnectionDrainStatus{{ActiveLeases: 1}}
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	copier := &fakeCopier{}
	r := testRunner(store, duckling, copier)

	go func() {
		time.Sleep(50 * time.Millisecond)
		store.mu.Lock()
		store.op.CancelRequested = true
		store.mu.Unlock()
	}()
	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateCancelled {
		t.Fatalf("state = %s (err %q), want cancelled", store.op.State, store.op.Error)
	}
	for _, k := range duckling.callKinds() {
		if k == "cnpg" || k == "external" {
			t.Fatal("cancelled op must not flip the duckling")
		}
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready", store.warehouseState)
	}
	if !store.hasLog("reshard report (cancelled)") {
		t.Fatal("cancel report missing")
	}
}

// TestReshardExtTargetWithoutPasswordFails pins the takeover semantics of the
// ephemeral password: a runner without the stash must fail with the re-run
// message, never proceed with an empty password.
func TestReshardExtTargetWithoutPasswordFails(t *testing.T) {
	op := cnpgOp()
	op.TargetKind = configstore.MetadataStoreKindExternal
	op.ToShard = ""
	op.TargetEndpoint = "escape.rds.amazonaws.com"
	op.TargetPasswordSecret = "escape-secret"
	store := newFakeReshardStore(op)
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	copier := &fakeCopier{}

	runOp(t, testRunner(store, duckling, copier), store) // no stash

	if store.op.State != configstore.ReshardStateFailed || !strings.Contains(store.op.Error, "password is not available") {
		t.Fatalf("state=%s err=%q, want ephemeral-password failure", store.op.State, store.op.Error)
	}
	for _, k := range copier.kinds() {
		if k == "copy" {
			t.Fatal("copy must not run without the target password")
		}
	}
	for _, k := range duckling.callKinds() {
		if k == "external" {
			t.Fatal("flip must not run when the copy never happened")
		}
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready after rollback", store.warehouseState)
	}
}

// TestReshardVerifyMismatchRollsBack pins the concurrent-writer detection: a
// source that changed after the snapshot fails verify and rolls back.
func TestReshardVerifyMismatchRollsBack(t *testing.T) {
	store := newFakeReshardStore(cnpgOp())
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	copier := &fakeCopier{
		copyResult:  CatalogCopyResult{Tables: 1, Rows: 1, PerTableRows: map[string]int64{"ducklake_snapshot": 1}},
		countsAfter: map[string]int64{"ducklake_snapshot": 2}, // writer snuck in
	}

	runOp(t, testRunner(store, duckling, copier), store)

	if store.op.State != configstore.ReshardStateFailed || !strings.Contains(store.op.Error, "concurrent writer") {
		t.Fatalf("state=%s err=%q, want concurrent-writer failure", store.op.State, store.op.Error)
	}
	// Rollback of a flipped cnpg→cnpg: shard value patched back + partial
	// target dropped; source NEVER dropped.
	for _, k := range copier.kinds() {
		if k == "dropdb" {
			t.Fatal("verify failure must never drop the source database")
		}
	}
	foundDropTables := false
	for _, k := range copier.kinds() {
		if k == "droptables" {
			foundDropTables = true
		}
	}
	if !foundDropTables {
		t.Fatal("partial target tables were not dropped on rollback")
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready after rollback", store.warehouseState)
	}
}
