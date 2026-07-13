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
	if v, ok := updates["backup_s3_uri"].(string); ok {
		f.op.BackupS3URI = v
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
	return f.countLog(substr) > 0
}

func (f *fakeReshardStore) countLog(substr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, l := range f.logs {
		if strings.Contains(l, substr) {
			n++
		}
	}
	return n
}

type ducklingCall struct {
	kind   string // "cnpg" | "external" | "compaction" | "retain" | "cnpg-adopt"
	shard  string
	ext    ExternalMetadataStoreSpec
	comp   *bool
	retain *bool
}

type fakeDuckling struct {
	mu sync.Mutex
	// status returned by Get; tests mutate it to simulate composition
	// convergence. The fake flips endpoint/type on Set* calls itself.
	status DucklingStatus

	compEnabled, compPresent bool
	calls                    []ducklingCall

	// cnpg→ext orphan-adopt escape hatch simulation.
	// retainFieldUnsupported=true models an XRD that predates
	// spec.metadataStore.retainCnpgOnFlip (the API server prunes the patch, so
	// the read-back reports present=false). retainFlag is the current spec
	// value; CnpgSourceMRsOrphaned reports the MRs as orphaned once it is true
	// (composition converges instantly).
	retainFieldUnsupported bool
	retainFlag             bool
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

func (f *fakeDuckling) SetMetadataStoreRetainCnpgOnFlip(_ context.Context, _ string, retain bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := retain
	f.calls = append(f.calls, ducklingCall{kind: "retain", retain: &r})
	if !f.retainFieldUnsupported {
		f.retainFlag = retain
	}
	return nil
}

func (f *fakeDuckling) GetMetadataStoreRetainCnpgOnFlip(context.Context, string) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.retainFieldUnsupported {
		return false, false, nil // XRD pruned the field
	}
	return f.retainFlag, true, nil
}

func (f *fakeDuckling) CnpgSourceMRsOrphaned(context.Context, string) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Composition converges instantly: once retain is set, the cnpg source MRs
	// reflect the no-Delete policy. present is always true (the source cnpg
	// Role/Database MRs exist while type is cnpg-shard).
	return f.retainFlag, true, nil
}

func (f *fakeDuckling) SetMetadataStoreCnpgAdopt(_ context.Context, _ string, shard string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ducklingCall{kind: "cnpg-adopt", shard: shard})
	// Composition re-adopts the orphaned role/DB and converges back to cnpg.
	f.status.MetadataStore.Type = configstore.MetadataStoreKindCnpgShard
	f.status.MetadataStore.Endpoint = shard + "-pooler.cnpg-shards.svc.cluster.local"
	f.status.MetadataStore.User = "mdstore_org"
	f.status.MetadataStore.Database = "mdstore_org"
	f.status.MetadataStore.Password = "pinned-pw"
	f.status.ReadyCondition = true
	f.retainFlag = false
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
	// countsAfter is what SnapshotCounts returns for the SOURCE stability
	// recheck (sslmode!=require); defaults to copyResult.PerTableRows (stable).
	countsAfter map[string]int64
	// externalCounts is what SnapshotCounts returns for the EXTERNAL target
	// verify (sslmode==require, cnpg→ext verifyExternalCatalog); defaults to
	// copyResult.PerTableRows (complete target).
	externalCounts map[string]int64
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

func (f *fakeCopier) SnapshotCounts(_ context.Context, ep CatalogEndpoint) (map[string]int64, error) {
	f.record(copierCall{kind: "counts", target: ep})
	// The external target is reached with sslmode=require (verifyExternalCatalog);
	// the cnpg source with sslmode=disable (verifySourceStable).
	if ep.SSLMode == "require" {
		if f.externalCounts != nil {
			return f.externalCounts, nil
		}
		return f.copyResult.PerTableRows, nil
	}
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

// fakeBackuper records Backup calls and returns a canned URI (or an injected
// error) so tests exercise the pre-flip backup seam without pg_dump or S3.
type fakeBackuper struct {
	mu        sync.Mutex
	calls     []CatalogEndpoint
	dests     []BackupDestination
	backupErr error
	uri       string
	size      int64
}

func (f *fakeBackuper) Backup(_ context.Context, source CatalogEndpoint, dest BackupDestination) (string, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, source)
	f.dests = append(f.dests, dest)
	if f.backupErr != nil {
		return "", 0, f.backupErr
	}
	uri := f.uri
	if uri == "" {
		uri = "s3://" + dest.Bucket + "/" + dest.Key
	}
	size := f.size
	if size == 0 {
		size = 4096
	}
	return uri, size, nil
}

func (f *fakeBackuper) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// ---- harness ----------------------------------------------------------------

func testRunner(store *fakeReshardStore, duckling reshardDucklingClient, copier *fakeCopier) *ReshardRunner {
	return &ReshardRunner{
		store:              store,
		duckling:           duckling,
		copier:             copier,
		backuper:           &fakeBackuper{},
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
	// Data store (unchanged by a reshard) — the pre-flip backup target.
	st.DataStore.BucketName = "org-a-data-bucket"
	st.DataStore.S3Region = "us-east-1"
	st.IAMRoleARN = "arn:aws:iam::123:role/duckling-org-a"
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
	wantSteps := []string{"blocking", "draining", "pausing_compaction", "backup_catalog", "cutover", "copying", "verifying", "cleaning_up", "finalizing"}
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
	// Regression (#observed in prod): the source drop must name the database
	// recorded from the duckling status. The op row's SourceDatabase is unset
	// for cnpg sources (the admin handler only fills it for external), so a
	// runner reading only op.SourceDatabase issued `DROP DATABASE ""`.
	for _, call := range copier.calls {
		if call.kind == "dropdb" {
			if call.dbName != "mdstore_org" {
				t.Fatalf("dropdb database = %q, want %q (the recorded source database)", call.dbName, "mdstore_org")
			}
			if !strings.HasPrefix(call.target.Host, "shard-001-pooler") {
				t.Fatalf("dropdb ran against %q, want the recorded shard-001 source", call.target.Host)
			}
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
	st.DataStore.BucketName = "org-a-data-bucket"
	st.DataStore.S3Region = "us-east-1"
	st.IAMRoleARN = "arn:aws:iam::123:role/duckling-org-a"
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

// TestReshardHappyPathCnpgToExt pins the ORPHAN-ADOPT-then-VERIFIED-DELETE
// escape-hatch ordering: copy + verify, then a TWO-STEP flip (retain flag set +
// MRs observed no-Delete WHILE still cnpg, THEN the type flip), then external
// catalog verify, then the explicit source drop (only after verify), then the
// retain flag cleared.
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
	// copy + source-stability recheck BEFORE the flip, external-catalog verify
	// AFTER the flip, then the explicit source drop.
	if fmt.Sprint(copier.kinds()) != fmt.Sprint([]string{"probe", "probe", "copy", "counts", "counts", "dropdb"}) {
		t.Fatalf("copier calls = %v", copier.kinds())
	}
	// The source drop names the recorded source DB and runs against the source.
	for _, call := range copier.calls {
		if call.kind == "dropdb" {
			if call.dbName != "mdstore_org" {
				t.Fatalf("dropdb database = %q, want the recorded source database mdstore_org", call.dbName)
			}
			if !strings.HasPrefix(call.target.Host, "shard-001-pooler") {
				t.Fatalf("dropdb ran against %q, want the recorded shard-001 source", call.target.Host)
			}
		}
	}
	// Two-step flip: retain(true) → type flip(external) → source drop clears
	// retain(false). Compaction pause/restore bracket it.
	dkinds := duckling.callKinds()
	if fmt.Sprint(dkinds) != fmt.Sprint([]string{"compaction", "retain", "external", "retain", "compaction"}) {
		t.Fatalf("duckling calls = %v, want retain-before-flip + retain-clear-after-drop", dkinds)
	}
	// The retain flag is set true BEFORE the flip and cleared false after.
	var retainVals []bool
	for _, c := range duckling.calls {
		if c.kind == "retain" && c.retain != nil {
			retainVals = append(retainVals, *c.retain)
		}
	}
	if len(retainVals) != 2 || retainVals[0] != true || retainVals[1] != false {
		t.Fatalf("retain patches = %v, want [true false]", retainVals)
	}
	wantSteps := []string{"blocking", "draining", "pausing_compaction", "backup_catalog", "copying", "verifying", "orphaning_source", "cutover", "verifying_external", "cleaning_up", "finalizing"}
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
	op := cnpgOp()
	op.CutoverTimeoutSeconds = 1 // per-op override; the runner default below is far larger
	store := newFakeReshardStore(op)
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	// Break convergence: Set keeps the CR pointing at the SOURCE (simulates a
	// bogus target shard the composition can't render).
	stuck := &stuckCnpgDuckling{fakeDuckling: duckling}
	copier := &fakeCopier{}

	r := testRunner(store, stuck, copier)
	r.flipTimeout = time.Hour // must NOT bound the wait — the per-op value does
	runOp(t, r, store)

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

// waitOpTerminal polls the fake store until the op reaches a terminal state
// (or the deadline), for tests that drive the runner via AdoptClaimedOperation
// (which launches execution on its own goroutine rather than via runOp).
func waitOpTerminal(t *testing.T, store *fakeReshardStore) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		st := store.op.State
		store.mu.Unlock()
		if st.Terminal() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("operation did not reach a terminal state in time")
}

// TestReshardAdoptClaimedExecutesWithStashedPassword pins the claim-on-create
// handoff: an op handed straight to the runner via AdoptClaimedOperation (as
// the admin start handler does for a create-claimed op) executes to completion
// using the stashed external password — no scanOnce poll tick, no password on
// the op row.
func TestReshardAdoptClaimedExecutesWithStashedPassword(t *testing.T) {
	op := cnpgOp()
	op.TargetKind = configstore.MetadataStoreKindExternal
	op.ToShard = ""
	op.TargetEndpoint = "escape.rds.amazonaws.com"
	op.TargetPasswordSecret = "escape-secret"
	op.TargetUser = "postgres"
	op.TargetDatabase = "postgres"
	// Already claimed by this CP (what CreateReshardOperationClaimed stamps).
	op.State = configstore.ReshardStateRunning
	op.RunnerCP = "cp-test"
	op.RunnerEpoch = 1
	store := newFakeReshardStore(op)
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1, PerTableRows: map[string]int64{"ducklake_metadata": 1}}}

	r := testRunner(store, duckling, copier)
	// AdoptClaimedOperation must run under the runner's lifecycle context, not
	// a request context — leave baseCtx nil to prove it falls back safely.
	r.AdoptClaimedOperation(op, "ext-pw")
	waitOpTerminal(t, store)

	if store.op.State != configstore.ReshardStateSucceeded {
		t.Fatalf("state = %s (err %q), want succeeded", store.op.State, store.op.Error)
	}
	// The stashed password flowed into the copy's target endpoint.
	var sawCopy bool
	for _, call := range copier.calls {
		if call.kind == "copy" {
			sawCopy = true
			if call.target.Password != "ext-pw" {
				t.Fatalf("copy target password = %q, want the stashed ext-pw", call.target.Password)
			}
		}
	}
	if !sawCopy {
		t.Fatalf("copy never ran; copier calls = %v", copier.kinds())
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

// stuckExternalDuckling flips the type but never populates the status
// password (as if ESO never syncs), so the cnpg→ext cutover wait times out
// and the runner must recover.
type stuckExternalDuckling struct {
	*fakeDuckling
}

func (s *stuckExternalDuckling) SetMetadataStoreExternal(_ context.Context, _ string, ext ExternalMetadataStoreSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, ducklingCall{kind: "external", ext: ext})
	s.status.MetadataStore.Type = configstore.MetadataStoreKindExternal
	s.status.MetadataStore.Endpoint = ext.Endpoint
	s.status.MetadataStore.Password = "" // ESO never syncs
	s.status.ReadyCondition = false
	return nil
}

func stuckExtOp() *configstore.ReshardOperation {
	op := cnpgOp()
	op.TargetKind = configstore.MetadataStoreKindExternal
	op.ToShard = ""
	op.TargetEndpoint = "escape.rds.amazonaws.com"
	op.TargetPasswordSecret = "rds/duckling-example/master"
	op.TargetUser = "postgres"
	op.TargetDatabase = "postgres"
	op.CutoverTimeoutSeconds = 1
	return op
}

// TestReshardExtFlipTimeoutRecovers pins the ORPHAN-ADOPT recovery: when the
// cnpg→ext cutover never becomes ready (the target's status password never
// syncs), the flip times out and the runner recovers by flipping back to the
// source shard and RE-ADOPTING the still-present (orphaned) cnpg role/DB — NO
// copy-back, NO empty-recreate — leaving the org in service. The source is
// never dropped (external verify never reached).
func TestReshardExtFlipTimeoutRecovers(t *testing.T) {
	store := newFakeReshardStore(stuckExtOp())
	duckling := &stuckExternalDuckling{fakeDuckling: &fakeDuckling{status: cnpgSourceStatus()}}
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1, PerTableRows: map[string]int64{"ducklake_metadata": 1}}}

	r := testRunner(store, duckling, copier)
	r.StashExternalPassword(store.op.ID, "ext-pw")
	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateFailed || !strings.Contains(store.op.Error, "did not become ready") {
		t.Fatalf("state=%s err=%q, want flip-timeout failure", store.op.State, store.op.Error)
	}
	if !store.hasLog("waiting for external target") {
		t.Fatalf("generic waiting line missing: %v", store.logs)
	}
	// Recovery is flip-back + adopt (SetMetadataStoreCnpgAdopt), never copy-back.
	if !containsStr(duckling.callKinds(), "cnpg-adopt") {
		t.Fatalf("recovery did not flip back + adopt (no cnpg-adopt call): %v", duckling.callKinds())
	}
	// Exactly ONE copy (the forward copy) — no copy-back to a recreated source.
	nCopy := 0
	for _, k := range copier.kinds() {
		if k == "copy" {
			nCopy++
		}
	}
	if nCopy != 1 {
		t.Fatalf("copy ran %d times, want exactly 1 (no copy-back on adopt recovery): %v", nCopy, copier.kinds())
	}
	// The orphaned cnpg source is NEVER dropped when the flip failed.
	if containsStr(copier.kinds(), "dropdb") {
		t.Fatalf("orphaned cnpg source was dropped on a failed flip — must be retained for adoption: %v", copier.kinds())
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready after recovery", store.warehouseState)
	}
}

// TestReshardBackupRecordedBeforeFlip pins the pre-flip catalog backup: it runs
// after pause-compaction and BEFORE the cutover, dumps the RECORDED source
// endpoint to the org's data bucket under _reshard_catalog_backups/, records
// backup_s3_uri on the op row, and logs a runnable pg_restore command.
func TestReshardBackupRecordedBeforeFlip(t *testing.T) {
	store := newFakeReshardStore(cnpgOp())
	duckling := &fakeDuckling{status: cnpgSourceStatus(), compPresent: true, compEnabled: true}
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1, PerTableRows: map[string]int64{"ducklake_metadata": 1}}}
	backuper := &fakeBackuper{}
	r := testRunner(store, duckling, copier)
	r.backuper = backuper

	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateSucceeded {
		t.Fatalf("state = %s (err %q), want succeeded", store.op.State, store.op.Error)
	}
	if backuper.count() != 1 {
		t.Fatalf("backuper called %d times, want 1", backuper.count())
	}
	// Backup step is sequenced before cutover.
	backupIdx, cutoverIdx := -1, -1
	for i, s := range store.steps {
		switch s {
		case "backup_catalog":
			backupIdx = i
		case "cutover":
			cutoverIdx = i
		}
	}
	if backupIdx < 0 || cutoverIdx < 0 || backupIdx > cutoverIdx {
		t.Fatalf("backup_catalog (%d) must precede cutover (%d): steps=%v", backupIdx, cutoverIdx, store.steps)
	}
	// The dump reads the recorded pre-flip source, not the post-flip target.
	if !strings.HasPrefix(backuper.calls[0].Host, "shard-001-pooler") {
		t.Fatalf("backup read from %q, want the recorded shard-001 source", backuper.calls[0].Host)
	}
	// Destination: org data bucket, reserved prefix, org IAM role.
	dest := backuper.dests[0]
	if dest.Bucket != "org-a-data-bucket" || dest.RoleARN != "arn:aws:iam::123:role/duckling-org-a" {
		t.Fatalf("backup dest bucket/role = %q/%q", dest.Bucket, dest.RoleARN)
	}
	if !strings.HasPrefix(dest.Key, "_reshard_catalog_backups/op-1-") || !strings.HasSuffix(dest.Key, ".dump") {
		t.Fatalf("backup key = %q, want _reshard_catalog_backups/op-1-<ts>.dump", dest.Key)
	}
	if store.op.BackupS3URI != "s3://org-a-data-bucket/"+dest.Key {
		t.Fatalf("op.BackupS3URI = %q, want the uploaded URI", store.op.BackupS3URI)
	}
	if !store.hasLog("catalog backup complete") || !store.hasLog("pg_restore --no-owner") {
		t.Fatalf("backup URI + restore command missing from log: %v", store.logs)
	}
	if !store.hasLog("pre-flip catalog backup: s3://org-a-data-bucket/") {
		t.Fatalf("backup URI missing from end-of-op report: %v", store.logs)
	}
}

// TestReshardBackupFailureCnpgToExtFailsBeforeFlip pins the HARD gate: on the
// destructive cnpg→external direction a backup failure fails the op BEFORE the
// flip — no cutover, source untouched, warehouse returned to ready.
func TestReshardBackupFailureCnpgToExtFailsBeforeFlip(t *testing.T) {
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
	r.backuper = &fakeBackuper{backupErr: fmt.Errorf("pg_dump exploded")}
	r.StashExternalPassword(op.ID, "ext-pw")

	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateFailed || !strings.Contains(store.op.Error, "backup is mandatory") {
		t.Fatalf("state=%s err=%q, want mandatory-backup failure", store.op.State, store.op.Error)
	}
	for _, k := range duckling.callKinds() {
		if k == "external" {
			t.Fatal("destructive flip must NOT run when the pre-flip backup failed")
		}
	}
	for _, k := range copier.kinds() {
		if k == "copy" {
			t.Fatal("copy must not run when the mandatory backup failed")
		}
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready after rollback", store.warehouseState)
	}
}

// TestReshardBackupFailureNonDestructiveContinues pins best-effort semantics:
// on a non-destructive direction (cnpg→cnpg, source survives) a backup failure
// logs a warning and the op proceeds to succeed.
func TestReshardBackupFailureNonDestructiveContinues(t *testing.T) {
	store := newFakeReshardStore(cnpgOp())
	duckling := &fakeDuckling{status: cnpgSourceStatus(), compPresent: true, compEnabled: true}
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1, PerTableRows: map[string]int64{"ducklake_metadata": 1}}}
	r := testRunner(store, duckling, copier)
	r.backuper = &fakeBackuper{backupErr: fmt.Errorf("s3 upload timed out")}

	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateSucceeded {
		t.Fatalf("state = %s (err %q), want succeeded despite best-effort backup failure", store.op.State, store.op.Error)
	}
	if !store.hasLog("pre-flip catalog backup failed") {
		t.Fatalf("best-effort backup-failure warning missing: %v", store.logs)
	}
	if store.op.BackupS3URI != "" {
		t.Fatalf("op.BackupS3URI = %q, want empty when backup failed", store.op.BackupS3URI)
	}
}

// TestReshardExtVerifyMismatchAdopts pins the step-5 gate: if the external
// target catalog does NOT exactly match the copied row counts, the runner
// REFUSES to drop the retained cnpg source and instead flips back + re-adopts
// it — no data loss.
func TestReshardExtVerifyMismatchAdopts(t *testing.T) {
	op := cnpgOp()
	op.TargetKind = configstore.MetadataStoreKindExternal
	op.ToShard = ""
	op.TargetEndpoint = "escape.rds.amazonaws.com"
	op.TargetPasswordSecret = "escape-secret"
	op.TargetUser = "postgres"
	op.TargetDatabase = "postgres"
	store := newFakeReshardStore(op)
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	copier := &fakeCopier{
		copyResult: CatalogCopyResult{Tables: 1, Rows: 5, PerTableRows: map[string]int64{"ducklake_data_file": 5}},
		// External target verify sees a DIFFERENT count than the copy snapshot.
		externalCounts: map[string]int64{"ducklake_data_file": 4},
	}

	r := testRunner(store, duckling, copier)
	r.StashExternalPassword(op.ID, "ext-pw")
	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateFailed || !strings.Contains(store.op.Error, "catalog incomplete") {
		t.Fatalf("state=%s err=%q, want external-verify (catalog incomplete) failure", store.op.State, store.op.Error)
	}
	// The retained cnpg source must NEVER be dropped on a verify mismatch.
	if containsStr(copier.kinds(), "dropdb") {
		t.Fatalf("verify mismatch dropped the source — must retain it for adoption: %v", copier.kinds())
	}
	// Recovery flips back + re-adopts.
	if !containsStr(duckling.callKinds(), "cnpg-adopt") {
		t.Fatalf("verify mismatch did not flip back + adopt: %v", duckling.callKinds())
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready after adopt recovery", store.warehouseState)
	}
}

// TestReshardCnpgToExtXRDUnsupportedRefuses pins the XRD-compat fallback: a
// cluster whose Duckling XRD predates spec.metadataStore.retainCnpgOnFlip (the
// patch is pruned) makes the runner REFUSE the cnpg→ext flip BEFORE touching
// the source — no flip, no drop, source intact, retain flag reset, org back to
// ready.
func TestReshardCnpgToExtXRDUnsupportedRefuses(t *testing.T) {
	op := cnpgOp()
	op.TargetKind = configstore.MetadataStoreKindExternal
	op.ToShard = ""
	op.TargetEndpoint = "escape.rds.amazonaws.com"
	op.TargetPasswordSecret = "escape-secret"
	op.TargetUser = "postgres"
	op.TargetDatabase = "postgres"
	store := newFakeReshardStore(op)
	duckling := &fakeDuckling{status: cnpgSourceStatus(), retainFieldUnsupported: true}
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1, PerTableRows: map[string]int64{"ducklake_metadata": 1}}}

	r := testRunner(store, duckling, copier)
	r.StashExternalPassword(op.ID, "ext-pw")
	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateFailed || !strings.Contains(store.op.Error, "does not support") {
		t.Fatalf("state=%s err=%q, want XRD-unsupported refusal", store.op.State, store.op.Error)
	}
	// Never flipped, never dropped — the source is untouched.
	if containsStr(duckling.callKinds(), "external") {
		t.Fatalf("flip ran despite an unsupported XRD: %v", duckling.callKinds())
	}
	if containsStr(copier.kinds(), "dropdb") {
		t.Fatalf("source dropped despite refusing the flip: %v", copier.kinds())
	}
	// The retain flag we attempted to set is reset on rollback.
	var retainVals []bool
	for _, c := range duckling.calls {
		if c.kind == "retain" && c.retain != nil {
			retainVals = append(retainVals, *c.retain)
		}
	}
	if len(retainVals) == 0 || retainVals[len(retainVals)-1] != false {
		t.Fatalf("retain patches = %v, want the last to reset to false on rollback", retainVals)
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready after refusal rollback", store.warehouseState)
	}
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
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
