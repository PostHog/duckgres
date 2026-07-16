//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/posthog/duckgres/controlplane/configstore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

	heartbeatErrs      []error
	heartbeatIdx       int
	heartbeatCalls     int
	failUnblockedWrite bool
}

func newFakeReshardStore(op *configstore.ReshardOperation) *fakeReshardStore {
	return &fakeReshardStore{
		op:             op,
		warehouseState: configstore.ManagedWarehouseStateReady,
		drainSeq:       []configstore.OrgConnectionDrainStatus{{}},
		workerSeq:      [][]configstore.WorkerRecord{{}},
	}
}

func (f *fakeReshardStore) ClaimReshardOperation(id int64, runnerCP string, staleAfter time.Duration) (*configstore.ReshardOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Mirror the real CAS: terminal ops and running ops with a FRESH heartbeat
	// are not claimable.
	if f.op.State.Terminal() {
		return nil, nil
	}
	if f.op.State == configstore.ReshardStateRunning && f.op.HeartbeatAt != nil && time.Since(*f.op.HeartbeatAt) <= staleAfter {
		return nil, nil
	}
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
	if _, ok := updates["unblocked_at"]; ok && f.failUnblockedWrite {
		return errors.New("persist unblocked_at failed")
	}
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

func (f *fakeReshardStore) TouchReshardHeartbeat(int64, string, int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeatCalls++
	if f.heartbeatIdx >= len(f.heartbeatErrs) {
		return nil
	}
	err := f.heartbeatErrs[f.heartbeatIdx]
	f.heartbeatIdx++
	return err
}

func (f *fakeReshardStore) FinishReshardOperation(_ int64, _ string, _ int64, state configstore.ReshardState, errMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.op.State = state
	f.op.Error = errMsg
	return nil
}

func (f *fakeReshardStore) FinalizeReshardOperation(_ int64, _ string, _ int64, _ string, updates map[string]interface{}, unblockedAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failUnblockedWrite {
		return errors.New("persist finalization failed")
	}
	if f.warehouseState != configstore.ManagedWarehouseStateResharding {
		return configstore.ErrWarehouseStateMismatch
	}
	if state, ok := updates["state"].(configstore.ManagedWarehouseProvisioningState); ok {
		f.warehouseState = state
	}
	f.warehouseUpds = append(f.warehouseUpds, updates)
	f.op.State = configstore.ReshardStateSucceeded
	f.op.UnblockedAt = &unblockedAt
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

// findLog returns the first op-log line containing substr ("" if none).
func (f *fakeReshardStore) findLog(substr string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, l := range f.logs {
		if strings.Contains(l, substr) {
			return l
		}
	}
	return ""
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
	// mrsReadErr scripts a CnpgSourceMRsOrphaned read failure (e.g. an RBAC
	// Forbidden, wrapped the way the real client wraps it).
	mrsReadErr error
	// mrsStuckWithDelete models a composition that never honors the retain
	// flag: the MRs stay at full lifecycle ["*"] no matter what.
	mrsStuckWithDelete bool
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

func (f *fakeDuckling) CnpgSourceMRsOrphaned(context.Context, string) (bool, bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mrsReadErr != nil {
		return false, false, "", f.mrsReadErr
	}
	if f.mrsStuckWithDelete {
		return false, true, `Role "org-a-role": managementPolicies ["*"]; Database "org-a-db": managementPolicies ["*"]`, nil
	}
	// Composition converges instantly: once retain is set, the cnpg source MRs
	// reflect the no-Delete policy. present is always true (the source cnpg
	// Role/Database MRs exist while type is cnpg-shard).
	if f.retainFlag {
		return true, true, `Role "org-a-role": managementPolicies ["Observe","Create","Update"]; Database "org-a-db": managementPolicies ["Observe","Create","Update"]`, nil
	}
	return false, true, `Role "org-a-role": managementPolicies ["*"]; Database "org-a-db": managementPolicies ["*"]`, nil
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
	kind   string // "probe" | "copy" | "droptables" | "dropdb"
	source CatalogEndpoint
	target CatalogEndpoint
	dbName string
}

type fakeCopier struct {
	mu    sync.Mutex
	calls []copierCall

	copyResult CatalogCopyResult
	copyErr    error
	// probeFn, when set, decides each Probe's result (nil = all probes
	// succeed). It sees the probed endpoint so tests can fail e.g. only the
	// post-flip-back recovery probe.
	probeFn func(ep CatalogEndpoint) error
}

type blockingCopier struct {
	*fakeCopier
	started  chan struct{}
	released chan struct{}
	canceled chan struct{}
}

func newBlockingCopier() *blockingCopier {
	return &blockingCopier{
		fakeCopier: &fakeCopier{},
		started:    make(chan struct{}),
		released:   make(chan struct{}),
		canceled:   make(chan struct{}),
	}
}

func (f *blockingCopier) Copy(ctx context.Context, source, target CatalogEndpoint, log func(string, string)) (CatalogCopyResult, error) {
	f.record(copierCall{kind: "copy", source: source, target: target})
	close(f.started)
	select {
	case <-ctx.Done():
		close(f.canceled)
		return CatalogCopyResult{}, ctx.Err()
	case <-f.released:
		return CatalogCopyResult{}, nil
	}
}

func (f *fakeCopier) record(c copierCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
}

func (f *fakeCopier) Probe(_ context.Context, ep CatalogEndpoint) error {
	f.record(copierCall{kind: "probe", target: ep})
	f.mu.Lock()
	fn := f.probeFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ep)
	}
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
		configPollInterval: time.Millisecond,
		heartbeatInterval:  time.Hour,
		staleAfter:         time.Minute,
		flipTimeout:        2 * time.Second,
		hotIdleGrace:       time.Millisecond,
		loopPoll:           5 * time.Millisecond,
		// Shrunk so tests observe the periodic wait-loop observations (15s in
		// production, > any test's whole runtime).
		progressLogInterval: 15 * time.Millisecond,
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
	if fmt.Sprint(kinds) != fmt.Sprint([]string{"probe", "probe", "probe", "copy", "dropdb"}) {
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
	if !store.hasLog("COPY command tags matched for 3 tables") {
		t.Fatalf("COPY verification log missing: %v", store.logs)
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
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1}}

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
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1}}

	r := testRunner(store, duckling, copier)
	r.StashExternalPassword(op.ID, "ext-pw")
	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateSucceeded {
		t.Fatalf("state = %s (err %q), want succeeded", store.op.State, store.op.Error)
	}
	// copy + source-stability recheck BEFORE the flip, external-catalog verify
	// AFTER the flip, then the explicit source drop.
	if fmt.Sprint(copier.kinds()) != fmt.Sprint([]string{"probe", "probe", "copy", "dropdb"}) {
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
	if !store.hasLog("COPY command tags matched for 1 tables") {
		t.Fatalf("COPY verification log missing: %v", store.logs)
	}
	if !store.hasLog("external target catalog copy completed") {
		t.Fatalf("external-verify announce missing from log: %v", store.logs)
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
	// The timeout error carries the last converge observation (root cause).
	if !strings.Contains(store.op.Error, "last observation:") {
		t.Fatalf("flip-timeout error lacks the last observation: %q", store.op.Error)
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

// rollbackPatchPendingDuckling accepts the forward and rollback spec patches,
// but holds status at the target after the rollback patch. This models the
// asynchronous Duckling composition window that can otherwise admit a worker
// against the failed target immediately after rollback.
type rollbackPatchPendingDuckling struct {
	*fakeDuckling
}

func (s *rollbackPatchPendingDuckling) SetMetadataStoreCnpg(ctx context.Context, name, shard string) error {
	if shard != "shard-001" {
		return s.fakeDuckling.SetMetadataStoreCnpg(ctx, name, shard)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, ducklingCall{kind: "cnpg", shard: shard})
	return nil // Spec patch accepted, but status still identifies shard-002.
}

// TestReshardRollbackWaitsForDucklingStatusBeforeUnblocking pins the safety
// boundary exposed by the bogus-shard e2e: a successful rollback PATCH alone
// is not recovery. Until Duckling status reports the recorded source as Ready
// (and its tenant catalog can be probed), traffic must remain blocked.
func TestReshardRollbackWaitsForDucklingStatusBeforeUnblocking(t *testing.T) {
	store := newFakeReshardStore(cnpgOp())
	duckling := &rollbackPatchPendingDuckling{fakeDuckling: &fakeDuckling{status: cnpgSourceStatus()}}
	copier := &fakeCopier{copyErr: errors.New("copy failed after cutover")}
	r := testRunner(store, duckling, copier)
	r.flipTimeout = 50 * time.Millisecond

	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateFailed {
		t.Fatalf("state = %s, want failed", store.op.State)
	}
	if store.warehouseState != configstore.ManagedWarehouseStateResharding {
		t.Fatalf("warehouse state = %s, want resharding while rollback status remains on the target", store.warehouseState)
	}
	if store.op.UnblockedAt != nil {
		t.Fatalf("unblocked_at = %v, want nil before Duckling reports the source ready", store.op.UnblockedAt)
	}
	if !store.hasLog("recovery: waiting up to") {
		t.Fatalf("recovery wait announcement missing: %v", store.logs)
	}
	terminal := store.findLog("recovery: source metadata store did not converge within")
	if terminal == "" || !strings.Contains(terminal, "shard-002-pooler") {
		t.Fatalf("recovery terminal diagnostic = %q, want target-status observation", terminal)
	}
	var compactionPatches int
	for _, call := range duckling.calls {
		if call.kind == "compaction" {
			compactionPatches++
		}
	}
	if compactionPatches != 1 {
		t.Fatalf("compaction patches = %d, want only the initial pause while source recovery remains unverified", compactionPatches)
	}
}

// rollbackExternalPatchWrongSourceDuckling accepts the rollback patch but
// reports a different Ready external catalog. Type+Ready alone must never be
// treated as recovery: the status must identify the recorded source endpoint.
type rollbackExternalPatchWrongSourceDuckling struct {
	*fakeDuckling
}

func (s *rollbackExternalPatchWrongSourceDuckling) SetMetadataStoreExternal(_ context.Context, _ string, ext ExternalMetadataStoreSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, ducklingCall{kind: "external", ext: ext})
	s.status.MetadataStore.Type = configstore.MetadataStoreKindExternal
	s.status.MetadataStore.Endpoint = "wrong-source.example.rds.amazonaws.com"
	s.status.MetadataStore.User = ext.User
	s.status.MetadataStore.Database = ext.Database
	s.status.MetadataStore.Password = "wrong-source-password"
	s.status.ReadyCondition = true
	return nil
}

// TestReshardExternalRollbackWaitsForRecordedSource confirms the same gate is
// used for external→cnpg rollback. A Ready external status for a different
// endpoint is not the catalog this operation recorded before the cutover.
func TestReshardExternalRollbackWaitsForRecordedSource(t *testing.T) {
	op := cnpgOp()
	op.SourceKind = configstore.MetadataStoreKindExternal
	op.FromShard = ""
	op.SourceEndpoint = "source.example.rds.amazonaws.com"
	op.SourceUser = "postgres"
	op.SourceDatabase = "postgres"
	op.SourcePasswordSecret = "source-secret"
	store := newFakeReshardStore(op)

	var source DucklingStatus
	source.MetadataStore.Type = configstore.MetadataStoreKindExternal
	source.MetadataStore.Endpoint = op.SourceEndpoint
	source.MetadataStore.User = op.SourceUser
	source.MetadataStore.Database = op.SourceDatabase
	source.MetadataStore.Password = "source-password"
	source.DataStore.BucketName = "org-a-data-bucket"
	source.DataStore.S3Region = "us-east-1"
	source.IAMRoleARN = "arn:aws:iam::123:role/duckling-org-a"
	source.ReadyCondition = true
	duckling := &rollbackExternalPatchWrongSourceDuckling{fakeDuckling: &fakeDuckling{status: source}}
	copier := &fakeCopier{copyErr: errors.New("copy failed after cutover")}
	r := testRunner(store, duckling, copier)
	r.flipTimeout = 50 * time.Millisecond

	runOp(t, r, store)

	if store.warehouseState != configstore.ManagedWarehouseStateResharding {
		t.Fatalf("warehouse state = %s, want resharding while external rollback reports the wrong source", store.warehouseState)
	}
	if store.op.UnblockedAt != nil {
		t.Fatalf("unblocked_at = %v, want nil before the recorded external source converges", store.op.UnblockedAt)
	}
	terminal := store.findLog("recovery: source metadata store did not converge within")
	if terminal == "" || !strings.Contains(terminal, "wrong-source.example.rds.amazonaws.com") {
		t.Fatalf("recovery terminal diagnostic = %q, want wrong-source observation", terminal)
	}
}

// TestReshardExternalRollbackConvergesBeforeUnblocking covers the successful
// external→cnpg reverse patch. The common recovery gate must accept the
// recorded external source only after it is Ready and its live status
// credentials answer a TLS probe.
func TestReshardExternalRollbackConvergesBeforeUnblocking(t *testing.T) {
	op := cnpgOp()
	op.SourceKind = configstore.MetadataStoreKindExternal
	op.FromShard = ""
	op.SourceEndpoint = "source.example.rds.amazonaws.com"
	op.SourceUser = "postgres"
	op.SourceDatabase = "postgres"
	op.SourcePasswordSecret = "source-secret"
	store := newFakeReshardStore(op)

	var source DucklingStatus
	source.MetadataStore.Type = configstore.MetadataStoreKindExternal
	source.MetadataStore.Endpoint = op.SourceEndpoint
	source.MetadataStore.User = op.SourceUser
	source.MetadataStore.Database = op.SourceDatabase
	source.MetadataStore.Password = "source-password"
	source.DataStore.BucketName = "org-a-data-bucket"
	source.DataStore.S3Region = "us-east-1"
	source.IAMRoleARN = "arn:aws:iam::123:role/duckling-org-a"
	source.ReadyCondition = true
	duckling := &fakeDuckling{status: source}
	copier := &fakeCopier{copyErr: errors.New("copy failed after cutover")}

	runOp(t, testRunner(store, duckling, copier), store)

	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready after external source recovery", store.warehouseState)
	}
	if !containsStr(duckling.callKinds(), "external") {
		t.Fatalf("external rollback patch missing: %v", duckling.callKinds())
	}
	var lastProbe *copierCall
	for i := range copier.calls {
		if copier.calls[i].kind == "probe" {
			lastProbe = &copier.calls[i]
		}
	}
	if lastProbe == nil {
		t.Fatalf("rollback source probe missing: %v", copier.kinds())
	}
	if lastProbe.target.Host != op.SourceEndpoint || lastProbe.target.SSLMode != "require" || lastProbe.target.Password != "ext-pw" {
		t.Fatalf("rollback source probe = %+v, want live external source credentials over TLS", lastProbe.target)
	}
}

// stuckCnpgDuckling records patches but never converges the status.
type stuckCnpgDuckling struct {
	*fakeDuckling
}

// failSourceRecoveryDuckling allows the initial cutover but rejects the
// rollback patch to the source shard, modeling an API or admission failure.
type failSourceRecoveryDuckling struct {
	*fakeDuckling
}

func (s *failSourceRecoveryDuckling) SetMetadataStoreCnpg(ctx context.Context, name, shard string) error {
	if shard == "shard-001" {
		return errors.New("source recovery rejected")
	}
	return s.fakeDuckling.SetMetadataStoreCnpg(ctx, name, shard)
}

// TestReshardRollbackKeepsWarehouseBlockedWhenSourceRecoveryFails asserts the
// safety invariant: traffic cannot be admitted unless rollback has positively
// restored a usable catalog. Logging and swallowing this error is unsafe.
func TestReshardRollbackKeepsWarehouseBlockedWhenSourceRecoveryFails(t *testing.T) {
	store := newFakeReshardStore(cnpgOp())
	duckling := &failSourceRecoveryDuckling{fakeDuckling: &fakeDuckling{status: cnpgSourceStatus()}}
	copier := &fakeCopier{copyErr: errors.New("copy failed after cutover")}

	runOp(t, testRunner(store, duckling, copier), store)

	if store.op.State != configstore.ReshardStateFailed {
		t.Fatalf("state = %s, want failed", store.op.State)
	}
	if store.warehouseState != configstore.ManagedWarehouseStateResharding {
		t.Fatalf("warehouse state = %s, want resharding after failed source recovery", store.warehouseState)
	}
	if store.op.UnblockedAt != nil {
		t.Fatalf("unblocked_at = %v after failed source recovery", store.op.UnblockedAt)
	}
}

// TestReshardRollbackRefusesInvalidFromShardPatch pins the never-emit-an-
// invalid-patch guard (prod incident): a cnpg→cnpg op recorded with an EMPTY
// from_shard (incomplete source identity — pre-guard CPs could create these)
// must NOT attempt the rollback re-point (the XRD rejects an empty cnpgShard,
// so the patch would just fail and mislead), must log explicit operator
// instructions, and must leave the warehouse BLOCKED in resharding — unblocking
// would let clients activate against the wrong (likely empty) target catalog.
func TestReshardRollbackRefusesInvalidFromShardPatch(t *testing.T) {
	op := cnpgOp()
	op.FromShard = "" // the incident shape: source identity was incomplete
	op.CutoverTimeoutSeconds = 1
	store := newFakeReshardStore(op)
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	stuck := &stuckCnpgDuckling{fakeDuckling: duckling} // flip never converges
	copier := &fakeCopier{}

	runOp(t, testRunner(store, stuck, copier), store)

	if store.op.State != configstore.ReshardStateFailed {
		t.Fatalf("state = %s (err %q), want failed", store.op.State, store.op.Error)
	}
	// Exactly ONE cnpg patch: the forward flip. No rollback patch was emitted.
	var shardPatches []string
	for _, call := range duckling.calls {
		if call.kind == "cnpg" {
			shardPatches = append(shardPatches, call.shard)
		}
	}
	if fmt.Sprint(shardPatches) != fmt.Sprint([]string{"shard-002"}) {
		t.Fatalf("shard patches = %v, want only the forward flip (no invalid rollback patch)", shardPatches)
	}
	// Loud operator instructions, and the deliberate left-blocked decision.
	if !store.hasLog("rollback CANNOT re-point the duckling at the source shard") {
		t.Fatalf("invalid-from_shard refusal missing from log: %v", store.logs)
	}
	if !store.hasLog("intentionally left in the resharding state") {
		t.Fatalf("left-blocked decision missing from log: %v", store.logs)
	}
	if store.warehouseState != configstore.ManagedWarehouseStateResharding {
		t.Fatalf("warehouse state = %s, want resharding (left blocked — never unblock onto the wrong store)", store.warehouseState)
	}
	if store.op.UnblockedAt != nil {
		t.Fatalf("unblocked_at = %v, want nil (org left blocked)", store.op.UnblockedAt)
	}
	if !store.hasLog("reshard report (failed)") {
		t.Fatal("failure report missing")
	}
}

// TestReshardRollbackRefusesIncompleteExternalSourcePatch is the ext→cnpg
// sibling of the invalid-from_shard guard: a post-flip rollback whose recorded
// external source identity is incomplete (no password secret — the admin
// handler records it; recordSource cannot) must not emit an invalid external
// spec patch, and must leave the warehouse blocked with operator instructions.
func TestReshardRollbackRefusesIncompleteExternalSourcePatch(t *testing.T) {
	op := cnpgOp()
	op.SourceKind = configstore.MetadataStoreKindExternal
	op.FromShard = ""
	op.SourcePasswordSecret = "" // incomplete: rollback could not re-render the spec
	op.CutoverTimeoutSeconds = 1
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
	stuck := &stuckCnpgDuckling{fakeDuckling: duckling} // flip never converges
	copier := &fakeCopier{}

	runOp(t, testRunner(store, stuck, copier), store)

	if store.op.State != configstore.ReshardStateFailed {
		t.Fatalf("state = %s (err %q), want failed", store.op.State, store.op.Error)
	}
	// No rollback flip to external was emitted.
	if containsStr(duckling.callKinds(), "external") {
		t.Fatalf("rollback emitted an external patch despite the incomplete source identity: %v", duckling.callKinds())
	}
	if !store.hasLog("rollback CANNOT re-point the duckling at the external source") {
		t.Fatalf("incomplete-external-source refusal missing from log: %v", store.logs)
	}
	if !store.hasLog("intentionally left in the resharding state") {
		t.Fatalf("left-blocked decision missing from log: %v", store.logs)
	}
	if store.warehouseState != configstore.ManagedWarehouseStateResharding {
		t.Fatalf("warehouse state = %s, want resharding (left blocked)", store.warehouseState)
	}
}

// TestReshardRecordSourceRefusesStatusKindDrift pins the runner-side defense
// in depth behind the admin submit-time guard: an op whose recorded source
// kind contradicts the duckling STATUS (where the catalog actually lives)
// fails PRE-flip — nothing is flipped, and the rollback unblocks the
// warehouse cleanly. This is the incident shape: an empty config-store
// metadata block defaulted to cnpg-shard while the org lived on an external
// RDS.
func TestReshardRecordSourceRefusesStatusKindDrift(t *testing.T) {
	op := cnpgOp() // recorded as cnpg→cnpg…
	store := newFakeReshardStore(op)
	// …but the duckling status says the catalog lives on an external RDS.
	var st DucklingStatus
	st.MetadataStore.Type = configstore.MetadataStoreKindExternal
	st.MetadataStore.Endpoint = "db.example.rds.amazonaws.com"
	st.MetadataStore.User = "postgres"
	st.MetadataStore.Database = "postgres"
	st.MetadataStore.Password = "rds-pw"
	st.ReadyCondition = true
	duckling := &fakeDuckling{status: st}
	copier := &fakeCopier{}

	runOp(t, testRunner(store, duckling, copier), store)

	if store.op.State != configstore.ReshardStateFailed || !strings.Contains(store.op.Error, "identity drift") {
		t.Fatalf("state=%s err=%q, want identity-drift failure", store.op.State, store.op.Error)
	}
	// Refused BEFORE any flip: the duckling was never patched to a store.
	for _, k := range duckling.callKinds() {
		if k == "cnpg" || k == "external" || k == "cnpg-adopt" {
			t.Fatalf("duckling flipped (%s) despite the identity drift — must refuse pre-flip", k)
		}
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready (pre-flip failure rolls back cleanly)", store.warehouseState)
	}
}

// TestReshardTakeoverEndpointsDeriveSSLModeFromKind pins that a takeover
// resume reconstructs each endpoint's sslmode from its metadata-store KIND —
// never hardcoded. Regression for the takeover path that hardcoded "disable"
// and hit RDS pg_hba "no encryption" against an external source in prod.
func TestReshardTakeoverEndpointsDeriveSSLModeFromKind(t *testing.T) {
	st := &DucklingStatus{}
	st.MetadataStore.Endpoint = "shard-002-pooler.cnpg-shards.svc.cluster.local"
	st.MetadataStore.User = "mdstore_org"
	st.MetadataStore.Database = "mdstore_org"
	st.MetadataStore.Password = "pinned-pw"

	cases := []struct {
		name                string
		sourceKind, tgtKind string
		wantSrcSSL, wantTgt string
	}{
		{"cnpg→cnpg", configstore.MetadataStoreKindCnpgShard, configstore.MetadataStoreKindCnpgShard, "disable", "disable"},
		{"ext→cnpg (external source needs TLS)", configstore.MetadataStoreKindExternal, configstore.MetadataStoreKindCnpgShard, "require", "disable"},
		{"cnpg→ext (external target needs TLS)", configstore.MetadataStoreKindCnpgShard, configstore.MetadataStoreKindExternal, "disable", "require"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			op := &configstore.ReshardOperation{
				SourceKind:     tc.sourceKind,
				TargetKind:     tc.tgtKind,
				SourceEndpoint: "src.example.internal",
				SourceUser:     "u",
				SourceDatabase: "d",
			}
			source, target := takeoverEndpoints(op, st)
			if source.SSLMode != tc.wantSrcSSL || target.SSLMode != tc.wantTgt {
				t.Fatalf("sslmodes = source %q / target %q, want %q / %q", source.SSLMode, target.SSLMode, tc.wantSrcSSL, tc.wantTgt)
			}
		})
	}

	if got := sslModeFor(configstore.MetadataStoreKindExternal); got != "require" {
		t.Fatalf("sslModeFor(external) = %q, want require", got)
	}
	if got := sslModeFor(configstore.MetadataStoreKindCnpgShard); got != "disable" {
		t.Fatalf("sslModeFor(cnpg-shard) = %q, want disable", got)
	}
}

// TestReshardTakeoverPreservesOriginalCompactionSnapshot ensures replay cannot
// replace the persisted pre-reshard setting with the already-paused live value.
func TestReshardTakeoverPreservesOriginalCompactionSnapshot(t *testing.T) {
	op := cnpgOp()
	op.Step = "pausing_compaction"
	op.CompactionWasPresent = true
	op.CompactionWasEnabled = true
	store := newFakeReshardStore(op)
	duckling := &fakeDuckling{status: cnpgSourceStatus(), compPresent: true, compEnabled: false}
	copier := &fakeCopier{copyResult: CatalogCopyResult{}}

	runOp(t, testRunner(store, duckling, copier), store)

	if !store.op.CompactionWasEnabled {
		t.Fatal("takeover overwrote original compaction=true snapshot with paused live value")
	}
}

// TestReshardTakeoverAfterCutoverDoesNotReplaySourceDiscovery models a runner
// crash after the CR already points at the target. A takeover must use the
// durable source identity and resume/reconcile from the persisted phase; it
// must never treat the live target status as the source.
func TestReshardTakeoverAfterCutoverDoesNotReplaySourceDiscovery(t *testing.T) {
	op := cnpgOp()
	op.Step = "copying"
	op.SourceEndpoint = "shard-001-pooler.cnpg-shards.svc.cluster.local"
	op.SourceUser = "mdstore_org"
	op.SourceDatabase = "mdstore_org"
	blocked := time.Now().UTC().Add(-time.Minute)
	op.BlockedAt = &blocked

	store := newFakeReshardStore(op)
	store.warehouseState = configstore.ManagedWarehouseStateResharding
	targetStatus := cnpgSourceStatus()
	targetStatus.MetadataStore.Endpoint = "shard-002-pooler.cnpg-shards.svc.cluster.local"
	duckling := &fakeDuckling{status: targetStatus, compPresent: true, compEnabled: false}
	copier := &fakeCopier{copyResult: CatalogCopyResult{}}

	runOp(t, testRunner(store, duckling, copier), store)

	if store.op.SourceEndpoint != "shard-001-pooler.cnpg-shards.svc.cluster.local" {
		t.Fatalf("durable source overwritten with live target: %q", store.op.SourceEndpoint)
	}
	for _, call := range copier.calls {
		if call.kind == "copy" && call.source.Host != "shard-001-pooler.cnpg-shards.svc.cluster.local" {
			t.Fatalf("takeover copied from %q, want durable source shard-001", call.source.Host)
		}
		if call.kind == "copy" && (call.source.SSLMode != "disable" || call.target.SSLMode != "disable") {
			// cnpg endpoints are in-cluster plaintext; the sslmode must be
			// KIND-derived (sslModeFor), never hardcoded — see
			// TestReshardTakeoverEndpointsDeriveSSLModeFromKind for the
			// external cases.
			t.Fatalf("takeover copy sslmodes = source %q / target %q, want disable/disable for cnpg→cnpg", call.source.SSLMode, call.target.SSLMode)
		}
		if call.kind == "dropdb" && call.target.Host == "shard-002-pooler.cnpg-shards.svc.cluster.local" {
			t.Fatal("takeover attempted destructive cleanup against the live target")
		}
	}
}

func executeWithBlockingCopy(t *testing.T, store *fakeReshardStore, heartbeatErr error) (*blockingCopier, <-chan struct{}) {
	t.Helper()
	store.heartbeatErrs = []error{heartbeatErr}
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	copier := newBlockingCopier()
	r := testRunner(store, duckling, copier.fakeCopier)
	r.copier = copier
	r.heartbeatInterval = 5 * time.Millisecond
	op, err := store.ClaimReshardOperation(store.op.ID, r.cpID, r.staleAfter)
	if err != nil || op == nil {
		t.Fatalf("claim = %v, %v", op, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.execute(context.Background(), op)
	}()
	select {
	case <-copier.started:
	case <-time.After(time.Second):
		t.Fatal("copy did not start")
	}
	return copier, done
}

// TestReshardHeartbeatFenceCancelsInFlightCopy asserts that epoch fencing is
// propagated through the operation context, including long-running external
// side effects. Merely closing a channel checked before the first step leaves a
// zombie copier alive after ownership has moved.
func TestReshardHeartbeatFenceCancelsInFlightCopy(t *testing.T) {
	store := newFakeReshardStore(cnpgOp())
	copier, done := executeWithBlockingCopy(t, store, configstore.ErrReshardFenced)
	defer close(copier.released)

	select {
	case <-copier.canceled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("in-flight copy context was not cancelled after heartbeat fencing")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fenced runner did not stop")
	}
}

// TestReshardTransientHeartbeatErrorDoesNotFenceRunner distinguishes a store
// outage from a lost epoch. A transient error must be retried while ownership
// is retained; it must not silently turn the active operation into a zombie.
func TestReshardTransientHeartbeatErrorDoesNotFenceRunner(t *testing.T) {
	store := newFakeReshardStore(cnpgOp())
	copier, done := executeWithBlockingCopy(t, store, errors.New("temporary config-store outage"))

	time.Sleep(30 * time.Millisecond)
	select {
	case <-copier.canceled:
		t.Fatal("transient heartbeat error was treated as fencing")
	default:
	}
	store.mu.Lock()
	heartbeats := store.heartbeatCalls
	store.mu.Unlock()
	if heartbeats < 2 {
		t.Fatalf("heartbeat attempts = %d, want retry after transient error", heartbeats)
	}
	close(copier.released)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runner did not finish after copy release")
	}
}

// TestReshardFinalizeFailureRollsBackBeforeReadmittingTraffic pins the final
// commit invariant: a failed atomic finalization enters the ordinary rollback
// path, and only that verified recovery may return the warehouse to ready.
func TestReshardFinalizeFailureRollsBackBeforeReadmittingTraffic(t *testing.T) {
	store := newFakeReshardStore(cnpgOp())
	store.failUnblockedWrite = true
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	copier := &fakeCopier{copyResult: CatalogCopyResult{}}

	runOp(t, testRunner(store, duckling, copier), store)

	if store.warehouseState != configstore.ManagedWarehouseStateReady || store.op.State != configstore.ReshardStateFailed {
		t.Fatalf("state after failed atomic finalization = warehouse %s/op %s, want verified rollback ready/failed", store.warehouseState, store.op.State)
	}
	if len(store.warehouseUpds) == 0 || store.warehouseUpds[len(store.warehouseUpds)-1]["status_message"] != "metadata-store reshard rolled back" {
		t.Fatalf("warehouse was not readmitted through rollback: %v", store.warehouseUpds)
	}
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

// TestReshardTakeoverCancelUnblocksWarehouse is the regression for the mw-dev
// incident: the runner that BLOCKED the warehouse crashed (OOM during the
// pre-flip backup); a different replica later claimed the op (a fresh opRun with
// all progress flags false) and, seeing cancel_requested, rolled back. Before
// the fix the rollback's `if o.blocked` / `if o.compactionPaused` guards were
// false on the takeover runner, so the op was marked cancelled while the
// warehouse stayed stuck in `resharding` and compaction stayed paused. With
// progress reconstructed from the persisted op row, the takeover rollback must
// unblock the warehouse and restore compaction exactly as the original runner
// would have.
func TestReshardTakeoverCancelUnblocksWarehouse(t *testing.T) {
	// A cnpg→ext op a prior epoch had already carried through backup_catalog
	// (the incident's exact step) before its runner died.
	op := cnpgOp()
	op.TargetKind = configstore.MetadataStoreKindExternal
	op.ToShard = ""
	op.TargetEndpoint = "escape.rds.amazonaws.com"
	op.TargetPasswordSecret = "escape-secret"
	blocked := time.Now().UTC().Add(-2 * time.Minute)
	op.BlockedAt = &blocked
	op.Step = "backup_catalog"
	op.CompactionWasPresent = true
	op.CompactionWasEnabled = false // pre-reshard compaction was already off
	op.CancelRequested = true       // operator hit cancel while the owner was dead

	store := newFakeReshardStore(op)
	store.warehouseState = configstore.ManagedWarehouseStateResharding // prior runner blocked it
	duckling := &fakeDuckling{status: cnpgSourceStatus(), compPresent: true, compEnabled: false}
	copier := &fakeCopier{}

	// A fresh runner (different replica) claims and executes the abandoned op.
	runOp(t, testRunner(store, duckling, copier), store)

	if store.op.State != configstore.ReshardStateCancelled {
		t.Fatalf("state = %s (err %q), want cancelled", store.op.State, store.op.Error)
	}
	// The whole point: the warehouse is returned to ready, not left resharding.
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready after takeover rollback", store.warehouseState)
	}
	if !store.hasLog("warehouse unblocked") {
		t.Fatal("takeover rollback did not unblock the warehouse")
	}
	// Compaction is restored to the recorded prior setting (a takeover must not
	// skip this): a compaction patch back to the recorded value is issued.
	if !store.hasLog("compaction setting restored") {
		t.Fatal("takeover rollback did not restore compaction")
	}
	// backup_catalog is before any flip, so nothing may be flipped.
	for _, k := range duckling.callKinds() {
		if k == "cnpg" || k == "external" || k == "cnpg-adopt" {
			t.Fatalf("takeover rollback flipped the duckling (%s) — source was never flipped", k)
		}
	}
}

// waitOpTerminal polls the fake store until the op reaches a terminal state
// (or the deadline), for tests that drive the runner on its own goroutine
// rather than via runOp.
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

// TestReshardRunSingleOperationWithStashedPassword pins the pod-model flow:
// RunSingleOperation claims the PENDING op (what the admin start handler now
// creates), executes it to completion using the stashed external password
// (what the pod pulled from the creating replica over the password URL), and
// returns nil — with no password on the op row.
func TestReshardRunSingleOperationWithStashedPassword(t *testing.T) {
	op := cnpgOp()
	op.TargetKind = configstore.MetadataStoreKindExternal
	op.ToShard = ""
	op.TargetEndpoint = "escape.rds.amazonaws.com"
	op.TargetPasswordSecret = "escape-secret"
	op.TargetUser = "postgres"
	op.TargetDatabase = "postgres"
	store := newFakeReshardStore(op)
	duckling := &fakeDuckling{status: cnpgSourceStatus()}
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1}}

	r := testRunner(store, duckling, copier)
	r.StashExternalPassword(op.ID, "ext-pw")
	if err := r.RunSingleOperation(context.Background(), op.ID); err != nil {
		t.Fatalf("RunSingleOperation: %v", err)
	}

	if store.op.State != configstore.ReshardStateSucceeded {
		t.Fatalf("state = %s (err %q), want succeeded", store.op.State, store.op.Error)
	}
	// The claim CAS ran: the op is fenced to this runner.
	if store.op.RunnerCP != "cp-test" || store.op.RunnerEpoch != 1 {
		t.Fatalf("op not claimed by the runner: cp=%q epoch=%d", store.op.RunnerCP, store.op.RunnerEpoch)
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

// TestReshardRunSingleOperationOutcomes pins the pod exit contract: a FAILED
// (rolled-back) op still returns nil (the op row is the outcome's source of
// truth — the pod exits 0); a terminal op is a no-op nil; an op owned by
// another live runner (fresh heartbeat) is an infrastructure ERROR (nonzero
// exit) — the pod must never run an op it could not claim.
func TestReshardRunSingleOperationOutcomes(t *testing.T) {
	// Failed op → nil: bogus flip target rolls back but the pod did its job.
	op := cnpgOp()
	store := newFakeReshardStore(op)
	duckling := &stuckCnpgDuckling{fakeDuckling: &fakeDuckling{status: cnpgSourceStatus()}}
	r := testRunner(store, duckling, &fakeCopier{})
	if err := r.RunSingleOperation(context.Background(), op.ID); err != nil {
		t.Fatalf("RunSingleOperation on a failing op must return nil (op row is the outcome), got %v", err)
	}
	if store.op.State != configstore.ReshardStateFailed {
		t.Fatalf("state = %s, want failed", store.op.State)
	}

	// Already-terminal op → nil no-op (the respawn-after-finish race).
	if err := r.RunSingleOperation(context.Background(), op.ID); err != nil {
		t.Fatalf("RunSingleOperation on a terminal op must be a nil no-op, got %v", err)
	}

	// Fresh-heartbeat op owned elsewhere → claim not winnable → error.
	op2 := cnpgOp()
	now := time.Now().UTC()
	op2.State = configstore.ReshardStateRunning
	op2.RunnerCP = "another-pod"
	op2.RunnerEpoch = 3
	op2.HeartbeatAt = &now
	store2 := newFakeReshardStore(op2)
	r2 := testRunner(store2, &fakeDuckling{status: cnpgSourceStatus()}, &fakeCopier{})
	if err := r2.RunSingleOperation(context.Background(), op2.ID); err == nil {
		t.Fatal("RunSingleOperation must error when the claim is not winnable")
	}
	if store2.op.State != configstore.ReshardStateRunning || store2.op.RunnerCP != "another-pod" {
		t.Fatalf("unclaimable op mutated: state=%s cp=%s", store2.op.State, store2.op.RunnerCP)
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
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1}}

	r := testRunner(store, duckling, copier)
	r.StashExternalPassword(store.op.ID, "ext-pw")
	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateFailed || !strings.Contains(store.op.Error, "did not become ready") {
		t.Fatalf("state=%s err=%q, want flip-timeout failure", store.op.State, store.op.Error)
	}
	if !store.hasLog("waiting for external target") {
		t.Fatalf("generic waiting line missing: %v", store.logs)
	}
	// The timeout error carries the last observed duckling state (root cause).
	if !strings.Contains(store.op.Error, "last observation:") ||
		!strings.Contains(store.op.Error, "ESO password synced=false") {
		t.Fatalf("flip-timeout error lacks the last ESO/Ready observation: %q", store.op.Error)
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
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1}}
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
	// The backup announces itself before pg_dump starts (streamed, duration and
	// size unknown up front — the log must not go silent while it runs).
	if !store.hasLog("backing up source catalog") || !store.hasLog("may take several minutes") {
		t.Fatalf("backup start announce missing from log: %v", store.logs)
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
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1}}
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
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1}}
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
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1}}

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

// TestReshardCnpgToExtOrphanWaitForbiddenFailsFast pins the RBAC fail-fast:
// when the runner's ServiceAccount cannot read the provider-sql Role/Database
// MRs (Forbidden), the orphan wait must NOT poll to its timeout and then blame
// the composition — it fails immediately with an error naming the missing
// grant, before any flip, and rolls back cleanly. Regression for the mw-dev
// reshard op that burned the full 15m wait on a Forbidden read.
func TestReshardCnpgToExtOrphanWaitForbiddenFailsFast(t *testing.T) {
	op := cnpgOp()
	op.TargetKind = configstore.MetadataStoreKindExternal
	op.ToShard = ""
	op.TargetEndpoint = "escape.rds.amazonaws.com"
	op.TargetPasswordSecret = "escape-secret"
	op.TargetUser = "postgres"
	op.TargetDatabase = "postgres"
	store := newFakeReshardStore(op)
	// Wrapped exactly the way the real client wraps a Get failure — the
	// runner's IsForbidden must see through the %w chain.
	forbidden := fmt.Errorf("get Database %q: %w", "org-a-db",
		apierrors.NewForbidden(
			schema.GroupResource{Group: "postgresql.sql.m.crossplane.io", Resource: "databases"},
			"org-a-db", errors.New("no RBAC grant")))
	duckling := &fakeDuckling{status: cnpgSourceStatus(), mrsReadErr: forbidden}
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1}}

	r := testRunner(store, duckling, copier)
	// An hour-long flip timeout proves the failure is the fail-fast, not the
	// deadline (runOp's 30s guard would trip if the wait polled to timeout).
	r.flipTimeout = time.Hour
	r.StashExternalPassword(op.ID, "ext-pw")
	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateFailed ||
		!strings.Contains(store.op.Error, "Forbidden") ||
		!strings.Contains(store.op.Error, "postgresql.sql.m.crossplane.io") {
		t.Fatalf("state=%s err=%q, want fast Forbidden failure naming the missing RBAC grant", store.op.State, store.op.Error)
	}
	// Never flipped, never dropped — the source is untouched.
	if containsStr(duckling.callKinds(), "external") {
		t.Fatalf("flip ran despite the Forbidden orphan check: %v", duckling.callKinds())
	}
	if containsStr(copier.kinds(), "dropdb") {
		t.Fatalf("source dropped despite refusing the flip: %v", copier.kinds())
	}
	// The retain flag we set is reset on rollback.
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
		t.Fatalf("warehouse state = %s, want ready after fail-fast rollback", store.warehouseState)
	}
}

// TestReshardCnpgToExtOrphanWaitTimeoutReportsObservation pins the wait-loop
// diagnostics: when the composition never re-renders the MRs without Delete,
// the periodic op-log lines and the final timeout error must carry the last
// observed managementPolicies, so a stuck wait is diagnosable from the op log
// alone.
func TestReshardCnpgToExtOrphanWaitTimeoutReportsObservation(t *testing.T) {
	op := cnpgOp()
	op.TargetKind = configstore.MetadataStoreKindExternal
	op.ToShard = ""
	op.TargetEndpoint = "escape.rds.amazonaws.com"
	op.TargetPasswordSecret = "escape-secret"
	op.TargetUser = "postgres"
	op.TargetDatabase = "postgres"
	store := newFakeReshardStore(op)
	duckling := &fakeDuckling{status: cnpgSourceStatus(), mrsStuckWithDelete: true}
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1}}

	r := testRunner(store, duckling, copier)
	r.flipTimeout = 150 * time.Millisecond
	r.StashExternalPassword(op.ID, "ext-pw")
	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateFailed ||
		!strings.Contains(store.op.Error, "charts/composition skew") ||
		!strings.Contains(store.op.Error, `managementPolicies ["*"]`) {
		t.Fatalf("state=%s err=%q, want composition-skew timeout carrying the last observed managementPolicies", store.op.State, store.op.Error)
	}
	// No flip, no drop — the source is untouched, and rollback recovers.
	if containsStr(duckling.callKinds(), "external") {
		t.Fatalf("flip ran despite the MRs never reflecting the retain policy: %v", duckling.callKinds())
	}
	if containsStr(copier.kinds(), "dropdb") {
		t.Fatalf("source dropped despite refusing the flip: %v", copier.kinds())
	}
	if store.warehouseState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("warehouse state = %s, want ready after timeout rollback", store.warehouseState)
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

// TestProbeErrorAuthClassification pins isAuthProbeError/describeProbeFailure
// against errors shaped the way catalog_copy.go::Probe actually wraps them
// (`connect <redacted>: %w` / `probe <redacted>: %w` around pgx errors, whose
// chain carries a *pgconn.PgError for server-side auth failures).
func TestProbeErrorAuthClassification(t *testing.T) {
	saslPgErr := &pgconn.PgError{Severity: "FATAL", Code: "28P01", Message: "SASL authentication failed"}
	pwPgErr := &pgconn.PgError{Severity: "FATAL", Code: "28P01", Message: `password authentication failed for user "mdstore_org"`}
	authSpecErr := &pgconn.PgError{Severity: "FATAL", Code: "28000", Message: `no pg_hba.conf entry for host "10.0.0.1"`}

	auth := []error{
		// Wrapped the way Probe wraps a pgx connect failure (the mw-dev
		// incident shape: pooler answered FATAL: SASL authentication failed).
		fmt.Errorf("connect host=shard-001-pooler user=mdstore_org: failed to connect to `user=mdstore_org database=mdstore_org`: server error: %w", saslPgErr),
		fmt.Errorf("probe host=shard-001-pooler user=mdstore_org: %w", pwPgErr),
		fmt.Errorf("connect host=x user=y: %w", authSpecErr),
		// Pre-flattened text (no PgError left in the chain).
		errors.New("connect host=x user=y: FATAL: SASL authentication failed (SQLSTATE 28P01)"),
		errors.New("failed to connect: password authentication failed for user \"mdstore_org\""),
	}
	for _, err := range auth {
		if !isAuthProbeError(err) {
			t.Errorf("isAuthProbeError(%v) = false, want true", err)
		}
		got := describeProbeFailure(err, "mdstore_org")
		if !strings.Contains(got, "AUTHENTICATION error") ||
			!strings.Contains(got, "stranded password") ||
			!strings.Contains(got, "ALTER ROLE mdstore_org WITH PASSWORD") {
			t.Errorf("describeProbeFailure(%v) lacks the named condition + operator remedy: %q", err, got)
		}
		if strings.Contains(got, "'pinned-pw'") {
			t.Errorf("describeProbeFailure must never embed a password: %q", got)
		}
	}

	notAuth := []error{
		errors.New("connect host=x user=y: dial tcp 10.0.0.1:5432: connect: connection refused"),
		context.DeadlineExceeded,
		fmt.Errorf("probe host=x user=y: %w", &pgconn.PgError{Severity: "FATAL", Code: "57P03", Message: "the database system is starting up"}),
	}
	for _, err := range notAuth {
		if isAuthProbeError(err) {
			t.Errorf("isAuthProbeError(%v) = true, want false", err)
		}
		if got := describeProbeFailure(err, "mdstore_org"); strings.Contains(got, "AUTHENTICATION") {
			t.Errorf("describeProbeFailure(%v) misclassified as auth: %q", err, got)
		}
	}
}

// TestReshardExtRecoveryProbeAuthFailureDiagnosed is the regression for the
// mw-dev incident where the cnpg→ext post-flip recovery spun silently for ~8
// minutes on `FATAL: SASL authentication failed` (the re-adopted role's actual
// password differed from the freshly regenerated status password) and the
// operator had to reproduce the probe from a shell to see why. The recovery
// wait must (a) announce its deadline, (b) periodically log the classified
// probe failure naming the stranded-password condition + the manual ALTER ROLE
// remedy, and (c) end with a timeout line carrying that last observation —
// all without ever logging a password.
func TestReshardExtRecoveryProbeAuthFailureDiagnosed(t *testing.T) {
	store := newFakeReshardStore(stuckExtOp())
	duckling := &stuckExternalDuckling{fakeDuckling: &fakeDuckling{status: cnpgSourceStatus()}}
	copier := &fakeCopier{copyResult: CatalogCopyResult{Tables: 1, Rows: 1}}

	// The forward copy pre-flight probes the cnpg source once (must succeed so
	// the op reaches the flip); every LATER probe of the cnpg endpoint is the
	// recovery loop re-probing the re-adopted role — those fail with the
	// incident-shaped SASL error, persistently (stranded password).
	saslErr := fmt.Errorf("connect host=shard-001-pooler user=mdstore_org: failed to connect to `user=mdstore_org database=mdstore_org`: server error: %w",
		&pgconn.PgError{Severity: "FATAL", Code: "28P01", Message: "SASL authentication failed"})
	var cnpgProbes atomic.Int32
	copier.probeFn = func(ep CatalogEndpoint) error {
		if ep.SSLMode != "disable" {
			return nil // external target pre-flight
		}
		if cnpgProbes.Add(1) == 1 {
			return nil // forward-copy source pre-flight
		}
		return saslErr
	}

	r := testRunner(store, duckling, copier)
	r.StashExternalPassword(store.op.ID, "ext-pw")
	runOp(t, r, store)

	if store.op.State != configstore.ReshardStateFailed {
		t.Fatalf("state=%s err=%q, want failed", store.op.State, store.op.Error)
	}
	// (a) The recovery wait announces itself with its deadline.
	if !store.hasLog("recovery: waiting up to") {
		t.Fatalf("recovery deadline announce missing: %v", store.logs)
	}
	// (b) Periodic observations name the auth failure + operator remedy.
	if n := store.countLog("AUTHENTICATION error"); n < 2 {
		t.Fatalf("want >=2 periodic auth-failure observations, got %d: %v", n, store.logs)
	}
	if !store.hasLog("stranded password") || !store.hasLog("ALTER ROLE mdstore_org WITH PASSWORD") {
		t.Fatalf("stranded-password condition/remedy missing from the periodic observations: %v", store.logs)
	}
	// (c) The terminal recovery-timeout line carries the last observation.
	terminal := store.findLog("recovery: source metadata store did not converge within")
	if terminal == "" {
		t.Fatalf("recovery timeout line missing: %v", store.logs)
	}
	if !strings.Contains(terminal, "last observation:") ||
		!strings.Contains(terminal, "SASL authentication failed") ||
		!strings.Contains(terminal, "AUTHENTICATION error") {
		t.Fatalf("recovery timeout line lacks the classified root cause: %q", terminal)
	}
	// Never a password in the op log — neither the pinned cnpg password nor
	// the ephemeral external one.
	store.mu.Lock()
	logs := append([]string(nil), store.logs...)
	store.mu.Unlock()
	for _, l := range logs {
		if strings.Contains(l, "pinned-pw") || strings.Contains(l, "ext-pw") {
			t.Fatalf("op log leaked a password: %q", l)
		}
	}
	// The recovery attempted flip-back + adopt, but traffic must remain blocked
	// because the tenant role never answered successfully.
	if !containsStr(duckling.callKinds(), "cnpg-adopt") {
		t.Fatalf("recovery did not flip back + adopt: %v", duckling.callKinds())
	}
	if store.warehouseState != configstore.ManagedWarehouseStateResharding {
		t.Fatalf("warehouse state = %s, want resharding after unverified recovery", store.warehouseState)
	}
}
