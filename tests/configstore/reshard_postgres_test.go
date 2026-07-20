//go:build linux || darwin

package configstore_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
)

func seedReadyWarehouse(t *testing.T, store *cpconfigstore.ConfigStore, orgID string) {
	t.Helper()
	// The warehouse row has an FK to the org row (fk_duckgres_orgs_warehouse).
	if err := store.DB().Create(&cpconfigstore.Org{
		Name:         orgID,
		DatabaseName: orgID,
	}).Error; err != nil {
		t.Fatalf("seed org: %v", err)
	}
	wh := &cpconfigstore.ManagedWarehouse{
		OrgID:        orgID,
		DucklingName: orgID,
		State:        cpconfigstore.ManagedWarehouseStateReady,
	}
	wh.MetadataStore.Kind = cpconfigstore.MetadataStoreKindCnpgShard
	if err := store.DB().Create(wh).Error; err != nil {
		t.Fatalf("seed warehouse: %v", err)
	}
}

func newReshardOp(orgID string) *cpconfigstore.ReshardOperation {
	return &cpconfigstore.ReshardOperation{
		OrgID:        orgID,
		DucklingName: orgID,
		SourceKind:   cpconfigstore.MetadataStoreKindCnpgShard,
		FromShard:    "shard-001",
		TargetKind:   cpconfigstore.MetadataStoreKindCnpgShard,
		ToShard:      "shard-002",
	}
}

// TestReshardOperationLifecyclePostgres pins create → claim (epoch bump) →
// fenced writes → finish, plus the one-active-op-per-org partial unique index.
func TestReshardOperationLifecyclePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	op := newReshardOp("org-r1")
	if err := store.CreateReshardOperation(op); err != nil {
		t.Fatalf("create: %v", err)
	}
	if op.ID == 0 || op.State != cpconfigstore.ReshardStatePending {
		t.Fatalf("create returned op %+v, want pending with id", op)
	}

	// Second active op for the same org → conflict (partial unique index).
	if err := store.CreateReshardOperation(newReshardOp("org-r1")); !errors.Is(err, cpconfigstore.ErrReshardConflict) {
		t.Fatalf("second active op error = %v, want ErrReshardConflict", err)
	}
	// A different org is unaffected.
	if err := store.CreateReshardOperation(newReshardOp("org-r2")); err != nil {
		t.Fatalf("other org create: %v", err)
	}

	claimed, err := store.ClaimReshardOperation(op.ID, "cp-a", 5*time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim: op=%v err=%v", claimed, err)
	}
	if claimed.State != cpconfigstore.ReshardStateRunning || claimed.RunnerCP != "cp-a" || claimed.RunnerEpoch != 1 {
		t.Fatalf("claimed = %+v, want running/cp-a/epoch=1", claimed)
	}
	if claimed.StartedAt == nil {
		t.Fatal("claim did not stamp started_at")
	}

	// A second claim of a fresh-heartbeat running op must lose.
	second, err := store.ClaimReshardOperation(op.ID, "cp-b", 5*time.Minute)
	if err != nil || second != nil {
		t.Fatalf("second claim = %v/%v, want nil (heartbeat fresh)", second, err)
	}

	if err := store.UpdateReshardStep(op.ID, "cp-a", 1, "draining"); err != nil {
		t.Fatalf("update step: %v", err)
	}
	if err := store.UpdateReshardFields(op.ID, "cp-a", 1, map[string]interface{}{"tables_copied": int64(7)}); err != nil {
		t.Fatalf("update fields: %v", err)
	}

	if err := store.FinishReshardOperation(op.ID, "cp-a", 1, cpconfigstore.ReshardStateSucceeded, ""); err != nil {
		t.Fatalf("finish: %v", err)
	}
	final, err := store.GetReshardOperation(op.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if final.State != cpconfigstore.ReshardStateSucceeded || final.Step != "draining" || final.TablesCopied != 7 || final.FinishedAt == nil {
		t.Fatalf("final = %+v, want succeeded with step+counters+finished_at", final)
	}

	// A terminal org can start a NEW operation (partial index only covers
	// pending/running).
	if err := store.CreateReshardOperation(newReshardOp("org-r1")); err != nil {
		t.Fatalf("post-terminal create: %v", err)
	}
}

// TestReshardTakeoverFencingPostgres pins the stale-heartbeat takeover and
// that the old runner's epoch is fenced out afterwards.
func TestReshardTakeoverFencingPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	op := newReshardOp("org-tk")
	if err := store.CreateReshardOperation(op); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := store.ClaimReshardOperation(op.ID, "cp-a", 5*time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Age the heartbeat past the stale threshold.
	if err := store.DB().Exec(
		`UPDATE duckgres_reshard_operations SET heartbeat_at = now() - interval '10 minutes' WHERE id = ?`, op.ID,
	).Error; err != nil {
		t.Fatalf("age heartbeat: %v", err)
	}

	taken, err := store.ClaimReshardOperation(op.ID, "cp-b", 5*time.Minute)
	if err != nil || taken == nil {
		t.Fatalf("takeover claim: op=%v err=%v", taken, err)
	}
	if taken.RunnerCP != "cp-b" || taken.RunnerEpoch != 2 {
		t.Fatalf("takeover = %+v, want cp-b epoch=2", taken)
	}

	// The zombie's epoch-1 writes are fenced.
	if err := store.UpdateReshardStep(op.ID, "cp-a", 1, "copying"); !errors.Is(err, cpconfigstore.ErrReshardFenced) {
		t.Fatalf("zombie step write error = %v, want ErrReshardFenced", err)
	}
	if err := store.TouchReshardHeartbeat(op.ID, "cp-a", 1); !errors.Is(err, cpconfigstore.ErrReshardFenced) {
		t.Fatalf("zombie heartbeat error = %v, want ErrReshardFenced", err)
	}
	// The new runner writes fine.
	if err := store.UpdateReshardStep(op.ID, "cp-b", 2, "copying"); err != nil {
		t.Fatalf("new runner step write: %v", err)
	}
}

// TestReshardPasswordURLAndActiveListPostgres pins the pod-model plumbing:
// the password pull URL is recordable only while the op is PENDING (the
// runner pod's claim freezes the wiring), it round-trips, it is absent from
// terminal updates, and ListActiveReshardOperations returns exactly the
// pending/running set the leader reconciler works on.
func TestReshardPasswordURLAndActiveListPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	op := newReshardOp("org-purl")
	if err := store.CreateReshardOperation(op); err != nil {
		t.Fatalf("create: %v", err)
	}
	url := "http://192.0.2.7:8080/api/v1/reshards/" + strconv.FormatInt(op.ID, 10) + "/password"
	if err := store.SetReshardOperationPasswordURL(op.ID, url); err != nil {
		t.Fatalf("set password url: %v", err)
	}
	fresh, err := store.GetReshardOperation(op.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if fresh.PasswordURL != url {
		t.Fatalf("password url = %q, want %q", fresh.PasswordURL, url)
	}

	// Reconciler working set: the pending op is listed…
	active, err := store.ListActiveReshardOperations()
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	if len(active) != 1 || active[0].ID != op.ID || active[0].PasswordURL != url {
		t.Fatalf("active ops = %+v, want just op %d with its password url", active, op.ID)
	}

	// …a claimed (running) op still is…
	claimed, err := store.ClaimReshardOperation(op.ID, "pod-a", 5*time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v / %+v", err, claimed)
	}
	if active, _ = store.ListActiveReshardOperations(); len(active) != 1 {
		t.Fatalf("running op missing from active list: %+v", active)
	}

	// …and once running the URL is frozen (CAS on pending fails).
	if err := store.SetReshardOperationPasswordURL(op.ID, "http://192.0.2.8:8080/x"); err == nil {
		t.Fatal("set password url on a running op must fail")
	}

	// Terminal ops leave the active set.
	if err := store.FinishReshardOperation(op.ID, "pod-a", claimed.RunnerEpoch, cpconfigstore.ReshardStateFailed, "x"); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if active, _ = store.ListActiveReshardOperations(); len(active) != 0 {
		t.Fatalf("terminal op still in active list: %+v", active)
	}
}

// TestReshardCancelPostgres pins the cancel flag on active ops and the
// immediate finish of unclaimed (pending) ops.
func TestReshardCancelPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	op := newReshardOp("org-cx")
	if err := store.CreateReshardOperation(op); err != nil {
		t.Fatalf("create: %v", err)
	}
	done, err := store.FinishPendingReshardOperation(op.ID, cpconfigstore.ReshardStateCancelled, "cancelled before start")
	if err != nil || !done {
		t.Fatalf("finish pending = %v/%v, want done", done, err)
	}
	if flagged, err := store.RequestReshardCancel(op.ID); err != nil || flagged {
		t.Fatalf("cancel of terminal op = %v/%v, want false", flagged, err)
	}

	op2 := newReshardOp("org-cy")
	if err := store.CreateReshardOperation(op2); err != nil {
		t.Fatalf("create 2: %v", err)
	}
	if _, err := store.ClaimReshardOperation(op2.ID, "cp-a", 5*time.Minute); err != nil {
		t.Fatalf("claim 2: %v", err)
	}
	// Pending-finish must lose against the claimed op.
	if done, err := store.FinishPendingReshardOperation(op2.ID, cpconfigstore.ReshardStateCancelled, "x"); err != nil || done {
		t.Fatalf("finish pending on running op = %v/%v, want false", done, err)
	}
	if flagged, err := store.RequestReshardCancel(op2.ID); err != nil || !flagged {
		t.Fatalf("cancel running op = %v/%v, want flagged", flagged, err)
	}
	fresh, err := store.GetReshardOperation(op2.ID)
	if err != nil || !fresh.CancelRequested {
		t.Fatalf("cancel_requested not set: %+v err=%v", fresh, err)
	}
}

// TestReshardLogPaginationPostgres pins the incremental after_id contract the
// admin console polls with.
func TestReshardLogPaginationPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	op := newReshardOp("org-lg")
	if err := store.CreateReshardOperation(op); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, msg := range []string{"one", "two", "three"} {
		if err := store.AppendReshardLog(op.ID, "info", msg); err != nil {
			t.Fatalf("append %q: %v", msg, err)
		}
	}
	all, err := store.ListReshardLog(op.ID, 0, 100)
	if err != nil || len(all) != 3 {
		t.Fatalf("list all = %d entries, err %v, want 3", len(all), err)
	}
	if all[0].Message != "one" || all[2].Message != "three" {
		t.Fatalf("ordering wrong: %+v", all)
	}
	rest, err := store.ListReshardLog(op.ID, all[0].ID, 100)
	if err != nil || len(rest) != 2 || rest[0].Message != "two" {
		t.Fatalf("after_id list = %+v err=%v, want [two three]", rest, err)
	}
}

// TestWarehouseReshardingBlocksLeaseGrantsPostgres is the LOAD-BEARING gate
// test: once SetWarehouseResharding commits, the lease-grant transaction
// refuses new grants for the org, and the drain-state query sees both leases
// and queued requests.
func TestWarehouseReshardingBlocksLeaseGrantsPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	seedReadyWarehouse(t, store, "org-block")

	now := time.Now()
	limits := cpconfigstore.OrgResourceLimits{}

	// Pre-reshard: a grant works.
	granted := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "req-granted", OrgID: "org-block", Username: "alice",
		CPInstanceID: "cp-a", PID: 1, Protocol: "postgres", RequestedVCPUs: 1,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(granted); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	lease, err := store.TryAcquireOrgConnectionLease(granted.RequestID, limits, now)
	if err != nil || lease == nil {
		t.Fatalf("pre-reshard grant = %v/%v, want lease", lease, err)
	}

	if err := store.SetWarehouseResharding("org-block"); err != nil {
		t.Fatalf("set resharding: %v", err)
	}
	// CAS is one-shot: a second flip must report the state mismatch.
	if err := store.SetWarehouseResharding("org-block"); !errors.Is(err, cpconfigstore.ErrWarehouseStateMismatch) {
		t.Fatalf("second flip error = %v, want ErrWarehouseStateMismatch", err)
	}

	// Post-flip: queued requests are never granted.
	blocked := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "req-blocked", OrgID: "org-block", Username: "bob",
		CPInstanceID: "cp-a", PID: 2, Protocol: "postgres", RequestedVCPUs: 1,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(blocked); err != nil {
		t.Fatalf("enqueue blocked: %v", err)
	}
	noLease, err := store.TryAcquireOrgConnectionLease(blocked.RequestID, limits, now)
	if err != nil {
		t.Fatalf("blocked acquire: %v", err)
	}
	if noLease != nil {
		t.Fatalf("lease granted during resharding: %+v", noLease)
	}

	// Other orgs are unaffected.
	seedReadyWarehouse(t, store, "org-free")
	free := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "req-free", OrgID: "org-free", Username: "carol",
		CPInstanceID: "cp-a", PID: 3, Protocol: "postgres", RequestedVCPUs: 1,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(free); err != nil {
		t.Fatalf("enqueue free: %v", err)
	}
	freeLease, err := store.TryAcquireOrgConnectionLease(free.RequestID, limits, now)
	if err != nil || freeLease == nil {
		t.Fatalf("other org grant = %v/%v, want lease", freeLease, err)
	}

	// Drain state sees the pre-flip lease AND the still-queued blocked
	// request; both must clear for a sound drain barrier.
	drain, err := store.OrgConnectionDrainState("org-block")
	if err != nil {
		t.Fatalf("drain state: %v", err)
	}
	if drain.ActiveLeases != 1 || drain.QueuedConns != 1 || drain.Drained() {
		t.Fatalf("drain = %+v, want 1 lease + 1 queued", drain)
	}

	// Unblock (resharding → ready): grants flow again.
	if err := store.UpdateWarehouseState("org-block", cpconfigstore.ManagedWarehouseStateResharding, map[string]interface{}{
		"state": cpconfigstore.ManagedWarehouseStateReady,
	}); err != nil {
		t.Fatalf("unblock: %v", err)
	}
	lease2, err := store.TryAcquireOrgConnectionLease(blocked.RequestID, limits, now)
	if err != nil || lease2 == nil {
		t.Fatalf("post-unblock grant = %v/%v, want lease", lease2, err)
	}
}

// TestListWorkerRecordsForOrgPostgres pins the drain's worker-gone source.
func TestListWorkerRecordsForOrgPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	seed := func(id int, org string, state cpconfigstore.WorkerState) {
		t.Helper()
		if err := store.UpsertWorkerRecord(&cpconfigstore.WorkerRecord{
			WorkerID: id, PodName: "pod-" + org + "-" + string(state) + "-" + time.Now().Format("150405.000000000"),
			State: state, OrgID: org, OwnerCPInstanceID: "cp-a", LastHeartbeatAt: time.Now(),
		}); err != nil {
			t.Fatalf("seed worker %d: %v", id, err)
		}
	}
	seed(1, "org-w", cpconfigstore.WorkerStateHot)
	seed(2, "org-w", cpconfigstore.WorkerStateHotIdle)
	seed(3, "org-w", cpconfigstore.WorkerStateRetired)
	seed(4, "org-w", cpconfigstore.WorkerStateLost)
	seed(5, "org-other", cpconfigstore.WorkerStateHot)

	records, err := store.ListWorkerRecordsForOrg("org-w")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2 (hot + hot_idle; terminal states excluded)", len(records))
	}
}

// TestListExternalMetadataStoresPostgres pins the reshard form's external
// target discovery: distinct external stores of live warehouses only.
func TestListExternalMetadataStoresPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	seed := func(org, endpoint string, state cpconfigstore.ManagedWarehouseProvisioningState) {
		t.Helper()
		if err := store.DB().Create(&cpconfigstore.Org{Name: org, DatabaseName: org}).Error; err != nil {
			t.Fatalf("seed org %s: %v", org, err)
		}
		wh := &cpconfigstore.ManagedWarehouse{OrgID: org, DucklingName: org, State: state}
		wh.MetadataStore.Kind = cpconfigstore.MetadataStoreKindExternal
		wh.MetadataStore.Endpoint = endpoint
		wh.MetadataStore.PasswordAWSSecret = "secret-" + org
		wh.MetadataStore.Username = "postgres"
		wh.MetadataStore.DatabaseName = "postgres"
		if err := store.DB().Create(wh).Error; err != nil {
			t.Fatalf("seed warehouse %s: %v", org, err)
		}
	}
	seed("ext-a", "a.rds.example.com", cpconfigstore.ManagedWarehouseStateReady)
	seed("ext-b", "b.rds.example.com", cpconfigstore.ManagedWarehouseStateReady)
	seed("ext-gone", "gone.rds.example.com", cpconfigstore.ManagedWarehouseStateDeleted)
	// A cnpg warehouse must not appear.
	seedReadyWarehouse(t, store, "cnpg-org")

	stores, err := store.ListExternalMetadataStores()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(stores) != 2 || stores[0].Endpoint != "a.rds.example.com" || stores[1].Endpoint != "b.rds.example.com" {
		t.Fatalf("stores = %+v, want a + b only (deleted + cnpg excluded)", stores)
	}
	if stores[0].PasswordAWSSecret != "secret-ext-a" || stores[0].User != "postgres" || stores[0].Database != "postgres" {
		t.Fatalf("store[0] = %+v", stores[0])
	}
}
