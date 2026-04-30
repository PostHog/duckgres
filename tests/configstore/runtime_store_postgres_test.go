//go:build linux || darwin

package configstore_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestRuntimeStorePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	runtimeSchema := store.RuntimeSchema()
	if runtimeSchema == "" {
		t.Fatal("expected runtime schema to be configured")
	}

	for _, table := range []string{"cp_instances", "worker_records", "flight_session_records"} {
		var count int64
		if err := store.DB().Raw(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			runtimeSchema,
			table,
		).Scan(&count).Error; err != nil {
			t.Fatalf("lookup %s.%s: %v", runtimeSchema, table, err)
		}
		if count != 1 {
			t.Fatalf("expected runtime table %s.%s to exist", runtimeSchema, table)
		}
	}

	startedAt := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	heartbeatAt := startedAt.Add(5 * time.Second)
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-1:boot-a",
		PodName:         "duckgres-abc",
		PodUID:          "pod-uid-1",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateActive,
		StartedAt:       startedAt,
		LastHeartbeatAt: heartbeatAt,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance: %v", err)
	}

	cp, err := store.GetControlPlaneInstance("cp-1:boot-a")
	if err != nil {
		t.Fatalf("GetControlPlaneInstance: %v", err)
	}
	if cp.PodName != "duckgres-abc" {
		t.Fatalf("expected pod name duckgres-abc, got %q", cp.PodName)
	}
	if !cp.LastHeartbeatAt.Equal(heartbeatAt) {
		t.Fatalf("expected heartbeat %v, got %v", heartbeatAt, cp.LastHeartbeatAt)
	}

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          42,
		PodName:           "duckgres-worker-42",
		State:             configstore.WorkerStateIdle,
		OwnerCPInstanceID: "cp-1:boot-a",
		OwnerEpoch:        7,
		LastHeartbeatAt:   heartbeatAt,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	worker, err := store.GetWorkerRecord(42)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if worker.State != configstore.WorkerStateIdle {
		t.Fatalf("expected worker state idle, got %q", worker.State)
	}
	if worker.OwnerEpoch != 7 {
		t.Fatalf("expected owner epoch 7, got %d", worker.OwnerEpoch)
	}

	sessionExpiry := startedAt.Add(5 * time.Minute)
	if err := store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken: "flight-token-1",
		Username:     "postgres",
		OrgID:        "analytics",
		WorkerID:     42,
		OwnerEpoch:   7,
		State:        configstore.FlightSessionStateActive,
		ExpiresAt:    sessionExpiry,
		LastSeenAt:   heartbeatAt,
	}); err != nil {
		t.Fatalf("UpsertFlightSessionRecord: %v", err)
	}

	session, err := store.GetFlightSessionRecord("flight-token-1")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if session.WorkerID != 42 {
		t.Fatalf("expected worker id 42, got %d", session.WorkerID)
	}
	if session.Username != "postgres" {
		t.Fatalf("expected username postgres, got %q", session.Username)
	}
	if session.State != configstore.FlightSessionStateActive {
		t.Fatalf("expected session state active, got %q", session.State)
	}
}

func TestClaimIdleWorkerPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	startedAt := time.Date(2026, time.March, 26, 13, 0, 0, 0, time.UTC)
	heartbeatAt := startedAt.Add(5 * time.Second)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          7,
		PodName:           "duckgres-worker-7",
		State:             configstore.WorkerStateIdle,
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   heartbeatAt,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	claimed, err := store.ClaimIdleWorker("cp-new:boot-b", "analytics", "", 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected idle worker claim to succeed")
		return
	}
	if claimed.WorkerID != 7 {
		t.Fatalf("expected worker id 7, got %d", claimed.WorkerID)
	}
	if claimed.State != configstore.WorkerStateReserved {
		t.Fatalf("expected reserved state, got %q", claimed.State)
	}
	if claimed.OwnerCPInstanceID != "cp-new:boot-b" {
		t.Fatalf("expected owner cp-instance cp-new:boot-b, got %q", claimed.OwnerCPInstanceID)
	}
	if claimed.OwnerEpoch != 3 {
		t.Fatalf("expected owner epoch 3, got %d", claimed.OwnerEpoch)
	}
	if claimed.OrgID != "analytics" {
		t.Fatalf("expected org analytics, got %q", claimed.OrgID)
	}
	persisted, err := store.GetWorkerRecord(7)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateReserved {
		t.Fatalf("expected persisted reserved state, got %q", persisted.State)
	}
	if persisted.OwnerEpoch != 3 {
		t.Fatalf("expected persisted owner epoch 3, got %d", persisted.OwnerEpoch)
	}
}

func TestClaimIdleWorkerReturnsNilWhenNoIdleWorkerExists(t *testing.T) {
	store := newIsolatedConfigStore(t)

	claimed, err := store.ClaimIdleWorker("cp-new:boot-b", "analytics", "", 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claim, got %#v", claimed)
	}
}

func TestClaimIdleWorkerRespectsOrgCapPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	now := time.Date(2026, time.March, 26, 13, 30, 0, 0, time.UTC)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          7,
		PodName:           "duckgres-worker-7",
		State:             configstore.WorkerStateIdle,
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(idle): %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          8,
		PodName:           "duckgres-worker-8",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        4,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(hot): %v", err)
	}

	claimed, err := store.ClaimIdleWorker("cp-new:boot-b", "analytics", "", 1)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected org cap to block claim, got %#v", claimed)
	}

	persisted, err := store.GetWorkerRecord(7)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateIdle {
		t.Fatalf("expected worker to remain idle, got %q", persisted.State)
	}
	if persisted.OrgID != "" {
		t.Fatalf("expected idle worker org to remain empty, got %q", persisted.OrgID)
	}
}

func TestClaimIdleWorkerRespectsImageAffinity(t *testing.T) {
	store := newIsolatedConfigStore(t)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID: 7,
		PodName:  "duckgres-worker-v1",
		State:    configstore.WorkerStateIdle,
		Image:    "duckgres:v1",
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID: 8,
		PodName:  "duckgres-worker-v2",
		State:    configstore.WorkerStateIdle,
		Image:    "duckgres:v2",
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	// Try claiming v2
	claimed, err := store.ClaimIdleWorker("cp-1", "org-1", "duckgres:v2", 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed == nil || claimed.WorkerID != 8 {
		t.Fatalf("expected to claim worker 8 (v2), got %#v", claimed)
	}

	// Try claiming v3 (none exist)
	claimed, err = store.ClaimIdleWorker("cp-1", "org-1", "duckgres:v3", 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claim for v3, got %#v", claimed)
	}

	// Neutral claim (no image filter) - should get v1 (lowest ID)
	claimed, err = store.ClaimIdleWorker("cp-1", "org-1", "", 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed == nil || claimed.WorkerID != 7 {
		t.Fatalf("expected to claim worker 7 (neutral), got %#v", claimed)
	}
}

func TestGetWorkerRecordReturnsNilNilForMissingRow(t *testing.T) {
	// cleanupOrphanedWorkerPods in k8s_pool.go treats (nil, nil) as "no DB
	// row — this pod is fully orphaned and safe to delete" and a non-nil
	// error as "skip this tick and retry." If GetWorkerRecord wrapped
	// gorm.ErrRecordNotFound as an error, the missing-row branch of the
	// reconciler would be unreachable in production and stranded pods with
	// no DB row would never be cleaned up. This test pins the contract.
	store := newIsolatedConfigStore(t)

	record, err := store.GetWorkerRecord(99999)
	if err != nil {
		t.Fatalf("expected (nil, nil) for missing row, got err=%v", err)
	}
	if record != nil {
		t.Fatalf("expected nil record for missing row, got %#v", record)
	}
}

func TestExpireControlPlaneInstancesPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	startedAt := time.Date(2026, time.March, 26, 14, 0, 0, 0, time.UTC)
	staleHeartbeat := startedAt.Add(5 * time.Second)
	freshHeartbeat := startedAt.Add(40 * time.Second)
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-stale:boot-a",
		PodName:         "duckgres-stale",
		PodUID:          "pod-stale",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateActive,
		StartedAt:       startedAt,
		LastHeartbeatAt: staleHeartbeat,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(stale): %v", err)
	}
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-fresh:boot-b",
		PodName:         "duckgres-fresh",
		PodUID:          "pod-fresh",
		BootID:          "boot-b",
		State:           configstore.ControlPlaneInstanceStateActive,
		StartedAt:       startedAt,
		LastHeartbeatAt: freshHeartbeat,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(fresh): %v", err)
	}

	expired, err := store.ExpireControlPlaneInstances(startedAt.Add(20 * time.Second))
	if err != nil {
		t.Fatalf("ExpireControlPlaneInstances: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expected 1 expired instance, got %d", expired)
	}

	stale, err := store.GetControlPlaneInstance("cp-stale:boot-a")
	if err != nil {
		t.Fatalf("GetControlPlaneInstance(stale): %v", err)
	}
	if stale.State != configstore.ControlPlaneInstanceStateExpired {
		t.Fatalf("expected stale instance to be expired, got %q", stale.State)
	}
	if stale.ExpiredAt == nil {
		t.Fatal("expected expired_at to be set for stale instance")
	}

	fresh, err := store.GetControlPlaneInstance("cp-fresh:boot-b")
	if err != nil {
		t.Fatalf("GetControlPlaneInstance(fresh): %v", err)
	}
	if fresh.State != configstore.ControlPlaneInstanceStateActive {
		t.Fatalf("expected fresh instance to stay active, got %q", fresh.State)
	}
	if fresh.ExpiredAt != nil {
		t.Fatalf("expected fresh instance expired_at to stay nil, got %v", fresh.ExpiredAt)
	}
}

func TestListLiveControlPlaneInstanceIDsPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 9, 12, 0, 0, 0, time.UTC)

	// Active CP — should appear.
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-active:boot-a",
		PodName:         "duckgres-active",
		PodUID:          "pod-active",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateActive,
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeatAt: now,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(active): %v", err)
	}
	// Draining CP — must also appear (still serving in-flight queries).
	drainingAt := now.Add(-time.Minute)
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-draining:boot-b",
		PodName:         "duckgres-draining",
		PodUID:          "pod-draining",
		BootID:          "boot-b",
		State:           configstore.ControlPlaneInstanceStateDraining,
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeatAt: now.Add(-30 * time.Second),
		DrainingAt:      &drainingAt,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(draining): %v", err)
	}
	// Expired CP — must NOT appear.
	expiredAt := now.Add(-2 * time.Minute)
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-expired:boot-c",
		PodName:         "duckgres-expired",
		PodUID:          "pod-expired",
		BootID:          "boot-c",
		State:           configstore.ControlPlaneInstanceStateExpired,
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeatAt: now.Add(-5 * time.Minute),
		ExpiredAt:       &expiredAt,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(expired): %v", err)
	}

	ids, err := store.ListLiveControlPlaneInstanceIDs()
	if err != nil {
		t.Fatalf("ListLiveControlPlaneInstanceIDs: %v", err)
	}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	if !got["cp-active:boot-a"] {
		t.Error("expected active CP to be listed as live")
	}
	if !got["cp-draining:boot-b"] {
		t.Error("expected draining CP to be listed as live (still serving in-flight queries)")
	}
	if got["cp-expired:boot-c"] {
		t.Error("expected expired CP to NOT be listed as live")
	}
}

func TestExpireDrainingControlPlaneInstancesPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	startedAt := time.Date(2026, time.March, 26, 14, 0, 0, 0, time.UTC)
	oldDrain := startedAt.Add(5 * time.Minute)
	recentDrain := startedAt.Add(20 * time.Minute)
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-draining-old:boot-a",
		PodName:         "duckgres-old",
		PodUID:          "pod-old",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateDraining,
		StartedAt:       startedAt,
		LastHeartbeatAt: recentDrain,
		DrainingAt:      &oldDrain,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(old draining): %v", err)
	}
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-draining-recent:boot-b",
		PodName:         "duckgres-recent",
		PodUID:          "pod-recent",
		BootID:          "boot-b",
		State:           configstore.ControlPlaneInstanceStateDraining,
		StartedAt:       startedAt,
		LastHeartbeatAt: recentDrain,
		DrainingAt:      &recentDrain,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(recent draining): %v", err)
	}

	expired, err := store.ExpireDrainingControlPlaneInstances(startedAt.Add(15 * time.Minute))
	if err != nil {
		t.Fatalf("ExpireDrainingControlPlaneInstances: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expected 1 overdue draining instance, got %d", expired)
	}

	old, err := store.GetControlPlaneInstance("cp-draining-old:boot-a")
	if err != nil {
		t.Fatalf("GetControlPlaneInstance(old draining): %v", err)
	}
	if old.State != configstore.ControlPlaneInstanceStateExpired {
		t.Fatalf("expected old draining instance to be expired, got %q", old.State)
	}
	if old.ExpiredAt == nil {
		t.Fatal("expected expired_at to be set for old draining instance")
	}

	recent, err := store.GetControlPlaneInstance("cp-draining-recent:boot-b")
	if err != nil {
		t.Fatalf("GetControlPlaneInstance(recent draining): %v", err)
	}
	if recent.State != configstore.ControlPlaneInstanceStateDraining {
		t.Fatalf("expected recent draining instance to remain draining, got %q", recent.State)
	}
	if recent.ExpiredAt != nil {
		t.Fatalf("expected recent draining instance expired_at to stay nil, got %v", recent.ExpiredAt)
	}
}

func TestCreateSpawningWorkerSlotPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	slot, err := store.CreateSpawningWorkerSlot("cp-new:boot-b", "analytics", "duckgres:test", 1, "duckgres-worker-test-cp", 3, 5)
	if err != nil {
		t.Fatalf("CreateSpawningWorkerSlot: %v", err)
	}
	if slot == nil {
		t.Fatal("expected spawning worker slot to be created")
		return
	}
	if slot.WorkerID <= 0 {
		t.Fatalf("expected positive worker id, got %d", slot.WorkerID)
	}
	if slot.State != configstore.WorkerStateSpawning {
		t.Fatalf("expected spawning state, got %q", slot.State)
	}
	if slot.PodName != "duckgres-worker-test-cp-"+strconv.Itoa(slot.WorkerID) {
		t.Fatalf("unexpected pod name %q for worker id %d", slot.PodName, slot.WorkerID)
	}
	if slot.OwnerCPInstanceID != "cp-new:boot-b" {
		t.Fatalf("expected owner cp-instance cp-new:boot-b, got %q", slot.OwnerCPInstanceID)
	}
	if slot.OwnerEpoch != 1 {
		t.Fatalf("expected owner epoch 1, got %d", slot.OwnerEpoch)
	}
	if slot.OrgID != "analytics" {
		t.Fatalf("expected org analytics, got %q", slot.OrgID)
	}

	persisted, err := store.GetWorkerRecord(slot.WorkerID)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateSpawning {
		t.Fatalf("expected persisted spawning state, got %q", persisted.State)
	}
}

func TestCreateSpawningWorkerSlotRespectsOrgAndGlobalCaps(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 13, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          9,
		PodName:           "duckgres-worker-existing-9",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        4,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(existing): %v", err)
	}

	orgLimited, err := store.CreateSpawningWorkerSlot("cp-new:boot-b", "analytics", "duckgres:test", 1, "duckgres-worker-test-cp", 1, 5)
	if err != nil {
		t.Fatalf("CreateSpawningWorkerSlot(org cap): %v", err)
	}
	if orgLimited != nil {
		t.Fatalf("expected org cap to block spawning, got %#v", orgLimited)
	}

	globalLimited, err := store.CreateSpawningWorkerSlot("cp-new:boot-b", "sales", "duckgres:test", 1, "duckgres-worker-test-cp", 2, 1)
	if err != nil {
		t.Fatalf("CreateSpawningWorkerSlot(global cap): %v", err)
	}
	if globalLimited != nil {
		t.Fatalf("expected global cap to block spawning, got %#v", globalLimited)
	}
}

func TestCreateNeutralWarmWorkerSlotRespectsSharedWarmTarget(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 13, 30, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          10,
		PodName:           "duckgres-worker-existing-10",
		State:             configstore.WorkerStateIdle,
		OrgID:             "",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        0,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(existing neutral): %v", err)
	}

	blocked, err := store.CreateNeutralWarmWorkerSlot("cp-new:boot-b", "duckgres-worker-test-cp", "duckgres:test", 1, 5)
	if err != nil {
		t.Fatalf("CreateNeutralWarmWorkerSlot(shared target): %v", err)
	}
	if blocked != nil {
		t.Fatalf("expected shared warm target to block spawning, got %#v", blocked)
	}

	slot, err := store.CreateNeutralWarmWorkerSlot("cp-new:boot-b", "duckgres-worker-test-cp", "duckgres:test", 2, 5)
	if err != nil {
		t.Fatalf("CreateNeutralWarmWorkerSlot(expand target): %v", err)
	}
	if slot == nil {
		t.Fatal("expected neutral warm slot to be created")
		return
	}
	if slot.OrgID != "" {
		t.Fatalf("expected neutral warm slot org to be empty, got %q", slot.OrgID)
	}
	if slot.State != configstore.WorkerStateSpawning {
		t.Fatalf("expected spawning state, got %q", slot.State)
	}
}

func TestListOrphanedAndStuckWorkersPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 14, 0, 0, 0, time.UTC)

	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-expired:boot-a",
		PodName:         "duckgres-old",
		PodUID:          "pod-old",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateExpired,
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeatAt: now.Add(-time.Minute),
		ExpiredAt:       ptrTime(now.Add(-time.Minute)),
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(expired): %v", err)
	}
	// Insert the live CP that owns worker 62 below. Without this row, the
	// dangling-owner branch of ListOrphanedWorkers would (correctly) flag
	// worker 62 as an orphan; we need it to remain in the "stuck with a
	// live owner" bucket here, since that's what ListStuckWorkers covers.
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-live:boot-b",
		PodName:         "duckgres-live",
		PodUID:          "pod-live",
		BootID:          "boot-b",
		State:           configstore.ControlPlaneInstanceStateActive,
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeatAt: now,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(live): %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          61,
		PodName:           "duckgres-worker-61",
		State:             configstore.WorkerStateReserved,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-expired:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(orphaned): %v", err)
	}
	// Retired and lost rows whose owning CP is expired must NOT be returned:
	// their pods are already deleted (or will be cleaned up by the K8s label
	// scan on the next CP startup), so re-listing them would loop the janitor
	// on a 404 from the K8s pod delete forever.
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          63,
		PodName:           "duckgres-worker-63",
		State:             configstore.WorkerStateRetired,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-expired:boot-a",
		OwnerEpoch:        3,
		LastHeartbeatAt:   now.Add(-time.Minute),
		RetireReason:      "normal",
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(retired orphan): %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          64,
		PodName:           "duckgres-worker-64",
		State:             configstore.WorkerStateLost,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-expired:boot-a",
		OwnerEpoch:        4,
		LastHeartbeatAt:   now.Add(-time.Minute),
		RetireReason:      "crash",
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(lost orphan): %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          62,
		PodName:           "duckgres-worker-62",
		State:             configstore.WorkerStateActivating,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-live:boot-b",
		OwnerEpoch:        1,
		LastHeartbeatAt:   now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(stuck): %v", err)
	}
	if err := store.DB().Table(store.RuntimeSchema()+".worker_records").
		Where("worker_id = ?", 62).
		Update("updated_at", now.Add(-3*time.Minute)).Error; err != nil {
		t.Fatalf("age stuck worker: %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	if len(orphaned) != 1 || orphaned[0].WorkerID != 61 {
		t.Fatalf("expected only active orphaned worker 61, got %#v", orphaned)
	}

	stuck, err := store.ListStuckWorkers(now.Add(-2*time.Minute), now.Add(-2*time.Minute))
	if err != nil {
		t.Fatalf("ListStuckWorkers: %v", err)
	}
	if len(stuck) != 1 || stuck[0].WorkerID != 62 {
		t.Fatalf("expected stuck worker 62, got %#v", stuck)
	}
}

// TestListOrphanedWorkersIncludesOwnerlessIdleWorkers seeds a row that
// reproduces the mw-dev incident: a neutral idle worker whose
// owner_cp_instance_id is the empty string. Today's INNER JOIN against
// the cp_instances table excludes such rows, so the orphan janitor never
// retires them, the warm-target check counts them as live capacity, and
// no new warm workers ever spawn. The fix LEFT JOINs and adds an explicit
// "ownerless and stale" branch.
func TestListOrphanedWorkersIncludesOwnerlessIdleWorkers(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 14, 0, 0, 0, time.UTC)

	// Stale ownerless idle worker — exactly the row that gets stuck today.
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          77,
		PodName:           "duckgres-worker-77",
		State:             configstore.WorkerStateIdle,
		OrgID:             "",
		OwnerCPInstanceID: "",
		OwnerEpoch:        0,
		LastHeartbeatAt:   now.Add(-1 * time.Hour),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(ownerless idle): %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	if len(orphaned) != 1 || orphaned[0].WorkerID != 77 {
		t.Fatalf("expected ownerless idle worker 77 to be returned as an orphan, got %#v", orphaned)
	}
}

// TestListOrphanedWorkersIncludesDanglingOwnerWorkers covers the
// "owner_cp_instance_id is set but no matching cp_instances row exists"
// case — the CP row was hard-deleted (or somehow skipped insertion) but
// the worker row is still active. Same blast radius as the ownerless
// case: invisible to today's INNER JOIN.
func TestListOrphanedWorkersIncludesDanglingOwnerWorkers(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 14, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          88,
		PodName:           "duckgres-worker-88",
		State:             configstore.WorkerStateIdle,
		OrgID:             "",
		OwnerCPInstanceID: "ghost-cp:boot-x", // no matching cp_instances row
		OwnerEpoch:        0,
		LastHeartbeatAt:   now.Add(-1 * time.Hour),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(dangling owner): %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	if len(orphaned) != 1 || orphaned[0].WorkerID != 88 {
		t.Fatalf("expected dangling-owner idle worker 88 to be returned as an orphan, got %#v", orphaned)
	}
}

// TestListOrphanedWorkersExcludesFreshOwnerlessWorker guards against
// over-eager cleanup. The spawn path creates the worker pod first, then
// inserts the DB row with a fresh heartbeat. There's also a brief window
// during slot creation before the owner is stamped. Either way, a freshly-
// stamped ownerless row must NOT be treated as an orphan, or we'd race
// every spawn into the orphan retirement path.
func TestListOrphanedWorkersExcludesFreshOwnerlessWorker(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 14, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          99,
		PodName:           "duckgres-worker-99",
		State:             configstore.WorkerStateIdle,
		OrgID:             "",
		OwnerCPInstanceID: "",
		OwnerEpoch:        0,
		LastHeartbeatAt:   now, // fresh
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(fresh ownerless): %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	if len(orphaned) != 0 {
		t.Fatalf("expected fresh ownerless worker NOT to be returned as orphan, got %#v", orphaned)
	}
}

// TestRetireOrphanWorkerHandlesAllActiveStates exercises the full set of
// states that ListOrphanedWorkers can return. The existing
// RetireIdleOrHotIdleWorker only transitions idle/hot_idle; for an orphan
// stuck in spawning/reserved/activating/hot/draining, that's a no-op and
// the row stays counted as live capacity. The new RetireOrphanWorker must
// transition any of these to retired.
func TestRetireOrphanWorkerHandlesAllActiveStates(t *testing.T) {
	cases := []struct {
		name  string
		state configstore.WorkerState
	}{
		{"spawning", configstore.WorkerStateSpawning},
		{"idle", configstore.WorkerStateIdle},
		{"reserved", configstore.WorkerStateReserved},
		{"activating", configstore.WorkerStateActivating},
		{"hot", configstore.WorkerStateHot},
		{"hot_idle", configstore.WorkerStateHotIdle},
		{"draining", configstore.WorkerStateDraining},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newIsolatedConfigStore(t)
			workerID := 1000 + i

			if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
				WorkerID:          workerID,
				PodName:           fmt.Sprintf("duckgres-worker-%d", workerID),
				State:             tc.state,
				OwnerCPInstanceID: "",
				LastHeartbeatAt:   time.Now().Add(-1 * time.Hour),
			}); err != nil {
				t.Fatalf("UpsertWorkerRecord(%s): %v", tc.state, err)
			}

			retired, err := store.RetireOrphanWorker(workerID, "test_orphan_cleanup")
			if err != nil {
				t.Fatalf("RetireOrphanWorker(%s): %v", tc.state, err)
			}
			if !retired {
				t.Fatalf("expected RetireOrphanWorker(%s) to transition the row, returned false", tc.state)
			}

			// Verify final DB state via a follow-up no-op call (returns false).
			retiredAgain, err := store.RetireOrphanWorker(workerID, "test_orphan_cleanup")
			if err != nil {
				t.Fatalf("RetireOrphanWorker(%s) follow-up: %v", tc.state, err)
			}
			if retiredAgain {
				t.Fatalf("RetireOrphanWorker(%s) was called twice; the second call should be a no-op", tc.state)
			}
		})
	}
}

// TestRetireOrphanWorkerNoOpOnTerminalStates: terminal rows (retired/lost)
// must not be touched. They're already done; transitioning them again
// would clobber retire_reason / updated_at and could mask original
// failure data.
func TestRetireOrphanWorkerNoOpOnTerminalStates(t *testing.T) {
	cases := []struct {
		name  string
		state configstore.WorkerState
	}{
		{"retired", configstore.WorkerStateRetired},
		{"lost", configstore.WorkerStateLost},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newIsolatedConfigStore(t)
			workerID := 2000 + i

			if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
				WorkerID:          workerID,
				PodName:           fmt.Sprintf("duckgres-worker-%d", workerID),
				State:             tc.state,
				OwnerCPInstanceID: "",
				LastHeartbeatAt:   time.Now().Add(-1 * time.Hour),
				RetireReason:      "original_reason",
			}); err != nil {
				t.Fatalf("UpsertWorkerRecord(%s): %v", tc.state, err)
			}

			retired, err := store.RetireOrphanWorker(workerID, "should_be_ignored")
			if err != nil {
				t.Fatalf("RetireOrphanWorker(%s): %v", tc.state, err)
			}
			if retired {
				t.Fatalf("RetireOrphanWorker on terminal %s should be a no-op, but returned true", tc.state)
			}
		})
	}
}

// TestListWorkersDueForCredentialRefreshScopesByOwner: only workers owned
// by the calling CP are returned. Workers owned by another CP — even if
// their creds have expired — are that CP's problem.
func TestListWorkersDueForCredentialRefreshScopesByOwner(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	expired := now.Add(-1 * time.Hour)

	mine := &configstore.WorkerRecord{
		WorkerID:               1,
		PodName:                "duckgres-worker-1",
		State:                  configstore.WorkerStateHot,
		OrgID:                  "acme",
		OwnerCPInstanceID:      "cp-me:boot-a",
		OwnerEpoch:             3,
		LastHeartbeatAt:        now,
		S3CredentialsExpiresAt: &expired,
	}
	other := &configstore.WorkerRecord{
		WorkerID:               2,
		PodName:                "duckgres-worker-2",
		State:                  configstore.WorkerStateHot,
		OrgID:                  "acme",
		OwnerCPInstanceID:      "cp-other:boot-b",
		OwnerEpoch:             5,
		LastHeartbeatAt:        now,
		S3CredentialsExpiresAt: &expired,
	}
	for _, w := range []*configstore.WorkerRecord{mine, other} {
		if err := store.UpsertWorkerRecord(w); err != nil {
			t.Fatalf("UpsertWorkerRecord(%d): %v", w.WorkerID, err)
		}
	}

	due, err := store.ListWorkersDueForCredentialRefresh("cp-me:boot-a", now)
	if err != nil {
		t.Fatalf("ListWorkersDueForCredentialRefresh: %v", err)
	}
	if len(due) != 1 || due[0].WorkerID != 1 {
		t.Fatalf("expected only worker 1 (mine), got %#v", due)
	}
}

// TestListWorkersDueForCredentialRefreshTreatsNullAsDue: a row with
// s3_credentials_expires_at = NULL needs immediate refresh. This covers
// pre-migration rows and any path where activation forgot to stamp.
func TestListWorkersDueForCredentialRefreshTreatsNullAsDue(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          7,
		PodName:           "duckgres-worker-7",
		State:             configstore.WorkerStateHot,
		OrgID:             "acme",
		OwnerCPInstanceID: "cp-me:boot-a",
		OwnerEpoch:        1,
		LastHeartbeatAt:   now,
		// S3CredentialsExpiresAt deliberately left nil
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	due, err := store.ListWorkersDueForCredentialRefresh("cp-me:boot-a", now)
	if err != nil {
		t.Fatalf("ListWorkersDueForCredentialRefresh: %v", err)
	}
	if len(due) != 1 || due[0].WorkerID != 7 {
		t.Fatalf("expected worker 7 (NULL expiry treated as due), got %#v", due)
	}
}

// TestListWorkersDueForCredentialRefreshSkipsHealthyAndNeutral:
//   - Healthy row (expiry comfortably in the future): not returned.
//   - Neutral warm row (org_id=”): not returned regardless of expiry.
//     A pre-activation worker has no STS creds to refresh.
//   - Terminal row (retired): not returned.
func TestListWorkersDueForCredentialRefreshSkipsHealthyAndNeutral(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	farFuture := now.Add(2 * time.Hour)
	pastDue := now.Add(-1 * time.Minute)

	rows := []*configstore.WorkerRecord{
		{
			WorkerID: 10, PodName: "duckgres-worker-10",
			State: configstore.WorkerStateHot, OrgID: "acme",
			OwnerCPInstanceID: "cp-me:boot-a", OwnerEpoch: 1,
			LastHeartbeatAt: now, S3CredentialsExpiresAt: &farFuture,
		},
		{
			WorkerID: 11, PodName: "duckgres-worker-11",
			State: configstore.WorkerStateIdle, OrgID: "",
			OwnerCPInstanceID: "cp-me:boot-a", OwnerEpoch: 1,
			LastHeartbeatAt: now, S3CredentialsExpiresAt: &pastDue,
		},
		{
			WorkerID: 12, PodName: "duckgres-worker-12",
			State: configstore.WorkerStateRetired, OrgID: "acme",
			OwnerCPInstanceID: "cp-me:boot-a", OwnerEpoch: 1,
			LastHeartbeatAt: now, S3CredentialsExpiresAt: &pastDue,
			RetireReason: "normal",
		},
	}
	for _, w := range rows {
		if err := store.UpsertWorkerRecord(w); err != nil {
			t.Fatalf("UpsertWorkerRecord(%d): %v", w.WorkerID, err)
		}
	}

	due, err := store.ListWorkersDueForCredentialRefresh("cp-me:boot-a", now)
	if err != nil {
		t.Fatalf("ListWorkersDueForCredentialRefresh: %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("expected no rows returned (healthy / neutral / terminal), got %#v", due)
	}
}

// TestMarkCredentialsRefreshedSucceeds: happy path — same owner, same
// epoch, write goes through and returns true.
func TestMarkCredentialsRefreshedSucceeds(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	oldExpiry := now.Add(-1 * time.Minute)
	newExpiry := now.Add(1 * time.Hour)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:               20,
		PodName:                "duckgres-worker-20",
		State:                  configstore.WorkerStateHot,
		OrgID:                  "acme",
		OwnerCPInstanceID:      "cp-me:boot-a",
		OwnerEpoch:             3,
		LastHeartbeatAt:        now,
		S3CredentialsExpiresAt: &oldExpiry,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	updated, err := store.MarkCredentialsRefreshed(20, "cp-me:boot-a", 3, newExpiry)
	if err != nil {
		t.Fatalf("MarkCredentialsRefreshed: %v", err)
	}
	if !updated {
		t.Fatalf("expected MarkCredentialsRefreshed to update the row, got false")
	}

	persisted, err := store.GetWorkerRecord(20)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.S3CredentialsExpiresAt == nil || !persisted.S3CredentialsExpiresAt.Equal(newExpiry) {
		t.Fatalf("expected expires_at = %v, got %v", newExpiry, persisted.S3CredentialsExpiresAt)
	}
}

// TestMarkCredentialsRefreshedFailsOnEpochMismatch: another CP took over
// the worker (bumped owner_epoch), and our just-minted creds are stale —
// the conditional update returns false rather than overwriting.
func TestMarkCredentialsRefreshedFailsOnEpochMismatch(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	oldExpiry := now.Add(-1 * time.Minute)
	originalExpiryRow := oldExpiry

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:               21,
		PodName:                "duckgres-worker-21",
		State:                  configstore.WorkerStateHot,
		OrgID:                  "acme",
		OwnerCPInstanceID:      "cp-me:boot-a",
		OwnerEpoch:             7, // newer than the caller will pass
		LastHeartbeatAt:        now,
		S3CredentialsExpiresAt: &originalExpiryRow,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	updated, err := store.MarkCredentialsRefreshed(21, "cp-me:boot-a", 6, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("MarkCredentialsRefreshed: %v", err)
	}
	if updated {
		t.Fatalf("expected stale-epoch caller to be rejected, got updated=true")
	}

	persisted, err := store.GetWorkerRecord(21)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.S3CredentialsExpiresAt == nil || !persisted.S3CredentialsExpiresAt.Equal(originalExpiryRow) {
		t.Fatalf("expected expires_at to be unchanged after epoch mismatch, got %v", persisted.S3CredentialsExpiresAt)
	}
}

// TestMarkCredentialsRefreshedFailsOnOwnerMismatch: another CP took
// ownership entirely. Same conditional-update protection.
func TestMarkCredentialsRefreshedFailsOnOwnerMismatch(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          22,
		PodName:           "duckgres-worker-22",
		State:             configstore.WorkerStateHot,
		OrgID:             "acme",
		OwnerCPInstanceID: "cp-other:boot-b",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	updated, err := store.MarkCredentialsRefreshed(22, "cp-me:boot-a", 2, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("MarkCredentialsRefreshed: %v", err)
	}
	if updated {
		t.Fatalf("expected wrong-owner caller to be rejected, got updated=true")
	}
}

func TestExpireFlightSessionRecordsPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 15, 0, 0, 0, time.UTC)

	if err := store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken: "flight-expire-me",
		Username:     "postgres",
		OrgID:        "analytics",
		WorkerID:     42,
		OwnerEpoch:   7,
		State:        configstore.FlightSessionStateActive,
		ExpiresAt:    now.Add(-time.Minute),
		LastSeenAt:   now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertFlightSessionRecord: %v", err)
	}

	expired, err := store.ExpireFlightSessionRecords(now)
	if err != nil {
		t.Fatalf("ExpireFlightSessionRecords: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expected one expired session record, got %d", expired)
	}

	record, err := store.GetFlightSessionRecord("flight-expire-me")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if record.State != configstore.FlightSessionStateExpired {
		t.Fatalf("expected expired flight session state, got %q", record.State)
	}
}

func TestGetTouchAndCloseFlightSessionRecordPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 16, 0, 0, 0, time.UTC)

	if err := store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken: "flight-touch-close",
		Username:     "postgres",
		OrgID:        "analytics",
		WorkerID:     42,
		OwnerEpoch:   8,
		State:        configstore.FlightSessionStateActive,
		ExpiresAt:    now.Add(time.Hour),
		LastSeenAt:   now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("UpsertFlightSessionRecord: %v", err)
	}

	record, err := store.GetFlightSessionRecord("flight-touch-close")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if record == nil || record.Username != "postgres" {
		t.Fatalf("expected durable record with username postgres, got %#v", record)
	}

	touchedAt := now.Add(2 * time.Minute)
	if err := store.TouchFlightSessionRecord("flight-touch-close", touchedAt); err != nil {
		t.Fatalf("TouchFlightSessionRecord: %v", err)
	}
	record, err = store.GetFlightSessionRecord("flight-touch-close")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if !record.LastSeenAt.Equal(touchedAt) {
		t.Fatalf("expected last_seen_at %v, got %v", touchedAt, record.LastSeenAt)
	}

	closedAt := now.Add(3 * time.Minute)
	if err := store.CloseFlightSessionRecord("flight-touch-close", closedAt); err != nil {
		t.Fatalf("CloseFlightSessionRecord: %v", err)
	}
	record, err = store.GetFlightSessionRecord("flight-touch-close")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if record.State != configstore.FlightSessionStateClosed {
		t.Fatalf("expected closed state, got %q", record.State)
	}
	if !record.LastSeenAt.Equal(closedAt) {
		t.Fatalf("expected close timestamp %v, got %v", closedAt, record.LastSeenAt)
	}
}

func TestGetFlightSessionRecordReturnsNilWhenMissing(t *testing.T) {
	store := newIsolatedConfigStore(t)

	record, err := store.GetFlightSessionRecord("missing-flight-session")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if record != nil {
		t.Fatalf("expected nil record for missing session, got %#v", record)
	}
}

func TestTakeOverWorkerPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 17, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          71,
		PodName:           "duckgres-worker-71",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        5,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	claimed, err := store.TakeOverWorker(71, "cp-new:boot-b", "analytics", 5)
	if err != nil {
		t.Fatalf("TakeOverWorker: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected takeover to succeed")
		return
	}
	if claimed.OwnerCPInstanceID != "cp-new:boot-b" {
		t.Fatalf("expected owner cp-instance cp-new:boot-b, got %q", claimed.OwnerCPInstanceID)
	}
	if claimed.OwnerEpoch != 6 {
		t.Fatalf("expected owner epoch 6, got %d", claimed.OwnerEpoch)
	}
	if claimed.State != configstore.WorkerStateReserved {
		t.Fatalf("expected reserved state, got %q", claimed.State)
	}

	missed, err := store.TakeOverWorker(71, "cp-third:boot-c", "analytics", 5)
	if err == nil {
		t.Fatal("expected stale takeover attempt to return an epoch mismatch error")
	}
	if !errors.Is(err, configstore.ErrWorkerOwnerEpochMismatch) {
		t.Fatalf("expected ErrWorkerOwnerEpochMismatch, got %v", err)
	}
	if missed != nil {
		t.Fatalf("expected stale takeover attempt to fail, got %#v", missed)
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}
