//go:build linux || darwin

package configstore_test

import (
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

	leaseExpiry := startedAt.Add(24 * time.Hour)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          42,
		PodName:           "duckgres-worker-42",
		State:             configstore.WorkerStateIdle,
		OwnerCPInstanceID: "cp-1:boot-a",
		OwnerEpoch:        7,
		LeaseExpiresAt:    leaseExpiry,
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

	leaseExpiry := startedAt.Add(24 * time.Hour)
	claimed, err := store.ClaimIdleWorker("cp-new:boot-b", "analytics", leaseExpiry)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected idle worker claim to succeed")
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
	if !claimed.LeaseExpiresAt.Equal(leaseExpiry) {
		t.Fatalf("expected lease expiry %v, got %v", leaseExpiry, claimed.LeaseExpiresAt)
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

	claimed, err := store.ClaimIdleWorker("cp-new:boot-b", "analytics", time.Date(2026, time.March, 27, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claim, got %#v", claimed)
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

func TestCreateSpawningWorkerSlotPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	leaseExpiry := time.Date(2026, time.March, 27, 12, 0, 0, 0, time.UTC)
	slot, err := store.CreateSpawningWorkerSlot("cp-new:boot-b", "analytics", 1, leaseExpiry, "duckgres-worker-test-cp", 3, 5)
	if err != nil {
		t.Fatalf("CreateSpawningWorkerSlot: %v", err)
	}
	if slot == nil {
		t.Fatal("expected spawning worker slot to be created")
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
	if !slot.LeaseExpiresAt.Equal(leaseExpiry) {
		t.Fatalf("expected lease expiry %v, got %v", leaseExpiry, slot.LeaseExpiresAt)
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
		LeaseExpiresAt:    now.Add(24 * time.Hour),
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(existing): %v", err)
	}

	orgLimited, err := store.CreateSpawningWorkerSlot("cp-new:boot-b", "analytics", 1, now.Add(2*time.Hour), "duckgres-worker-test-cp", 1, 5)
	if err != nil {
		t.Fatalf("CreateSpawningWorkerSlot(org cap): %v", err)
	}
	if orgLimited != nil {
		t.Fatalf("expected org cap to block spawning, got %#v", orgLimited)
	}

	globalLimited, err := store.CreateSpawningWorkerSlot("cp-new:boot-b", "sales", 1, now.Add(2*time.Hour), "duckgres-worker-test-cp", 2, 1)
	if err != nil {
		t.Fatalf("CreateSpawningWorkerSlot(global cap): %v", err)
	}
	if globalLimited != nil {
		t.Fatalf("expected global cap to block spawning, got %#v", globalLimited)
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
		t.Fatalf("expected orphaned worker 61, got %#v", orphaned)
	}

	stuck, err := store.ListStuckWorkers(now.Add(-2*time.Minute), now.Add(-2*time.Minute))
	if err != nil {
		t.Fatalf("ListStuckWorkers: %v", err)
	}
	if len(stuck) != 1 || stuck[0].WorkerID != 62 {
		t.Fatalf("expected stuck worker 62, got %#v", stuck)
	}
}

func TestExpireFlightSessionRecordsPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 15, 0, 0, 0, time.UTC)

	if err := store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken: "flight-expire-me",
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

func ptrTime(t time.Time) *time.Time {
	return &t
}
