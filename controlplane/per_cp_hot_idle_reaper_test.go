package controlplane

import (
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakePerCPHotIdleStore struct {
	expired  []configstore.WorkerSnapshot
	listErr  error
	counts   map[string]int
	listCPID string
}

func (s *fakePerCPHotIdleStore) ListExpiredHotIdleSnapshotsForCP(cpID string, now time.Time, ttl time.Duration) ([]configstore.WorkerSnapshot, error) {
	s.listCPID = cpID
	return s.expired, s.listErr
}

func (s *fakePerCPHotIdleStore) CountHotIdleWorkers(orgID, image, cpu, mem string) (int, error) {
	return s.counts[orgID], nil
}

func hotIdleSnap(id int, org string) configstore.WorkerSnapshot {
	return configstore.NewWorkerSnapshot(configstore.WorkerRecord{
		WorkerID: id,
		State:    configstore.WorkerStateHotIdle,
		OrgID:    org,
		Image:    "img",
	})
}

// The fallback reaper retires every expired hot-idle worker this CP owns,
// scoping the list to its own cp instance id, and stamps the last-run gauge.
func TestPerCPHotIdleReaperRetiresExpired(t *testing.T) {
	store := &fakePerCPHotIdleStore{expired: []configstore.WorkerSnapshot{hotIdleSnap(1, "a"), hotIdleSnap(2, "b")}}
	lcStore := &fakeLifecycleStore{terminalReturn: true}
	r := &perCPHotIdleReaper{
		store:        store,
		lifecycle:    NewWorkerLifecycle(lcStore, &fakePhysicalCleanup{}),
		cpInstanceID: "cp-self",
		hotIdleTTL:   time.Minute,
	}

	r.runOnce()

	if store.listCPID != "cp-self" {
		t.Fatalf("expected list scoped to own cp id, got %q", store.listCPID)
	}
	if len(lcStore.terminalTransitions) != 2 {
		t.Fatalf("expected 2 retires, got %d", len(lcStore.terminalTransitions))
	}
	for _, c := range lcStore.terminalTransitions {
		if c.target != configstore.WorkerStateRetired {
			t.Fatalf("expected retired target, got %q", c.target)
		}
		if c.reason != "hot_idle_ttl_expired" {
			t.Fatalf("expected hot_idle_ttl_expired reason, got %q", c.reason)
		}
	}
}

// A per-org DefaultWorkerMinHotIdle floor must be honored: a worker is NOT
// reaped while the org is at/under its floor, so the fallback can't erode a
// warm reserve the leader would have preserved.
func TestPerCPHotIdleReaperHonorsFloor(t *testing.T) {
	store := &fakePerCPHotIdleStore{
		expired: []configstore.WorkerSnapshot{hotIdleSnap(1, "protected")},
		counts:  map[string]int{"protected": 1}, // exactly at floor
	}
	lcStore := &fakeLifecycleStore{terminalReturn: true}
	r := &perCPHotIdleReaper{
		store:        store,
		lifecycle:    NewWorkerLifecycle(lcStore, &fakePhysicalCleanup{}),
		cpInstanceID: "cp-self",
		hotIdleTTL:   time.Minute,
		hotIdleFloor: func(configstore.WorkerSnapshot) int { return 1 },
	}

	r.runOnce()

	if len(lcStore.terminalTransitions) != 0 {
		t.Fatalf("expected floor-protected worker NOT to be retired, got %d retires", len(lcStore.terminalTransitions))
	}
}

// Disabled (hotIdleTTL<=0) is a no-op — no listing, no retires.
func TestPerCPHotIdleReaperDisabled(t *testing.T) {
	store := &fakePerCPHotIdleStore{expired: []configstore.WorkerSnapshot{hotIdleSnap(1, "a")}}
	lcStore := &fakeLifecycleStore{terminalReturn: true}
	r := &perCPHotIdleReaper{
		store:        store,
		lifecycle:    NewWorkerLifecycle(lcStore, &fakePhysicalCleanup{}),
		cpInstanceID: "cp-self",
		hotIdleTTL:   0,
	}

	r.runOnce()

	if len(lcStore.terminalTransitions) != 0 {
		t.Fatalf("expected no retires when disabled, got %d", len(lcStore.terminalTransitions))
	}
}
