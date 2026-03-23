//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestOrgReservedPoolAcquireReservesOrgWorker(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewOrgReservedPool(shared, "analytics", 2)
	worker, err := pool.AcquireWorker(context.Background())
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if worker.activeSessions != 1 {
		t.Fatalf("expected active session claim, got %d", worker.activeSessions)
	}

	state := worker.SharedState()
	if state.Assignment == nil || state.Assignment.OrgID != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
	if state.Lifecycle != WorkerLifecycleReserved {
		t.Fatalf("expected reserved lifecycle, got %q", state.Lifecycle)
	}
}

func TestOrgReservedPoolAcquireSkipsOtherOrgsWorkers(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	other := &ManagedWorker{ID: 1, done: make(chan struct{})}
	if err := other.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			OrgID:          "billing",
			LeaseExpiresAt: time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("SetSharedState(other): %v", err)
	}
	shared.workers[other.ID] = other

	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewOrgReservedPool(shared, "analytics", 2)
	worker, err := pool.AcquireWorker(context.Background())
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if worker.ID == other.ID {
		t.Fatal("expected analytics pool to reserve its own worker, not borrow another org's worker")
	}
	if state := worker.SharedState(); state.Assignment == nil || state.Assignment.OrgID != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
}

func TestOrgReservedPoolReleaseWorkerRetiresOnLastSession(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	worker := &ManagedWorker{ID: 9, activeSessions: 1, done: make(chan struct{})}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			OrgID:          "analytics",
			LeaseExpiresAt: time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("SetSharedState(worker): %v", err)
	}
	shared.workers[worker.ID] = worker

	pool := NewOrgReservedPool(shared, "analytics", 1)
	pool.ReleaseWorker(worker.ID)

	time.Sleep(100 * time.Millisecond)
	if _, ok := shared.Worker(worker.ID); ok {
		t.Fatal("expected worker to be retired after last session release")
	}
}

func TestOrgReservedWorkerPoolAcquireActivatesReservedWorkerWhenEnabledWithOrgConfig(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	activated := false
	pool := NewOrgReservedPool(shared, "analytics", 2)
	pool.sharedWarmWorkers = true
	pool.resolveOrgConfig = func() (*configstore.OrgConfig, error) {
		return &configstore.OrgConfig{
			Name: "analytics",
			Users: map[string]string{
				"alice": "ignored",
			},
		}, nil
	}
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker, org *configstore.OrgConfig) error {
		if org == nil || org.Name != "analytics" {
			t.Fatalf("expected analytics org config, got %#v", org)
		}
		activated = true
		return nil
	}

	worker, err := pool.AcquireWorker(context.Background())
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if !activated {
		t.Fatal("expected reserved worker activation")
	}
	if got := worker.SharedState().Lifecycle; got != WorkerLifecycleHot {
		t.Fatalf("expected hot lifecycle after activation, got %q", got)
	}
}

func TestOrgReservedWorkerPoolAcquireActivatesUsingLatestResolvedOrgConfig(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	currentOrg := &configstore.OrgConfig{
		Name: "analytics",
		Users: map[string]string{
			"alice": "ignored",
		},
	}

	pool := NewOrgReservedPool(shared, "analytics", 2)
	pool.sharedWarmWorkers = true
	pool.resolveOrgConfig = func() (*configstore.OrgConfig, error) {
		return currentOrg, nil
	}
	var capturedOrg *configstore.OrgConfig
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker, org *configstore.OrgConfig) error {
		capturedOrg = org
		return nil
	}

	currentOrg = &configstore.OrgConfig{
		Name: "analytics",
		Users: map[string]string{
			"bob": "ignored",
		},
	}

	worker, err := pool.AcquireWorker(context.Background())
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if got := worker.SharedState().Lifecycle; got != WorkerLifecycleHot {
		t.Fatalf("expected hot lifecycle after activation, got %q", got)
	}
	if capturedOrg == nil {
		t.Fatal("expected activation to receive resolved org config")
	}
	if len(capturedOrg.Users) != 1 || capturedOrg.Users["bob"] != "ignored" {
		t.Fatalf("expected latest resolved org config to be used, got %#v", capturedOrg.Users)
	}
}
