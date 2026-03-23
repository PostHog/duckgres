//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTeamReservedWorkerPoolAcquireReservesTeamWorker(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewTeamReservedWorkerPool(shared, "analytics", 2)
	worker, err := pool.AcquireWorker(context.Background())
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if worker.activeSessions != 1 {
		t.Fatalf("expected active session claim, got %d", worker.activeSessions)
	}

	state := worker.SharedState()
	if state.Assignment == nil || state.Assignment.TeamName != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
	if state.Lifecycle != WorkerLifecycleReserved {
		t.Fatalf("expected reserved lifecycle, got %q", state.Lifecycle)
	}
}

func TestTeamReservedWorkerPoolAcquireSkipsOtherTeamsWorkers(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	other := &ManagedWorker{ID: 1, done: make(chan struct{})}
	if err := other.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			TeamName:       "billing",
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

	pool := NewTeamReservedWorkerPool(shared, "analytics", 2)
	worker, err := pool.AcquireWorker(context.Background())
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if worker.ID == other.ID {
		t.Fatal("expected analytics pool to reserve its own worker, not borrow another team's worker")
	}
	if state := worker.SharedState(); state.Assignment == nil || state.Assignment.TeamName != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
}

func TestTeamReservedWorkerPoolReleaseWorkerRetiresOnLastSession(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	worker := &ManagedWorker{ID: 9, activeSessions: 1, done: make(chan struct{})}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			TeamName:       "analytics",
			LeaseExpiresAt: time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("SetSharedState(worker): %v", err)
	}
	shared.workers[worker.ID] = worker

	pool := NewTeamReservedWorkerPool(shared, "analytics", 1)
	pool.ReleaseWorker(worker.ID)

	time.Sleep(100 * time.Millisecond)
	if _, ok := shared.Worker(worker.ID); ok {
		t.Fatal("expected worker to be retired after last session release")
	}
}

func TestTeamReservedWorkerPoolAcquireActivatesReservedWorkerWhenEnabledWithTeamConfig(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	activated := false
	pool := NewTeamReservedWorkerPool(shared, "analytics", 2)
	pool.sharedWarmWorkers = true
	pool.resolveTeamConfig = func() (*configstore.TeamConfig, error) {
		return &configstore.TeamConfig{
			Name: "analytics",
			Users: map[string]string{
				"alice": "ignored",
			},
		}, nil
	}
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker, team *configstore.TeamConfig) error {
		if team == nil || team.Name != "analytics" {
			t.Fatalf("expected analytics team config, got %#v", team)
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

func TestTeamReservedWorkerPoolAcquireActivatesUsingLatestResolvedTeamConfig(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	currentTeam := &configstore.TeamConfig{
		Name: "analytics",
		Users: map[string]string{
			"alice": "ignored",
		},
	}

	pool := NewTeamReservedWorkerPool(shared, "analytics", 2)
	pool.sharedWarmWorkers = true
	pool.resolveTeamConfig = func() (*configstore.TeamConfig, error) {
		return currentTeam, nil
	}
	var capturedTeam *configstore.TeamConfig
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker, team *configstore.TeamConfig) error {
		capturedTeam = team
		return nil
	}

	currentTeam = &configstore.TeamConfig{
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
	if capturedTeam == nil {
		t.Fatal("expected activation to receive resolved team config")
	}
	if len(capturedTeam.Users) != 1 || capturedTeam.Users["bob"] != "ignored" {
		t.Fatalf("expected latest resolved team config to be used, got %#v", capturedTeam.Users)
	}
}
