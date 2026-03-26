package controlplane

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakeRuntimeInstanceStore struct {
	mu      sync.Mutex
	records []configstore.ControlPlaneInstance
}

func (f *fakeRuntimeInstanceStore) UpsertControlPlaneInstance(instance *configstore.ControlPlaneInstance) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, *instance)
	return nil
}

func (f *fakeRuntimeInstanceStore) snapshot() []configstore.ControlPlaneInstance {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]configstore.ControlPlaneInstance, len(f.records))
	copy(out, f.records)
	return out
}

func TestControlPlaneRuntimeTrackerStartHeartbeats(t *testing.T) {
	store := &fakeRuntimeInstanceStore{}
	tracker := NewControlPlaneRuntimeTracker(store, "cp-1:boot-a", "duckgres-0", "pod-uid-1", "boot-a", 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := tracker.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	deadline := time.Now().Add(250 * time.Millisecond)
	for len(store.snapshot()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	records := store.snapshot()
	if len(records) < 2 {
		t.Fatalf("expected at least 2 upserts, got %d", len(records))
	}
	if records[0].State != configstore.ControlPlaneInstanceStateActive {
		t.Fatalf("expected initial state active, got %q", records[0].State)
	}
	if records[0].ID != "cp-1:boot-a" {
		t.Fatalf("expected id cp-1:boot-a, got %q", records[0].ID)
	}
}

func TestControlPlaneRuntimeTrackerMarkDraining(t *testing.T) {
	store := &fakeRuntimeInstanceStore{}
	tracker := NewControlPlaneRuntimeTracker(store, "cp-1:boot-a", "duckgres-0", "pod-uid-1", "boot-a", time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := tracker.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := tracker.MarkDraining(); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}
	if !tracker.Draining() {
		t.Fatal("expected tracker to report draining")
	}

	records := store.snapshot()
	last := records[len(records)-1]
	if last.State != configstore.ControlPlaneInstanceStateDraining {
		t.Fatalf("expected draining state, got %q", last.State)
	}
	if last.DrainingAt == nil {
		t.Fatal("expected draining_at to be set")
	}
}
