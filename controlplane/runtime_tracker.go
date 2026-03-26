package controlplane

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type runtimeInstanceStore interface {
	UpsertControlPlaneInstance(instance *configstore.ControlPlaneInstance) error
}

type ControlPlaneRuntimeTracker struct {
	store             runtimeInstanceStore
	id                string
	podName           string
	podUID            string
	bootID            string
	heartbeatInterval time.Duration
	now               func() time.Time

	draining atomic.Bool

	mu         sync.Mutex
	started    bool
	startedAt  time.Time
	drainingAt *time.Time
}

func NewControlPlaneRuntimeTracker(store runtimeInstanceStore, id, podName, podUID, bootID string, heartbeatInterval time.Duration) *ControlPlaneRuntimeTracker {
	if heartbeatInterval <= 0 {
		heartbeatInterval = 5 * time.Second
	}
	return &ControlPlaneRuntimeTracker{
		store:             store,
		id:                id,
		podName:           podName,
		podUID:            podUID,
		bootID:            bootID,
		heartbeatInterval: heartbeatInterval,
		now:               time.Now,
	}
}

func (t *ControlPlaneRuntimeTracker) Start(ctx context.Context) error {
	t.mu.Lock()
	if t.started {
		t.mu.Unlock()
		return nil
	}
	now := t.now()
	t.started = true
	t.startedAt = now
	t.mu.Unlock()

	if err := t.upsertAt(now); err != nil {
		return err
	}

	go t.heartbeatLoop(ctx)
	return nil
}

func (t *ControlPlaneRuntimeTracker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(t.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := t.upsertAt(t.now()); err != nil {
				slog.Warn("Runtime tracker heartbeat failed.", "cp_instance_id", t.id, "error", err)
			}
		}
	}
}

func (t *ControlPlaneRuntimeTracker) MarkDraining() error {
	now := t.now()
	t.mu.Lock()
	if t.drainingAt == nil {
		drainingAt := now
		t.drainingAt = &drainingAt
	}
	if !t.started {
		t.started = true
		t.startedAt = now
	}
	t.mu.Unlock()
	t.draining.Store(true)
	return t.upsertAt(now)
}

func (t *ControlPlaneRuntimeTracker) Draining() bool {
	return t.draining.Load()
}

func (t *ControlPlaneRuntimeTracker) upsertAt(now time.Time) error {
	state := configstore.ControlPlaneInstanceStateActive

	t.mu.Lock()
	startedAt := t.startedAt
	var drainingAt *time.Time
	if t.draining.Load() {
		state = configstore.ControlPlaneInstanceStateDraining
		if t.drainingAt != nil {
			drainingCopy := *t.drainingAt
			drainingAt = &drainingCopy
		}
	}
	t.mu.Unlock()

	return t.store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              t.id,
		PodName:         t.podName,
		PodUID:          t.podUID,
		BootID:          t.bootID,
		State:           state,
		StartedAt:       startedAt,
		LastHeartbeatAt: now,
		DrainingAt:      drainingAt,
	})
}
