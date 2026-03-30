package controlplane

import (
	"context"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// defaultHotIdleTTL is how long a hot-idle worker retains its org assignment
// before being retired. During this window, any CP pod can reclaim it for the
// same org without re-activation.
const defaultHotIdleTTL = 5 * time.Minute

const (
	janitorRetireReasonOrphaned        = "orphaned"
	janitorRetireReasonStuckActivating = "stuck_activating"
)

type controlPlaneExpiryStore interface {
	ExpireControlPlaneInstances(cutoff time.Time) (int64, error)
	ExpireDrainingControlPlaneInstances(before time.Time) (int64, error)
	ListOrphanedWorkers(before time.Time) ([]configstore.WorkerRecord, error)
	ListStuckWorkers(spawningBefore, activatingBefore time.Time) ([]configstore.WorkerRecord, error)
	ExpireFlightSessionRecords(before time.Time) (int64, error)
	ListExpiredHotIdleWorkers(before time.Time) ([]configstore.WorkerRecord, error)
}

type ControlPlaneJanitor struct {
	store                 controlPlaneExpiryStore
	interval              time.Duration
	expiryTimeout         time.Duration
	orphanGrace           time.Duration
	spawnTimeout          time.Duration
	activateTimeout       time.Duration
	maxDrainTimeout       time.Duration
	hotIdleTTL            time.Duration
	now                   func() time.Time
	retireWorker          func(record configstore.WorkerRecord, reason string)
	retireLocalWorker     func(workerID int, reason string) // retires from in-memory pool + pod
	reconcileWarmCapacity func()
}

func NewControlPlaneJanitor(store controlPlaneExpiryStore, interval, expiryTimeout time.Duration) *ControlPlaneJanitor {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if expiryTimeout <= 0 {
		expiryTimeout = 20 * time.Second
	}
	return &ControlPlaneJanitor{
		store:           store,
		interval:        interval,
		expiryTimeout:   expiryTimeout,
		orphanGrace:     30 * time.Second,
		spawnTimeout:    2 * time.Minute,
		activateTimeout: 2 * time.Minute,
		maxDrainTimeout: 15 * time.Minute,
		now:             time.Now,
	}
}

func (j *ControlPlaneJanitor) Run(ctx context.Context) {
	if j == nil || j.store == nil {
		return
	}

	j.runOnce()

	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.runOnce()
		}
	}
}

func (j *ControlPlaneJanitor) runOnce() {
	cutoff := j.now().Add(-j.expiryTimeout)
	expired, err := j.store.ExpireControlPlaneInstances(cutoff)
	if err != nil {
		slog.Warn("Janitor failed to expire stale control-plane instances.", "error", err)
	} else if expired > 0 {
		slog.Info("Janitor expired stale control-plane instances.", "count", expired, "cutoff", cutoff)
	}

	if j.maxDrainTimeout > 0 {
		drainingBefore := j.now().Add(-j.maxDrainTimeout)
		expiredDraining, err := j.store.ExpireDrainingControlPlaneInstances(drainingBefore)
		if err != nil {
			slog.Warn("Janitor failed to expire overdue draining control-plane instances.", "error", err)
		} else if expiredDraining > 0 {
			slog.Info("Janitor expired overdue draining control-plane instances.", "count", expiredDraining, "cutoff", drainingBefore)
		}
	}

	orphanedBefore := j.now().Add(-j.orphanGrace)
	orphaned, err := j.store.ListOrphanedWorkers(orphanedBefore)
	if err != nil {
		slog.Warn("Janitor failed to list orphaned workers.", "error", err)
	} else {
		for _, worker := range orphaned {
			j.retireRuntimeWorker(worker, janitorRetireReasonOrphaned)
		}
	}

	spawningBefore := j.now().Add(-j.spawnTimeout)
	activatingBefore := j.now().Add(-j.activateTimeout)
	stuckWorkers, err := j.store.ListStuckWorkers(spawningBefore, activatingBefore)
	if err != nil {
		slog.Warn("Janitor failed to list stuck workers.", "error", err)
	} else {
		for _, worker := range stuckWorkers {
			j.retireRuntimeWorker(worker, janitorRetireReasonStuckActivating)
		}
	}

	if j.hotIdleTTL > 0 {
		cutoff := j.now().Add(-j.hotIdleTTL)
		expired, err := j.store.ListExpiredHotIdleWorkers(cutoff)
		if err != nil {
			slog.Warn("Janitor failed to list expired hot-idle workers.", "error", err)
		}
		for _, record := range expired {
			// Use retireLocalWorker for workers in our in-memory pool to
			// remove the stale ManagedWorker entry. retireClaimedWorker
			// only handles the DB record and pod deletion, leaving the
			// in-memory worker schedulable.
			if j.retireLocalWorker != nil {
				j.retireLocalWorker(record.WorkerID, "hot_idle_ttl_expired")
			} else {
				j.retireRuntimeWorker(record, "hot_idle_ttl_expired")
			}
		}
	}

	if _, err := j.store.ExpireFlightSessionRecords(j.now()); err != nil {
		slog.Warn("Janitor failed to expire stale Flight sessions.", "error", err)
	}

	if j.reconcileWarmCapacity != nil {
		j.reconcileWarmCapacity()
	}
}

func (j *ControlPlaneJanitor) retireRuntimeWorker(record configstore.WorkerRecord, reason string) {
	if j == nil || j.retireWorker == nil {
		return
	}
	j.retireWorker(record, reason)
}
