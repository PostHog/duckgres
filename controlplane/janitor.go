package controlplane

import (
	"context"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

const (
	janitorRetireReasonOrphaned        = "orphaned"
	janitorRetireReasonStuckActivating = "stuck_activating"
	defaultWarmCapacityMissBucketTTL   = 15 * time.Minute
)

type controlPlaneExpiryStore interface {
	ExpireControlPlaneInstances(cutoff time.Time) (int64, error)
	ExpireDrainingControlPlaneInstances(before time.Time) (int64, error)
	ListOrphanedWorkers(before time.Time) ([]configstore.WorkerRecord, error)
	ListOrphanedWorkerSnapshots(before time.Time) ([]configstore.WorkerSnapshot, error)
	ListStuckWorkers(spawningBefore, activatingBefore time.Time) ([]configstore.WorkerRecord, error)
	ExpireFlightSessionRecords(before time.Time) (int64, error)
	ListExpiredHotIdleWorkers(before time.Time) ([]configstore.WorkerRecord, error)
	ListExpiredHotIdleSnapshots(before time.Time) ([]configstore.WorkerSnapshot, error)
	RetireHotIdleWorker(record *configstore.WorkerRecord) (bool, error)
}

type warmCapacityMissBucketPruner interface {
	PruneWarmCapacityMissBuckets(before time.Time) (int64, error)
}

type ControlPlaneJanitor struct {
	store                         controlPlaneExpiryStore
	interval                      time.Duration
	expiryTimeout                 time.Duration
	orphanGrace                   time.Duration
	spawnTimeout                  time.Duration
	activateTimeout               time.Duration
	maxDrainTimeout               time.Duration
	hotIdleTTL                    time.Duration
	warmCapacityMissBucketTTL     time.Duration
	now                           func() time.Time
	retireWorker                  func(record configstore.WorkerRecord, reason string)
	retireOrphanWorker            func(record configstore.WorkerRecord, reason string) // orphan-cleanup variant: handles any active state, skips local-pool bookkeeping
	retireLocalWorker             func(workerID int, reason string) bool               // retires from in-memory pool + pod, returns false if not local
	deleteRetiredWorker           func(record configstore.WorkerRecord, reason string) // post-CAS cleanup: only remove local state / delete pod
	lifecycle                     *WorkerLifecycle                                     // typed lifecycle transitions; when set, hot-idle path goes through it
	reconcileWarmCapacity         func()
	retireMismatchedVersionWorker func() // reaps one warm idle worker whose Deployment version differs from this CP's (leader-only)
	cleanupOrphanedWorkerPods     func() // deletes K8s worker pods whose DB row is terminal (retired/lost) or missing (leader-only)
}

func NewControlPlaneJanitor(store controlPlaneExpiryStore, interval, expiryTimeout time.Duration) *ControlPlaneJanitor {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if expiryTimeout <= 0 {
		expiryTimeout = 20 * time.Second
	}
	return &ControlPlaneJanitor{
		store:                     store,
		interval:                  interval,
		expiryTimeout:             expiryTimeout,
		orphanGrace:               30 * time.Second,
		spawnTimeout:              2 * time.Minute,
		activateTimeout:           2 * time.Minute,
		maxDrainTimeout:           15 * time.Minute,
		warmCapacityMissBucketTTL: DefaultWarmCapacityDemandTTL,
		now:                       time.Now,
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
	if j.lifecycle != nil {
		// Lifecycle path: snapshot-typed list + RetireOrphanFromSnapshot,
		// which carries the orphan-specific CP-revival fence already.
		orphaned, err := j.store.ListOrphanedWorkerSnapshots(orphanedBefore)
		if err != nil {
			slog.Warn("Janitor failed to list orphaned workers.", "error", err)
		} else {
			if len(orphaned) > 0 {
				slog.Info("Janitor retiring orphaned workers.", "count", len(orphaned))
			}
			for _, snap := range orphaned {
				outcome, err := j.lifecycle.RetireOrphanFromSnapshot(snap, janitorRetireReasonOrphaned)
				if err != nil {
					slog.Warn("Janitor failed to retire orphan worker.", "worker_id", snap.WorkerID(), "error", err)
					continue
				}
				_ = outcome // metrics-bearing outcome consumed once PR 6 lands.
			}
		}
	} else {
		orphaned, err := j.store.ListOrphanedWorkers(orphanedBefore)
		if err != nil {
			slog.Warn("Janitor failed to list orphaned workers.", "error", err)
		} else {
			if len(orphaned) > 0 {
				slog.Info("Janitor retiring orphaned workers.", "count", len(orphaned))
			}
			for _, worker := range orphaned {
				// Legacy path retained for deployments that don't wire a
				// lifecycle service. PR 4 prunes once every deployment
				// uses the lifecycle path.
				if j.retireOrphanWorker != nil {
					j.retireOrphanWorker(worker, janitorRetireReasonOrphaned)
				} else {
					j.retireRuntimeWorker(worker, janitorRetireReasonOrphaned)
				}
			}
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
		if j.lifecycle != nil {
			// Lifecycle path: one snapshot-fenced call per expired row.
			// The lifecycle service handles both the durable CAS and the
			// post-CAS pod/secret cleanup, so the janitor no longer has
			// to choose between retireLocalWorker / retireWorker /
			// deleteRetiredWorker fallbacks.
			expired, err := j.store.ListExpiredHotIdleSnapshots(cutoff)
			if err != nil {
				slog.Warn("Janitor failed to list expired hot-idle workers.", "error", err)
			}
			for _, snap := range expired {
				outcome, err := j.lifecycle.RetireFromSnapshot(snap, configstore.WorkerStateRetired, "hot_idle_ttl_expired")
				if err != nil {
					slog.Warn("Janitor failed to retire hot-idle worker.", "worker_id", snap.WorkerID(), "error", err)
					continue
				}
				_ = outcome // metrics-bearing outcome consumed once PR 6 lands.
			}
		} else {
			// Legacy path retained for process-pool / non-lifecycle
			// deployments. Removed entirely once PR 4 prunes the
			// retireWorker / retireLocalWorker / deleteRetiredWorker
			// lambdas after every lifecycle-bearing caller is migrated.
			expired, err := j.store.ListExpiredHotIdleWorkers(cutoff)
			if err != nil {
				slog.Warn("Janitor failed to list expired hot-idle workers.", "error", err)
			}
			for _, record := range expired {
				retired, err := j.store.RetireHotIdleWorker(&record)
				if err != nil {
					slog.Warn("Janitor failed to retire hot-idle worker.", "worker_id", record.WorkerID, "error", err)
					continue
				}
				if !retired {
					continue
				}
				if j.deleteRetiredWorker != nil {
					j.deleteRetiredWorker(record, "hot_idle_ttl_expired")
					continue
				}
				localRetired := j.retireLocalWorker != nil && j.retireLocalWorker(record.WorkerID, "hot_idle_ttl_expired")
				if !localRetired {
					j.retireRuntimeWorker(record, "hot_idle_ttl_expired")
				}
			}
		}
	}

	if _, err := j.store.ExpireFlightSessionRecords(j.now()); err != nil {
		slog.Warn("Janitor failed to expire stale Flight sessions.", "error", err)
	}

	if pruner, ok := j.store.(warmCapacityMissBucketPruner); ok {
		ttl := j.warmCapacityMissBucketTTL
		if ttl <= 0 {
			ttl = defaultWarmCapacityMissBucketTTL
		}
		cutoff := j.now().Add(-ttl).UTC().Truncate(configstore.WarmCapacityMissBucketSize)
		pruned, err := pruner.PruneWarmCapacityMissBuckets(cutoff)
		if err != nil {
			slog.Warn("Janitor failed to prune warm capacity miss buckets.", "error", err)
		} else if pruned > 0 {
			slog.Info("Janitor pruned warm capacity miss buckets.", "count", pruned, "cutoff", cutoff)
		}
	}

	// Gradual rolling replacement of warm workers whose Deployment version
	// differs from this CP's. Runs only when this CP holds the janitor
	// leader lease, so at most one CP at a time is retiring workers and the
	// process stalls until a new-version CP is elected leader.
	if j.retireMismatchedVersionWorker != nil {
		j.retireMismatchedVersionWorker()
	}

	// Reconcile K8s pods against the DB state store: delete any worker pod
	// whose DB row is terminal (retired/lost) or missing entirely. Catches
	// pods leaked by a previous CP that died mid-shutdown (ShutdownAll marked
	// the row retired before the K8s delete completed).
	if j.cleanupOrphanedWorkerPods != nil {
		j.cleanupOrphanedWorkerPods()
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
