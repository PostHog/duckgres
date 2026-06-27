package controlplane

import (
	"context"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// perCPHotIdleStore is the store surface the per-CP fallback reaper needs.
type perCPHotIdleStore interface {
	ListExpiredHotIdleSnapshotsForCP(ownerCPInstanceID string, now time.Time, defaultTTL time.Duration) ([]configstore.WorkerSnapshot, error)
	CountHotIdleWorkers(orgID, image, profileCPU, profileMemory string) (int, error)
}

// perCPHotIdleReaper retires THIS replica's own expired hot-idle workers on a
// local timer, INDEPENDENT of the janitor leader lease. It is the backstop for
// the leader-only janitor reaper (janitor.go): if leadership is wedged or
// absent, idle worker pods — and the r6gd nodes behind them — would otherwise
// accumulate fleet-wide until a CP restart. Each replica reaps only workers it
// owns (owner_cp_instance_id == self); orphaned/cross-CP rows remain the leader
// janitor's responsibility. The retire goes through the same fenced CAS
// (WorkerLifecycle.RetireFromSnapshot) as the leader path, so a worker retired
// concurrently by both simply CAS-misses on one side — no double delete.
type perCPHotIdleReaper struct {
	store        perCPHotIdleStore
	lifecycle    *WorkerLifecycle
	cpInstanceID string
	hotIdleTTL   time.Duration
	// hotIdleFloor mirrors the janitor's floor function so a per-org
	// DefaultWorkerMinHotIdle warm reserve is honored here too (the fallback
	// must not over-reap a floor the leader would have preserved). Counts are
	// global (CountHotIdleWorkers), so the floor holds across replicas.
	hotIdleFloor func(configstore.WorkerSnapshot) int
	interval     time.Duration
	now          func() time.Time
}

func (r *perCPHotIdleReaper) Run(ctx context.Context) {
	if r == nil || r.store == nil || r.lifecycle == nil {
		return
	}
	interval := r.interval
	if interval <= 0 {
		interval = time.Minute
	}
	r.runOnce()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runOnce()
		}
	}
}

func (r *perCPHotIdleReaper) runOnce() {
	if r.hotIdleTTL <= 0 {
		return
	}
	now := r.now
	if now == nil {
		now = time.Now
	}
	expired, err := r.store.ListExpiredHotIdleSnapshotsForCP(r.cpInstanceID, now(), r.hotIdleTTL)
	if err != nil {
		slog.Warn("Per-CP hot-idle reaper failed to list expired workers.", "cp", r.cpInstanceID, "error", err)
		return
	}
	for _, snap := range expired {
		if r.hotIdleFloor != nil {
			if floor := r.hotIdleFloor(snap); floor > 0 {
				count, err := r.store.CountHotIdleWorkers(snap.OrgID(), snap.Image(), snap.ProfileCPU(), snap.ProfileMemory())
				if err != nil {
					slog.Warn("Per-CP hot-idle reaper failed to count compatible hot-idle workers.",
						"worker", snap.WorkerID(), "org", snap.OrgID(), "error", err)
					continue
				}
				if count <= floor {
					continue
				}
			}
		}
		if _, err := r.lifecycle.RetireFromSnapshot(snap, configstore.WorkerStateRetired, "hot_idle_ttl_expired", LifecycleOriginPerCPHotIdleTTL); err != nil {
			slog.Warn("Per-CP hot-idle reaper failed to retire worker.",
				"worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "error", err)
		}
	}
	// Stamp the last-successful-reap gauge even when nothing was expired: the
	// alert cares that the reap path RAN recently, not that it found work.
	observeHotIdleReapRun(now())
}
