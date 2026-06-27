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
	// ListOrphanedWorkerSnapshots returns workers whose owning CP instance is no
	// longer live (expired/missing past the cutoff). Reused here so the per-CP
	// reaper can reclaim hot-idle workers orphaned by a rollout/crash without
	// waiting on the leader.
	ListOrphanedWorkerSnapshots(before time.Time) ([]configstore.WorkerSnapshot, error)
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
	// orphanGrace is how long an owning CP instance must have been gone before
	// its hot-idle workers are treated as orphaned and reaped here. 0 disables
	// the per-CP orphan pass (self-owned reaping still runs). Matches the leader
	// janitor's orphanGrace so timing is identical, just leader-independent.
	orphanGrace time.Duration
	interval    time.Duration
	now         func() time.Time
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
	now := r.now
	if now == nil {
		now = time.Now
	}

	// Orphan pass: reap hot-idle workers whose OWNING CP instance is no longer
	// live. cpInstanceID is freshly minted on every CP start, so a rollout/crash
	// leaves the prior process's hot-idle rows owned by a dead instance — owned
	// by no live CP, they are invisible to the self-owned pass below and were
	// previously reaped ONLY by the leader janitor's orphan sweep. Running it
	// here makes hot-idle reclamation genuinely leader-independent (the whole
	// point of this reaper) so a rollout concurrent with a leaderless window
	// can't accumulate orphaned idle pods. RetireOrphanFromSnapshot re-checks
	// owner-still-expired in its fenced CAS, so a worker meanwhile reclaimed by a
	// live CP (ClaimHotIdleWorker) is skipped; no floor is applied (a dead
	// owner's worker is not a warm reserve), matching the leader sweep. Runs
	// independent of hotIdleTTL — like the leader's orphan sweep, which is not
	// gated on the hot-idle TTL block — so a (hypothetical) hotIdleTTL<=0 config
	// can't disable the orphan backstop.
	r.reapOrphanedHotIdle(now())

	if r.hotIdleTTL <= 0 {
		return
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

	// Stamp the last-successful-reap gauge after a successful self-owned list
	// (reached only if that list did not error): the alert cares that the reap
	// path RAN recently, not that it found work.
	observeHotIdleReapRun(now())
}

func (r *perCPHotIdleReaper) reapOrphanedHotIdle(now time.Time) {
	if r.orphanGrace <= 0 {
		return
	}
	orphaned, err := r.store.ListOrphanedWorkerSnapshots(now.Add(-r.orphanGrace))
	if err != nil {
		slog.Warn("Per-CP hot-idle reaper failed to list orphaned workers.", "error", err)
		return
	}
	for _, snap := range orphaned {
		// Only hot-idle: other orphan states (spawning/reserved/activating/
		// draining) stay the leader orphan sweep's job — they can carry
		// in-flight work this reaper must not second-guess.
		if snap.State() != configstore.WorkerStateHotIdle {
			continue
		}
		if _, err := r.lifecycle.RetireOrphanFromSnapshot(snap, "orphaned", LifecycleOriginPerCPOrphan); err != nil {
			slog.Warn("Per-CP hot-idle reaper failed to retire orphaned worker.",
				"worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "error", err)
		}
	}
}
