package controlplane

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	janitorRetireReasonOrphaned        = "orphaned"
	janitorRetireReasonStuckActivating = "stuck_activating"
	janitorRetireReasonHotIdleCap      = "hot_idle_cap_exceeded"
)

// orgHotIdleLimit is one org's configured hot-idle pool ceiling. Zero/empty
// fields are unlimited. DefaultCPU/DefaultMemory are the org's default worker
// profile, used to resolve the shape of a parked worker that carries no
// explicit profile (same fallback as the fleet/hot-idle reporting).
type orgHotIdleLimit struct {
	Workers       int
	CPU           string
	Memory        string
	DefaultCPU    string
	DefaultMemory string
}

func (l orgHotIdleLimit) unlimited() bool {
	return l.Workers <= 0 && l.CPU == "" && l.Memory == ""
}

type controlPlaneExpiryStore interface {
	ExpireControlPlaneInstances(cutoff time.Time) (int64, error)
	ExpireDrainingControlPlaneInstances(before time.Time) (int64, error)
	ListOrphanedWorkerSnapshots(before time.Time) ([]configstore.WorkerSnapshot, error)
	ListStuckWorkerSnapshots(spawningBefore, activatingBefore time.Time) ([]configstore.WorkerSnapshot, error)
	ListExpiredHotIdleSnapshots(now time.Time, defaultTTL time.Duration) ([]configstore.WorkerSnapshot, error)
	CountHotIdleWorkers(orgID, image, profileCPU, profileMemory string) (int, error)
	ListOrgHotIdleSnapshots(orgID string) ([]configstore.WorkerSnapshot, error)
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
	now                           func() time.Time
	lifecycle                     *WorkerLifecycle // every per-worker transition flows through this; nil disables per-worker reaping for that tick.
	lifecycleNilWarned            sync.Once        // one-shot guard so the misconfiguration error doesn't flood at the janitor tick rate.
	observeWorkerLifecycle        func()           // refreshes the per-image worker lifecycle gauges (leader-only)
	onStop                        func()
	retireMismatchedVersionWorker func() // reaps one idle worker whose Deployment version differs from this CP's (leader-only)
	cleanupOrphanedWorkerPods     func() // deletes K8s worker pods whose DB row is terminal (retired/lost) or missing (leader-only)
	reconcileHeadroom             func() // maintains low-priority placeholder pods at the dynamic headroom target (spawn-log-derived count/size; leader-only)
	hotIdleFloor                  func(configstore.WorkerSnapshot) int
	// hotIdleCaps returns every org's configured hot-idle pool ceiling (orgs
	// with no limit set may be omitted). nil disables the cap sweep. Wired
	// from the config snapshot in multitenant.go, so a cap edit takes effect
	// on the next tick after the poll reload.
	hotIdleCaps func() map[string]orgHotIdleLimit
}

func NewControlPlaneJanitor(store controlPlaneExpiryStore, interval, expiryTimeout time.Duration) *ControlPlaneJanitor {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if expiryTimeout <= 0 {
		expiryTimeout = 20 * time.Second
	}
	return &ControlPlaneJanitor{
		store:         store,
		interval:      interval,
		expiryTimeout: expiryTimeout,
		orphanGrace:   30 * time.Second,
		// The stuck-worker cutoffs must exceed the detached spawn+activate
		// budget (workerSpawnActivateTimeout, 8m: pod-ready incl. Karpenter
		// node provisioning + gRPC connect + activation). A spawning row's
		// updated_at is not touched between CreateSpawningWorkerSlot and the
		// Reserved transition, and ListStuckWorkers does not exclude rows
		// whose owner CP is alive — so a cutoff below the budget makes the
		// janitor leader delete legitimately in-flight pods mid-spawn
		// (doomed-spawn thrash on every cold-node spawn slower than the
		// cutoff). Reserved/Activating rows get their updated_at bumped at
		// each lifecycle transition, so activateTimeout only needs to cover
		// the connect+activate tail, with slack for clock/poll skew.
		spawnTimeout:    10 * time.Minute,
		activateTimeout: 5 * time.Minute,
		maxDrainTimeout: 15 * time.Minute,
		now:             time.Now,
	}
}

func (j *ControlPlaneJanitor) Run(ctx context.Context) {
	if j == nil || j.store == nil {
		return
	}
	defer func() {
		if j.onStop != nil {
			j.onStop()
		}
	}()

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

	// Per-worker reaping paths (orphan, stuck, hot-idle) all flow
	// through the lifecycle service. A nil lifecycle here is a wiring
	// bug — the only janitor constructor (multitenant.go) sets it
	// unconditionally. The guard remains as a fail-soft so that
	// misconfiguration doesn't NPE the entire tick (the rest of
	// runOnce — version reaping and stranded-pod cleanup
	// cleanup, worker-lifecycle observation — still runs); the
	// slog.Error makes the misconfiguration loud rather than silent.
	if j.lifecycle == nil {
		j.lifecycleNilWarned.Do(func() {
			slog.Error("Janitor running without a lifecycle service; per-worker reaping disabled. This is a wiring bug — fix the constructor.")
		})
	} else {
		orphanedBefore := j.now().Add(-j.orphanGrace)
		orphaned, err := j.store.ListOrphanedWorkerSnapshots(orphanedBefore)
		if err != nil {
			slog.Warn("Janitor failed to list orphaned workers.", "error", err)
		} else {
			if len(orphaned) > 0 {
				slog.Info("Janitor retiring orphaned workers.", "count", len(orphaned))
			}
			for _, snap := range orphaned {
				if _, err := j.lifecycle.RetireOrphanFromSnapshot(snap, janitorRetireReasonOrphaned, LifecycleOriginJanitorOrphan); err != nil {
					slog.Warn("Janitor failed to retire orphan worker.", "worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "error", err)
				}
			}
		}

		spawningBefore := j.now().Add(-j.spawnTimeout)
		activatingBefore := j.now().Add(-j.activateTimeout)
		stuckWorkers, err := j.store.ListStuckWorkerSnapshots(spawningBefore, activatingBefore)
		if err != nil {
			slog.Warn("Janitor failed to list stuck workers.", "error", err)
		} else {
			for _, snap := range stuckWorkers {
				if _, err := j.lifecycle.RetireFromSnapshot(snap, configstore.WorkerStateRetired, janitorRetireReasonStuckActivating, LifecycleOriginJanitorStuckActivating); err != nil {
					slog.Warn("Janitor failed to retire stuck worker.", "worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "error", err)
				}
			}
		}

		if j.hotIdleTTL > 0 {
			// Per-worker TTL: a worker's ttl_minutes governs its hot-idle life;
			// hotIdleTTL is the fallback for default/warm/legacy workers (ttl=0).
			expired, err := j.store.ListExpiredHotIdleSnapshots(j.now(), j.hotIdleTTL)
			if err != nil {
				// Do NOT stamp the last-successful-reap gauge on a list failure:
				// it backs the wedged-reaper alert, and a config-store outage (the
				// moment reaping is actually dark) would otherwise keep it fresh
				// and silence the alert. The per-CP reaper returns early here too.
				slog.Warn("Janitor failed to list expired hot-idle workers.", "error", err)
			} else {
				for _, snap := range expired {
					if j.hotIdleFloor != nil {
						floor := j.hotIdleFloor(snap)
						if floor > 0 {
							count, err := j.store.CountHotIdleWorkers(snap.OrgID(), snap.Image(), snap.ProfileCPU(), snap.ProfileMemory())
							if err != nil {
								slog.Warn("Janitor failed to count compatible hot-idle workers.", "worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "error", err)
								continue
							}
							if count <= floor {
								slog.Debug("Janitor skipping hot-idle TTL because worker is protected by floor.", "worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "floor", floor, "compatible_hot_idle", count)
								continue
							}
						}
					}
					if _, err := j.lifecycle.RetireFromSnapshot(snap, configstore.WorkerStateRetired, "hot_idle_ttl_expired", LifecycleOriginJanitorHotIdleTTL); err != nil {
						slog.Warn("Janitor failed to retire hot-idle worker.", "worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "error", err)
					}
				}
				// Stamp the last-successful-reap gauge only after a successful
				// list pass so an alert can detect a wedged/absent reaper. The
				// per-CP fallback reaper stamps the same gauge, so max() across
				// replicas tracks "some replica reaped recently".
				observeHotIdleReapRun(j.now())
			}
		}

		j.reapHotIdleCaps()
	}

	// Gradual rolling replacement of workers whose Deployment version
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

	if j.observeWorkerLifecycle != nil {
		j.observeWorkerLifecycle()
	}

	if j.reconcileHeadroom != nil {
		j.reconcileHeadroom()
	}

}

// reapHotIdleCaps retires each capped org's OLDEST hot-idle workers until the
// org's parked pool is within ALL of its configured limits (count, total
// vCPU, total memory). It is deliberately a convergent sweep on the janitor
// tick — not a park-time check — so it uniformly covers every park path
// (session release, drain sweep, takeover) AND cap decreases: lower the cap
// in the admin console and the excess drains on the next ticks. The fenced
// RetireFromSnapshot CAS makes concurrent leader/fallback/janitor retires
// safe; the leader janitor runs this, and a wedged leader is already caught
// by the TTL reaper's last-reap gauge. On conflict with the warm-pool floor
// (DefaultWorkerMinHotIdle) the CAP wins — it is the explicit operator
// intent; the floor only guards the TTL reaper. Disabled when hotIdleCaps is
// nil. Runs even when hotIdleTTL is 0 (the TTL reaper is off) — a cap is a
// hard ceiling, not a freshness rule.
func (j *ControlPlaneJanitor) reapHotIdleCaps() {
	if j.hotIdleCaps == nil {
		return
	}
	caps := j.hotIdleCaps()
	if len(caps) == 0 {
		return
	}
	for org, lim := range caps {
		if lim.unlimited() {
			continue
		}
		snaps, err := j.store.ListOrgHotIdleSnapshots(org)
		if err != nil {
			slog.Warn("Janitor failed to list org hot-idle workers for cap sweep.", "org", org, "error", err)
			continue
		}
		cpuCap := quantityCores(lim.CPU)    // 0 = unlimited
		memCap := quantityBytes(lim.Memory) // 0 = unlimited

		// Resolve each parked worker's shape (explicit profile, else the org
		// default — same fallback as the reporting) and total the pool.
		type shape struct {
			cpu float64
			mem int64
		}
		shapes := make([]shape, len(snaps))
		var totalCPU float64
		var totalMem int64
		for i, s := range snaps {
			cpu, mem := s.ProfileCPU(), s.ProfileMemory()
			if cpu == "" {
				cpu = lim.DefaultCPU
			}
			if mem == "" {
				mem = lim.DefaultMemory
			}
			shapes[i] = shape{cpu: quantityCores(cpu), mem: quantityBytes(mem)}
			totalCPU += shapes[i].cpu
			totalMem += shapes[i].mem
		}

		// Retire the oldest-first prefix until within ALL limits. On a retire
		// error, STOP this org (rather than marching on and over-reaping on a
		// transient failure) — the next tick retries.
		remaining := len(snaps)
		for i, s := range snaps {
			overCount := lim.Workers > 0 && remaining > lim.Workers
			overCPU := cpuCap > 0 && totalCPU > cpuCap
			overMem := memCap > 0 && totalMem > memCap
			if !overCount && !overCPU && !overMem {
				break
			}
			if _, err := j.lifecycle.RetireFromSnapshot(s, configstore.WorkerStateRetired, janitorRetireReasonHotIdleCap, LifecycleOriginJanitorHotIdleCap); err != nil {
				slog.Warn("Janitor failed to retire hot-idle worker over cap.", "worker", s.WorkerID(), "worker_pod", s.PodName(), "org", org, "error", err)
				break
			}
			slog.Info("Janitor retired hot-idle worker over the org's cap.", "worker", s.WorkerID(), "worker_pod", s.PodName(), "org", org)
			remaining--
			totalCPU -= shapes[i].cpu
			totalMem -= shapes[i].mem
		}
	}
}

// quantityCores/quantityBytes parse k8s quantity strings ("4", "500m",
// "16Gi") for cap accounting. Unparseable or empty input is 0 — for CAPS
// that means unlimited, for worker shapes it means the worker contributes
// nothing (consistent with the fleet/hot-idle reporting's resolution).
func quantityCores(s string) float64 {
	if s == "" {
		return 0
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	return q.AsApproximateFloat64()
}

func quantityBytes(s string) int64 {
	if s == "" {
		return 0
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	return q.Value()
}
