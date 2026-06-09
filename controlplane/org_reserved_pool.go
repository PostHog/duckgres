//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// OrgReservedPool presents one org's reserved slice of a shared K8s warm pool.
// It preserves the existing WorkerPool contract for SessionManager while ensuring
// workers are reserved to a single org for their lifetime and retired after use.
type OrgReservedPool struct {
	shared                 *K8sWorkerPool
	orgID                  string
	maxWorkers             int
	image                  string
	stsBroker              *STSBroker
	activateReservedWorker func(context.Context, *ManagedWorker) error
	// gate serializes the slow acquisition path (no idle worker → claim/spawn) in
	// FIFO arrival order, so the next worker to become available goes to the
	// earliest waiting connection and a later one cannot snatch it.
	gate *orgAcquireGate
}

func NewOrgReservedPool(shared *K8sWorkerPool, orgID string, maxWorkers int, image string, stsBroker *STSBroker) *OrgReservedPool {
	pool := &OrgReservedPool{
		shared:     shared,
		orgID:      orgID,
		maxWorkers: maxWorkers,
		image:      image,
		stsBroker:  stsBroker,
		gate:       newOrgAcquireGate(),
	}
	pool.activateReservedWorker = pool.activateReservedWorkerDefault
	return pool
}

func (p *OrgReservedPool) AcquireWorker(ctx context.Context, profile *WorkerProfile) (*ManagedWorker, error) {
	// Fast path: reuse an already-assigned, idle (Hot) worker of the requested
	// shape. Such a worker is already this org's and free, so reusing it
	// concurrently is safe and needs no FIFO ordering — it is not a freshly
	// spawned/neutral worker that a queued waiter is owed (those are claimed only
	// via the gated slow path below).
	if w := p.tryReuseIdleAssigned(profile); w != nil {
		return w, nil
	}

	// Slow path: no idle worker — we must claim a warm worker or wait for one to
	// spawn. Serialize per org in FIFO arrival order so the next worker to become
	// available is handed to the EARLIEST waiting connection; a later-arriving
	// connection cannot snatch a worker the control plane scaled up for someone
	// already waiting. Bounded by the request ctx.
	//
	// NOTE: while one waiter holds the gate and waits for its spawn, queued
	// waiters do not yet trigger their own spawn, so a large cold burst ramps
	// roughly sequentially. Correctness (anti-snatch + clear cap errors) is the
	// priority here; parallelizing the cold-burst ramp is a perf follow-up.
	if err := p.gate.acquire(ctx); err != nil {
		return nil, err
	}
	defer p.gate.release()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// A worker may have freed while we waited for our FIFO turn.
		if w := p.tryReuseIdleAssigned(profile); w != nil {
			return w, nil
		}

		p.shared.mu.Lock()
		if p.shared.shuttingDown {
			p.shared.mu.Unlock()
			return nil, fmt.Errorf("pool is shutting down")
		}
		maxWorkers := p.maxWorkers
		image := p.image
		p.shared.mu.Unlock()

		// At the org's max concurrent workers with all busy → fail fast with the
		// clear org-cap message; waiting cannot help (no new worker will spawn).
		if p.atOrgWorkerCap() {
			return nil, NewWorkerCapacityExhaustedErrorForReason(
				configstore.WorkerClaimMissReasonOrgCap, DefaultWorkerSpawnRetryAfter)
		}

		// Under cap: reuse a hot-idle worker for this org, or spawn one on demand.
		// ReserveSharedWorker re-checks the org/global cap authoritatively
		// (CreateSpawningWorkerSlot, cross-CP) and returns the reason-specific cap
		// error; an org/global-cap or shutdown error won't resolve by waiting.
		worker, err := p.shared.ReserveSharedWorker(ctx, &WorkerAssignment{
			OrgID:      p.orgID,
			MaxWorkers: maxWorkers,
			Image:      image,
			Profile:    profile,
		})
		if err != nil {
			return nil, err
		}

		if err := p.activateWorkerForOrg(ctx, worker); err != nil {
			slog.Warn("Worker activation failed.", "worker", worker.ID, "org", p.orgID, "error", err)
			observeActivationFailure(worker.image)
			p.shared.retireWorkerWithReason(worker.ID, RetireReasonActivationFailure, LifecycleOriginActivationFailure)
			return nil, err
		}

		p.shared.mu.Lock()
		if owned := p.workerBelongsToOrgLocked(worker); owned {
			worker.activeSessions++
			if worker.activeSessions > worker.peakSessions {
				worker.peakSessions = worker.activeSessions
			}
			p.shared.mu.Unlock()
			return worker, nil
		}
		p.shared.mu.Unlock()
		// Worker is no longer ours (raced with retirement/reclaim); try again.
	}
}

// atOrgWorkerCap reports whether the org has reached its maximum concurrent
// exclusive workers (count cap). Hot-idle and colocated workers are excluded
// (they don't consume the count budget), so a true result means every counted
// worker is actively assigned — a new exclusive worker cannot be added.
func (p *OrgReservedPool) atOrgWorkerCap() bool {
	p.shared.mu.Lock()
	defer p.shared.mu.Unlock()
	return p.maxWorkers > 0 && p.assignedWorkerCountLocked() >= p.maxWorkers
}

// tryReuseIdleAssigned returns an already-assigned, idle (Hot) worker of the
// requested shape with its session count bumped, or nil if none is available.
func (p *OrgReservedPool) tryReuseIdleAssigned(profile *WorkerProfile) *ManagedWorker {
	p.shared.mu.Lock()
	defer p.shared.mu.Unlock()
	if p.shared.shuttingDown {
		return nil
	}
	p.shared.cleanDeadWorkersLocked()
	idle := p.findIdleAssignedWorkerLocked(profile)
	if idle == nil {
		return nil
	}
	idle.activeSessions++
	if idle.activeSessions > idle.peakSessions {
		idle.peakSessions = idle.activeSessions
	}
	return idle
}

func (p *OrgReservedPool) ReleaseWorker(id int) {
	if p.shared.TransitionToHotIdleIfNoSessions(id) {
		return
	}
	// TransitionToHotIdleIfNoSessions already decremented activeSessions.
	// If the worker is not hot (e.g. draining during shutdown) and has no
	// remaining sessions, retire it so the org slot is freed immediately.
	p.shared.RetireIfDrainingAndEmpty(id, LifecycleOriginPublicAPI)
}

func (p *OrgReservedPool) RetireWorker(id int) {
	if _, ok := p.Worker(id); !ok {
		return
	}
	p.shared.RetireWorker(id)
}

func (p *OrgReservedPool) RetireWorkerIfNoSessions(id int) bool {
	if _, ok := p.Worker(id); !ok {
		return false
	}
	return p.shared.RetireWorkerIfNoSessions(id)
}

func (p *OrgReservedPool) Worker(id int) (*ManagedWorker, bool) {
	p.shared.mu.RLock()
	defer p.shared.mu.RUnlock()
	w, ok := p.shared.workers[id]
	if !ok || !p.workerBelongsToOrgLocked(w) {
		return nil, false
	}
	return w, true
}

func (p *OrgReservedPool) SpawnMinWorkers(count int) error {
	return nil
}

func (p *OrgReservedPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
}

func (p *OrgReservedPool) SetMaxWorkers(n int) {
	p.shared.mu.Lock()
	defer p.shared.mu.Unlock()
	p.maxWorkers = n
}

func (p *OrgReservedPool) SetWorkerImage(image string) {
	p.shared.mu.Lock()
	defer p.shared.mu.Unlock()
	p.image = image
}

func (p *OrgReservedPool) ShutdownAll() {
	p.shared.mu.RLock()
	workers := make([]int, 0, len(p.shared.workers))
	for id, w := range p.shared.workers {
		if p.workerBelongsToOrgLocked(w) {
			workers = append(workers, id)
		}
	}
	p.shared.mu.RUnlock()

	for _, id := range workers {
		p.shared.retireWorkerWithReason(id, RetireReasonShutdown, LifecycleOriginOrgShutdown)
	}
}

func (p *OrgReservedPool) findIdleAssignedWorkerLocked(profile *WorkerProfile) *ManagedWorker {
	want := profile.MatchKey()
	for _, w := range p.shared.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		// Only reuse a worker of the requested shape — a colocated request must
		// not land on a default/exclusive worker (and vice versa).
		if w.profile.MatchKey() != want {
			continue
		}
		if w.activeSessions == 0 && p.workerReadyForSchedulingLocked(w) {
			return w
		}
	}
	return nil
}

func (p *OrgReservedPool) assignedWorkerCountLocked() int {
	count := 0
	for _, w := range p.shared.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		// Hot-idle workers have released their slot and are waiting to be
		// reclaimed via the DB or retired by the janitor. Don't count them
		// against maxWorkers so AcquireWorker can reach ReserveSharedWorker
		// and ClaimHotIdleWorker.
		if w.SharedState().NormalizedLifecycle() == WorkerLifecycleHotIdle {
			continue
		}
		if p.workerBelongsToOrgLocked(w) {
			count++
		}
	}
	return count
}

func (p *OrgReservedPool) workerBelongsToOrgLocked(w *ManagedWorker) bool {
	state := w.SharedState()
	return state.Assignment != nil && state.Assignment.OrgID == p.orgID && state.NormalizedLifecycle() != WorkerLifecycleRetired
}

func (p *OrgReservedPool) workerReadyForSchedulingLocked(w *ManagedWorker) bool {
	if !p.workerBelongsToOrgLocked(w) {
		return false
	}
	lifecycle := w.SharedState().NormalizedLifecycle()
	return lifecycle == WorkerLifecycleHot
}

func (p *OrgReservedPool) activateWorkerForOrg(ctx context.Context, worker *ManagedWorker) error {
	p.shared.mu.Lock()
	state := worker.SharedState().NormalizedLifecycle()
	if state == WorkerLifecycleReserved {
		nextState, err := worker.SharedState().Transition(WorkerLifecycleActivating, nil)
		if err != nil {
			p.shared.mu.Unlock()
			return err
		}
		if err := worker.SetSharedState(nextState); err != nil {
			p.shared.mu.Unlock()
			return err
		}
	}
	p.shared.mu.Unlock()

	if err := p.activateReservedWorker(ctx, worker); err != nil {
		return err
	}

	p.shared.mu.Lock()
	defer p.shared.mu.Unlock()
	switch worker.SharedState().NormalizedLifecycle() {
	case WorkerLifecycleActivating:
		nextState, err := worker.SharedState().Transition(WorkerLifecycleHot, nil)
		if err != nil {
			return err
		}
		if err := worker.SetSharedState(nextState); err != nil {
			return err
		}
		if !worker.reservedAt.IsZero() {
			observeActivationDuration(time.Since(worker.reservedAt), worker.image)
		}
		return nil
	case WorkerLifecycleHot:
		if !worker.reservedAt.IsZero() {
			observeActivationDuration(time.Since(worker.reservedAt), worker.image)
		}
		return nil
	default:
		return fmt.Errorf("worker %d finished activation in unexpected lifecycle %q", worker.ID, worker.SharedState().NormalizedLifecycle())
	}
}

func (p *OrgReservedPool) ReconnectFlightWorker(ctx context.Context, workerID int, ownerEpoch int64) (*ManagedWorker, error) {
	p.shared.mu.RLock()
	maxWorkers := p.maxWorkers
	image := p.image
	p.shared.mu.RUnlock()

	worker, err := p.shared.claimSpecificWorker(ctx, workerID, ownerEpoch, &WorkerAssignment{
		OrgID:      p.orgID,
		MaxWorkers: maxWorkers,
		Image:      image,
	})
	if err != nil {
		return nil, err
	}
	if err := p.activateWorkerForOrg(ctx, worker); err != nil {
		p.shared.retireWorkerWithReason(worker.ID, RetireReasonActivationFailure, LifecycleOriginActivationFailure)
		return nil, err
	}
	return worker, nil
}

func (p *OrgReservedPool) activateReservedWorkerDefault(_ context.Context, _ *ManagedWorker) error {
	return fmt.Errorf("reserved worker activator is not configured for org %s", p.orgID)
}
