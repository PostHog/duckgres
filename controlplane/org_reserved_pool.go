//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// isRetryableWarmMiss reports whether a worker-acquire error is a transient
// "no idle warm worker" miss — the only capacity miss that resolves on its own
// once the warm pool replenishes. Org/global-cap and shutdown misses are not
// retried (waiting won't change them).
func isRetryableWarmMiss(err error) bool {
	var capErr *WarmCapacityExhaustedError
	if !errors.As(err, &capErr) {
		return false
	}
	return capErr.missReason() == configstore.WorkerClaimMissReasonNoIdle
}

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
	// Per-org colocated resource caps. A count-based maxWorkers is insufficient
	// when colocated pods vary in size, so colocated CPU/memory is also bounded.
	// 0 = unbounded on that axis. Only colocated workers count against these.
	maxColocatedCPU      int
	maxColocatedMemBytes uint64
	// gate serializes the slow acquisition path (no idle worker → claim/spawn) in
	// FIFO arrival order, so the next worker to become available goes to the
	// earliest waiting connection and a later one cannot snatch it.
	gate *orgAcquireGate
}

func NewOrgReservedPool(shared *K8sWorkerPool, orgID string, maxWorkers int, image string, stsBroker *STSBroker, maxColocatedCPU int, maxColocatedMemBytes uint64) *OrgReservedPool {
	pool := &OrgReservedPool{
		shared:               shared,
		orgID:                orgID,
		maxWorkers:           maxWorkers,
		image:                image,
		stsBroker:            stsBroker,
		maxColocatedCPU:      maxColocatedCPU,
		maxColocatedMemBytes: maxColocatedMemBytes,
		gate:                 newOrgAcquireGate(),
	}
	pool.activateReservedWorker = pool.activateReservedWorkerDefault
	return pool
}

// assignedColocatedResourcesLocked sums the CPU (cores) and memory (bytes) of
// this org's currently-assigned colocated workers. Caller holds p.shared.mu.
func (p *OrgReservedPool) assignedColocatedResourcesLocked() (cpu int, memBytes uint64) {
	for _, w := range p.shared.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if w.SharedState().NormalizedLifecycle() == WorkerLifecycleHotIdle {
			continue
		}
		if !p.workerBelongsToOrgLocked(w) || !w.profile.Colocate {
			continue
		}
		cpu += parseK8sCPU(w.profile.CPU)
		memBytes += parseK8sMemory(w.profile.Memory)
	}
	return cpu, memBytes
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

	// Throttle warm-miss recording (the miss is still recorded for demand metrics
	// even though we now foreground-spawn rather than wait for a replenish).
	var lastWarmMissAt time.Time
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
		// Resource-aware quota for colocated workers: a new colocated worker must
		// not push the org over its colocated CPU/memory budget. (The count cap is
		// enforced authoritatively, cross-CP, inside ReserveSharedWorker's claim,
		// which exempts colocated workers; the CPU/mem budget is enforced here.)
		if profile != nil && profile.Colocate && (p.maxColocatedCPU > 0 || p.maxColocatedMemBytes > 0) {
			curCPU, curMem := p.assignedColocatedResourcesLocked()
			reqCPU, reqMem := parseK8sCPU(profile.CPU), parseK8sMemory(profile.Memory)
			if (p.maxColocatedCPU > 0 && curCPU+reqCPU > p.maxColocatedCPU) ||
				(p.maxColocatedMemBytes > 0 && curMem+reqMem > p.maxColocatedMemBytes) {
				p.shared.mu.Unlock()
				observeOrgColocatedQuotaRejection(p.orgID)
				return nil, ErrOrgResourceQuotaExceeded
			}
		}
		maxWorkers := p.maxWorkers
		image := p.image
		p.shared.mu.Unlock()

		// While waiting (below) we poll every WarmAcquireRetryInterval, but record
		// the warm miss (demand + metric) at most once per WarmMissRecordInterval
		// so one waiting connection doesn't inflate the demand signal / miss counter.
		recordMiss := lastWarmMissAt.IsZero() || time.Since(lastWarmMissAt) >= WarmMissRecordInterval

		worker, err := p.shared.ReserveSharedWorker(ctx, &WorkerAssignment{
			OrgID:                  p.orgID,
			MaxWorkers:             maxWorkers,
			Image:                  image,
			Profile:                profile,
			MaxColocatedCPU:        p.maxColocatedCPU,
			MaxColocatedMemBytes:   p.maxColocatedMemBytes,
			SuppressWarmMissRecord: !recordMiss,
		})
		if err != nil {
			if recordMiss {
				lastWarmMissAt = time.Now()
			}
			// An org/global-cap or shutdown miss will NOT resolve by waiting —
			// surface it immediately with its clear, reason-specific message.
			if !isRetryableWarmMiss(err) {
				return nil, err
			}
			// A retryable "no idle warm worker" miss: distinguish two cases.
			//   - At the org's max concurrent (exclusive) workers and all busy →
			//     waiting cannot help (no new worker will spawn); fail fast with the
			//     clear org-cap message so the client knows it hit its own limit.
			//     This also makes the contract hold without a runtime store, whose
			//     in-memory claim cannot itself classify the miss as org-cap.
			//   - Under the cap → a worker is (being) spawned, so hold up to
			//     warmAcquireTimeout for it rather than bouncing the client. Applies
			//     to default/exclusive requests too, not just colocated.
			isColocated := profile != nil && profile.Colocate
			if !isColocated && p.atOrgWorkerCap() {
				return nil, NewWarmCapacityExhaustedErrorForReason(
					configstore.WorkerClaimMissReasonOrgCap, DefaultWarmCapacityRetryAfter)
			}
			// No warm pool: foreground-spawn a worker for this request instead of
			// waiting for a warm replenish that will never come. A sized request
			// already foreground-spawned inside ReserveSharedWorker (so it returned a
			// worker, not this miss); this path covers default requests. The spawn
			// re-checks the org/global cap (CreateSpawningWorkerSlot) and reserves the
			// worker, which then activates below.
			worker, err = p.shared.spawnReservedWorker(ctx, &WorkerAssignment{
				OrgID:                p.orgID,
				MaxWorkers:           maxWorkers,
				Image:                image,
				Profile:              profile,
				MaxColocatedCPU:      p.maxColocatedCPU,
				MaxColocatedMemBytes: p.maxColocatedMemBytes,
			})
			if err != nil {
				return nil, err
			}
			// worker is now a freshly reserved worker; fall through to activation.
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
		// Colocated workers are unbounded and must not consume the exclusive
		// worker budget that maxWorkers governs.
		if w.profile.Colocate {
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
