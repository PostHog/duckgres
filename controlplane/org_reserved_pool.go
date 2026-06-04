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
	// Server-side patience: block up to warmAcquireTimeout for the warm pool to
	// replenish before surfacing a "no warm worker" miss to the client. Always
	// bounded by the request ctx, so a client with a short deadline still fails
	// fast. 0 = legacy fail-fast.
	warmDeadline := time.Now().Add(p.shared.warmAcquireTimeout)
	// Throttle warm-miss recording across the wait's repeated polls.
	var lastWarmMissAt time.Time
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		p.shared.mu.Lock()
		if p.shared.shuttingDown {
			p.shared.mu.Unlock()
			return nil, fmt.Errorf("pool is shutting down")
		}

		p.shared.cleanDeadWorkersLocked()

		if idle := p.findIdleAssignedWorkerLocked(profile); idle != nil {
			idle.activeSessions++
			if idle.activeSessions > idle.peakSessions {
				idle.peakSessions = idle.activeSessions
			}
			p.shared.mu.Unlock()
			return idle, nil
		}

		// The count cap bounds only exclusive workers (each pins a dedicated
		// node). A colocated request bin-packs and is intentionally unbounded:
		// never refuse it because the exclusive budget is full.
		isColocated := profile != nil && profile.Colocate
		assignedCount := p.assignedWorkerCountLocked()
		if p.maxWorkers == 0 || isColocated || assignedCount < p.maxWorkers {
			// Resource-aware quota for colocated workers: a new colocated worker
			// must not push the org over its colocated CPU/memory budget. Reusing
			// an idle worker (handled above) adds nothing, so this gates only the
			// spawn of a new one.
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

			// While waiting (below) we poll every WarmAcquireRetryInterval, but
			// record the warm miss (demand + metric) at most once per
			// WarmMissRecordInterval so one waiting connection doesn't inflate the
			// demand signal / miss counter.
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
				// Server-side patience: a transient no-idle miss on a colocated
				// request resolves once the warm pool replenishes (a colocated
				// shape may first need a cold node, minutes). Wait and retry until
				// warmAcquireTimeout elapses rather than failing immediately;
				// bounded by ctx. Restricted to colocated requests — a default /
				// exclusive miss replenishes quickly and the client retries, so we
				// don't block interactive/default traffic (e.g. data imports) for
				// minutes.
				if p.shared.warmAcquireTimeout > 0 && isColocated && isRetryableWarmMiss(err) && time.Now().Before(warmDeadline) {
					timer := time.NewTimer(WarmAcquireRetryInterval)
					select {
					case <-ctx.Done():
						timer.Stop()
						return nil, ctx.Err()
					case <-timer.C:
					}
					continue
				}
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
			continue
		}

		p.shared.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
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

func (p *OrgReservedPool) leastLoadedAssignedWorkerLocked() *ManagedWorker {
	var best *ManagedWorker
	for _, w := range p.shared.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if !p.workerReadyForSchedulingLocked(w) {
			continue
		}
		if best == nil || w.activeSessions < best.activeSessions {
			best = w
		}
	}
	return best
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
