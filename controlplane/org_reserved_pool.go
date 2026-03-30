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
	stsBroker              *STSBroker
	activateReservedWorker func(context.Context, *ManagedWorker) error
}

func NewOrgReservedPool(shared *K8sWorkerPool, orgID string, maxWorkers int, stsBroker *STSBroker) *OrgReservedPool {
	pool := &OrgReservedPool{
		shared:     shared,
		orgID:      orgID,
		maxWorkers: maxWorkers,
		stsBroker:  stsBroker,
	}
	pool.activateReservedWorker = pool.activateReservedWorkerDefault
	return pool
}

func (p *OrgReservedPool) AcquireWorker(ctx context.Context) (*ManagedWorker, error) {
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

		if idle := p.findIdleAssignedWorkerLocked(); idle != nil {
			// If the worker is in hot_idle, transition to reserved and
			// re-activate. This refreshes STS credentials and ensures
			// the DuckLake config is current.
			if idle.SharedState().NormalizedLifecycle() == WorkerLifecycleHotIdle {
				nextState, err := idle.SharedState().Transition(WorkerLifecycleReserved, nil)
				if err != nil {
					slog.Warn("Hot-idle transition to reserved failed.", "worker", idle.ID, "error", err)
					p.shared.mu.Unlock()
					continue
				}
				_ = idle.SetSharedState(nextState)
				// Bump epoch so activateTenant accepts the new payload
				// (it requires OwnerEpoch > current for same-tenant reactivation).
				idle.IncrementOwnerEpoch()
				// Persist the reserved state to the DB before releasing the lock.
				// This prevents another CP pod from claiming the same worker
				// via ClaimHotIdleWorker while we're reactivating it.
				reservedRecord := p.shared.workerRecordFor(idle.ID, idle, idle.OwnerEpoch(), configstore.WorkerStateReserved, "", nil)
				p.shared.persistWorkerRecord(reservedRecord)
				p.shared.mu.Unlock()

				// Re-activate with fresh credentials (same path as fresh workers).
				if err := p.activateWorkerForOrg(ctx, idle); err != nil {
					slog.Warn("Hot-idle re-activation failed.", "worker", idle.ID, "org", p.orgID, "error", err)
					observeActivationFailure()
					p.shared.retireWorkerWithReason(idle.ID, RetireReasonActivationFailure)
					return nil, err
				}

				p.shared.mu.Lock()
				idle.activeSessions++
				if idle.activeSessions > idle.peakSessions {
					idle.peakSessions = idle.activeSessions
				}
				p.shared.mu.Unlock()
				return idle, nil
			}
			idle.activeSessions++
			if idle.activeSessions > idle.peakSessions {
				idle.peakSessions = idle.activeSessions
			}
			p.shared.mu.Unlock()
			return idle, nil
		}

		assignedCount := p.assignedWorkerCountLocked()
		if p.maxWorkers == 0 || assignedCount < p.maxWorkers {
			p.shared.mu.Unlock()

			worker, err := p.shared.ReserveSharedWorker(ctx, &WorkerAssignment{
				OrgID: p.orgID,
			})
			if err != nil {
				return nil, err
			}

			if err := p.activateWorkerForOrg(ctx, worker); err != nil {
				slog.Warn("Worker activation failed.", "worker", worker.ID, "org", p.orgID, "error", err)
				observeActivationFailure()
				p.shared.retireWorkerWithReason(worker.ID, RetireReasonActivationFailure)
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
	// Worker still has sessions; the decrement already happened inside
	// TransitionToHotIdleIfNoSessions. Nothing more to do until the
	// last session ends.
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
		p.shared.retireWorkerWithReason(id, RetireReasonShutdown)
	}
}

func (p *OrgReservedPool) findIdleAssignedWorkerLocked() *ManagedWorker {
	for _, w := range p.shared.workers {
		select {
		case <-w.done:
			continue
		default:
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
	return lifecycle == WorkerLifecycleHot || lifecycle == WorkerLifecycleHotIdle
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
		observeWarmPoolLifecycleGauges(p.shared.workers)
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
			observeActivationDuration(time.Since(worker.reservedAt))
		}
		observeWarmPoolLifecycleGauges(p.shared.workers)
		return nil
	case WorkerLifecycleHot:
		if !worker.reservedAt.IsZero() {
			observeActivationDuration(time.Since(worker.reservedAt))
		}
		return nil
	default:
		return fmt.Errorf("worker %d finished activation in unexpected lifecycle %q", worker.ID, worker.SharedState().NormalizedLifecycle())
	}
}

func (p *OrgReservedPool) ReconnectFlightWorker(ctx context.Context, workerID int, ownerEpoch int64) (*ManagedWorker, error) {
	worker, err := p.shared.claimSpecificWorker(ctx, workerID, ownerEpoch, &WorkerAssignment{
		OrgID:      p.orgID,
		MaxWorkers: p.maxWorkers,
	})
	if err != nil {
		return nil, err
	}
	if err := p.activateWorkerForOrg(ctx, worker); err != nil {
		p.shared.retireWorkerWithReason(worker.ID, RetireReasonActivationFailure)
		return nil, err
	}
	return worker, nil
}

func (p *OrgReservedPool) activateReservedWorkerDefault(_ context.Context, _ *ManagedWorker) error {
	return fmt.Errorf("reserved worker activator is not configured for org %s", p.orgID)
}
