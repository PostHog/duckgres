//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

const defaultSharedWorkerReservationLease = 24 * time.Hour

// OrgReservedPool presents one org's reserved slice of a shared K8s warm pool.
// It preserves the existing WorkerPool contract for SessionManager while ensuring
// workers are reserved to a single org for their lifetime and retired after use.
type OrgReservedPool struct {
	shared                 *K8sWorkerPool
	orgID                  string
	maxWorkers             int
	leaseDuration          time.Duration
	resolveOrgConfig       func() (*configstore.OrgConfig, error)
	activateReservedWorker func(context.Context, *ManagedWorker, *configstore.OrgConfig) error
}

func NewOrgReservedPool(shared *K8sWorkerPool, orgID string, maxWorkers int) *OrgReservedPool {
	pool := &OrgReservedPool{
		shared:        shared,
		orgID:         orgID,
		maxWorkers:    maxWorkers,
		leaseDuration: defaultSharedWorkerReservationLease,
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
				OrgID:          p.orgID,
				LeaseExpiresAt: time.Now().Add(p.leaseDuration),
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
	_ = p.RetireWorkerIfNoSessions(id)
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
	return w.SharedState().NormalizedLifecycle() == WorkerLifecycleHot
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

	org, err := p.lookupOrgConfig()
	if err != nil {
		return err
	}

	if err := p.activateReservedWorker(ctx, worker, org); err != nil {
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

func (p *OrgReservedPool) activateReservedWorkerDefault(ctx context.Context, worker *ManagedWorker, org *configstore.OrgConfig) error {
	if org == nil {
		return fmt.Errorf("org config is required for activation")
	}
	payload, err := BuildTenantActivationPayload(ctx, p.shared.clientset, p.shared.namespace, org)
	if err != nil {
		return err
	}
	if state := worker.SharedState(); state.Assignment != nil {
		payload.LeaseExpiresAt = state.Assignment.LeaseExpiresAt
	}
	return p.shared.ActivateReservedWorker(ctx, worker, payload)
}

func (p *OrgReservedPool) lookupOrgConfig() (*configstore.OrgConfig, error) {
	if p.resolveOrgConfig == nil {
		return nil, fmt.Errorf("org config resolver is not configured for org %s", p.orgID)
	}
	org, err := p.resolveOrgConfig()
	if err != nil {
		return nil, err
	}
	if org == nil {
		return nil, fmt.Errorf("org config resolver returned nil for org %s", p.orgID)
	}
	return org, nil
}
