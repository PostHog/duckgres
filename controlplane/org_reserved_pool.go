//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"time"
)

const defaultSharedWorkerReservationLease = 24 * time.Hour

// OrgReservedPool presents one org's reserved slice of a shared K8s warm pool.
// It preserves the existing WorkerPool contract for SessionManager while ensuring
// workers are reserved to a single org for their lifetime and retired after use.
type OrgReservedPool struct {
	shared        *K8sWorkerPool
	orgID         string
	maxWorkers    int
	leaseDuration time.Duration
}

func NewOrgReservedPool(shared *K8sWorkerPool, orgID string, maxWorkers int) *OrgReservedPool {
	return &OrgReservedPool{
		shared:        shared,
		orgID:         orgID,
		maxWorkers:    maxWorkers,
		leaseDuration: defaultSharedWorkerReservationLease,
	}
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

			p.shared.mu.Lock()
			if owned := p.workerBelongsToOrgLocked(worker); owned {
				worker.activeSessions++
				p.shared.mu.Unlock()
				return worker, nil
			}
			p.shared.mu.Unlock()
			continue
		}

		if w := p.leastLoadedAssignedWorkerLocked(); w != nil {
			w.activeSessions++
			p.shared.mu.Unlock()
			return w, nil
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
		p.shared.RetireWorker(id)
	}
}

func (p *OrgReservedPool) findIdleAssignedWorkerLocked() *ManagedWorker {
	for _, w := range p.shared.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if w.activeSessions == 0 && p.workerBelongsToOrgLocked(w) {
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
		if !p.workerBelongsToOrgLocked(w) {
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
