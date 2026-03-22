//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"time"
)

const defaultSharedWorkerReservationLease = 24 * time.Hour

// TeamReservedWorkerPool presents one team's reserved slice of a shared K8s warm pool.
// It preserves the existing WorkerPool contract for SessionManager while ensuring
// workers are reserved to a single team for their lifetime and retired after use.
type TeamReservedWorkerPool struct {
	shared        *K8sWorkerPool
	teamName      string
	maxWorkers    int
	leaseDuration time.Duration
}

func NewTeamReservedWorkerPool(shared *K8sWorkerPool, teamName string, maxWorkers int) *TeamReservedWorkerPool {
	return &TeamReservedWorkerPool{
		shared:        shared,
		teamName:      teamName,
		maxWorkers:    maxWorkers,
		leaseDuration: defaultSharedWorkerReservationLease,
	}
}

func (p *TeamReservedWorkerPool) AcquireWorker(ctx context.Context) (*ManagedWorker, error) {
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
				TeamName:       p.teamName,
				LeaseExpiresAt: time.Now().Add(p.leaseDuration),
			})
			if err != nil {
				return nil, err
			}

			p.shared.mu.Lock()
			if owned := p.workerBelongsToTeamLocked(worker); owned {
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

func (p *TeamReservedWorkerPool) ReleaseWorker(id int) {
	_ = p.RetireWorkerIfNoSessions(id)
}

func (p *TeamReservedWorkerPool) RetireWorker(id int) {
	if _, ok := p.Worker(id); !ok {
		return
	}
	p.shared.RetireWorker(id)
}

func (p *TeamReservedWorkerPool) RetireWorkerIfNoSessions(id int) bool {
	if _, ok := p.Worker(id); !ok {
		return false
	}
	return p.shared.RetireWorkerIfNoSessions(id)
}

func (p *TeamReservedWorkerPool) Worker(id int) (*ManagedWorker, bool) {
	p.shared.mu.RLock()
	defer p.shared.mu.RUnlock()
	w, ok := p.shared.workers[id]
	if !ok || !p.workerBelongsToTeamLocked(w) {
		return nil, false
	}
	return w, true
}

func (p *TeamReservedWorkerPool) SpawnMinWorkers(count int) error {
	return nil
}

func (p *TeamReservedWorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
}

func (p *TeamReservedWorkerPool) SetMaxWorkers(n int) {
	p.shared.mu.Lock()
	defer p.shared.mu.Unlock()
	p.maxWorkers = n
}

func (p *TeamReservedWorkerPool) ShutdownAll() {
	p.shared.mu.RLock()
	workers := make([]int, 0, len(p.shared.workers))
	for id, w := range p.shared.workers {
		if p.workerBelongsToTeamLocked(w) {
			workers = append(workers, id)
		}
	}
	p.shared.mu.RUnlock()

	for _, id := range workers {
		p.shared.RetireWorker(id)
	}
}

func (p *TeamReservedWorkerPool) findIdleAssignedWorkerLocked() *ManagedWorker {
	for _, w := range p.shared.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if w.activeSessions == 0 && p.workerBelongsToTeamLocked(w) {
			return w
		}
	}
	return nil
}

func (p *TeamReservedWorkerPool) leastLoadedAssignedWorkerLocked() *ManagedWorker {
	var best *ManagedWorker
	for _, w := range p.shared.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if !p.workerBelongsToTeamLocked(w) {
			continue
		}
		if best == nil || w.activeSessions < best.activeSessions {
			best = w
		}
	}
	return best
}

func (p *TeamReservedWorkerPool) assignedWorkerCountLocked() int {
	count := 0
	for _, w := range p.shared.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if p.workerBelongsToTeamLocked(w) {
			count++
		}
	}
	return count
}

func (p *TeamReservedWorkerPool) workerBelongsToTeamLocked(w *ManagedWorker) bool {
	state := w.SharedState()
	return state.Assignment != nil && state.Assignment.TeamName == p.teamName && state.NormalizedLifecycle() != WorkerLifecycleRetired
}
