//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// OrgReservedPool presents one org's reserved slice of a shared K8s pool.
// It preserves the existing WorkerPool contract for SessionManager while ensuring
// workers are reserved to a single org for their lifetime and retired after use.
type OrgReservedPool struct {
	shared                 *K8sWorkerPool
	orgID                  string
	maxWorkers             int
	image                  string
	stsBroker              *STSBroker
	activateReservedWorker func(context.Context, *ManagedWorker) error
	// gate serializes the slow path's short DECISION section (re-check idle
	// reuse → hot-idle claim → spawning-slot creation; see acquireDecision) in
	// FIFO arrival order, so the next worker to become available goes to the
	// earliest waiting connection and a later one cannot snatch it. The
	// multi-minute spawn+activate runs OUTSIDE the gate, 1:1 bound to the
	// waiter that owns the claim, so a cold burst ramps spawns in parallel.
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

func (p *OrgReservedPool) AcquireWorker(ctx context.Context, profile *WorkerProfile) (worker *ManagedWorker, err error) {
	// End-to-end acquire latency (fast path included), observed via a named-
	// return defer so every exit — reuse, capacity error, ctx cancel — lands in
	// the same histogram with an outcome label.
	acquireStart := time.Now()
	defer func() {
		observeAcquireTotal(time.Since(acquireStart), acquireTotalOutcome(err))
		if worker != nil {
			// Session assigned: veto voluntary disruption (consolidation/drift)
			// of the worker's node until release. Freshly spawned pods are
			// already born with the annotation; this covers hot-idle reuse and
			// cross-CP claims. Synchronous so it cannot reorder after a later
			// release's removal.
			p.shared.markWorkerProtected(worker)
		}
	}()

	// Fast path: reuse an already-assigned, idle (Hot) worker of the requested
	// shape. Such a worker is already this org's and free, so reusing it
	// concurrently is safe and needs no FIFO ordering — it is not a freshly
	// spawned worker that a queued waiter is owed (those are claimed only
	// via the gated slow path below).
	if w := p.tryReuseIdleAssigned(profile); w != nil {
		return w, nil
	}

	// Slow path: no idle worker — claim a hot-idle worker or spawn one on demand.
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		claim, assignment, worker, err := p.acquireDecision(ctx, profile)
		if err != nil {
			return nil, err
		}
		if worker != nil {
			return worker, nil // reused an idle worker that freed while queued
		}

		// Execute the multi-minute spawn/adopt + activate OUTSIDE the gate (so a
		// cold burst of N waiters ramps N spawns in parallel instead of
		// sequentially) and DETACHED from the request ctx (so a requester that
		// gives up doesn't kill an in-flight pod spawn).
		out := p.completeAcquireBoundToRequester(ctx, claim, assignment)
		if out.retry {
			// Stale hot-idle claim or the worker stopped being ours mid-flight
			// (raced with retirement/reclaim); re-run the gated decision.
			continue
		}
		if out.err != nil {
			return nil, out.err
		}
		return out.worker, nil
	}
}

// acquireDecision is the short, gate-held decision section of the slow path:
// re-check idle reuse, then claim a hot-idle worker or create a spawning slot
// (reserveSharedWorkerDecision). Returns exactly one of: an idle worker reused
// while queued, or the claim this waiter now owns.
//
// The gate serializes per org in FIFO arrival order, but is held ONLY across
// this decision — not across the multi-minute spawn+activate. The anti-snatch
// invariant ("a worker the CP scaled up for an earlier waiter cannot be
// snatched by a later connection") is preserved without holding the gate
// longer: each slow-path waiter leaves the gate 1:1 bound to the specific
// hot-idle claim or spawning slot it owns (the durable claim/slot row is
// already assigned to it), and the spawned worker's session is pre-claimed
// before it ever becomes visible as Hot (executeAcquire), so no later
// connection can grab it. Freed/parked workers re-enter circulation only via
// the idle-reuse re-check and the hot-idle claim inside this gated section,
// which queued waiters reach in FIFO order — the earliest waiter still gets
// the next available worker.
func (p *OrgReservedPool) acquireDecision(ctx context.Context, profile *WorkerProfile) (*sharedWorkerClaim, *WorkerAssignment, *ManagedWorker, error) {
	gateStart := time.Now()
	if err := p.gate.acquire(ctx); err != nil {
		observeAcquireGateWait(time.Since(gateStart), "canceled")
		return nil, nil, nil, err
	}
	observeAcquireGateWait(time.Since(gateStart), "acquired")
	defer p.gate.release()

	// A worker may have freed while we waited for our FIFO turn.
	if w := p.tryReuseIdleAssigned(profile); w != nil {
		return nil, nil, w, nil
	}

	p.shared.mu.Lock()
	if p.shared.shuttingDown {
		p.shared.mu.Unlock()
		return nil, nil, nil, fmt.Errorf("pool is shutting down")
	}
	maxWorkers := p.maxWorkers
	image := p.image
	p.shared.mu.Unlock()

	// At the org's max concurrent workers with all busy → fail fast with the
	// clear org-cap message; waiting cannot help (no new worker will spawn).
	// This in-memory pre-check is the fast-fail; the authoritative cross-CP cap
	// re-check happens transactionally in CreateSpawningWorkerSlot below.
	if p.atOrgWorkerCap() {
		return nil, nil, nil, NewWorkerCapacityExhaustedErrorForReason(
			configstore.WorkerClaimMissReasonOrgCap, DefaultWorkerSpawnRetryAfter)
	}

	// Under cap: claim a hot-idle worker for this org, or create a spawning
	// slot. reserveSharedWorkerDecision re-checks the org/global cap
	// authoritatively (CreateSpawningWorkerSlot, cross-CP) and returns the
	// reason-specific cap error; an org/global-cap or shutdown error won't
	// resolve by waiting.
	assignment := &WorkerAssignment{
		OrgID:      p.orgID,
		MaxWorkers: maxWorkers,
		Image:      image,
		Profile:    profile,
	}
	claim, err := p.shared.reserveSharedWorkerDecision(assignment)
	if err != nil {
		return nil, nil, nil, err
	}
	return claim, assignment, nil, nil
}

// orgAcquireOutcome is the result of executing a slow-path claim. Exactly one
// of worker / retry / err is meaningful: a Hot worker with this waiter's
// session claimed, a request to re-run the gated decision, or a terminal error.
type orgAcquireOutcome struct {
	worker *ManagedWorker
	retry  bool
	err    error
}

// completeAcquireBoundToRequester runs the slow spawn/adopt + activation for
// the claim this waiter owns. The work runs under a context DETACHED from the
// request (context.WithoutCancel keeps trace/log values) with its own deadline
// (workerSpawnActivateTimeout): a requester that gives up must not kill an
// in-flight pod spawn — if node provisioning reliably exceeds the client's
// budget, a requester-scoped spawn would be deleted on every timeout and the
// org would never converge on a ready worker. The requester waits for the
// result or its own ctx, whichever comes first. If the requester abandons:
//   - eventual success → the worker (Hot, session pre-claimed by executeAcquire)
//     is parked for the org via ReleaseWorker → TransitionToHotIdleIfNoSessions,
//     persisting its hot_idle record, so the org's next connection reclaims it
//     through the normal hot-idle claim path;
//   - eventual failure → executeAcquire's failure paths have already retired it
//     (spawn failure / activation failure), so nothing leaks in
//     Reserved/Activating state.
func (p *OrgReservedPool) completeAcquireBoundToRequester(ctx context.Context, claim *sharedWorkerClaim, assignment *WorkerAssignment) orgAcquireOutcome {
	bgCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workerSpawnActivateTimeout)
	resultCh := make(chan orgAcquireOutcome, 1)
	go func() {
		defer cancel()
		resultCh <- p.executeAcquire(bgCtx, claim, assignment)
	}()

	select {
	case out := <-resultCh:
		return out
	case <-ctx.Done():
		// Abandoned: let the spawn/activate finish in the background, then park
		// the worker for org reuse (failure paths have already retired it).
		go func() {
			out := <-resultCh
			if out.worker == nil {
				return
			}
			slog.Info("Requester abandoned worker spawn/activate; parking worker hot-idle for org reuse.",
				"worker", out.worker.ID, "org", p.orgID)
			p.ReleaseWorker(out.worker.ID)
		}()
		return orgAcquireOutcome{err: ctx.Err()}
	}
}

// executeAcquire completes the reservation (pod spawn or hot-idle adoption)
// and activates the worker for the org. On success the returned worker is Hot
// with this waiter's session already claimed: the session is pre-claimed while
// the worker is still Reserved, before it ever becomes visible as Hot, so a
// concurrently-arriving connection's idle-reuse fast path can never grab the
// worker out from under the waiter that spawned it (the 1:1 waiter↔worker
// binding behind the anti-snatch invariant).
func (p *OrgReservedPool) executeAcquire(ctx context.Context, claim *sharedWorkerClaim, assignment *WorkerAssignment) orgAcquireOutcome {
	worker, retry, err := p.shared.completeSharedWorkerReservation(ctx, claim, assignment)
	if retry {
		return orgAcquireOutcome{retry: true}
	}
	if err != nil {
		return orgAcquireOutcome{err: err}
	}

	p.shared.mu.Lock()
	worker.claimSessionLocked()
	p.shared.mu.Unlock()

	if err := p.activateWorkerForOrg(ctx, worker); err != nil {
		slog.Warn("Worker activation failed.", "worker", worker.ID, "org", p.orgID, "error", err)
		observeActivationFailure(worker.image)
		p.shared.retireWorkerWithReason(worker.ID, RetireReasonActivationFailure, LifecycleOriginActivationFailure)
		return orgAcquireOutcome{err: err}
	}

	p.shared.mu.Lock()
	owned := p.workerBelongsToOrgLocked(worker)
	if !owned {
		// Worker is no longer ours (raced with retirement/reclaim). Undo the
		// pre-claim before re-deciding: if this CP still tracks the worker, a
		// stranded activeSessions=1 would make findIdleAssignedWorkerLocked and
		// TransitionToHotIdleIfNoSessions skip it forever (fail-safe direction,
		// but the worker leaks until a TTL reaper retires it).
		worker.activeSessions--
	}
	p.shared.mu.Unlock()
	if !owned {
		return orgAcquireOutcome{retry: true}
	}
	return orgAcquireOutcome{worker: worker}
}

// atOrgWorkerCap reports whether the org has reached its maximum concurrent
// workers (count cap). Hot-idle workers are excluded (they don't consume the
// count budget), so a true result means every counted worker is actively
// assigned — a new worker cannot be added.
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
		// Parked hot-idle: drop the do-not-disrupt veto so Karpenter may
		// consolidate a node holding only idle workers/placeholders. The next
		// acquire re-adds it before the session starts.
		if w, ok := p.shared.Worker(id); ok {
			p.shared.markWorkerDisruptable(w)
		}
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
		// Only reuse a worker of the requested shape — a differently-sized
		// request must not land on a worker of another shape (and vice versa).
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

func (p *OrgReservedPool) activateWorkerForOrg(ctx context.Context, worker *ManagedWorker) (err error) {
	activateStart := time.Now()
	defer func() {
		observeAcquirePhase("activate", time.Since(activateStart), err)
	}()

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
