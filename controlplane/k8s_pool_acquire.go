//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	stderrors "errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/flightclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AcquireWorker returns a worker for a new session.
func (p *K8sWorkerPool) AcquireWorker(ctx context.Context, _ *WorkerProfile) (*ManagedWorker, error) {
	// The flat K8s pool is the single-tenant backend; worker-profile selection is
	// a multi-tenant (OrgReservedPool) feature, so the profile is ignored here.
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		p.mu.Lock()
		if p.shuttingDown {
			p.mu.Unlock()
			return nil, fmt.Errorf("pool is shutting down")
		}

		p.cleanDeadWorkersLocked()

		// 1. Try to claim an idle worker
		idle := p.findIdleWorkerLocked()
		if idle != nil {
			idle.claimSessionLocked()
			p.mu.Unlock()
			slog.Debug("Reusing idle worker.", append(workerLogAttrs(idle), "active_sessions", idle.activeSessions)...)
			return idle, nil
		}

		// 2. No idle worker — check if we have any live workers at all
		liveCount := p.liveWorkerCountLocked()
		canSpawn := p.maxWorkers == 0 || liveCount < p.maxWorkers

		if liveCount > 0 {
			// We have live workers. Assign to the least-loaded one immediately
			// and spawn a new worker in the background if below capacity.
			w := p.leastLoadedWorkerLocked()
			if w != nil {
				w.claimSessionLocked()
				if canSpawn {
					id := p.allocateBackgroundSpawnIDLocked()
					p.spawning++
					p.mu.Unlock()
					slog.Debug("Assigned to least-loaded worker, spawning new worker in background.",
						"worker", w.ID, "active_sessions", w.activeSessions, "background_worker", id)
					go p.spawnWorkerBackground(id, p.workerImage)
				} else {
					p.mu.Unlock()
					slog.Debug("Assigned to least-loaded worker (at capacity).",
						"worker", w.ID, "active_sessions", w.activeSessions)
				}
				return w, nil
			}
		}

		// 3. No live workers at all (cold start or all dead) — must block on spawn
		if canSpawn {
			id := p.allocateBackgroundSpawnIDLocked()
			p.spawning++
			p.mu.Unlock()

			p.logw(id).Info("No live workers, blocking on spawn.")
			err := p.SpawnWorker(ctx, id, p.workerImage)

			p.mu.Lock()
			p.spawning--
			p.mu.Unlock()

			if err != nil {
				return nil, err
			}

			w, ok := p.Worker(id)
			if !ok {
				return nil, fmt.Errorf("worker %d not found after spawn", id)
			}
			p.mu.Lock()
			w.claimSessionLocked()
			p.mu.Unlock()
			return w, nil
		}

		// At capacity with all workers dead (spawning in progress) — wait and retry
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// ReleaseWorker decrements the active session count for a worker.
func (p *K8sWorkerPool) ReleaseWorker(id int) {
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return
	}
	if w.activeSessions > 0 {
		w.activeSessions--
	}
	w.lastUsed = time.Now()
	p.mu.Unlock()
}

// Worker returns a worker by ID.
func (p *K8sWorkerPool) Worker(id int) (*ManagedWorker, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	w, ok := p.workers[id]
	return w, ok
}

// ActivateReservedWorker transitions a reserved worker through activating to hot.
// Failed activations retire the worker immediately.
func (p *K8sWorkerPool) ActivateReservedWorker(ctx context.Context, worker *ManagedWorker, payload TenantActivationPayload) error {
	p.mu.Lock()
	var err error
	var prevState SharedWorkerState
	hadPrevState := false
	var activatingRecord *configstore.WorkerRecord
	switch worker.SharedState().NormalizedLifecycle() {
	case WorkerLifecycleReserved:
		prevState = worker.SharedState()
		hadPrevState = true
		nextState, transitionErr := worker.SharedState().Transition(WorkerLifecycleActivating, nil)
		if transitionErr == nil {
			transitionErr = worker.SetSharedState(nextState)
		}
		if transitionErr == nil {
			now := time.Now()
			activatingRecord = p.workerRecordFor(worker.ID, worker, worker.OwnerEpoch(), configstore.WorkerStateActivating, "", &now)
		}
		err = transitionErr
	case WorkerLifecycleActivating:
		err = nil
		now := time.Now()
		activatingRecord = p.workerRecordFor(worker.ID, worker, worker.OwnerEpoch(), configstore.WorkerStateActivating, "", &now)
	default:
		err = fmt.Errorf("worker %d is not reserved for activation", worker.ID)
	}
	p.mu.Unlock()
	if err != nil {
		return err
	}
	// A failed activating persist is self-correcting: the hot persist below is a
	// full upsert that overwrites the durable state once activation succeeds, and
	// a failed activation retires the worker. Log it (it shouldn't happen) but do
	// not abort activation on a transient write blip.
	if err := p.persistWorkerRecord(activatingRecord); err != nil {
		p.logw(worker.ID).Warn("Failed to persist activating worker record.", "error", err)
	}

	activate := p.activateTenantFunc
	if activate == nil {
		activate = func(ctx context.Context, worker *ManagedWorker, payload TenantActivationPayload) error {
			return worker.ActivateTenant(ctx, server.WorkerActivationPayload{
				WorkerControlMetadata: server.WorkerControlMetadata{
					WorkerID:     worker.ID,
					OwnerEpoch:   worker.OwnerEpoch(),
					CPInstanceID: worker.OwnerCPInstanceID(),
				},
				OrgID:    payload.OrgID,
				DuckLake: payload.DuckLake,
			})
		}
	}

	// Heartbeat the activating row while activate() runs — it can include a long,
	// in-band DuckLake migration. The leader stuck-activating reaper keys off
	// updated_at, and (unlike the pool-local reaper) cannot see the live
	// in-memory session; without this, a legitimately-progressing activation that
	// outlives activateTimeout (5m) would be CAS-retired mid-session by the
	// durable reaper and its pod deleted. The heartbeat ties "fresh" to "this
	// activation goroutine is alive and running": a wedged activation returns via
	// its ctx deadline (workerSpawnActivateTimeout) and is retired below, and a
	// crashed CP's goroutine dies so the row goes stale and IS reaped — so
	// genuinely-stuck rows are still cleaned up. The fenced upsert can't resurrect
	// a worker another path retired (state/epoch guard).
	stopHB := make(chan struct{})
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		p.heartbeatActivatingWorker(worker, stopHB)
	}()
	// stopHeartbeat is idempotent (sync.Once) and deferred as a panic-safe
	// backstop AND called explicitly on the normal path. The explicit call
	// joins the heartbeat BEFORE the Hot-commit block below, so a heartbeat can
	// never clobber the committed Hot row; the defer guarantees the goroutine +
	// ticker are not leaked if activate() panics.
	var stopOnce sync.Once
	stopHeartbeat := func() { stopOnce.Do(func() { close(stopHB); <-hbDone }) }
	defer stopHeartbeat()

	activateErr := activate(ctx, worker, payload)
	stopHeartbeat()

	if err := activateErr; err != nil {
		if hadPrevState {
			p.mu.Lock()
			_ = worker.SetSharedState(prevState)
			p.mu.Unlock()
		}
		p.retireWorkerWithReason(worker.ID, RetireReasonActivationFailure, LifecycleOriginActivationFailure)
		return err
	}

	p.mu.Lock()
	// Cache the activation payload for potential hot-idle reuse.
	cached := payload
	worker.cachedActivationPayload = &cached

	if worker.SharedState().NormalizedLifecycle() == WorkerLifecycleHot {
		p.mu.Unlock()
		return nil
	}
	nextState, err := worker.SharedState().Transition(WorkerLifecycleHot, nil)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	// Persist the hot row BEFORE committing the in-memory Hot transition. If we
	// flipped in-memory to Hot but the durable row stayed at activating (the old
	// swallowed-error behavior), the leader stuck-activating reaper would later
	// CAS-retire this LIVE, about-to-serve worker and delete its pod mid-session.
	// On persist failure leave the worker Activating and return the error: the
	// caller (activateWorkerForOrg) retires the worker and surfaces the error to
	// the client (no transparent retry), which is wasteful but never strands a
	// durable=activating / in-memory=Hot split. workerRecordFor takes the target
	// state explicitly, so building it before the in-memory commit is safe.
	hotRecord := p.workerRecordFor(worker.ID, worker, worker.OwnerEpoch(), configstore.WorkerStateHot, "", nil)
	if err := p.persistWorkerRecord(hotRecord); err != nil {
		p.mu.Unlock()
		return fmt.Errorf("persist hot worker record: %w", err)
	}
	if setErr := worker.SetSharedState(nextState); setErr != nil {
		p.mu.Unlock()
		return setErr
	}
	p.mu.Unlock()
	return nil
}

// activatingHeartbeatInterval is how often a long-running activation refreshes
// its durable activating row so the leader stuck-activating reaper (which keys
// off updated_at) does not treat a progressing activation as wedged. Well below
// the janitor's activateTimeout (5m).
const activatingHeartbeatInterval = 60 * time.Second

// heartbeatActivatingWorker re-persists the worker's activating row on a timer
// until stop is closed, keeping updated_at fresh while activate() is in flight.
// Stops (and skips) the moment the worker is no longer this CP's and still
// Activating — so once activation commits Hot under p.mu, a racing heartbeat
// sees the new state and can never clobber the Hot row back to activating.
func (p *K8sWorkerPool) heartbeatActivatingWorker(worker *ManagedWorker, stop <-chan struct{}) {
	interval := p.activatingHBInterval
	if interval <= 0 {
		interval = activatingHeartbeatInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			// Build AND persist under p.mu so this serializes with the Hot
			// transition's persist: the lifecycle re-check below means a heartbeat
			// that wins the lock after Hot was committed skips instead of writing
			// a stale activating row. persistWorkerRecord doesn't take p.mu and
			// logs via plain slog, so holding the lock here is deadlock-safe
			// (mirrors markWorkerRetiredLocked).
			p.mu.Lock()
			w, ok := p.workers[worker.ID]
			if !ok || w != worker || w.SharedState().NormalizedLifecycle() != WorkerLifecycleActivating {
				p.mu.Unlock()
				return
			}
			now := time.Now()
			rec := p.workerRecordFor(worker.ID, worker, worker.OwnerEpoch(), configstore.WorkerStateActivating, "", &now)
			_ = p.persistWorkerRecord(rec)
			p.mu.Unlock()
		}
	}
}

// ReserveSharedWorker claims a hot-idle worker or spawns one on demand.
// Composition of the gateable decision (reserveSharedWorkerDecision) and the slow
// completion (completeSharedWorkerReservation); OrgReservedPool calls the two
// halves separately so the per-org FIFO gate covers only the decision.
func (p *K8sWorkerPool) ReserveSharedWorker(ctx context.Context, assignment *WorkerAssignment) (*ManagedWorker, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		claim, err := p.reserveSharedWorkerDecision(assignment)
		if err != nil {
			return nil, err
		}
		worker, retry, err := p.completeSharedWorkerReservation(ctx, claim, assignment)
		if retry {
			continue
		}
		if err != nil {
			return nil, err
		}
		return worker, nil
	}
}

// sharedWorkerClaim is the outcome of the short reservation decision: either a
// compatible hot-idle worker claimed in the runtime store or a freshly created
// spawning slot. Any claim/slot is already bound to the caller in the durable
// store (or locally allocated in runtime-store-less mode), so the caller may
// finish multi-minute completion without holding any per-org serialization.
type sharedWorkerClaim struct {
	hotClaimed *configstore.WorkerRecord
	slotID     int
	// slot is the durable spawning-slot row (state=spawning) created by
	// CreateSpawningWorkerSlot, retained so a failed spawn can CAS it terminal
	// on the request thread instead of leaking org+global cap until the leader's
	// stale-spawning sweep. nil in runtime-store-less mode (no durable row).
	slot *configstore.WorkerRecord
}

// reserveSharedWorkerDecision is the short, DB-only half of ReserveSharedWorker:
// claim a hot-idle worker for the org (fast path: DuckLake is already attached,
// only needs an epoch bump) or create a spawning worker slot.
// CreateSpawningWorkerSlot re-checks the per-org and global caps transactionally
// cross-CP (authoritative) and yields the reason-specific cap error at the
// ceiling. No pod I/O happens here — OrgReservedPool holds its FIFO acquire gate
// across exactly this call.
func (p *K8sWorkerPool) reserveSharedWorkerDecision(assignment *WorkerAssignment) (*sharedWorkerClaim, error) {
	if err := validateWorkerAssignment(assignment); err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.shuttingDown {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is shutting down")
	}
	p.cleanDeadWorkersLocked()
	p.mu.Unlock()

	if p.runtimeStore != nil && assignment.OrgID != "" {
		hotProfileCPU, hotProfileMem := assignment.Profile.Parts()
		hotClaimed, hotMissReason, err := p.runtimeStore.ClaimHotIdleWorker(p.cpInstanceID, assignment.OrgID, assignment.Image, hotProfileCPU, hotProfileMem, assignment.MaxWorkers)
		if err != nil {
			return nil, err
		}
		if hotClaimed == nil && hotMissReason != configstore.WorkerClaimMissReasonNone && hotMissReason != configstore.WorkerClaimMissReasonNoIdle {
			return nil, NewWorkerCapacityExhaustedErrorForReason(hotMissReason, DefaultWorkerSpawnRetryAfter)
		}
		if hotClaimed != nil {
			return &sharedWorkerClaim{hotClaimed: hotClaimed}, nil
		}
	}

	// No reusable hot-idle worker for this org — spawn one on demand, sized from
	// the request profile (or the pool-global default for a default request).
	// Allocate a real, unique worker id. In the runtime-store path this MUST come
	// from the DB via CreateSpawningWorkerSlot (which enforces the per-org worker
	// cap cross-CP and returns a nil slot when capped) — the background id
	// allocator returns a placeholder 0 that collides across spawns.
	if p.runtimeStore != nil {
		spawnProfileCPU, spawnProfileMem := assignment.Profile.Parts()
		slot, err := p.runtimeStore.CreateSpawningWorkerSlot(
			p.cpInstanceID, assignment.OrgID, assignment.Image, spawnProfileCPU, spawnProfileMem, 0,
			p.workerPodNamePrefix(), assignment.MaxWorkers)
		if err != nil {
			return nil, err
		}
		if slot == nil {
			return nil, NewWorkerCapacityExhaustedErrorForReason(configstore.WorkerClaimMissReasonOrgCap, DefaultWorkerSpawnRetryAfter)
		}
		return &sharedWorkerClaim{slotID: slot.WorkerID, slot: slot}, nil
	}

	// Runtime-store-less mode (unit tests / standalone k8s): no durable pool,
	// just allocate a local id and spawn a worker on demand.
	p.mu.Lock()
	id := p.allocateWorkerIDLocked()
	p.mu.Unlock()
	return &sharedWorkerClaim{slotID: id}, nil
}

// completeSharedWorkerReservation is the slow half of ReserveSharedWorker:
// adopt/health-check a claimed hot-idle worker, or spawn the slot's pod and
// reserve it. Runs without any per-org serialization — the claim/slot already
// belongs to this caller. retry=true means the claim went stale or the hot-idle
// worker was unusable (and has been retired); the caller should re-run the
// decision (which will typically create a spawning slot).
func (p *K8sWorkerPool) completeSharedWorkerReservation(ctx context.Context, claim *sharedWorkerClaim, assignment *WorkerAssignment) (worker *ManagedWorker, retry bool, err error) {
	if claim.hotClaimed != nil {
		// hot_idle_claim covers the adopt/health-check I/O for a claimed
		// hot-idle worker (the DB claim itself happened in the gated decision
		// and is microseconds). Stale-claim retries observe as error per
		// attempt.
		hotClaimStart := time.Now()
		worker, reserveErr := p.reserveClaimedWorker(ctx, claim.hotClaimed, assignment)
		observeAcquirePhase("hot_idle_claim", assignment.OrgID, time.Since(hotClaimStart), reserveErr)
		if reserveErr == nil {
			worker.hotIdleReclaimed = true
			return worker, false, nil
		}
		if stderrors.Is(reserveErr, errStaleRuntimeWorkerClaim) {
			slog.Warn("Hot-idle worker claim was stale, retrying.", "worker", claim.hotClaimed.WorkerID, "worker_pod", claim.hotClaimed.PodName, "org", claim.hotClaimed.OrgID, "error", reserveErr)
			return nil, true, reserveErr
		}
		slog.Warn("Hot-idle worker could not be reserved, retiring.", "worker", claim.hotClaimed.WorkerID, "worker_pod", claim.hotClaimed.PodName, "org", claim.hotClaimed.OrgID, "error", reserveErr)
		p.retireClaimedWorker(claim.hotClaimed, RetireReasonCrash, LifecycleOriginReserveFailure)
		// Pre-split ReserveSharedWorker fell through to a fresh spawn here; the
		// retry re-runs the decision, which lands on the spawning-slot path.
		return nil, true, reserveErr
	}
	worker, err = p.spawnReservedWorkerForSlot(ctx, claim.slotID, assignment)
	if err != nil && claim.slot != nil {
		// The spawning-slot row counts against the org + global worker caps until
		// it is reaped. Free it on the request thread (CAS spawning->terminal)
		// instead of waiting for the leader-only 10m stale-spawning sweep —
		// otherwise a burst of transient spawn failures drives the org to a false
		// at-cap state (spurious OrgCap refusals) and the ghost slots survive a
		// janitor-leader outage indefinitely. If spawnReservedWorkerForSlot
		// already retired the row via its post-spawn failure paths, this CAS
		// simply misses (row no longer spawning) and is a harmless no-op.
		p.retireClaimedWorker(claim.slot, RetireReasonSpawnFailure, LifecycleOriginSpawnFailure)
	}
	return worker, false, err
}

func (p *K8sWorkerPool) reserveClaimedWorker(ctx context.Context, claimed *configstore.WorkerRecord, assignment *WorkerAssignment) (*ManagedWorker, error) {
	p.mu.Lock()
	if p.shuttingDown {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is shutting down")
	}
	p.cleanDeadWorkersLocked()
	worker, ok := p.workers[claimed.WorkerID]
	p.mu.Unlock()

	if !ok {
		adopted, err := p.adoptClaimedWorker(ctx, claimed)
		if err != nil {
			return nil, err
		}
		p.mu.Lock()
		if existing, exists := p.workers[claimed.WorkerID]; exists {
			p.mu.Unlock()
			if adopted.client != nil {
				_ = adopted.client.Close()
			}
			worker = existing
		} else {
			p.workers[claimed.WorkerID] = adopted
			p.mu.Unlock()
			worker = adopted
		}
	}

	p.mu.Lock()
	var reservedRecord *configstore.WorkerRecord
	if p.shuttingDown {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is shutting down")
	}
	if claimed.PodName != "" {
		worker.podName = claimed.PodName
	}
	// Carry the worker's persisted pod-shape so the reserved record and any later
	// reconciliation round-trip it. Default/legacy rows yield the zero profile.
	// TTL resets to the new request's value on reuse (a query hitting the worker
	// extends its life by the requested ttl); fall back to the worker's persisted
	// ttl when the request carries none.
	reuseTTL := time.Duration(claimed.TTLMinutes) * time.Minute
	if assignment != nil && assignment.Profile != nil && assignment.Profile.TTL > 0 {
		reuseTTL = assignment.Profile.TTL
	}
	worker.profile = WorkerProfile{CPU: claimed.ProfileCPU, Memory: claimed.ProfileMemory, TTL: reuseTTL}
	if worker.OwnerEpoch() > 0 && claimed.OwnerEpoch < worker.OwnerEpoch() {
		currentEpoch := worker.OwnerEpoch()
		p.mu.Unlock()
		return nil, fmt.Errorf("worker %d claimed with stale owner epoch %d behind current %d: %w", claimed.WorkerID, claimed.OwnerEpoch, currentEpoch, errStaleRuntimeWorkerClaim)
	}
	if isDuplicateRuntimeWorkerClaim(worker, claimed, assignment) {
		p.mu.Unlock()
		return nil, fmt.Errorf("worker %d already has in-flight claim for owner epoch %d: %w", claimed.WorkerID, claimed.OwnerEpoch, errStaleRuntimeWorkerClaim)
	}
	worker.SetOwnerCPInstanceID(claimed.OwnerCPInstanceID)
	worker.SetOwnerEpoch(claimed.OwnerEpoch)
	nextState, err := worker.SharedState().Transition(WorkerLifecycleReserved, assignment)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if err := worker.SetSharedState(nextState); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	worker.reservedAt = time.Now()
	if claimed.State != configstore.WorkerStateReserved {
		reservedRecord = p.workerRecordFor(worker.ID, worker, worker.OwnerEpoch(), configstore.WorkerStateReserved, "", nil)
	}
	p.mu.Unlock()
	if reservedRecord != nil {
		_ = p.persistWorkerRecord(reservedRecord)
	}
	if err := p.checkReservedWorkerLiveness(ctx, worker); err != nil {
		slog.Warn("Claimed worker failed liveness recheck.", append(workerLogAttrs(worker), "error", err)...)
		p.retireWorkerWithReason(worker.ID, RetireReasonCrash, LifecycleOriginReserveFailure)
		return nil, err
	}
	return worker, nil
}

func isDuplicateRuntimeWorkerClaim(worker *ManagedWorker, claimed *configstore.WorkerRecord, assignment *WorkerAssignment) bool {
	if worker == nil || claimed == nil || assignment == nil {
		return false
	}
	if worker.OwnerCPInstanceID() != claimed.OwnerCPInstanceID || worker.OwnerEpoch() != claimed.OwnerEpoch {
		return false
	}
	state := worker.SharedState()
	switch state.NormalizedLifecycle() {
	case WorkerLifecycleReserved, WorkerLifecycleActivating:
	default:
		return false
	}
	if state.Assignment == nil {
		return false
	}
	return state.Assignment.OrgID == assignment.OrgID
}

func (p *K8sWorkerPool) claimSpecificWorker(ctx context.Context, workerID int, expectedOwnerEpoch int64, assignment *WorkerAssignment) (*ManagedWorker, error) {
	if p.runtimeStore == nil {
		return nil, fmt.Errorf("runtime worker store is not configured")
	}
	if err := validateWorkerAssignment(assignment); err != nil {
		return nil, err
	}
	record, err := p.runtimeStore.TakeOverWorker(workerID, p.cpInstanceID, assignment.OrgID, expectedOwnerEpoch)
	if err != nil {
		if stderrors.Is(err, configstore.ErrWorkerOwnerEpochMismatch) {
			return nil, fmt.Errorf("worker %d ownership changed before takeover: %w", workerID, err)
		}
		return nil, err
	}
	if record == nil {
		return nil, fmt.Errorf("worker %d could not be claimed", workerID)
	}

	if assignment.Image != "" && record.Image != assignment.Image {
		return nil, fmt.Errorf("worker %d image mismatch (expected %q, got %q)", workerID, assignment.Image, record.Image)
	}
	if expCPU, expMem := assignment.Profile.Parts(); record.ProfileCPU != expCPU || record.ProfileMemory != expMem {
		return nil, fmt.Errorf("worker %d profile mismatch (expected %s/%s, got %s/%s)",
			workerID, expCPU, expMem, record.ProfileCPU, record.ProfileMemory)
	}

	return p.reserveClaimedWorker(ctx, record, assignment)
}

func (p *K8sWorkerPool) adoptClaimedWorker(ctx context.Context, claimed *configstore.WorkerRecord) (*ManagedWorker, error) {
	token, _, err := p.readWorkerRPCSecurity(ctx, claimed.PodName)
	if err != nil {
		return nil, fmt.Errorf("read worker RPC security: %w", err)
	}
	pod, err := p.clientset.CoreV1().Pods(p.namespace).Get(ctx, claimed.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get claimed worker pod %s: %w", claimed.PodName, err)
	}

	// For hot-idle workers, skip the epoch-validated health check. The worker's
	// epoch and CP instance ID are from the previous owner, and ClaimHotIdleWorker
	// already bumped the epoch in the DB. The health check requires exact epoch
	// match, so neither the old epoch (worker has N, we'd send N+1) nor the new
	// CP instance ID will pass. ActivateTenant validates the epoch properly
	// (accepts > current). For freshly spawned workers (epoch 0), use the normal
	// health-checked path.
	var client *flightsql.Client
	if claimed.OwnerEpoch > 1 {
		client, err = p.connectWorkerDirect(ctx, claimed.PodName, pod.Status.PodIP, token)
	} else {
		client, err = p.connectWorker(ctx, claimed.PodName, pod.Status.PodIP, token)
	}
	if err != nil {
		return nil, err
	}
	worker := &ManagedWorker{
		ID:          claimed.WorkerID,
		podName:     claimed.PodName,
		image:       claimed.Image,
		bearerToken: token,
		client:      client,
		// Restore the worker's size + TTL from the persisted record so a re-adopted
		// worker keeps its shape. Without this, re-adopting a sized worker reset its
		// profile to the default and the next hot-idle persist dropped its
		// cpu/mem/ttl, so a same-size request could no longer reuse it.
		profile: WorkerProfile{
			CPU:    claimed.ProfileCPU,
			Memory: claimed.ProfileMemory,
			TTL:    time.Duration(claimed.TTLMinutes) * time.Minute,
		},
		done: make(chan struct{}),
	}
	worker.SetOwnerCPInstanceID(claimed.OwnerCPInstanceID)
	worker.SetOwnerEpoch(claimed.OwnerEpoch)
	return worker, nil
}

func (p *K8sWorkerPool) connectWorker(ctx context.Context, podName, podIP, bearerToken string) (*flightsql.Client, error) {
	return p.connectWorkerWithHealthCheck(ctx, podName, podIP, bearerToken, server.WorkerHealthCheckPayload{})
}

// connectWorkerDirect establishes a gRPC connection without running a health
// check. Used for hot-idle adoption where the worker's epoch/CP metadata
// doesn't match the new owner yet (ActivateTenant will update it).
func (p *K8sWorkerPool) connectWorkerDirect(ctx context.Context, podName, podIP, bearerToken string) (*flightsql.Client, error) {
	if p.connectWorkerFunc != nil {
		return p.connectWorkerFunc(ctx, podName, podIP, bearerToken)
	}
	if podIP == "" {
		return nil, fmt.Errorf("worker pod %s has no IP", podName)
	}
	addr := fmt.Sprintf("%s:%d", podIP, p.workerPort)
	_, serverCertPEM, err := p.readWorkerRPCSecurity(ctx, podName)
	if err != nil {
		return nil, fmt.Errorf("read worker RPC security: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(serverCertPEM) {
		return nil, fmt.Errorf("parse worker RPC server certificate")
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: workerRPCDNSName,
	}
	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(flightclient.MaxGRPCMessageSize),
		grpc.MaxCallSendMsgSize(flightclient.MaxGRPCMessageSize),
	))
	dialOpts = append(dialOpts, server.OTELGRPCClientHandler())
	if bearerToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&workerTLSBearerCreds{token: bearerToken}))
	}
	client, err := flightsql.NewClient(addr, nil, nil, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("connect to claimed worker %s: %w", podName, err)
	}
	return client, nil
}

func (p *K8sWorkerPool) connectWorkerWithHealthCheck(ctx context.Context, podName, podIP, bearerToken string, hcPayload server.WorkerHealthCheckPayload) (*flightsql.Client, error) {
	if p.connectWorkerFunc != nil {
		return p.connectWorkerFunc(ctx, podName, podIP, bearerToken)
	}
	if podIP == "" {
		return nil, fmt.Errorf("worker pod %s has no IP", podName)
	}
	addr := fmt.Sprintf("%s:%d", podIP, p.workerPort)
	_, serverCertPEM, err := p.readWorkerRPCSecurity(ctx, podName)
	if err != nil {
		return nil, fmt.Errorf("read worker RPC security: %w", err)
	}
	client, err := waitForWorkerTCPWithMetadata(addr, bearerToken, serverCertPEM, 30*time.Second, hcPayload)
	if err != nil {
		return nil, fmt.Errorf("connect to claimed worker %s: %w", podName, err)
	}
	return client, nil
}

func (p *K8sWorkerPool) checkReservedWorkerLiveness(ctx context.Context, worker *ManagedWorker) error {
	check := p.healthCheckFunc
	if check == nil {
		check = func(ctx context.Context, worker *ManagedWorker) error {
			if worker == nil || worker.client == nil {
				return fmt.Errorf("worker client is not available")
			}
			hctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			result, err := doHealthCheckWithMetadata(hctx, worker.client, p.healthCheckPayloadForWorker(worker))
			if err != nil {
				return err
			}
			return validateReservedWorkerHealth(result)
		}
	}
	return check(ctx, worker)
}

func validateReservedWorkerHealth(result *healthCheckResult) error {
	if result == nil {
		return fmt.Errorf("worker health check returned no result")
	}
	if !result.Healthy {
		return fmt.Errorf("worker health check reported unhealthy")
	}
	if result.Draining {
		return fmt.Errorf("worker is draining")
	}
	return nil
}

// SpawnMinWorkers is a no-op for the K8s pool: workers are created on demand.
// pre-spawn. Workers are created on demand per request (ReserveSharedWorker)
// and reused while hot-idle until their TTL. Present only to satisfy the
// WorkerPool interface (the process backend uses it for --process-min-workers).
func (p *K8sWorkerPool) SpawnMinWorkers(count int) error {
	return nil
}

// --- Shared scheduling helpers (same logic as FlightWorkerPool) ---

// stampNodeFirstSeenLocked records now as the first-seen time for nodeName
// if we haven't seen it before. Caller must hold p.mu.
func (p *K8sWorkerPool) stampNodeFirstSeenLocked(nodeName string) {
	if nodeName == "" || p.nodeFirstSeen == nil {
		return
	}
	if _, ok := p.nodeFirstSeen[nodeName]; !ok {
		p.nodeFirstSeen[nodeName] = time.Now()
	}
}

// nodeSeenAtLocked returns the first-seen time for nodeName. Missing or
// unknown entries return `fallback` (typically time.Now()) so callers can
// treat them as "new" for ranking — newer sorts later, so unknown nodes
// get reaped first and claimed last, which is the safe direction.
// Caller must hold p.mu.
func (p *K8sWorkerPool) nodeSeenAtLocked(nodeName string, fallback time.Time) time.Time {
	if nodeName == "" || p.nodeFirstSeen == nil {
		return fallback
	}
	if t, ok := p.nodeFirstSeen[nodeName]; ok {
		return t
	}
	return fallback
}

// pruneNodeFirstSeenLocked drops nodeName from the first-seen map when no
// remaining worker references it, to keep the map bounded as nodes turn over.
// Caller must hold p.mu.
func (p *K8sWorkerPool) pruneNodeFirstSeenLocked(nodeName string) {
	if nodeName == "" || p.nodeFirstSeen == nil {
		return
	}
	for _, w := range p.workers {
		if w.nodeName == nodeName {
			return
		}
	}
	delete(p.nodeFirstSeen, nodeName)
}

func (p *K8sWorkerPool) findIdleWorkerLocked() *ManagedWorker {
	// Prefer the idle worker on the oldest-seen node so sessions land on
	// cache-warm NVMe rather than a cold new node. Workers with no known
	// nodeName sort as "new" (claimed last) to avoid stalling the claim
	// path on stale bookkeeping.
	now := time.Now()
	var best *ManagedWorker
	var bestSeenAt time.Time
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if !p.isIdleWorkerLocked(w) {
			continue
		}
		seen := p.nodeSeenAtLocked(w.nodeName, now)
		if best == nil || seen.Before(bestSeenAt) {
			best = w
			bestSeenAt = seen
		}
	}
	return best
}

func (p *K8sWorkerPool) leastLoadedWorkerLocked() *ManagedWorker {
	var best *ManagedWorker
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if !p.isGenericSessionSchedulableWorkerLocked(w) {
			continue
		}
		if best == nil || w.activeSessions < best.activeSessions {
			best = w
		}
	}
	return best
}

func (p *K8sWorkerPool) liveWorkerCountLocked() int {
	count := p.spawning
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
			count++
		}
	}
	return count
}

func (p *K8sWorkerPool) isGenericSessionSchedulableWorkerLocked(w *ManagedWorker) bool {
	return w.SharedState().NormalizedLifecycle() == WorkerLifecycleIdle
}

func (p *K8sWorkerPool) isIdleWorkerLocked(w *ManagedWorker) bool {
	return w.activeSessions == 0 && p.isGenericSessionSchedulableWorkerLocked(w)
}
