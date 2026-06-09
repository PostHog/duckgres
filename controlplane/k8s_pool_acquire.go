//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	stderrors "errors"
	"fmt"
	"log/slog"
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
			idle.activeSessions++
			if idle.activeSessions > idle.peakSessions {
				idle.peakSessions = idle.activeSessions
			}
			p.mu.Unlock()
			slog.Debug("Reusing idle worker.", "worker", idle.ID, "active_sessions", idle.activeSessions)
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
				w.activeSessions++
				if w.activeSessions > w.peakSessions {
					w.peakSessions = w.activeSessions
				}
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

			slog.Info("No live workers, blocking on spawn.", "worker", id)
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
			w.activeSessions++
			if w.activeSessions > w.peakSessions {
				w.peakSessions = w.activeSessions
			}
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
	_ = p.persistWorkerRecord(activatingRecord)

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
				Iceberg:  payload.Iceberg,
			})
		}
	}

	if err := activate(ctx, worker, payload); err != nil {
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
	if setErr := worker.SetSharedState(nextState); setErr != nil {
		p.mu.Unlock()
		return setErr
	}
	hotRecord := p.workerRecordFor(worker.ID, worker, worker.OwnerEpoch(), configstore.WorkerStateHot, "", nil)
	p.mu.Unlock()
	_ = p.persistWorkerRecord(hotRecord)
	return nil
}

// ReserveSharedWorker reserves a neutral warm worker for later tenant activation.
func (p *K8sWorkerPool) ReserveSharedWorker(ctx context.Context, assignment *WorkerAssignment) (*ManagedWorker, error) {
	if err := validateWorkerAssignment(assignment); err != nil {
		return nil, err
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if p.runtimeStore != nil {
			// Try reclaiming a hot-idle worker for the same org first (fast path:
			// DuckLake is already attached, only needs epoch bump).
			if assignment.OrgID != "" {
				hotProfileCPU, hotProfileMem := assignment.Profile.Parts()
				hotClaimed, hotMissReason, err := p.runtimeStore.ClaimHotIdleWorker(p.cpInstanceID, assignment.OrgID, hotProfileCPU, hotProfileMem, assignment.MaxWorkers)
				if err != nil {
					return nil, err
				}
				if hotClaimed == nil && hotMissReason != configstore.WorkerClaimMissReasonNone && hotMissReason != configstore.WorkerClaimMissReasonNoIdle {
					return nil, NewWorkerCapacityExhaustedErrorForReason(hotMissReason, DefaultWorkerSpawnRetryAfter)
				}
				if hotClaimed != nil {
					// ClaimHotIdleWorker already filters by profile, so a reclaimed
					// hot-idle worker always matches the requested shape. It is NOT
					// filtered by image, so still retire on a version mismatch (e.g.
					// after an image bump) rather than serve a stale-version worker.
					if assignment.Image != "" && hotClaimed.Image != assignment.Image {
						slog.Info("Hot-idle worker image mismatch, retiring mismatched worker.", "worker_id", hotClaimed.WorkerID, "expected", assignment.Image, "got", hotClaimed.Image)
						p.retireClaimedWorker(hotClaimed, RetireReasonMismatchedVersion, LifecycleOriginReserveImageMismatch)
						// Fall through to neutral idle claim or capacity backpressure.
					} else {
						worker, reserveErr := p.reserveClaimedWorker(ctx, hotClaimed, assignment)
						if reserveErr == nil {
							worker.hotIdleReclaimed = true
							return worker, nil
						}
						if stderrors.Is(reserveErr, errStaleRuntimeWorkerClaim) {
							slog.Warn("Hot-idle worker claim was stale, retrying.", "worker_id", hotClaimed.WorkerID, "error", reserveErr)
							continue
						}
						slog.Warn("Hot-idle worker could not be reserved, retiring.", "worker_id", hotClaimed.WorkerID, "error", reserveErr)
						p.retireClaimedWorker(hotClaimed, RetireReasonCrash, LifecycleOriginReserveFailure)
						// Fall through to neutral idle claim.
					}
				}
			}

			// No reusable hot-idle worker for this org — spawn one on demand,
			// sized from the request profile (or the pool-global default for a
			// default request). spawnReservedWorker enforces the per-org + global
			// caps via CreateSpawningWorkerSlot and returns the cap error at the
			// ceiling.
			worker, spawnErr := p.spawnReservedWorker(ctx, assignment)
			if spawnErr != nil {
				return nil, spawnErr
			}
			return worker, nil
		}

		// Runtime-store-less mode (unit tests / standalone k8s): no durable pool,
		// just spawn a worker on demand.
		p.mu.Lock()
		if p.shuttingDown {
			p.mu.Unlock()
			return nil, fmt.Errorf("pool is shutting down")
		}
		p.cleanDeadWorkersLocked()
		p.mu.Unlock()

		worker, spawnErr := p.spawnReservedWorker(ctx, assignment)
		if spawnErr != nil {
			return nil, spawnErr
		}
		return worker, nil
	}
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
		slog.Warn("Claimed worker failed liveness recheck.", "worker", worker.ID, "worker_pod", worker.PodName(), "error", err)
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
	// (accepts > current). For fresh neutral workers (epoch 0), use the normal
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

// SpawnMinWorkers is a no-op for the K8s pool: there is no warm pool to
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
		if !p.isWarmIdleWorkerLocked(w) {
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

func (p *K8sWorkerPool) isWarmIdleWorkerLocked(w *ManagedWorker) bool {
	return w.activeSessions == 0 && p.isGenericSessionSchedulableWorkerLocked(w)
}
