//go:build kubernetes

package controlplane

import (
	"context"
	stderrors "errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/controlplane/configstore"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RetireWorker removes a worker from the pool and deletes its pod.
func (p *K8sWorkerPool) RetireWorker(id int) {
	p.retireWorkerWithReason(id, RetireReasonNormal, LifecycleOriginPublicAPI)
}

// retireWorkerWithReason retires a worker and deletes its pod.
// Returns true if the worker was found and retired. origin labels the
// originating subsystem on the retire_local metric; it is required and
// has no default fallback so every call site is forced to declare its
// context.
func (p *K8sWorkerPool) retireWorkerWithReason(id int, reason string, origin LifecycleOrigin) bool {
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return false
	}
	if !p.markWorkerRetiredLocked(w, reason, origin) {
		p.mu.Unlock()
		return false
	}
	delete(p.workers, id)
	workerCount := len(p.workers)
	p.mu.Unlock()
	observeControlPlaneWorkers(workerCount)

	go p.retireWorkerPod(id, w)
	return true
}

// RetireWorkerIfNoSessions retires a worker only if it has no active sessions.
func (p *K8sWorkerPool) RetireWorkerIfNoSessions(id int) bool {
	return p.retireWorkerIfNoSessionsWithReason(id, RetireReasonNormal, LifecycleOriginPublicAPI)
}

func (p *K8sWorkerPool) retireWorkerIfNoSessionsWithReason(id int, reason string, origin LifecycleOrigin) bool {
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return false
	}
	if w.activeSessions > 0 {
		w.activeSessions--
	}
	if w.activeSessions == 0 {
		if !p.markWorkerRetiredLocked(w, reason, origin) {
			p.mu.Unlock()
			return false
		}
		delete(p.workers, id)
		workerCount := len(p.workers)
		p.mu.Unlock()
		observeControlPlaneWorkers(workerCount)
		go p.retireWorkerPod(id, w)
		return true
	}
	p.mu.Unlock()
	return false
}

// RetireIfDrainingAndEmpty retires a worker that is draining and has no active
// sessions. Does NOT decrement activeSessions (caller must have already done so).
// Used by ReleaseWorker when TransitionToHotIdleIfNoSessions skips a non-hot worker.
//
// The transition is draining → retired, which has to go through the
// lifecycle service's dedicated RetireDrainingWorker CAS — NOT the
// generic UpsertWorkerRecord path that markWorkerRetiredLocked uses,
// because UpsertWorkerRecord's ON CONFLICT WHERE clause rejects
// updates against existing rows in {draining, retired, lost}. Routing
// this transition through UpsertWorkerRecord would always fence-miss
// and (post-P1-gate) leave the drained empty worker stuck in both the
// in-memory map and on K8s.
//
// On CAS miss / DB error the durable row stays in draining for the
// orphan sweep on a peer leader to reconcile; we don't touch the
// in-memory worker or the K8s pod because we haven't proven we still
// own the lease — the same peer-pod-delete safety that motivated the
// markWorkerRetiredLocked gate.
func (p *K8sWorkerPool) RetireIfDrainingAndEmpty(id int, origin LifecycleOrigin) {
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok || w.activeSessions > 0 {
		p.mu.Unlock()
		return
	}
	if w.SharedState().NormalizedLifecycle() != WorkerLifecycleDraining {
		p.mu.Unlock()
		return
	}
	lifecycle := p.ensureLifecycle()
	if lifecycle == nil {
		// No durable store wired (process backend / minimal test pool):
		// fall back to the in-memory + upsert path. persistWorkerRecord
		// is a no-op when runtimeStore is nil, so there's no fence-miss
		// to worry about.
		if !p.markWorkerRetiredLocked(w, RetireReasonNormal, origin) {
			p.mu.Unlock()
			return
		}
	} else {
		lease := configstore.NewWorkerLease(w.ID, p.cpInstanceID, w.OwnerEpoch(), w.image)
		outcome, err := lifecycle.RetireDrained(lease, RetireReasonNormal, origin)
		if err != nil {
			p.logw(id).Warn("RetireIfDrainingAndEmpty: CAS to retired failed; orphan sweep will reconcile.",
				"error", err)
			p.mu.Unlock()
			return
		}
		if !outcome.Transitioned {
			p.logw(id).Debug("RetireIfDrainingAndEmpty: retire-drained CAS missed; orphan sweep will reconcile.")
			p.mu.Unlock()
			return
		}
		p.markWorkerRetiredInMemoryLocked(w)
	}
	delete(p.workers, id)
	workerCount := len(p.workers)
	p.mu.Unlock()
	observeControlPlaneWorkers(workerCount)
	go p.retireWorkerPod(id, w)
}

// TransitionToHotIdleIfNoSessions decrements the worker's active session count
// and transitions a hot worker to hot_idle when its last session ends. The worker
// keeps its org assignment and DuckLake attachment so it can be quickly reclaimed
// for the same org. Returns true if the worker transitioned to hot_idle.
// The session decrement always happens regardless of the return value.
func (p *K8sWorkerPool) TransitionToHotIdleIfNoSessions(id int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	w, ok := p.workers[id]
	if !ok {
		return false
	}
	if w.activeSessions > 0 {
		w.activeSessions--
	}
	if w.activeSessions != 0 {
		return false
	}
	if w.SharedState().NormalizedLifecycle() != WorkerLifecycleHot {
		return false
	}
	nextState, err := w.SharedState().Transition(WorkerLifecycleHotIdle, nil)
	if err != nil {
		return false
	}
	// Persist the hot_idle row BEFORE committing the in-memory transition
	// (mirrors markWorkerRetiredLocked). workerRecordFor takes the target state
	// explicitly and reads only stable fields (profile, owner, assignment), so
	// it is safe to build before the in-memory commit. If the durable write
	// fails we must NOT advance to hot_idle: a durable-hot / in-memory-hot_idle
	// split hides the worker from BOTH reuse (ClaimHotIdleWorker filters
	// state=hot_idle) AND the TTL reaper (same filter) until this CP restarts,
	// stranding an idle pod forever. Leaving it Hot keeps it reusable by its org
	// (findIdleAssignedWorkerLocked reuses Hot, activeSessions==0 workers) and a
	// later release/retire retries the park; the session decrement above already
	// happened, so the worker is honestly idle, just not yet durably parked.
	hotIdleRecord := p.workerRecordFor(id, w, w.OwnerEpoch(), configstore.WorkerStateHotIdle, "", nil)
	if err := p.persistWorkerRecord(hotIdleRecord); err != nil {
		observeHotIdlePersistFailure(w.image)
		p.logw(id).Warn("Failed to persist hot_idle transition; leaving worker hot for retry.", "error", err)
		return false
	}
	if err := w.SetSharedState(nextState); err != nil {
		return false
	}
	w.lastUsed = time.Now()
	return true
}

func (p *K8sWorkerPool) retireClaimedWorker(claimed *configstore.WorkerRecord, reason string, origin LifecycleOrigin) (configstore.TransitionOutcome, error) {
	if claimed == nil {
		return configstore.TransitionOutcome{}, nil
	}
	lifecycle := p.ensureLifecycle()
	if lifecycle == nil {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, stderrors.New("worker lifecycle service not configured")
	}
	terminalState := configstore.WorkerStateRetired
	if reason == RetireReasonCrash {
		terminalState = configstore.WorkerStateLost
	}
	// The claimed record is fresh enough to act as a snapshot — it came
	// back from a Claim*/TakeOver*/Create*Slot call and any concurrent
	// change will simply CAS-miss inside MarkWorkerTerminalIfCurrent.
	// Pod cleanup is scheduled by the lifecycle on a successful CAS via
	// the WorkerPhysicalCleanup (this pool's DeleteWorkerArtifacts).
	snap := configstore.NewWorkerSnapshot(*claimed)
	outcome, err := lifecycle.RetireFromSnapshot(snap, terminalState, reason, origin)
	if err != nil {
		p.logw(claimed.WorkerID).Warn("Failed to retire claimed worker.",
			"reason", reason, "origin", origin, "error", err)
	}
	return outcome, err
}

// DeleteWorkerArtifacts implements WorkerPhysicalCleanup. Schedules pod
// + RPC secret + local-pool cleanup for the named worker, called by
// WorkerLifecycle after a durable CAS to terminal has landed.
func (p *K8sWorkerPool) DeleteWorkerArtifacts(workerID int, podName, reason string) {
	p.deleteRetiredRuntimeWorker(&configstore.WorkerRecord{WorkerID: workerID, PodName: podName}, reason)
}

// deleteRetiredRuntimeWorker performs the post-CAS cleanup for a worker whose
// runtime-store row is already terminal. It intentionally avoids another DB
// transition so janitor paths can retire once, then reliably remove local state
// and delete the pod.
func (p *K8sWorkerPool) deleteRetiredRuntimeWorker(record *configstore.WorkerRecord, reason string) {
	if record == nil {
		return
	}

	worker := &ManagedWorker{
		ID:      record.WorkerID,
		podName: record.PodName,
		done:    make(chan struct{}),
	}
	worker.SetOwnerCPInstanceID(record.OwnerCPInstanceID)
	worker.SetOwnerEpoch(record.OwnerEpoch)

	removedLocal := false
	workerCount := 0
	p.mu.Lock()
	if local, ok := p.workers[record.WorkerID]; ok {
		worker = local
		p.markWorkerRetiredInMemoryLocked(worker)
		delete(p.workers, record.WorkerID)
		workerCount = len(p.workers)
		removedLocal = true
	}
	p.mu.Unlock()
	if removedLocal {
		observeControlPlaneWorkers(workerCount)
	}

	go p.retireWorkerPod(record.WorkerID, worker)
}

// HealthCheckLoop periodically checks worker health.
func (p *K8sWorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var mu sync.Mutex
	failures := make(map[workerLeaseSnapshot]int)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.RLock()
			if p.shuttingDown {
				p.mu.RUnlock()
				return
			}
			type healthCheckTarget struct {
				worker *ManagedWorker
				lease  workerLeaseSnapshot
			}
			targets := make([]healthCheckTarget, 0, len(p.workers))
			for _, w := range p.workers {
				targets = append(targets, healthCheckTarget{
					worker: w,
					lease:  p.workerLeaseSnapshot(w),
				})
			}
			p.mu.RUnlock()

			var wg sync.WaitGroup
			for _, target := range targets {
				wg.Add(1)
				go func(w *ManagedWorker, lease workerLeaseSnapshot) {
					defer wg.Done()

					select {
					case <-ctx.Done():
						return
					case <-w.done:
						// Pod terminated (detected by informer) — distinct
						// from the periodic-probe path below, which uses
						// LifecycleOriginHealthCheckCrash.
						mu.Lock()
						delete(failures, lease)
						mu.Unlock()

						removedWorker, workerCount, err := p.removeWorkerAfterDrainedLease(lease, LifecycleOriginWorkerDrain)
						if err != nil {
							p.logw(lease.workerID).Error("K8s worker terminated after draining but retire CAS failed; leaving cleanup to retry.", "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "error", err)
							return
						}
						if removedWorker != nil {
							observeControlPlaneWorkers(workerCount)
							p.logw(lease.workerID).Info("K8s worker terminated after draining.")
							if removedWorker.client != nil {
								_ = removedWorker.client.Close()
							}
							return
						}

						lostDisposition, err := p.markWorkerLostForHealthLease(lease, LifecycleOriginInformerCrash)
						if err != nil {
							p.logw(lease.workerID).Error("K8s worker terminated but lease validation failed; leaving cleanup to retry.", "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "error", err)
							return
						}
						if lostDisposition == workerLostLeaseRetry {
							p.logw(lease.workerID).Warn("K8s worker terminated while runtime lease is newer for this CP; leaving cleanup to retry.", "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch)
							return
						}
						if lostDisposition == workerLostLeaseStale {
							p.mu.Lock()
							removedWorker, workerCount := p.dropLocalWorkerIfSameLeaseLocked(lease)
							p.mu.Unlock()
							if removedWorker == nil {
								return
							}
							observeControlPlaneWorkers(workerCount)
							p.logw(lease.workerID).Warn("K8s worker terminated under stale lease; dropping local worker without pod delete.", "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch)
							if removedWorker.client != nil {
								_ = removedWorker.client.Close()
							}
							return
						}

						p.mu.Lock()
						removedWorker, workerCount = p.removeWorkerAfterLostLeaseLocked(lease)
						p.mu.Unlock()
						if removedWorker == nil {
							return
						}
						observeControlPlaneWorkers(workerCount)
						p.logw(lease.workerID).Warn("K8s worker crashed.")
						if onCrash != nil {
							onCrash(lease.workerID)
						}
						if removedWorker.client != nil {
							_ = removedWorker.client.Close()
						}
						// Delete the failed pod from K8s
						podName := p.workerPodName(removedWorker)
						delCtx, delCancel := context.WithTimeout(context.Background(), 10*time.Second)
						_ = p.clientset.CoreV1().Pods(p.namespace).Delete(delCtx, podName, metav1.DeleteOptions{
							GracePeriodSeconds: int64Ptr(0),
						})
						delCancel()
					default:
						// Worker alive, do health check
						var healthErr error
						var hcResult *healthCheckResult
						func() {
							defer recoverWorkerPanic(&healthErr)
							hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
							hcResult, healthErr = doHealthCheckWithMetadata(hctx, w.client, p.healthCheckPayloadForLease(lease))
							cancel()
						}()
						// Skip both metric emission AND failure-counter
						// increment when the parent loop ctx was canceled
						// (CP shutting down). Without this guard:
						// (a) every graceful rollout would spike
						//     duckgres_worker_health_checks_total{result=fail}
						//     indistinguishably from real worker crashes;
						// (b) a worker one failure short of
						//     maxConsecutiveHealthFailures could be tipped
						//     over by a shutdown-induced Canceled and
						//     erroneously marked Lost.
						// Per-probe deadline (DeadlineExceeded) is a real
						// failure and still counted via the else branch.
						if stderrors.Is(healthErr, context.Canceled) {
							return
						}
						if healthErr != nil {
							observeHealthCheck(HealthCheckResultFail, lease.image)
						} else {
							observeHealthCheck(HealthCheckResultPass, lease.image)
						}

						if healthErr != nil {
							if p.workerLeaseLocallyDraining(lease) {
								if p.workerLeaseDurablyDrainingOrRepair(lease) {
									p.logw(lease.workerID).Warn("K8s worker health check failed while worker is draining; waiting for pod exit.", "error", healthErr)
									return
								}
								p.logw(lease.workerID).Warn("K8s worker health check failed while worker is locally draining but durable state is not draining; treating as health failure.", "error", healthErr)
							}
							mu.Lock()
							failures[lease]++
							count := failures[lease]
							mu.Unlock()

							p.logw(lease.workerID).Warn("K8s worker health check failed.", "error", healthErr, "consecutive_failures", count)

							if count >= maxConsecutiveHealthFailures {
								lostDisposition, err := p.markWorkerLostForHealthLease(lease, LifecycleOriginHealthCheckCrash)
								if err != nil {
									p.logw(lease.workerID).Error("K8s worker unresponsive but lease validation failed; leaving cleanup to retry.", "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "consecutive_failures", count, "error", err)
									return
								}

								if lostDisposition == workerLostLeaseRetry {
									p.logw(lease.workerID).Warn("K8s worker unresponsive while runtime lease is newer for this CP; leaving cleanup to retry.", "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "consecutive_failures", count)
									return
								}

								mu.Lock()
								delete(failures, lease)
								mu.Unlock()

								if lostDisposition == workerLostLeaseStale {
									p.mu.Lock()
									removedWorker, workerCount := p.dropLocalWorkerIfSameLeaseLocked(lease)
									p.mu.Unlock()
									if removedWorker == nil {
										return
									}
									observeControlPlaneWorkers(workerCount)
									p.logw(lease.workerID).Warn("K8s worker unresponsive under stale lease; dropping local worker without crash notification or pod delete.", "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "consecutive_failures", count)
									if removedWorker.client != nil {
										_ = removedWorker.client.Close()
									}
									return
								}

								p.mu.Lock()
								removedWorker, workerCount := p.removeWorkerAfterLostLeaseLocked(lease)
								p.mu.Unlock()
								if removedWorker == nil {
									return
								}
								observeControlPlaneWorkers(workerCount)

								if onCrash != nil {
									onCrash(lease.workerID)
								}

								// Snapshot pod/container state before the delete below removes
								// the easiest source of OOMKilled/Evicted/exit-code evidence.
								podName := p.workerPodName(removedWorker)
								deleteAttrs := []any{"consecutive_failures", count}
								if podName != "" && p.clientset != nil {
									statusCtx, statusCancel := context.WithTimeout(ctx, 2*time.Second)
									pod, err := p.clientset.CoreV1().Pods(p.namespace).Get(statusCtx, podName, metav1.GetOptions{})
									statusCancel()
									if err != nil {
										deleteAttrs = append(deleteAttrs, "pod_status_error", err)
									} else {
										deleteAttrs = append(deleteAttrs, workerPodStatusLogAttrs(pod)...)
									}
								}
								logger := slog.With(workerLogAttrs(removedWorker)...)
								logger.Error("K8s worker unresponsive, deleting pod.", deleteAttrs...)
								deleteResultAttrs := append(append([]any{}, deleteAttrs...), "grace_period_seconds", 10)
								if podName == "" || p.clientset == nil {
									logger.Warn("K8s worker pod delete skipped.", append(deleteResultAttrs, "reason", "missing_pod_or_client")...)
								} else {
									deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 10*time.Second)
									err := p.clientset.CoreV1().Pods(p.namespace).Delete(deleteCtx, podName, metav1.DeleteOptions{
										GracePeriodSeconds: int64Ptr(10),
									})
									deleteCancel()
									switch {
									case err == nil:
										logger.Info("K8s worker pod delete requested.", deleteResultAttrs...)
									case errors.IsNotFound(err):
										logger.Info("K8s worker pod already gone before delete request completed.", append(deleteResultAttrs, "error", err)...)
									default:
										logger.Warn("K8s worker pod delete request failed.", append(deleteResultAttrs, "error", err)...)
									}
								}
								if removedWorker.client != nil {
									_ = removedWorker.client.Close()
								}
							}
						} else {
							mu.Lock()
							delete(failures, lease)
							mu.Unlock()

							if hcResult != nil && hcResult.Draining {
								p.markWorkerDrainingFromHealth(lease)
							}

							// Forward progress data to the control plane.
							if onProgress != nil && hcResult != nil {
								if sp := hcResult.toSessionProgress(); len(sp) > 0 {
									onProgress(lease.workerID, sp)
								}
							}
						}
					}
				}(target.worker, target.lease)
			}
			wg.Wait()
		}
	}
}

// ShutdownAll stops all workers by deleting their pods. Per worker it runs a
// 3-step CAS chain against the runtime store and K8s API:
//
//  1. MarkWorkerDraining — atomic SQL CAS from a non-terminal state to
//     draining. Fences the worker against claims by other CPs: their claim
//     queries match state=idle/hot_idle, which no longer apply. If the CAS
//     misses (row already terminal or owned by another CP) the worker is
//     skipped entirely.
//  2. K8s pod delete. Only reached after the CAS succeeds. NotFound is
//     treated as success (the pod is gone by some other path).
//  3. RetireDrainingWorker — atomic SQL CAS draining → retired. Only reached
//     on successful pod delete. On delete failure the row stays in
//     draining, which lets ListOrphanedWorkers pick it up once the CP's
//     heartbeat expires, and lets cleanupOrphanedWorkerPods delete the pod
//     by label on the next janitor tick.
//
// This ordering closes the old race where the DB row was flipped to retired
// before the pod delete: if the delete failed, the pod survived forever
// because terminal-state rows are excluded from ListOrphanedWorkers.
func (p *K8sWorkerPool) ShutdownAll() {
	p.mu.Lock()
	if p.shuttingDown {
		p.mu.Unlock()
		return
	}
	p.shuttingDown = true
	workers := make([]*ManagedWorker, 0, len(p.workers))
	for _, w := range p.workers {
		workers = append(workers, w)
	}
	p.mu.Unlock()

	close(p.shutdownCh)
	close(p.stopInform)

	ctx := context.Background()
	preserved := make(map[int]*ManagedWorker)
	for _, w := range workers {
		podName := p.workerPodName(w)

		// A worker that's serving sessions must not be torn down during
		// CP shutdown — pod-deletion would kill in-flight customer queries
		// at exactly the moment ShutdownAll runs (the failure mode hit by
		// the production worker-40761 incident on a 15-minute drain wall).
		// Leave the worker in `hot` state, owned by this dying CP. Two
		// downstream guarantees keep this safe:
		//   (1) Flight clients can reconnect by session token; a peer CP
		//       claims via TakeOverWorker and the query resumes.
		//   (2) ListOrphanedWorkers' JOIN against flight_session_records
		//       prevents peer CPs' janitors from retiring the worker
		//       while a session is still active or reconnecting; once the
		//       session record is expired the worker is retired normally.
		// pgwire customers connected to this CP have already lost their
		// connection (the CP socket is going away) — protecting the
		// worker doesn't help them, but it doesn't hurt either.
		if w.activeSessions > 0 {
			p.logw(w.ID).Info("ShutdownAll: worker has active sessions; leaving pod alive for Flight reconnect.", "worker_pod", podName, "active_sessions", w.activeSessions)
			preserved[w.ID] = w
			continue
		}

		p.logw(w.ID).Info("Shutting down K8s worker.", "worker_pod", podName)

		// Step 1: CAS to draining. Skip the worker on CAS miss or error —
		// there's no safe way to proceed if we don't own the row.
		// Mint the lease right before the CAS so the in-memory epoch
		// reflects any concurrent cred-refresh bump that landed between
		// the workers-list snapshot above and now. Step 3 below re-mints
		// for the same reason — see the comment there.
		lifecycle := p.ensureLifecycle()
		if lifecycle == nil {
			continue
		}
		// Wrap the per-worker drain in a closure. Returns true when
		// the in-memory worker should be flipped to Retired — i.e.
		// whenever this CP has taken any irreversible local action
		// against the worker (closed its gRPC client). That covers
		// both the fully-successful drain and the pod-delete-error
		// branch (where the pod is still alive but we closed the
		// client because this CP is shutting down). Returns false
		// only when Drain itself missed and no local mutation
		// happened.
		shouldMarkRetired := func() bool {
			drainStart := time.Now()

			lease := configstore.NewWorkerLease(w.ID, p.cpInstanceID, w.OwnerEpoch(), w.image)
			outcome, err := lifecycle.Drain(lease, LifecycleOriginShutdownAll)
			if err != nil {
				p.logw(w.ID).Warn("ShutdownAll: CAS to draining failed; orphan sweep will reconcile.",
					"error", err)
				return false
			}
			if !outcome.Transitioned {
				p.logw(w.ID).Debug("ShutdownAll: worker not owned by us or already terminal; skipping.")
				return false
			}

			// Step 2: delete pod. On API error other than NotFound we
			// leave the durable row in draining for the orphan sweep
			// AND close the client — this CP is going away regardless
			// and the gRPC client serves no further purpose. Returning
			// true keeps the in-memory state consistent with the
			// closed client (no goroutine holding `w` should see
			// Idle/Hot while reaching for a dead connection).
			gracePeriod := int64(10)
			if err := p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
				GracePeriodSeconds: &gracePeriod,
			}); err != nil && !errors.IsNotFound(err) {
				p.logw(w.ID).Warn("ShutdownAll: pod delete failed; worker left in draining for orphan sweep/reconciler.", "worker_pod", podName, "error", err)
				if w.client != nil {
					_ = w.client.Close()
				}
				return true
			}
			_ = p.deleteWorkerRPCSecret(ctx, podName)
			if w.client != nil {
				_ = w.client.Close()
			}
			// Record drain duration at the pod-delete-success
			// milestone — every sample reflects a real
			// gone-from-the-cluster event, so p99 alerts track the
			// operationally meaningful tail. The terminal CAS below
			// is best-effort cleanup against an already-dead worker.
			observeDrainTotalDuration(time.Since(drainStart))

			// Step 3: best-effort terminal CAS. Re-read w.OwnerEpoch()
			// so a cred-refresh bump that landed between Step 1 and
			// now uses the fresh epoch. CAS-miss (Transitioned=false,
			// err=nil) is logged at Debug for diagnostic
			// continuity — the durable row stays in draining for the
			// orphan sweep on a peer CP to reconcile.
			lateLease := configstore.NewWorkerLease(w.ID, p.cpInstanceID, w.OwnerEpoch(), w.image)
			retireOutcome, err := lifecycle.RetireDrained(lateLease, RetireReasonShutdown, LifecycleOriginShutdownAll)
			switch {
			case err != nil:
				p.logw(w.ID).Warn("ShutdownAll: CAS to retired failed; orphan sweep will reconcile durable row.",
					"error", err)
			case !retireOutcome.Transitioned:
				p.logw(w.ID).Debug("ShutdownAll: retire-drained CAS missed; orphan sweep will reconcile durable row.")
			}
			return true
		}()

		if !shouldMarkRetired {
			continue
		}

		// In-memory lifecycle bookkeeping. Intentionally skips
		// persistence (the CAS chain already persisted retired/draining)
		// and the retire_local metric (lifecycle.RetireDrained already
		// observed retire_drained for this transition).
		p.mu.Lock()
		p.markWorkerRetiredInMemoryLocked(w)
		p.mu.Unlock()
	}

	// Workers preserved due to active sessions stay in the in-memory map
	// so any remaining session bookkeeping inside this CP still finds them
	// during the residual shutdown window. The map is wiped on process
	// exit; preservation is purely about not yanking the rug here.
	p.mu.Lock()
	p.workers = preserved
	p.mu.Unlock()
	observeControlPlaneWorkers(len(preserved))
}

// retireWorkerPod closes the gRPC client and deletes the worker pod.
// Acquires the retire semaphore to limit concurrent K8s API calls.
func (p *K8sWorkerPool) retireWorkerPod(id int, w *ManagedWorker) {
	p.retireSem <- struct{}{}
	defer func() { <-p.retireSem }()

	podName := p.workerPodName(w)
	p.logw(id).Info("Retiring K8s worker.", "worker_pod", podName)
	if w.client != nil {
		_ = w.client.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: int64Ptr(10),
	}); err != nil {
		p.logw(id).Warn("Failed to delete worker pod.", "worker_pod", podName, "error", err)
	}
	if err := p.deleteWorkerRPCSecret(ctx, podName); err != nil {
		p.logw(id).Warn("Failed to delete worker RPC secret.", "worker_pod", podName, "error", err)
	}
}

// idleReaper periodically retires workers that have been idle too long and
// reaps stuck activating/reserved workers.
func (p *K8sWorkerPool) idleReaper() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.shutdownCh:
			return
		case <-ticker.C:
			if p.idleTimeout > 0 {
				p.reapIdleWorkers()
			}
			p.reapStuckActivatingWorkers()
		}
	}
}

func (p *K8sWorkerPool) reapIdleWorkers() {
	p.mu.Lock()
	var toRetire []struct {
		id int
		w  *ManagedWorker
	}
	now := time.Now()
	idleCount := 0
	for _, w := range p.workers {
		if p.isIdleWorkerLocked(w) {
			idleCount++
		}
	}

	// Build the list of reap-eligible workers and sort by the first time
	// this CP saw their node — newest node first. We prefer evicting workers
	// on nodes we only just started using so older nodes keep their cached
	// NVMe parquet cache for longer. Workers with no known nodeName sort as
	// "new" (they're reaped first) to avoid stalling on stale state.
	type candidate struct {
		id     int
		w      *ManagedWorker
		seenAt time.Time
	}
	var candidates []candidate
	for id, w := range p.workers {
		if p.isIdleWorkerLocked(w) && !w.lastUsed.IsZero() && now.Sub(w.lastUsed) > p.idleTimeout {
			candidates = append(candidates, candidate{id: id, w: w, seenAt: p.nodeSeenAtLocked(w.nodeName, now)})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		// Newest-seen nodes reaped first.
		return candidates[i].seenAt.After(candidates[j].seenAt)
	})

	for _, c := range candidates {
		if idleCount <= 0 {
			break
		}
		{
			id, w := c.id, c.w
			if !p.markWorkerRetiredLocked(w, RetireReasonIdleTimeout, LifecycleOriginIdleTimeout) {
				continue
			}
			toRetire = append(toRetire, struct {
				id int
				w  *ManagedWorker
			}{id, w})
			delete(p.workers, id)
			p.pruneNodeFirstSeenLocked(w.nodeName)
			idleCount--
		}
	}
	workerCount := len(p.workers)
	p.mu.Unlock()

	if len(toRetire) > 0 {
		slog.Info("Reaping idle K8s workers.", "count", len(toRetire))
		observeControlPlaneWorkers(workerCount)
		for _, entry := range toRetire {
			go p.retireWorkerPod(entry.id, entry.w)
		}
	}
}

// reapStuckActivatingWorkers retires workers that have been in reserved or
// activating state for longer than the activating timeout.
func (p *K8sWorkerPool) reapStuckActivatingWorkers() {
	timeout := p.activatingTimeout
	if timeout <= 0 {
		timeout = defaultActivatingTimeout
	}

	p.mu.Lock()
	var toRetire []struct {
		id int
		w  *ManagedWorker
	}
	now := time.Now()
	for id, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		lifecycle := w.SharedState().NormalizedLifecycle()
		if (lifecycle == WorkerLifecycleReserved || lifecycle == WorkerLifecycleActivating) &&
			!w.reservedAt.IsZero() && now.Sub(w.reservedAt) > timeout {
			if !p.markWorkerRetiredLocked(w, RetireReasonStuckActivating, LifecycleOriginPoolStuckActivating) {
				continue
			}
			toRetire = append(toRetire, struct {
				id int
				w  *ManagedWorker
			}{id, w})
			delete(p.workers, id)
		}
	}

	workerCount := len(p.workers)
	p.mu.Unlock()

	if len(toRetire) > 0 {
		slog.Warn("Reaping stuck activating workers.", "count", len(toRetire))
		observeControlPlaneWorkers(workerCount)
		for _, entry := range toRetire {
			go p.retireWorkerPod(entry.id, entry.w)
		}
	}
}

func (p *K8sWorkerPool) cleanDeadWorkersLocked() {
	removedAny := false
	for id, w := range p.workers {
		select {
		case <-w.done:
			var removedWorker *ManagedWorker
			deletePod := true
			if p.runtimeStore != nil {
				lease := p.workerLeaseSnapshot(w)
				if p.workerMatchesLease(w, lease) && w.SharedState().NormalizedLifecycle() == WorkerLifecycleDraining {
					lc := p.ensureLifecycle()
					if lc != nil {
						retired, err := p.retireLocalDrainingLease(lease, RetireReasonNormal, LifecycleOriginInformerCrash)
						if err != nil {
							p.logw(id).Warn("Clean dead worker: retire draining CAS failed; leaving cleanup to retry.", "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "error", err)
							continue
						}
						if !retired {
							continue
						}
					}
					p.markWorkerRetiredInMemoryLocked(w)
					delete(p.workers, id)
					removedWorker = w
					deletePod = false
				} else {
					// cleanDeadWorkersLocked sweeps for pods whose informer
					// fired w.done — cluster-driven termination (eviction,
					// OOM, manual delete), not our health-check decision.
					lostDisposition, err := p.markWorkerLostForHealthLease(lease, LifecycleOriginInformerCrash)
					if err != nil {
						p.logw(id).Warn("Clean dead worker: lease validation failed; leaving cleanup to retry.", "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "error", err)
						continue
					}
					switch lostDisposition {
					case workerLostLeaseStale:
						deletePod = false
						removedWorker, _ = p.dropLocalWorkerIfSameLeaseLocked(lease)
					case workerLostLeaseCurrent, workerLostLeaseAlreadyLost, workerLostLeaseRetry:
						continue
					}
				}
			} else {
				removedWorker, _ = p.removeWorkerLocked(id)
			}
			if removedWorker == nil {
				continue
			}
			removedAny = true
			if removedWorker.client != nil {
				go func(c *flightsql.Client) { _ = c.Close() }(removedWorker.client)
			}
			if !deletePod {
				continue
			}
			// Delete the failed pod from K8s to avoid accumulating terminated pods
			go func(worker *ManagedWorker) {
				podName := p.workerPodName(worker)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
					GracePeriodSeconds: int64Ptr(0),
				})
			}(removedWorker)
		default:
		}
	}
	if removedAny {
		observeControlPlaneWorkers(len(p.workers))
	}
}

func (p *K8sWorkerPool) removeWorkerLocked(id int) (*ManagedWorker, int) {
	w, ok := p.workers[id]
	if !ok {
		return nil, len(p.workers)
	}
	// Reached from cleanDeadWorkersLocked's no-runtime-store fallback
	// (informer fired w.done). Not the periodic health-check path —
	// that goes through markWorkerLostForHealthLease above and never
	// calls into here.
	if !p.markWorkerRetiredLocked(w, RetireReasonCrash, LifecycleOriginInformerCrash) {
		return nil, len(p.workers)
	}
	delete(p.workers, id)
	return w, len(p.workers)
}

type workerLeaseSnapshot struct {
	workerID          int
	ownerCPInstanceID string
	ownerEpoch        int64
	image             string
}

func (p *K8sWorkerPool) workerLeaseSnapshot(w *ManagedWorker) workerLeaseSnapshot {
	ownerCPInstanceID := w.OwnerCPInstanceID()
	if ownerCPInstanceID == "" {
		ownerCPInstanceID = p.cpInstanceID
	}
	return workerLeaseSnapshot{
		workerID:          w.ID,
		ownerCPInstanceID: ownerCPInstanceID,
		ownerEpoch:        w.OwnerEpoch(),
		image:             w.image,
	}
}

func (p *K8sWorkerPool) workerMatchesLease(w *ManagedWorker, lease workerLeaseSnapshot) bool {
	return w != nil && p.workerLeaseSnapshot(w) == lease
}

type workerLostLeaseDisposition int

const (
	workerLostLeaseCurrent workerLostLeaseDisposition = iota
	workerLostLeaseStale
	workerLostLeaseRetry
	workerLostLeaseAlreadyLost
)

func (p *K8sWorkerPool) markWorkerLostForHealthLease(lease workerLeaseSnapshot, origin LifecycleOrigin) (workerLostLeaseDisposition, error) {
	if p.runtimeStore == nil {
		return workerLostLeaseCurrent, nil
	}
	if lease.ownerCPInstanceID != p.cpInstanceID {
		return workerLostLeaseStale, nil
	}
	currentLease, err := p.markWorkerLostIfCurrentLease(lease, origin)
	if err != nil {
		return workerLostLeaseRetry, err
	}
	if currentLease {
		return workerLostLeaseCurrent, nil
	}

	record, err := p.runtimeStore.GetWorkerRecord(lease.workerID)
	if err != nil {
		return workerLostLeaseRetry, err
	}
	if record == nil || record.OwnerCPInstanceID != p.cpInstanceID {
		return workerLostLeaseStale, nil
	}
	if record.OwnerEpoch != lease.ownerEpoch {
		return workerLostLeaseRetry, nil
	}
	if record.State == configstore.WorkerStateLost && record.RetireReason == RetireReasonCrash {
		return workerLostLeaseAlreadyLost, nil
	}
	return workerLostLeaseStale, nil
}

func (p *K8sWorkerPool) markWorkerLostIfCurrentLease(lease workerLeaseSnapshot, origin LifecycleOrigin) (bool, error) {
	lc := p.ensureLifecycle()
	if lc == nil {
		// No durable store wired (process backend / minimal test pool).
		// The health-check caller treats true here as "CAS landed";
		// for the no-store case we let it proceed to its in-memory
		// cleanup path, which is the same behavior the old direct-
		// store call had when runtimeStore was nil.
		return true, nil
	}
	if lease.ownerCPInstanceID != p.cpInstanceID {
		return false, nil
	}
	outcome, err := lc.MarkLostFromLease(
		configstore.NewWorkerLease(lease.workerID, p.cpInstanceID, lease.ownerEpoch, lease.image),
		RetireReasonCrash,
		origin)
	if err != nil {
		return false, err
	}
	return outcome.Transitioned, nil
}

func (p *K8sWorkerPool) workerLeaseLocallyDraining(lease workerLeaseSnapshot) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	w, ok := p.workers[lease.workerID]
	return ok && p.workerMatchesLease(w, lease) && w.SharedState().NormalizedLifecycle() == WorkerLifecycleDraining
}

func (p *K8sWorkerPool) workerLeaseDurablyDrainingOrRepair(lease workerLeaseSnapshot) bool {
	if lease.ownerCPInstanceID != p.cpInstanceID {
		return false
	}
	lc := p.ensureLifecycle()
	if lc == nil {
		return true
	}
	record, err := p.runtimeStore.GetWorkerRecord(lease.workerID)
	if err != nil {
		p.logw(lease.workerID).Warn("K8s worker health check failed while worker is locally draining, but durable worker record could not be verified.",
			"owner_epoch", lease.ownerEpoch, "error", err)
		return false
	}
	if record == nil || record.OwnerCPInstanceID != lease.ownerCPInstanceID || record.OwnerEpoch != lease.ownerEpoch {
		return false
	}
	if record.State == configstore.WorkerStateDraining {
		return true
	}

	outcome, err := lc.Drain(
		configstore.NewWorkerLease(lease.workerID, lease.ownerCPInstanceID, lease.ownerEpoch, lease.image),
		LifecycleOriginWorkerDrain)
	if err != nil {
		p.logw(lease.workerID).Warn("K8s worker health check failed while worker is locally draining, but durable drain repair failed.",
			"owner_epoch", lease.ownerEpoch, "error", err)
		return false
	}
	if outcome.Transitioned {
		return true
	}

	record, err = p.runtimeStore.GetWorkerRecord(lease.workerID)
	if err != nil {
		p.logw(lease.workerID).Warn("K8s worker health check failed while worker is locally draining, but durable worker record could not be verified after repair miss.",
			"owner_epoch", lease.ownerEpoch, "error", err)
		return false
	}
	return record != nil &&
		record.OwnerCPInstanceID == lease.ownerCPInstanceID &&
		record.OwnerEpoch == lease.ownerEpoch &&
		record.State == configstore.WorkerStateDraining
}

func (p *K8sWorkerPool) markWorkerDrainingFromHealth(lease workerLeaseSnapshot) {
	if lease.ownerCPInstanceID != p.cpInstanceID {
		return
	}
	p.mu.Lock()
	w, ok := p.workers[lease.workerID]
	if !ok || !p.workerMatchesLease(w, lease) {
		p.mu.Unlock()
		return
	}
	previous := w.SharedState()
	alreadyDraining := false
	switch previous.NormalizedLifecycle() {
	case WorkerLifecycleRetired:
		p.mu.Unlock()
		return
	case WorkerLifecycleDraining:
		alreadyDraining = true
	default:
		next, err := previous.Transition(WorkerLifecycleDraining, previous.Assignment)
		if err != nil {
			p.mu.Unlock()
			p.logw(lease.workerID).Warn("Worker reported draining but local lifecycle transition failed.",
				"state", previous.NormalizedLifecycle(), "error", err)
			return
		}
		w.sharedState = next
	}
	p.mu.Unlock()

	durableVerified := false
	defer func() {
		if durableVerified || alreadyDraining {
			return
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		w, ok := p.workers[lease.workerID]
		if !ok || !p.workerMatchesLease(w, lease) {
			return
		}
		if w.SharedState().NormalizedLifecycle() == WorkerLifecycleDraining {
			w.sharedState = previous
		}
	}()

	lc := p.ensureLifecycle()
	if lc != nil {
		if alreadyDraining {
			record, err := p.runtimeStore.GetWorkerRecord(lease.workerID)
			if err != nil {
				p.logw(lease.workerID).Warn("Worker reported draining but durable worker record could not be verified.",
					"owner_epoch", lease.ownerEpoch, "error", err)
				return
			}
			if record == nil || record.OwnerCPInstanceID != p.cpInstanceID || record.OwnerEpoch != lease.ownerEpoch {
				return
			}
			if record.State == configstore.WorkerStateDraining {
				durableVerified = true
				return
			}
		}
		outcome, err := lc.Drain(
			configstore.NewWorkerLease(lease.workerID, p.cpInstanceID, lease.ownerEpoch, lease.image),
			LifecycleOriginWorkerDrain)
		if err != nil {
			p.logw(lease.workerID).Warn("Worker reported draining but durable drain CAS failed; keeping local worker schedulable until retry.",
				"owner_epoch", lease.ownerEpoch, "error", err)
			return
		}
		if !outcome.Transitioned {
			record, err := p.runtimeStore.GetWorkerRecord(lease.workerID)
			if err != nil {
				p.logw(lease.workerID).Warn("Worker reported draining but durable worker record could not be verified after CAS miss.",
					"owner_epoch", lease.ownerEpoch, "error", err)
				return
			}
			if record == nil || record.OwnerCPInstanceID != p.cpInstanceID || record.OwnerEpoch != lease.ownerEpoch || record.State != configstore.WorkerStateDraining {
				return
			}
		}
	}
	durableVerified = true
}

func (p *K8sWorkerPool) removeWorkerAfterDrainedLease(lease workerLeaseSnapshot, origin LifecycleOrigin) (*ManagedWorker, int, error) {
	p.mu.RLock()
	w, ok := p.workers[lease.workerID]
	if !ok || !p.workerMatchesLease(w, lease) || w.SharedState().NormalizedLifecycle() != WorkerLifecycleDraining {
		p.mu.RUnlock()
		return nil, 0, nil
	}
	p.mu.RUnlock()

	lc := p.ensureLifecycle()
	if lc != nil {
		retired, err := p.retireLocalDrainingLease(lease, RetireReasonNormal, origin)
		if err != nil {
			return nil, 0, err
		}
		if !retired {
			return nil, 0, nil
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	current, ok := p.workers[lease.workerID]
	if !ok || !p.workerMatchesLease(current, lease) || current.SharedState().NormalizedLifecycle() != WorkerLifecycleDraining {
		return nil, 0, nil
	}
	p.markWorkerRetiredInMemoryLocked(current)
	delete(p.workers, current.ID)
	return current, len(p.workers), nil
}

func (p *K8sWorkerPool) retireLocalDrainingLease(lease workerLeaseSnapshot, reason string, origin LifecycleOrigin) (bool, error) {
	lc := p.ensureLifecycle()
	if lc == nil {
		return true, nil
	}
	durableLease := configstore.NewWorkerLease(lease.workerID, lease.ownerCPInstanceID, lease.ownerEpoch, lease.image)
	outcome, err := lc.RetireDrained(durableLease, reason, origin)
	if err != nil {
		return false, err
	}
	if outcome.Transitioned {
		return true, nil
	}
	record, err := p.runtimeStore.GetWorkerRecord(lease.workerID)
	if err != nil {
		return false, err
	}
	if record == nil || record.OwnerCPInstanceID != lease.ownerCPInstanceID || record.OwnerEpoch != lease.ownerEpoch {
		return false, nil
	}
	if record.State == configstore.WorkerStateRetired {
		return true, nil
	}
	if record.State != configstore.WorkerStateDraining {
		drainOutcome, err := lc.Drain(durableLease, origin)
		if err != nil {
			return false, err
		}
		if !drainOutcome.Transitioned {
			record, err = p.runtimeStore.GetWorkerRecord(lease.workerID)
			if err != nil {
				return false, err
			}
			if record == nil || record.OwnerCPInstanceID != lease.ownerCPInstanceID ||
				record.OwnerEpoch != lease.ownerEpoch || record.State != configstore.WorkerStateDraining {
				return false, nil
			}
		}
	}
	outcome, err = lc.RetireDrained(durableLease, reason, origin)
	if err != nil {
		return false, err
	}
	if outcome.Transitioned {
		return true, nil
	}
	record, err = p.runtimeStore.GetWorkerRecord(lease.workerID)
	if err != nil {
		return false, err
	}
	return record != nil &&
		record.OwnerCPInstanceID == lease.ownerCPInstanceID &&
		record.OwnerEpoch == lease.ownerEpoch &&
		record.State == configstore.WorkerStateRetired, nil
}

func (p *K8sWorkerPool) removeWorkerAfterLostLeaseLocked(lease workerLeaseSnapshot) (*ManagedWorker, int) {
	current, ok := p.workers[lease.workerID]
	if !ok || !p.workerMatchesLease(current, lease) {
		return nil, len(p.workers)
	}
	p.markWorkerRetiredInMemoryLocked(current)
	delete(p.workers, current.ID)
	return current, len(p.workers)
}

func (p *K8sWorkerPool) dropLocalWorkerIfSameLeaseLocked(lease workerLeaseSnapshot) (*ManagedWorker, int) {
	current, ok := p.workers[lease.workerID]
	if !ok || !p.workerMatchesLease(current, lease) {
		return nil, len(p.workers)
	}
	delete(p.workers, current.ID)
	return current, len(p.workers)
}

// markWorkerRetiredLocked is the in-memory retire wrapper for paths
// that bypass the WorkerLifecycle CAS service: it upserts the durable
// row first, then (on success) transitions the in-memory shared state
// and emits a duckgres_worker_lifecycle_transitions_total sample under
// operation=retire_local with the caller-supplied origin. Used by
// retireWorkerWithReason (idle reaper, stuck-activating reaper, public
// RetireWorker, activation-failure / liveness-recheck fallbacks,
// per-org ShutdownAll). Callers that have already gone through the
// lifecycle service (deleteRetiredRuntimeWorker, ShutdownAll's drain
// chain, removeWorkerAfterLostLeaseLocked) call
// markWorkerRetiredInMemoryLocked directly to avoid double-counting.
//
// origin is required so the metric label reflects the actual call site
// (e.g. pool-local reaper vs. janitor reaper, per-org vs. CP-wide
// shutdown) — the prior reason→origin mapping was lossy because the
// same RetireReason* constant is reused across distinct call sites.
//
// Ordering: persist BEFORE the in-memory transition so a fence-miss
// (peer CP advanced the lease) doesn't leave this CP with a stale
// in-memory view that pretends we still own the worker. On any
// persistWorkerRecord error (fence-miss or real DB error), the
// in-memory state is left untouched, a retire_local failure outcome is
// emitted, and callers must skip local removal plus pod deletion.
func (p *K8sWorkerPool) markWorkerRetiredLocked(w *ManagedWorker, reason string, origin LifecycleOrigin) bool {
	workerState := configstore.WorkerStateRetired
	if reason == RetireReasonCrash {
		workerState = configstore.WorkerStateLost
	}
	if err := p.persistWorkerRecord(p.workerRecordFor(w.ID, w, w.OwnerEpoch(), workerState, reason, nil)); err != nil {
		outcome := configstore.TransitionOutcomeStoreError
		if stderrors.Is(err, configstore.ErrWorkerRecordUpsertFenceMiss) {
			outcome = configstore.TransitionOutcomeFenceMissLease
		}
		observeLifecycleTransition(
			LifecycleOpRetireLocal,
			outcome,
			w.image,
			origin)
		return false
	}
	// markWorkerRetiredInMemoryLocked returns false only when the
	// SharedState machine refuses Retired (e.g., already terminal).
	// Under p.mu that's unreachable in practice — the worker is in
	// p.workers and no one else can have flipped the state — but we
	// don't gate the return on it for two reasons:
	// (1) the durable retire HAS landed (persist succeeded above), so
	//     "transitioned" is honest at the lifecycle level even if the
	//     local mirror was a no-op;
	// (2) returning false would strand the now-durably-retired worker
	//     in p.workers (caller skips delete()) with the K8s pod still
	//     up — the orphan reconciler would eventually delete the pod
	//     but never the in-memory entry, breaking local capacity
	//     accounting.
	// The discarded bool stays for diagnostic value if the state
	// machine ever surfaces a real refusal.
	_ = p.markWorkerRetiredInMemoryLocked(w)
	observeLifecycleTransition(
		LifecycleOpRetireLocal,
		configstore.TransitionOutcomeTransitioned,
		w.image,
		origin)
	return true
}

// markWorkerRetiredInMemoryLocked performs only the in-memory lifecycle
// transition for a worker retirement, without persisting to the runtime
// store and without emitting any metric. Used by callers that have
// already advanced the DB state via a scoped CAS (e.g. ShutdownAll's
// draining chain, the lifecycle service's post-CAS cleanup hook,
// removeWorkerAfterLostLease) and don't want an unconditional
// UpsertWorkerRecord to overwrite fields set by that CAS. Returns true
// when the transition actually happened.
//
// Metric emission lives in markWorkerRetiredLocked (the wrapper that
// also persists) because the CAS-bypassing path is the only one not
// already observed via the lifecycle service.
func (p *K8sWorkerPool) markWorkerRetiredInMemoryLocked(w *ManagedWorker) bool {
	nextState, err := w.SharedState().Transition(WorkerLifecycleRetired, nil)
	if err != nil {
		return false
	}
	_ = w.SetSharedState(nextState)
	return true
}
