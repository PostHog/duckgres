//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// RetireOneMismatchedVersionWorker scans all shared worker pods in the
// namespace for one whose duckgres/control-plane label identifies a different
// Deployment ReplicaSet (pod-template-hash) than this CP, atomically marks
// its idle runtime-store row retired, and deletes the pod.
//
// The janitor leader is expected to invoke this once per tick: old-version idle
// workers are retired one at a time, and the next session for that org spawns a
// pod from the leader's current binary. This gives gradual rolling worker
// replacement driven entirely by leader election — no cross-CP sweeps needed.
//
// Retirement is limited to workers currently in WorkerStateIdle. Busy or
// hot-idle workers are left alone so in-flight sessions aren't disturbed;
// they'll be retired on a subsequent tick once they become idle (or when
// their hot-idle TTL expires).
//
// Returns true when a worker was retired this call.
func (p *K8sWorkerPool) RetireOneMismatchedVersionWorker(ctx context.Context) bool {
	if p.runtimeStore == nil || p.clientset == nil {
		return false
	}
	myVersion := trimK8sPodHashSuffix(p.cpID)
	if myVersion == p.cpID {
		// cpID doesn't carry a Deployment pod-template-hash suffix (e.g.
		// bare pod or StatefulSet). Without that we can't compare versions
		// across pods, so nothing to do.
		return false
	}

	pods, err := p.clientset.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		slog.Warn("Version-aware reaper failed to list worker pods.", "error", err)
		return false
	}

	for _, pod := range pods.Items {
		label := pod.Labels["duckgres/control-plane"]
		if label == "" {
			continue
		}
		idStr := pod.Labels["duckgres/worker-id"]
		if idStr == "" {
			continue
		}
		workerID, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}

		var snap *configstore.WorkerSnapshot
		if p.runtimeStore != nil {
			var err error
			snap, err = p.runtimeStore.ObserveWorker(workerID)
			if err != nil {
				slog.Warn("Version-aware reaper failed to observe worker.", "worker", workerID, "worker_pod", pod.Name, "org", pod.Labels["duckgres/active-org"], "error", err)
			}
		}

		// Resolve the target version for this specific pod.
		// For unassigned workers, the target is the global binary version.
		// For assigned workers, the target is the tenant's configured image.
		var isMismatched bool
		podOrgID := pod.Labels["duckgres/org"]
		if podOrgID == "" && snap != nil {
			podOrgID = snap.OrgID()
		}

		if podOrgID != "" && p.resolveOrgConfig != nil {
			// Per-tenant version check
			org, err := p.resolveOrgConfig(podOrgID)
			if err != nil {
				// Best-effort: skip if we can't resolve config this tick
				continue
			}
			targetImage := p.workerImage
			if org.Warehouse != nil && org.Warehouse.Image != "" {
				targetImage = org.Warehouse.Image
			}

			actualImage := workerImageForPod(&pod)
			if actualImage == "" && snap != nil {
				actualImage = snap.Image()
			}
			if actualImage != "" && actualImage != targetImage {
				isMismatched = true
				slog.Debug("Detected per-tenant worker image mismatch.",
					"org", podOrgID, "worker_pod", pod.Name, "actual", actualImage, "target", targetImage)
			}
		} else {
			// Global CP binary version check
			if trimK8sPodHashSuffix(label) != myVersion {
				isMismatched = true
			}
		}

		if !isMismatched {
			continue
		}

		lifecycle := p.ensureLifecycle()
		if snap == nil || lifecycle == nil {
			continue
		}
		outcome, err := lifecycle.RetireIdleVariantFromSnapshot(*snap, RetireReasonMismatchedVersion, LifecycleOriginMismatchedVersionReaper)
		if err != nil {
			slog.Warn("Version-aware reaper failed to retire idle row.", "worker", workerID, "worker_pod", pod.Name, "org", pod.Labels["duckgres/active-org"], "error", err)
			continue
		}
		if !outcome.Transitioned {
			// Not currently idle or hot-idle (busy, reserved, or already
			// retired). Leave it for a later tick. The lifecycle service
			// also skips physical pod delete on a CAS miss.
			continue
		}
		slog.Info("Retiring mismatched-version worker pod.",
			"worker_id", workerID,
			"org", podOrgID,
			"worker_pod", pod.Name,
		)
		// The lifecycle service already scheduled an async pod + secret
		// delete via DeleteWorkerArtifacts. We additionally issue a
		// synchronous Delete here so the reaper's one-per-call contract
		// is observable to callers (and to tests) without waiting for
		// the async cleanup goroutine. The second async Delete is
		// idempotent — it returns NotFound on the already-gone pod and
		// just logs at Warn, which is harmless.
		gracePeriod := int64(10)
		if err := p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
		}); err != nil && !errors.IsNotFound(err) {
			slog.Warn("Version-aware reaper failed to delete pod.", "worker_pod", pod.Name, "error", err)
		}
		return true
	}
	return false
}

func workerImageForPod(pod *corev1.Pod) string {
	for _, container := range pod.Spec.Containers {
		if container.Name == "duckdb-worker" {
			return container.Image
		}
	}
	slog.Debug("workerImageForPod: duckdb-worker container not found in pod spec.", "worker_pod", pod.Name)
	return ""
}

// cleanupOrphanedWorkerPods deletes worker pods whose DB row is in a terminal
// state (retired/lost) or has no DB row at all, reconciling K8s against the
// state store. Runs from the janitor loop (leader-only).
//
// This closes a gap between ShutdownAll and the janitor's orphan sweep.
// ShutdownAll marks the worker row retired in the DB before issuing the K8s
// pod delete, and the delete is fire-and-forget — so if the delete fails (API
// hiccup) or the CP is SIGKILL'd mid-shutdown, the pod survives while the DB
// row is already terminal. ListOrphanedWorkers explicitly excludes
// terminal-state rows, so without this reconciler those pods live forever.
// Bare worker pods have no owner reference, so nothing else in the cluster
// reaps them either.
//
// minAge protects newly-spawned pods: the spawn path creates the pod BEFORE
// upserting the DB row, so there's a brief window where a live pod has no DB
// record. Without the age gate the reconciler would race the spawner.
//
// Returns the number of pods deleted this call. Per-pod delete failures
// are recorded against
// duckgres_worker_stranded_pods_reconciled_total{outcome="delete_failed"}
// and do not bubble up here — they don't disqualify the sweep, just one
// candidate.
func (p *K8sWorkerPool) cleanupOrphanedWorkerPods(ctx context.Context, minAge time.Duration) int {
	if p.runtimeStore == nil || p.clientset == nil {
		return 0
	}
	pods, err := p.clientset.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		slog.Warn("Stranded-pod reconciler failed to list worker pods.", "error", err)
		return 0
	}
	cutoff := time.Now().Add(-minAge)
	deleted := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.CreationTimestamp.Time.After(cutoff) {
			continue
		}
		idStr := pod.Labels["duckgres/worker-id"]
		if idStr == "" {
			continue
		}
		workerID, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}
		rec, err := p.runtimeStore.GetWorkerRecord(workerID)
		if err != nil {
			slog.Warn("Stranded-pod reconciler failed to load worker record.", "worker", workerID, "worker_pod", pod.Name, "org", pod.Labels["duckgres/active-org"], "error", err)
			continue
		}
		dbState := "missing"
		outcome := StrandedOutcomeDeletedMissingRow
		if rec != nil {
			if rec.State != configstore.WorkerStateRetired && rec.State != configstore.WorkerStateLost {
				// Pod is still claimed by an active runtime row — not
				// stranded. Record it under "kept" so dashboards can
				// see reconciler saturation (large kept counts =
				// reaper iterating many healthy pods per tick).
				observeStrandedPodReconciled(StrandedOutcomeKept)
				continue
			}
			dbState = string(rec.State)
			outcome = StrandedOutcomeDeletedTerminalRow
		}
		gracePeriod := int64(10)
		if err := p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
		}); err != nil && !errors.IsNotFound(err) {
			slog.Warn("Stranded-pod reconciler failed to delete pod.", "worker", workerID, "worker_pod", pod.Name, "org", pod.Labels["duckgres/active-org"], "error", err)
			observeStrandedPodReconciled(StrandedOutcomeDeleteFailed)
			continue
		}
		_ = p.deleteWorkerRPCSecret(ctx, pod.Name)
		slog.Info("Stranded worker pod reconciled.", "worker", workerID, "worker_pod", pod.Name, "org", pod.Labels["duckgres/active-org"], "db_state", dbState)
		observeStrandedPodReconciled(outcome)
		deleted++
	}
	return deleted
}

// cleanupOrphanedWorkerSecrets deletes worker RPC secrets whose pod
// no longer exists. Closes the recovery gap where a CP crashes
// between secret creation and pod creation — the secret would
// otherwise leak indefinitely because cleanupOrphanedWorkerPods
// only iterates pods. Runs from the janitor leader on the same
// tick as cleanupOrphanedWorkerPods.
//
// minAge protects newly-created secrets the spawner is still using
// (the spawn flow creates the secret first, then the pod).
//
// Returns the number of secrets deleted this call.
func (p *K8sWorkerPool) cleanupOrphanedWorkerSecrets(ctx context.Context, minAge time.Duration) int {
	if p.clientset == nil {
		return 0
	}
	// Worker RPC secrets are labeled at creation (worker_rpc_security.go
	// ensureWorkerRPCSecret) with app=duckgres +
	// duckgres/control-plane=<p.cpID> + duckgres/worker-pod=<podName>.
	// The selector narrows by control-plane so each CP only reaps
	// its own secrets — critical during rolling restarts and blue/green
	// deployments where multiple CP replicas share the namespace and
	// a peer's freshly-created secret (whose pod hasn't been spawned
	// yet) must not be reaped. p.cpID is used unsanitized to match
	// the value the creation path stamps.
	secrets, err := p.clientset.CoreV1().Secrets(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=duckgres,duckgres/control-plane=%s,duckgres/worker-pod", p.cpID),
	})
	if err != nil {
		slog.Warn("Stranded-secret reconciler failed to list worker RPC secrets.", "error", err)
		return 0
	}
	cutoff := time.Now().Add(-minAge)
	deleted := 0
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		if secret.CreationTimestamp.Time.After(cutoff) {
			continue
		}
		podName := secret.Labels["duckgres/worker-pod"]
		if podName == "" {
			continue
		}
		if _, err := p.clientset.CoreV1().Pods(p.namespace).Get(ctx, podName, metav1.GetOptions{}); err == nil {
			// Pod exists — leave the secret alone, the regular
			// cleanupOrphanedWorkerPods path will reap both
			// together when the pod's DB row is terminal. Recorded
			// as "kept" for symmetry with the pods sweep, so
			// dashboards comparing the two reapers' kept counts
			// see parallel behavior.
			observeStrandedSecretReconciled(StrandedOutcomeKept)
			continue
		} else if !errors.IsNotFound(err) {
			slog.Warn("Stranded-secret reconciler failed to load worker pod.", "worker_pod", podName, "error", err)
			continue
		}
		if err := p.clientset.CoreV1().Secrets(p.namespace).Delete(ctx, secret.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			slog.Warn("Stranded-secret reconciler failed to delete secret.", "secret", secret.Name, "worker_pod", podName, "error", err)
			observeStrandedSecretReconciled(StrandedOutcomeDeleteFailed)
			continue
		}
		slog.Info("Stranded worker RPC secret reconciled.", "secret", secret.Name, "worker_pod", podName)
		observeStrandedSecretReconciled(StrandedOutcomeDeleted)
		deleted++
	}
	return deleted
}

// startInformer starts a SharedIndexInformer to watch worker pods.
func (p *K8sWorkerPool) startInformer() {
	labelSelector := fmt.Sprintf("duckgres/control-plane=%s", p.cpID)
	if p.orgID != "" {
		labelSelector += fmt.Sprintf(",duckgres/org=%s", p.orgID)
	}
	factory := informers.NewSharedInformerFactoryWithOptions(
		p.clientset,
		30*time.Second,
		informers.WithNamespace(p.namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = labelSelector
		}),
	)
	p.informer = factory.Core().V1().Pods().Informer()

	p.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			newPod, ok := newObj.(*corev1.Pod)
			if !ok {
				return
			}
			// Signal pod readiness to waiters (replaces polling waitForPodIP)
			if newPod.Status.PodIP != "" && newPod.Status.Phase == corev1.PodRunning {
				if ch, ok := p.podReady.LoadAndDelete(newPod.Name); ok {
					select {
					case ch.(chan podReadyInfo) <- podReadyInfo{ip: newPod.Status.PodIP, nodeName: newPod.Spec.NodeName}:
					default:
					}
				}
			}
			// Detect pod phase transition to Failed/Succeeded (crash/OOM)
			if newPod.Status.Phase == corev1.PodFailed || newPod.Status.Phase == corev1.PodSucceeded {
				// Unblock any waiter with an error signal
				if ch, ok := p.podReady.LoadAndDelete(newPod.Name); ok {
					close(ch.(chan podReadyInfo))
				}
				p.onPodTerminated(newPod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				pod, ok = tombstone.Obj.(*corev1.Pod)
				if !ok {
					return
				}
			}
			if ch, loaded := p.podReady.LoadAndDelete(pod.Name); loaded {
				close(ch.(chan podReadyInfo))
			}
			p.onPodTerminated(pod)
		},
	})

	go p.informer.Run(p.stopInform)
}

// onPodTerminated handles a worker pod being terminated (crash, eviction, etc.).
// It closes the done channel on the corresponding ManagedWorker so the health
// check loop and session manager detect the loss.
func (p *K8sWorkerPool) onPodTerminated(pod *corev1.Pod) {
	idStr := pod.Labels["duckgres/worker-id"]
	if idStr == "" {
		return
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return
	}
	p.mu.RLock()
	w, ok := p.workers[id]
	p.mu.RUnlock()
	if !ok {
		return
	}
	// Close the done channel to signal crash (idempotent via select)
	select {
	case <-w.done:
		// Already closed
	default:
		attrs := []any{
			"worker", id,
			"worker_pod", pod.Name,
			"org", pod.Labels["duckgres/active-org"],
			"phase", pod.Status.Phase,
		}
		attrs = append(attrs, workerPodStatusLogAttrs(pod)...)
		slog.Warn("Worker pod terminated.", attrs...)
		close(w.done)
	}
}
