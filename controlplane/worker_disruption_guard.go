//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Drain-aware worker eviction (Tier 1a + Tier 2a of the 2026-06-04 RCA).
//
// A Karpenter Drift roll tainted worker nodes out from under running queries:
// each in-flight query died a median 5s after its node was tainted, while the
// node still had ~108s of life left, and the control plane *itself* canceled
// the query (3 failed health checks → "worker unresponsive"). 31 queries were
// killed this way. The control plane drains correctly on its own roll (900s
// grace, unbounded drain); workers had none of that. These two mechanisms give
// a busy worker the same protection:
//
//	1a. While a worker is running a query it carries karpenter.sh/do-not-disrupt,
//	    so Karpenter's *voluntary* disruption (drift, consolidation, expiry)
//	    skips its node. The annotation is cleared when the worker goes idle.
//	2a. If a worker fails health checks while its pod is already Terminating
//	    (a planned node drain), the health loop does NOT mark it Lost / cancel
//	    its query — it defers to the informer-driven pod-terminated path, letting
//	    the worker drain (bounded by terminationGracePeriodSeconds).
//
// This protects against *voluntary* disruption only. Involuntary loss (spot
// reclaim, node/hardware failure) still kills the query; that residual tail is
// handled by transparent statement retry for commit-safe statements (separate
// change), not here.

const (
	// karpenterDoNotDisruptAnnotation, when set on a pod, tells Karpenter not to
	// voluntarily disrupt the node hosting it (drift/consolidation/expiry).
	// Involuntary disruption (spot reclaim, NodePool terminationGracePeriod
	// ceiling) is unaffected — which is why we still need the Tier 2a guard and
	// a bounded grace period.
	karpenterDoNotDisruptAnnotation = "karpenter.sh/do-not-disrupt"
	karpenterDoNotDisruptValue      = "true"

	// disruptionGuardReconcileInterval is how often the busy/idle → annotation
	// reconcile runs. A worker busy for longer than this is protected before the
	// next Karpenter disruption decision; the only exposure is a query that both
	// starts and is selected for voluntary disruption within one interval, which
	// would be a sub-interval query anyway. Patches only fire on transitions, so
	// steady state costs zero API calls.
	disruptionGuardReconcileInterval = 5 * time.Second

	// workerTerminationGracePeriodSeconds is the worker pod's grace period.
	// Unset historically (→ k8s default 30s), far too short for analytical
	// queries. This is the fallback drain window for the residual race (worker
	// became busy inside the reconcile gap) and for involuntary eviction; the
	// primary protection for long queries is the do-not-disrupt annotation
	// above. The NodePool-level terminationGracePeriod (infra) remains the hard
	// ceiling so a security roll always completes.
	workerTerminationGracePeriodSeconds int64 = 600
)

// disruptionGuardReconciler keeps karpenter.sh/do-not-disrupt in sync with each
// worker's busy/idle state. Started once from newK8sWorkerPool alongside
// idleReaper. Covers both the flat pool and OrgReservedPool, which share this
// pool's worker map.
func (p *K8sWorkerPool) disruptionGuardReconciler() {
	ticker := time.NewTicker(disruptionGuardReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.shutdownCh:
			return
		case <-ticker.C:
			p.reconcileDisruptionGuards(context.Background())
		}
	}
}

// reconcileDisruptionGuards patches the do-not-disrupt annotation onto workers
// that became busy and removes it from workers that went idle.
//
// Desired state (busy) is compared against the pod's ACTUAL annotation, read
// from the pod informer cache — not an in-memory flag. This keeps the reconcile
// stateless and self-correcting across control-plane restarts and failover:
// when a CP dies, the worker's session(s) die with it (so the worker goes idle)
// but its pod keeps the annotation the dead CP stamped. A surviving/replacement
// CP that adopts the worker has no in-memory memory of having set it; reading
// the live annotation lets it see desired=idle vs current=set and clear the
// orphan. (An in-memory "applied" flag would miss this — it would read
// applied=false==busy=false and never clear the stale annotation.)
//
// It snapshots desired state under the lock, reads current state from the
// cache, and issues K8s API calls without holding the lock.
func (p *K8sWorkerPool) reconcileDisruptionGuards(ctx context.Context) {
	type guardTarget struct {
		worker  *ManagedWorker
		podName string
		busy    bool
	}

	var targets []guardTarget
	p.mu.RLock()
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue // worker exiting; nothing to guard
		default:
		}
		targets = append(targets, guardTarget{worker: w, podName: p.workerPodName(w), busy: w.activeSessions > 0})
	}
	p.mu.RUnlock()

	for _, t := range targets {
		if t.podName == "" {
			continue
		}
		if t.busy == p.podHasDoNotDisrupt(t.podName) {
			continue // pod already in the desired state (steady state: no API call)
		}
		if err := p.patchWorkerDoNotDisrupt(ctx, t.podName, t.busy); err != nil {
			if apierrors.IsNotFound(err) {
				continue // pod gone; nothing to guard
			}
			slog.Warn("Failed to reconcile worker do-not-disrupt annotation.",
				"worker", t.worker.ID, "pod", t.podName, "busy", t.busy, "error", err)
			continue
		}
		slog.Debug("Reconciled worker do-not-disrupt annotation.",
			"worker", t.worker.ID, "pod", t.podName, "do_not_disrupt", t.busy)
	}
}

// podHasDoNotDisrupt reports whether the worker pod currently carries the
// karpenter.sh/do-not-disrupt annotation, read from the pod informer cache
// (no API call). Returns false if the pod is not in the cache yet.
func (p *K8sWorkerPool) podHasDoNotDisrupt(podName string) bool {
	if p.informer == nil || podName == "" {
		return false
	}
	obj, exists, err := p.informer.GetIndexer().GetByKey(p.namespace + "/" + podName)
	if err != nil || !exists {
		return false
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return false
	}
	return pod.Annotations[karpenterDoNotDisruptAnnotation] == karpenterDoNotDisruptValue
}

// patchWorkerDoNotDisrupt sets (enable) or removes (disable) the
// karpenter.sh/do-not-disrupt annotation on a worker pod via a JSON merge patch.
// A null value removes the key.
func (p *K8sWorkerPool) patchWorkerDoNotDisrupt(ctx context.Context, podName string, enable bool) error {
	var value interface{}
	if enable {
		value = karpenterDoNotDisruptValue
	} else {
		value = nil // JSON merge patch: null deletes the annotation key
	}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				karpenterDoNotDisruptAnnotation: value,
			},
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = p.clientset.CoreV1().Pods(p.namespace).Patch(ctx, podName, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

// workerPodTerminating reports whether the worker's pod is already being torn
// down (has a deletionTimestamp), read from the pod informer cache — no API
// call, no extra RBAC. The control plane evicts workers via Karpenter node
// drains and kubelet, both of which set deletionTimestamp before the worker
// stops answering health checks; a genuine crash leaves it nil. Used by the
// health-check loop to distinguish a planned drain (let the query finish) from
// a crash (mark Lost).
func (p *K8sWorkerPool) workerPodTerminating(podName string) bool {
	if p.informer == nil || podName == "" {
		return false
	}
	obj, exists, err := p.informer.GetIndexer().GetByKey(p.namespace + "/" + podName)
	if err != nil || !exists {
		return false
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return false
	}
	return pod.DeletionTimestamp != nil
}
