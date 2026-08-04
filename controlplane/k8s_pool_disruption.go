//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// doNotDisruptAnnotation is Karpenter's voluntary-disruption veto: a pod
// carrying it blocks consolidation/drift of its node, but NOT kube-scheduler
// preemption (so headroom placeholders are still preempted by real workers).
const doNotDisruptAnnotation = "karpenter.sh/do-not-disrupt"

// setWorkerDoNotDisrupt toggles the Karpenter do-not-disrupt annotation on a
// worker pod. Workers are BUSY-protected only: the annotation is present from
// pod creation (covering the expensive spawn+activate window and the first
// session) and while a session is assigned, and removed when the worker parks
// hot-idle — so consolidation (WhenEmptyOrUnderutilized) can reclaim nodes
// holding only idle workers and placeholders, while a node running an active
// query is never voluntarily disrupted. Pinning is therefore bounded by query
// lifetime, not the up-to-24h worker TTL (which would stall drift rollouts).
//
// Best-effort by design: a failed PATCH must never fail the session path. The
// failure modes are benign — a busy worker briefly without the annotation is
// still backed by the drain protocol (#690: SIGTERM → finish in-flight work),
// and an idle worker briefly with it just delays one consolidation pass.
func (p *K8sWorkerPool) setWorkerDoNotDisrupt(podName string, protected bool) {
	if p.clientset == nil || podName == "" {
		return
	}
	var patch []byte
	if protected {
		patch = []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:"true"}}}`, doNotDisruptAnnotation))
	} else {
		patch = []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, doNotDisruptAnnotation))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := p.clientset.CoreV1().Pods(p.namespace).Patch(ctx, podName, types.StrategicMergePatchType, patch, metav1.PatchOptions{}); err != nil {
		slog.Warn("Failed to toggle worker do-not-disrupt annotation.",
			"worker_pod", podName, "protected", protected, "error", err)
	}
}

// markWorkerProtected flags a worker as actively serving (session assigned):
// its node must not be voluntarily disrupted. Synchronous (one fast PATCH) so
// the protect cannot be reordered after a subsequent release's unprotect.
func (p *K8sWorkerPool) markWorkerProtected(w *ManagedWorker) {
	if w == nil {
		return
	}
	p.setWorkerDoNotDisrupt(p.workerPodName(w), true)
}

// markWorkerDisruptable flags a parked (hot-idle) worker as fair game for
// Karpenter consolidation. Async: release latency doesn't matter, and the
// worst-case race (annotation lingering briefly) only delays consolidation.
func (p *K8sWorkerPool) markWorkerDisruptable(w *ManagedWorker) {
	if w == nil {
		return
	}
	podName := p.workerPodName(w)
	go p.setWorkerDoNotDisrupt(podName, false)
}
