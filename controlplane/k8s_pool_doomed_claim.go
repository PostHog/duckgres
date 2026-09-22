//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// karpenterDisruptedTaintKeys are the taints Karpenter stamps on a node it has
// decided to disrupt (consolidation, drift, expiration). `karpenter.sh/disrupted`
// is the v1 API taint (Karpenter >= 1.0); `karpenter.sh/disruption` is the
// v1beta1 spelling, kept so a cluster mid-upgrade is still recognised.
var karpenterDisruptedTaintKeys = map[string]struct{}{
	"karpenter.sh/disrupted":  {},
	"karpenter.sh/disruption": {},
}

// Reasons a claimed hot-idle worker is refused at adoption. Closed set: the
// metric label below is built from it.
const (
	doomedClaimReasonPodTerminating = "pod_terminating"
	doomedClaimReasonPodNotRunning  = "pod_not_running"
	doomedClaimReasonNodeDisrupted  = "node_disrupted"
)

// errDoomedHotIdleClaim marks a hot-idle claim whose pod is already on its way
// out: the pod carries a deletionTimestamp, is not Running, or its node has
// been tainted for disruption by Karpenter. The claim is retired and the
// acquisition retries (next hot-idle candidate or a fresh spawn) instead of
// creating a session on a pod that is about to receive SIGTERM.
var errDoomedHotIdleClaim = errors.New("claimed hot-idle worker pod is being disrupted")

// doomedClaimNodeLookupTimeout bounds the node read; the claim path is on the
// client's connect critical path, so an unresponsive API server must not turn
// into a slow connect. A failed lookup is treated as "unknown", not "doomed".
const doomedClaimNodeLookupTimeout = 2 * time.Second

var hotIdleClaimSkippedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_control_plane_hot_idle_claim_skipped_total",
	Help: "Hot-idle worker claims refused at adoption because the pod was already being disrupted (terminating, not running, or its node tainted by Karpenter). The claim is retired and the acquisition falls through to the next candidate or a fresh spawn. Partitioned by reason and image.",
}, []string{"reason", "image"})

func observeHotIdleClaimSkipped(reason, image string) {
	r := strings.TrimSpace(reason)
	if r == "" {
		return
	}
	img := strings.TrimSpace(image)
	if img == "" {
		img = "unknown"
	}
	hotIdleClaimSkippedCounter.WithLabelValues(r, img).Inc()
}

// claimedPodDoomReason reports why a claimed hot-idle worker's pod must not be
// adopted, or "" when it looks usable. Karpenter honours the do-not-disrupt
// annotation only while a session is assigned (#757), so between the durable
// claim CAS and adoption an idle worker can already have been SIGTERM'd: its
// pod then carries a deletionTimestamp (the worker drains and exits), or its
// node is tainted `karpenter.sh/disrupted` ahead of eviction. Adopting such a
// pod creates a session that fails on its first RPC. The checks are ordered
// cheapest-first; the node read is skipped entirely when the pod already
// answers the question. A node lookup failure is deliberately NOT doomed —
// refusing every claim during an API-server blip would turn a transient into
// a fleet-wide spawn storm.
func (p *K8sWorkerPool) claimedPodDoomReason(ctx context.Context, pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	if pod.DeletionTimestamp != nil {
		return doomedClaimReasonPodTerminating
	}
	if pod.Status.Phase != corev1.PodRunning {
		return doomedClaimReasonPodNotRunning
	}
	if p.clientset == nil || pod.Spec.NodeName == "" {
		return ""
	}
	nodeCtx, cancel := context.WithTimeout(ctx, doomedClaimNodeLookupTimeout)
	defer cancel()
	node, err := p.clientset.CoreV1().Nodes().Get(nodeCtx, pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		slog.Debug("Could not read claimed worker's node for disruption taints; proceeding with adoption.",
			"worker_pod", pod.Name, "node", pod.Spec.NodeName, "error", err)
		return ""
	}
	if nodeHasDisruptionTaint(node) {
		return doomedClaimReasonNodeDisrupted
	}
	return ""
}

func nodeHasDisruptionTaint(node *corev1.Node) bool {
	if node == nil {
		return false
	}
	for _, taint := range node.Spec.Taints {
		if _, ok := karpenterDisruptedTaintKeys[taint.Key]; ok {
			return true
		}
	}
	return false
}
