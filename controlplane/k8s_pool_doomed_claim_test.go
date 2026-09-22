//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func doomedClaimSkippedCount(t *testing.T, reason string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "duckgres_control_plane_hot_idle_claim_skipped_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metricHasLabels(metric, map[string]string{"reason": reason}) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func runningPodOnNode(name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "127.0.0.1"},
	}
}

func TestClaimedPodDoomReason(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	ctx := context.Background()

	if _, err := cs.CoreV1().Nodes().Create(ctx, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-ok"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().Nodes().Create(ctx, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-disrupted"},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{{
			Key:    "karpenter.sh/disrupted",
			Effect: corev1.TaintEffectNoSchedule,
		}}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	now := metav1.Now()
	terminating := runningPodOnNode("w-terminating", "node-ok")
	terminating.DeletionTimestamp = &now
	pending := runningPodOnNode("w-pending", "node-ok")
	pending.Status.Phase = corev1.PodPending

	cases := []struct {
		name string
		pod  *corev1.Pod
		want string
	}{
		{"nil pod", nil, ""},
		{"deletion timestamp set", terminating, doomedClaimReasonPodTerminating},
		{"not running", pending, doomedClaimReasonPodNotRunning},
		{"node tainted for disruption", runningPodOnNode("w-tainted", "node-disrupted"), doomedClaimReasonNodeDisrupted},
		{"healthy pod on healthy node", runningPodOnNode("w-ok", "node-ok"), ""},
		{"node lookup fails is not doomed", runningPodOnNode("w-orphan", "node-missing"), ""},
		{"unscheduled pod skips node read", runningPodOnNode("w-nonode", ""), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pool.claimedPodDoomReason(ctx, tc.pod); got != tc.want {
				t.Fatalf("claimedPodDoomReason = %q, want %q", got, tc.want)
			}
		})
	}

	t.Run("nil clientset never reads the node", func(t *testing.T) {
		noAPI := &K8sWorkerPool{}
		if got := noAPI.claimedPodDoomReason(ctx, runningPodOnNode("w", "node-disrupted")); got != "" {
			t.Fatalf("expected no verdict without a clientset, got %q", got)
		}
		if got := noAPI.claimedPodDoomReason(ctx, terminating); got != doomedClaimReasonPodTerminating {
			t.Fatalf("pod-level checks must still work without a clientset, got %q", got)
		}
	})
}

func TestNodeHasDisruptionTaintRecognisesBothKarpenterSpellings(t *testing.T) {
	for _, key := range []string{"karpenter.sh/disrupted", "karpenter.sh/disruption"} {
		node := &corev1.Node{Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: key, Value: "disrupting", Effect: corev1.TaintEffectNoSchedule}}}}
		if !nodeHasDisruptionTaint(node) {
			t.Fatalf("taint %q not recognised", key)
		}
	}
	other := &corev1.Node{Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "node.kubernetes.io/unschedulable", Effect: corev1.TaintEffectNoSchedule}}}}
	if nodeHasDisruptionTaint(other) {
		t.Fatal("unrelated taint must not count as disruption")
	}
	if nodeHasDisruptionTaint(nil) {
		t.Fatal("nil node must not count as disruption")
	}
}

// Regression for the claim-on-draining race: a hot-idle worker whose pod is
// already terminating (Karpenter SIGTERM landed between the durable claim and
// adoption) must not be adopted. The claim is retired as pod_doomed (retired,
// not lost), no local worker is registered for it, the skip is counted, and the
// acquisition falls through to a fresh spawn instead of failing the client.
func TestReserveSharedWorkerSkipsDoomedHotIdleClaim(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const doomedID = 41
	const spawnedID = 42
	doomedPod := "duckgres-worker-test-cp-41"

	if _, err := pool.ensureWorkerRPCSecret(ctx, doomedPod); err != nil {
		t.Fatalf("ensureWorkerRPCSecret: %v", err)
	}
	now := metav1.Now()
	pod := runningPodOnNode(doomedPod, "node-a")
	pod.DeletionTimestamp = &now
	if _, err := cs.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create terminating pod: %v", err)
	}
	if got, err := cs.CoreV1().Pods("default").Get(ctx, doomedPod, metav1.GetOptions{}); err != nil || got.DeletionTimestamp == nil {
		t.Fatalf("fake pod did not keep its deletionTimestamp (err=%v)", err)
	}

	store := &captureRuntimeWorkerStore{
		hotIdleClaimResult: &configstore.WorkerRecord{
			WorkerID:          doomedID,
			PodName:           doomedPod,
			State:             configstore.WorkerStateHotIdle,
			OrgID:             "analytics",
			Image:             pool.workerImage,
			OwnerCPInstanceID: pool.cpInstanceID,
			OwnerEpoch:        2,
		},
		hotIdleClaimOnce: true,
		spawned: &configstore.WorkerRecord{
			WorkerID:          spawnedID,
			PodName:           "duckgres-worker-test-cp-42",
			State:             configstore.WorkerStateSpawning,
			OrgID:             "analytics",
			Image:             pool.workerImage,
			OwnerCPInstanceID: pool.cpInstanceID,
		},
	}
	pool.runtimeStore = store
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error { return nil }
	pool.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
		w := makeTestWorker(WorkerLifecycleIdle, nil)
		w.ID = id
		w.image = image
		pool.mu.Lock()
		pool.workers[id] = w
		pool.mu.Unlock()
		return nil
	}

	before := doomedClaimSkippedCount(t, doomedClaimReasonPodTerminating)
	worker, err := pool.ReserveSharedWorker(ctx, &WorkerAssignment{
		OrgID:      "analytics",
		MaxWorkers: 5,
		Image:      pool.workerImage,
	})
	if err != nil {
		t.Fatalf("ReserveSharedWorker: %v", err)
	}
	if worker == nil || worker.ID != spawnedID {
		t.Fatalf("expected fall-through to fresh spawn %d, got %#v", spawnedID, worker)
	}
	if worker.hotIdleReclaimed {
		t.Fatal("a freshly spawned replacement must not be reported as a hot-idle reclaim")
	}
	if after := doomedClaimSkippedCount(t, doomedClaimReasonPodTerminating); after-before != 1 {
		t.Fatalf("expected one pod_terminating skip counted, got delta %v", after-before)
	}

	retired := false
	for i, id := range store.markTerminalCalledIDs {
		if id != doomedID {
			continue
		}
		retired = true
		if store.markTerminalReasons[i] != RetireReasonPodDoomed {
			t.Fatalf("expected retire reason %q, got %q", RetireReasonPodDoomed, store.markTerminalReasons[i])
		}
		if store.markTerminalStates[i] != configstore.WorkerStateRetired {
			t.Fatalf("expected terminal state retired (not lost), got %q", store.markTerminalStates[i])
		}
	}
	if !retired {
		t.Fatalf("expected doomed claim %d retired; MarkWorkerTerminalIfCurrent ids=%v", doomedID, store.markTerminalCalledIDs)
	}

	pool.mu.Lock()
	_, adopted := pool.workers[doomedID]
	pool.mu.Unlock()
	if adopted {
		t.Fatal("doomed claim must not be registered as a local worker")
	}
}
