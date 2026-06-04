//go:build kubernetes

package controlplane

import (
	"context"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	k8stesting "k8s.io/client-go/testing"
)

// TestReconcileDisruptionGuardsSetsAndClears verifies the reconciler stamps
// karpenter.sh/do-not-disrupt onto a busy worker's pod and removes it once the
// worker goes idle, and that it only patches on busy<->idle transitions.
func TestReconcileDisruptionGuardsSetsAndClears(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	ctx := context.Background()

	busyPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "wpod-busy", Namespace: "default",
		Labels: map[string]string{"duckgres/worker-id": "1"},
	}}
	idlePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "wpod-idle", Namespace: "default",
		Labels:      map[string]string{"duckgres/worker-id": "2"},
		Annotations: map[string]string{karpenterDoNotDisruptAnnotation: karpenterDoNotDisruptValue},
	}}
	for _, pod := range []*corev1.Pod{busyPod, idlePod} {
		if _, err := cs.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", pod.Name, err)
		}
	}

	var patches int32
	cs.PrependReactor("patch", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		atomic.AddInt32(&patches, 1)
		return false, nil, nil // fall through to the default reactor (applies the patch)
	})

	// w1 busy and not yet guarded; w2 idle but still carrying the annotation.
	pool.workers[1] = &ManagedWorker{ID: 1, podName: "wpod-busy", activeSessions: 1, doNotDisruptApplied: false, done: make(chan struct{})}
	pool.workers[2] = &ManagedWorker{ID: 2, podName: "wpod-idle", activeSessions: 0, doNotDisruptApplied: true, done: make(chan struct{})}

	pool.reconcileDisruptionGuards(ctx)

	got, err := cs.CoreV1().Pods("default").Get(ctx, "wpod-busy", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Annotations[karpenterDoNotDisruptAnnotation] != karpenterDoNotDisruptValue {
		t.Fatalf("busy worker: want do-not-disrupt=true, got annotations=%v", got.Annotations)
	}
	if !pool.workers[1].doNotDisruptApplied {
		t.Fatal("busy worker: expected doNotDisruptApplied=true after reconcile")
	}

	got, err = cs.CoreV1().Pods("default").Get(ctx, "wpod-idle", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Annotations[karpenterDoNotDisruptAnnotation]; ok {
		t.Fatalf("idle worker: want do-not-disrupt cleared, got annotations=%v", got.Annotations)
	}
	if pool.workers[2].doNotDisruptApplied {
		t.Fatal("idle worker: expected doNotDisruptApplied=false after reconcile")
	}

	if n := atomic.LoadInt32(&patches); n != 2 {
		t.Fatalf("expected exactly 2 patches (1 set, 1 clear), got %d", n)
	}

	// Steady state: nothing changed, so no further patches.
	pool.reconcileDisruptionGuards(ctx)
	if n := atomic.LoadInt32(&patches); n != 2 {
		t.Fatalf("expected no additional patches on steady state, got %d", n)
	}
}

// TestReconcileDisruptionGuardsSkipsExitingWorker verifies a worker whose done
// channel is closed (exiting) is not patched — nothing to guard.
func TestReconcileDisruptionGuardsSkipsExitingWorker(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	ctx := context.Background()

	if _, err := cs.CoreV1().Pods("default").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "wpod-exiting", Namespace: "default",
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	var patches int32
	cs.PrependReactor("patch", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		atomic.AddInt32(&patches, 1)
		return false, nil, nil
	})

	done := make(chan struct{})
	close(done)
	pool.workers[1] = &ManagedWorker{ID: 1, podName: "wpod-exiting", activeSessions: 1, done: done}

	pool.reconcileDisruptionGuards(ctx)
	if n := atomic.LoadInt32(&patches); n != 0 {
		t.Fatalf("expected no patches for an exiting worker, got %d", n)
	}
}

// TestWorkerPodTerminating verifies the cache-only Terminating check that the
// health-check loop uses to distinguish a planned node drain from a crash.
func TestWorkerPodTerminating(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	pool.informer = informers.NewSharedInformerFactory(cs, 0).Core().V1().Pods().Informer()

	now := metav1.Now()
	terminating := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "wpod-term", Namespace: "default", DeletionTimestamp: &now}}
	alive := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "wpod-alive", Namespace: "default"}}
	if err := pool.informer.GetIndexer().Add(terminating); err != nil {
		t.Fatal(err)
	}
	if err := pool.informer.GetIndexer().Add(alive); err != nil {
		t.Fatal(err)
	}

	if !pool.workerPodTerminating("wpod-term") {
		t.Fatal("expected Terminating pod (deletionTimestamp set) → true")
	}
	if pool.workerPodTerminating("wpod-alive") {
		t.Fatal("expected live pod → false")
	}
	if pool.workerPodTerminating("wpod-missing") {
		t.Fatal("expected pod absent from cache → false")
	}
	if pool.workerPodTerminating("") {
		t.Fatal("expected empty pod name → false")
	}
}
