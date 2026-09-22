//go:build kubernetes

package controlplane

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// newDrainingOrphanFixture builds a pool holding worker 8 in the Draining
// lifecycle with a matching durable `draining` row, and stubs the health
// probe to fail on every tick (the pod is unreachable). The informer never
// fires w.done — exactly the situation of a worker adopted from another CP,
// whose pod events this CP's informer does not receive.
func newDrainingOrphanFixture(t *testing.T) (*K8sWorkerPool, *fake.Clientset, *captureRuntimeWorkerStore, *ManagedWorker, *atomic.Int32) {
	t.Helper()
	pool, cs := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{
		preloadedRecords: map[int]*configstore.WorkerRecord{
			8: {
				WorkerID:          8,
				PodName:           "test-cp-worker-8",
				State:             configstore.WorkerStateDraining,
				OwnerCPInstanceID: pool.cpInstanceID,
				OwnerEpoch:        4,
			},
		},
	}
	pool.runtimeStore = store
	pool.lifecycle = NewWorkerLifecycle(store, pool)

	worker := &ManagedWorker{ID: 8, podName: "test-cp-worker-8", done: make(chan struct{})}
	worker.SetOwnerCPInstanceID(pool.cpInstanceID)
	worker.SetOwnerEpoch(4)
	if err := worker.SetSharedState(SharedWorkerState{Lifecycle: WorkerLifecycleDraining}); err != nil {
		t.Fatalf("set worker state: %v", err)
	}
	pool.workers[worker.ID] = worker

	origHealthCheck := doHealthCheckWithMetadata
	t.Cleanup(func() { doHealthCheckWithMetadata = origHealthCheck })
	var checks atomic.Int32
	doHealthCheckWithMetadata = func(ctx context.Context, _ *flightsql.Client, _ server.WorkerHealthCheckPayload) (*healthCheckResult, error) {
		checks.Add(1)
		return nil, context.DeadlineExceeded
	}
	return pool, cs, store, worker, &checks
}

// Regression for the prod leak: a locally-draining worker whose pod is
// already gone, and whose done channel nobody will ever close, used to be
// probed every tick forever ("waiting for pod exit"). After the verify
// threshold the loop must ask the API server, see NotFound, and take the
// same drained-lease path the informer would have — retiring the durable
// row and dropping the local worker, without a crash notification.
func TestK8sPoolHealthCheckLoopRetiresDrainingWorkerWhosePodIsGoneWithoutInformer(t *testing.T) {
	pool, _, store, worker, checks := newDrainingOrphanFixture(t)

	crashed := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.HealthCheckLoop(ctx, 2*time.Millisecond, func(workerID int) {
		crashed <- workerID
	}, nil)

	deadline := time.After(2 * time.Second)
	for {
		pool.mu.RLock()
		_, stillPresent := pool.workers[worker.ID]
		pool.mu.RUnlock()
		if !stillPresent {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("draining worker with NotFound pod must be removed by the probe fallback; still present after %d checks", checks.Load())
		case <-time.After(2 * time.Millisecond):
		}
	}
	if n := checks.Load(); n < drainingPodVerifyThreshold {
		t.Fatalf("pod must only be verified after %d consecutive misses, worker removed after %d checks", drainingPodVerifyThreshold, n)
	}

	select {
	case workerID := <-crashed:
		t.Fatalf("drained pod exit must not notify sessions as crashed, got worker %d", workerID)
	default:
	}
	store.mu.Lock()
	markLostCalls := store.markLostCalls
	retireDrainingCalls := store.retireDrainingCalls
	recordState := store.preloadedRecords[worker.ID].State
	store.mu.Unlock()
	if markLostCalls != 0 {
		t.Fatalf("drained pod exit must not mark worker lost, got %d lost calls", markLostCalls)
	}
	if retireDrainingCalls != 1 {
		t.Fatalf("expected one retire-draining CAS, got %d", retireDrainingCalls)
	}
	if recordState != configstore.WorkerStateRetired {
		t.Fatalf("expected durable worker state retired, got %q", recordState)
	}
}

// The verification must be a real pod check, not a timer: while the pod
// object still exists (a slow drain of live work) the worker keeps waiting
// for its exit, however many probes fail.
func TestK8sPoolHealthCheckLoopKeepsDrainingWorkerWhilePodStillExists(t *testing.T) {
	pool, cs, store, worker, checks := newDrainingOrphanFixture(t)
	if _, err := cs.CoreV1().Pods(pool.namespace).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cp-worker-8", Namespace: pool.namespace},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	crashed := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.HealthCheckLoop(ctx, 2*time.Millisecond, func(workerID int) {
		crashed <- workerID
	}, nil)

	// Wait for well past the verify threshold so the pod check has run
	// (repeatedly) and had every chance to wrongly remove the worker.
	deadline := time.After(2 * time.Second)
	for checks.Load() < 4*drainingPodVerifyThreshold {
		select {
		case <-deadline:
			t.Fatalf("expected at least %d health checks, got %d", 4*drainingPodVerifyThreshold, checks.Load())
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()

	pool.mu.RLock()
	_, stillPresent := pool.workers[worker.ID]
	pool.mu.RUnlock()
	if !stillPresent {
		t.Fatal("draining worker whose pod still exists must not be dropped by the probe fallback")
	}
	select {
	case workerID := <-crashed:
		t.Fatalf("draining worker must not be reported crashed while its pod exists, got worker %d", workerID)
	default:
	}
	store.mu.Lock()
	retireDrainingCalls := store.retireDrainingCalls
	markLostCalls := store.markLostCalls
	recordState := store.preloadedRecords[worker.ID].State
	store.mu.Unlock()
	if retireDrainingCalls != 0 || markLostCalls != 0 {
		t.Fatalf("no durable transition expected while pod exists, got retireDraining=%d markLost=%d", retireDrainingCalls, markLostCalls)
	}
	if recordState != configstore.WorkerStateDraining {
		t.Fatalf("expected durable worker state to remain draining, got %q", recordState)
	}
}

// A pool without a clientset (process backend / minimal pools) cannot verify
// pods and must keep today's wait-for-informer behavior rather than guess.
func TestK8sPoolHealthCheckLoopDrainingVerifyIsNoopWithoutClientset(t *testing.T) {
	pool, _, store, worker, checks := newDrainingOrphanFixture(t)
	pool.clientset = nil

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.HealthCheckLoop(ctx, 2*time.Millisecond, nil, nil)

	deadline := time.After(2 * time.Second)
	for checks.Load() < 4*drainingPodVerifyThreshold {
		select {
		case <-deadline:
			t.Fatalf("expected at least %d health checks, got %d", 4*drainingPodVerifyThreshold, checks.Load())
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()

	pool.mu.RLock()
	_, stillPresent := pool.workers[worker.ID]
	pool.mu.RUnlock()
	if !stillPresent {
		t.Fatal("without a clientset the draining worker must be left for the informer")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.retireDrainingCalls != 0 {
		t.Fatalf("no retire expected without pod verification, got %d", store.retireDrainingCalls)
	}
}

// A locally-draining worker whose durable row is draining under ANOTHER
// owner (it was claimed by a sibling CP after this CP's local object went
// stale) must not be waited on forever either: the durable check fails, the
// failure falls through to the ordinary mark-lost path, and after
// maxConsecutiveHealthFailures the lease is recognised as stale and the local
// object dropped — no retire-draining CAS, no crash notification, and the
// other owner's row untouched.
func TestK8sPoolHealthCheckLoopDropsLocallyDrainingWorkerWhoseRowBelongsToAnotherCP(t *testing.T) {
	pool, _, store, worker, checks := newDrainingOrphanFixture(t)
	store.mu.Lock()
	store.preloadedRecords[worker.ID].OwnerCPInstanceID = "some-other-cp:boot-xyz"
	store.preloadedRecords[worker.ID].OwnerEpoch = 7
	store.mu.Unlock()

	crashed := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.HealthCheckLoop(ctx, 2*time.Millisecond, func(workerID int) {
		crashed <- workerID
	}, nil)

	deadline := time.After(2 * time.Second)
	for {
		pool.mu.RLock()
		_, stillPresent := pool.workers[worker.ID]
		pool.mu.RUnlock()
		if !stillPresent {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("stale draining worker must be dropped after %d failures, still present after %d checks", maxConsecutiveHealthFailures, checks.Load())
		case <-time.After(2 * time.Millisecond):
		}
	}
	select {
	case workerID := <-crashed:
		t.Fatalf("stale lease drop must not notify sessions as crashed, got worker %d", workerID)
	default:
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.retireDrainingCalls != 0 {
		t.Fatalf("another CP's draining row must not be retired by a stale holder, got %d retire-draining CAS", store.retireDrainingCalls)
	}
	if rec := store.preloadedRecords[worker.ID]; rec.State != configstore.WorkerStateDraining || rec.OwnerCPInstanceID != "some-other-cp:boot-xyz" || rec.OwnerEpoch != 7 {
		t.Fatalf("other owner's row must be untouched, got %+v", *rec)
	}
}

// The pool-side helper the janitor sweep is wired to: NotFound and terminal
// phases are "gone"; a running pod is not; a missing clientset is an error
// (never a silent "gone").
func TestK8sPoolWorkerPodGone(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	ctx := context.Background()
	for _, pod := range []*corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "w-running", Namespace: pool.namespace}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
		{ObjectMeta: metav1.ObjectMeta{Name: "w-succeeded", Namespace: pool.namespace}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
		{ObjectMeta: metav1.ObjectMeta{Name: "w-failed", Namespace: pool.namespace}, Status: corev1.PodStatus{Phase: corev1.PodFailed}},
	} {
		if _, err := cs.CoreV1().Pods(pool.namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	cases := map[string]bool{"w-running": false, "w-succeeded": true, "w-failed": true, "w-missing": true}
	for name, want := range cases {
		got, err := pool.workerPodGone(ctx, name)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", name, err)
		}
		if got != want {
			t.Fatalf("%s: gone=%v, want %v", name, got, want)
		}
	}
	pool.clientset = nil
	if _, err := pool.workerPodGone(ctx, "w-missing"); err == nil {
		t.Fatal("missing clientset must be an error, not a verdict")
	}
}
