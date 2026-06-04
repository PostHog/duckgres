//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// guardPodAnnotated builds a worker pod with/without the do-not-disrupt annotation.
func guardPodAnnotated(name string, annotated bool) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	if annotated {
		pod.Annotations = map[string]string{karpenterDoNotDisruptAnnotation: karpenterDoNotDisruptValue}
	}
	return pod
}

// withGuardInformer attaches a (non-running) pod informer to the pool and seeds
// the cache, so podHasDoNotDisrupt reads the pods we stage. Returns a function
// to (re)sync the cache from a set of pods, simulating the informer observing a
// patch.
func withGuardInformer(t *testing.T, pool *K8sWorkerPool, cs *fake.Clientset, pods ...*corev1.Pod) func(...*corev1.Pod) {
	t.Helper()
	pool.informer = informers.NewSharedInformerFactory(cs, 0).Core().V1().Pods().Informer()
	idx := pool.informer.GetIndexer()
	sync := func(ps ...*corev1.Pod) {
		for _, p := range ps {
			if err := idx.Add(p); err != nil {
				t.Fatalf("seed informer: %v", err)
			}
		}
	}
	sync(pods...)
	return sync
}

func countPatchReactor(cs *fake.Clientset, n *int32) {
	cs.PrependReactor("patch", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		atomic.AddInt32(n, 1)
		return false, nil, nil // fall through to default reactor (applies the patch)
	})
}

// TestReconcileDisruptionGuardsSetsAndClears: a busy worker's pod gets the
// annotation; an idle worker's pod has it removed; steady state issues no patch.
func TestReconcileDisruptionGuardsSetsAndClears(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	ctx := context.Background()

	busyPod := guardPodAnnotated("wpod-busy", false)
	idlePod := guardPodAnnotated("wpod-idle", true)
	for _, pod := range []*corev1.Pod{busyPod, idlePod} {
		if _, err := cs.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", pod.Name, err)
		}
	}
	resync := withGuardInformer(t, pool, cs, busyPod, idlePod)

	var patches int32
	countPatchReactor(cs, &patches)

	pool.workers[1] = &ManagedWorker{ID: 1, podName: "wpod-busy", activeSessions: 1, done: make(chan struct{})}
	pool.workers[2] = &ManagedWorker{ID: 2, podName: "wpod-idle", activeSessions: 0, done: make(chan struct{})}

	pool.reconcileDisruptionGuards(ctx)

	if got := podAnno(t, cs, "wpod-busy"); got != karpenterDoNotDisruptValue {
		t.Fatalf("busy worker: want do-not-disrupt=true, got %q", got)
	}
	if got := podAnno(t, cs, "wpod-idle"); got != "" {
		t.Fatalf("idle worker: want do-not-disrupt cleared, got %q", got)
	}
	if n := atomic.LoadInt32(&patches); n != 2 {
		t.Fatalf("expected exactly 2 patches (1 set, 1 clear), got %d", n)
	}

	// Steady state: cache now reflects the patched annotations -> no more patches.
	resync(guardPodAnnotated("wpod-busy", true), guardPodAnnotated("wpod-idle", false))
	pool.reconcileDisruptionGuards(ctx)
	if n := atomic.LoadInt32(&patches); n != 2 {
		t.Fatalf("expected no additional patches in steady state, got %d", n)
	}
}

// TestRelabelAdoptedPodToThisCP verifies the adoption relabel makes a pod
// spawned by another CP carry THIS CP's control-plane label (so this CP's
// label-scoped informer can see it) while preserving the pod's other labels.
func TestRelabelAdoptedPodToThisCP(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5) // pool.cpID == "test-cp"
	ctx := context.Background()

	if _, err := cs.CoreV1().Pods("default").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "wpod-adopted",
		Namespace: "default",
		Labels:    map[string]string{"duckgres/control-plane": "old-cp", "duckgres/worker-id": "9"},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := pool.relabelAdoptedPodToThisCP(ctx, "wpod-adopted"); err != nil {
		t.Fatal(err)
	}

	got, err := cs.CoreV1().Pods("default").Get(ctx, "wpod-adopted", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Labels["duckgres/control-plane"] != "test-cp" {
		t.Fatalf("want control-plane=test-cp after relabel, got %q", got.Labels["duckgres/control-plane"])
	}
	if got.Labels["duckgres/worker-id"] != "9" {
		t.Fatalf("merge patch clobbered other labels: %v", got.Labels)
	}
}

// TestReconcileDisruptionGuardsClearsStaleAnnotationAfterFailover is the
// CP-failover regression: a CP stamps do-not-disrupt on a busy worker, then
// dies; its sessions die with it so the worker is idle, but the pod keeps the
// annotation. A surviving/replacement CP (no in-memory record of having set it)
// must still clear the orphan. In production the pod becomes visible to the new
// CP's label-scoped informer via relabelAdoptedPodToThisCP on adoption (tested
// above); this test seeds the cache to that post-relabel state and asserts the
// cache-based reconcile clears the orphan. An in-memory "applied" flag would not
// (it would read applied=false==busy=false and skip).
func TestReconcileDisruptionGuardsClearsStaleAnnotationAfterFailover(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	ctx := context.Background()

	stale := guardPodAnnotated("wpod-orphan", true) // annotation left by the dead CP
	if _, err := cs.CoreV1().Pods("default").Create(ctx, stale, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	withGuardInformer(t, pool, cs, stale)

	// Adopted worker, freshly in memory: idle (sessions died with the old CP).
	pool.workers[7] = &ManagedWorker{ID: 7, podName: "wpod-orphan", activeSessions: 0, done: make(chan struct{})}

	pool.reconcileDisruptionGuards(ctx)

	if got := podAnno(t, cs, "wpod-orphan"); got != "" {
		t.Fatalf("orphaned do-not-disrupt not cleared after failover: got %q", got)
	}
}

// TestReconcileDisruptionGuardsSkipsExitingWorker: a worker whose done channel
// is closed (exiting) is not patched.
func TestReconcileDisruptionGuardsSkipsExitingWorker(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	ctx := context.Background()

	if _, err := cs.CoreV1().Pods("default").Create(ctx, guardPodAnnotated("wpod-exiting", false), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	withGuardInformer(t, pool, cs, guardPodAnnotated("wpod-exiting", false))

	var patches int32
	countPatchReactor(cs, &patches)

	done := make(chan struct{})
	close(done)
	pool.workers[1] = &ManagedWorker{ID: 1, podName: "wpod-exiting", activeSessions: 1, done: done}

	pool.reconcileDisruptionGuards(ctx)
	if n := atomic.LoadInt32(&patches); n != 0 {
		t.Fatalf("expected no patches for an exiting worker, got %d", n)
	}
}

// TestWorkerPodTerminating verifies the cache-only Terminating check the health
// loop uses to distinguish a planned node drain from a crash.
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
		t.Fatal("expected Terminating pod (deletionTimestamp set) -> true")
	}
	if pool.workerPodTerminating("wpod-alive") {
		t.Fatal("expected live pod -> false")
	}
	if pool.workerPodTerminating("wpod-missing") {
		t.Fatal("expected pod absent from cache -> false")
	}
	if pool.workerPodTerminating("") {
		t.Fatal("expected empty pod name -> false")
	}
}

// TestReconcileDisruptionGuardsNoDataRace runs the reconciler in a tight loop
// while many goroutines flip activeSessions under the pool lock (as Acquire/
// ReleaseWorker do). Run with -race, this asserts the reconciler's snapshot-
// under-RLock / patch-without-lock discipline has no data race against concurrent
// session churn.
func TestReconcileDisruptionGuardsNoDataRace(t *testing.T) {
	pool, cs := newTestK8sPool(t, 100)
	ctx := context.Background()

	const n = 16
	var pods []*corev1.Pod
	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("wpod-%d", i)
		pod := guardPodAnnotated(name, i%2 == 0)
		if _, err := cs.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		pods = append(pods, pod)
		pool.workers[i] = &ManagedWorker{ID: i, podName: name, done: make(chan struct{})}
	}
	withGuardInformer(t, pool, cs, pods...)

	stop := make(chan struct{})

	// Mutators: flip activeSessions under the pool lock, mirroring Acquire/Release.
	var muWG sync.WaitGroup
	for i := 1; i <= n; i++ {
		muWG.Add(1)
		go func(id int) {
			defer muWG.Done()
			for j := 0; j < 2000; j++ {
				pool.mu.Lock()
				if w := pool.workers[id]; w != nil {
					w.activeSessions ^= 1
				}
				pool.mu.Unlock()
			}
		}(i)
	}

	// Reconciler + health-path read, looping concurrently with the mutators.
	var recWG sync.WaitGroup
	recWG.Add(1)
	go func() {
		defer recWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				pool.reconcileDisruptionGuards(ctx)
				pool.workerPodTerminating("wpod-1")
			}
		}
	}()

	muWG.Wait()
	close(stop)
	recWG.Wait()
}

func podAnno(t *testing.T, cs *fake.Clientset, name string) string {
	t.Helper()
	pod, err := cs.CoreV1().Pods("default").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod %s: %v", name, err)
	}
	return pod.Annotations[karpenterDoNotDisruptAnnotation]
}
